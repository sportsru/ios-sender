package main

import (
	"encoding/json"
	"errors"
	"expvar"
	"flag"
	"fmt"
	// stdlog "log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/davecgh/go-spew/spew"
	nsq "github.com/nsqio/go-nsq"
	"gopkg.in/inconshreveable/log15.v2"
)

var hubStat = expvar.NewMap("APNSconections")

// VERSION application version
// (should be redefined at build phase)
var VERSION = "2.0-UnknownBuild"

// MessageProducer produce message consumer with nsq.Handler
// 1. get nsq.Handler interface on input
// 2. initialize consumer and returns it
type MessageProducer interface {
	RunWithHandler(h nsq.Handler) *nsq.Consumer
}

// NsqConsumerLocked boxing nsq.Consumer with mutex and current log level value
type NsqConsumerLocked struct {
	consumer *nsq.Consumer
	loglevel string
	sync.Mutex
}

// Hub main App struct
// implements nsq.Handler interface
// stores all global structures
type Hub struct {
	// all connections to APNS (per application bundle)
	Consumers map[string]*GatewayClient
	// interface for consume messages from NSQ and produce it further
	Producer MessageProducer
	// nsq consumer object
	NsqConsumerL NsqConsumerLocked //*nsq.Consumer
	// global hub statistics
	MessagesStat *HubMessagesStat
	// config from toml file
	Config *TomlConfig

	// TTL in seconds
	ProducerTTL int64
	ConsumerTTL int64

	L      log15.Logger
	logctx logcontext

	sync.RWMutex
}

type logcontext struct {
	hostname string
	handler  log15.Handler
	logLvl   log15.Lvl
}

// HubMessagesStat global messages stat
type HubMessagesStat struct {
	total      int32
	totalUniq  int32
	resend     int32
	resendUniq int32
	drop       int32
	skip       int32
	send       int32
	sendUniq   int32
}

// NSQpush json struct of awaited data from NSQ topic
type NSQpush struct {
	Extra       extraField   `json:"extra"`
	PayloadAPNS payloadField `json:"payload_apns"`
	AppInfo     appInfoField `json:"nsq_to_nsq"`
	Message     string       `json:"notification_message"`
	DbRes       dbResField   `json:"db_res"`
}

type appInfoField struct {
	Topic         string
	AppBundleName string `json:"app_bundle_id"`
	Token         string
	Sandbox       bool
}

type dbResField struct {
	Timestamp int64
}

type payloadField struct {
	Sound string
}

type extraField struct {
	EventID int32 `json:"event_id"`
	SportID int32 `json:"sport_id"`
	Type    string
}

type payloadExtraField struct {
	EventID  int32  `json:"event_id"`
	SportID  int32  `json:"sport_id"`
	FlagType string `json:"flagType"`
	Version  int32  `json:"v"`
}

// NSQSource NSQ Topic connection config
type NSQSource struct {
	Topic       string
	Channel     string
	LookupAddrs []string
	NsqdAddrs   []string
	MaxInFlight int
	Concurrency int
	LogLevel    nsq.LogLevel
}

var (
	configPath  = flag.String("config", "config.toml", "config file")
	silent      = flag.Bool("silent", false, "no verbose output")
	debug       = flag.Bool("debug", false, "debug mode (very verbose output)")
	notSend     = flag.Bool("null", false, "don't send messages (/dev/null mode)")
	jsonLog     = flag.Bool("json-log", false, "use JSON for logging")
	httpAddr    = flag.String("http-stat", ":9090", "stat's http addr")
	showVersion = flag.Bool("version", false, "show version")

	onlyTestAPNS = flag.Bool("test-only", false, "test APNS connections and exit")
)

var GlobalLog = log15.New()

//var GlobalStderrLog = log15.New()

// var globalLog log15.Logger

func main() {
	var err error
	hostname, err := os.Hostname()
	if err != nil {
		LogAndDieShort(GlobalLog, err)
	}

	flag.Parse()
	if *showVersion {
		fmt.Println(VERSION)
		os.Exit(0)
	}
	logLvl := log15.LvlInfo
	if *debug {
		logLvl = log15.LvlDebug
	} else if *silent {
		logLvl = log15.LvlError
	}

	// logging setup
	GlobalLog = log15.New("host", hostname)
	basehandler := log15.StdoutHandler
	if *jsonLog {
		basehandler = log15.StreamHandler(os.Stdout, log15.JsonFormat())
	}
	loghandler := log15.LvlFilterHandler(logLvl, basehandler)
	GlobalLog.SetHandler(loghandler)

	// configure
	var config TomlConfig
	if _, err = toml.DecodeFile(*configPath, &config); err != nil {
		LogAndDieShort(GlobalLog, err)
	}
	if config.APNS.PayloadMaxSize > PayloadMaxSize {
		config.APNS.PayloadMaxSize = PayloadMaxSize
	}
	if *debug {
		fmt.Fprintln(os.Stderr, "Config => ", spew.Sdump(&config))
	}

	// create & configure hub
	hub := &Hub{
		logctx: logcontext{
			hostname: hostname,
			handler:  loghandler,
			logLvl:   logLvl,
		},
		L: GlobalLog,
	}
	hub.InitWithConfig(config)

	// run hub
	end := make(chan struct{})
	go func() {
		hub.Run()
		end <- struct{}{}
	}()

	// run webserver (expvar & control)
	server := &WebServer{nc: &hub.NsqConsumerL}
	err = server.Run(*httpAddr)
	if err != nil {
		LogAndDieShort(GlobalLog, err)
	}
	<-end
	GlobalLog.Info("Bye!")
}

// InitHubWithConfig create *Hub struct based on config and default values
func (h *Hub) InitWithConfig(config TomlConfig) {
	var (
		err       error
		errorsCnt int
	)

	connections := make(map[string]*GatewayClient)
	for nick, appCfg := range config.APNSapp {
		_ = nick
		gateway := Gateway
		if appCfg.Sandbox {
			gateway = GatewaySandbox
		}
		//testAPNS(name, addr, open, private string) (client *GatewayClient, err error) {
		client := NewGatewayClient(appCfg.Name, gateway, appCfg.KeyOpen, appCfg.KeyPrivate)
		clientLog := log15.New("host", h.logctx.hostname, "app", appCfg.Name)
		//clientLog.SetHandler(log15.LvlFilterHandler(h.logctx.logLvl, h.logctx.handler))
		clientLog.SetHandler(h.logctx.handler)

		client.L = clientLog

		connections[appCfg.Name] = client
		err = testAPNS(client)
		hubStat.Set(appCfg.Name, client.Stat)
		if err != nil {
			h.L.Error("APNS client test failed"+err.Error(), "app", appCfg.Name)
			errorsCnt++
			continue
		}
		clientLog.Info("connection OK")
	}
	if *onlyTestAPNS {
		os.Exit(errorsCnt)
	}

	// TODO: move source ini to separate func
	concurrency := config.NSQ.Concurrency
	if concurrency <= 0 {
		concurrency = 100 // FIXME: move to const
	}
	h.L.Debug("set Nsq consumer concurrency", "n", concurrency)

	source := &NSQSource{
		Topic:       config.NSQ.Topic,
		Channel:     config.NSQ.Channel,
		LookupAddrs: config.NSQ.LookupAddrs,
		NsqdAddrs:   config.NSQ.NsqdAddrs,
		MaxInFlight: config.NSQ.MaxInFlight,
		Concurrency: config.NSQ.Concurrency,
	}

	if len(config.NSQ.LogLevel) < 1 {
		config.NSQ.LogLevel = "info" // FIXME: move to const
	}
	source.LogLevel, err = GetNSQLogLevel(config.NSQ.LogLevel)
	if err != nil {
		LogAndDieShort(h.L, err)
	}
	h.L.Debug("Init Nsq producer", "config", spew.Sdump(&source))

	h.Consumers = connections
	h.Producer = source
	h.MessagesStat = &HubMessagesStat{}

	h.Config = &config
	h.ProducerTTL = parseTTLtoSeconds(config.NSQ.TTL)
	h.ConsumerTTL = parseTTLtoSeconds(config.APNS.TTL)
}

func parseTTLtoSeconds(s string) int64 {
	dur, err := time.ParseDuration(s)
	if err != nil {
		panic(err)
	}
	return int64(dur.Seconds())
}

// Run main messages routing loop
// 1. call RunWithHandler on Producer
// 2. handle system interraptions and NSQ stop channel
func (h *Hub) Run() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	nsqConsumer := h.Producer.RunWithHandler(h)

	h.NsqConsumerL.Lock()
	h.NsqConsumerL.consumer = nsqConsumer
	h.NsqConsumerL.loglevel = h.Config.NSQ.LogLevel
	h.NsqConsumerL.Unlock()

	for {
		select {
		case <-nsqConsumer.StopChan:
			// MAYBE: disconnect all (connections cleanup)
			// spew.Dump(&h.MessagesStat)
			h.L.Info("hub stopped", "stat", hubStat.String())
			return
		case sig := <-sigChan:
			// TODO: ADD exit by timeout (if any problem with NSQ)
			h.L.Debug("Got stop signal", "num", sig.String())
			_ = sig
			nsqConsumer.Stop()
			h.L.Debug("nsqConsumer.Stopped")
		}
	}
}

func testAPNS(client *GatewayClient) (err error) {
	err = client.Connect()
	if err != nil {
		return
	}

	client.L.Debug("wait for connection test")
	time.Sleep(50 * time.Millisecond)
	if !client.Connected {
		err = errors.New("disconnected unexpectedly")
		return
	}

	return client.Close()
}

// HandleMessage process NSQ messages
// more info: https://godoc.org/github.com/nsqio/go-nsq#HandlerFunc.HandleMessage
func (h *Hub) HandleMessage(m *nsq.Message) error {
	var err error

	stat := h.MessagesStat
	counter := atomic.AddInt32(&stat.total, 1)

	// TODO: if *debug {
	h.L.Debug("handle NSQ message", "counter", counter)
	defer h.L.Debug("finished NSQ message", "counter", counter)
	// }
	m.DisableAutoResponse()

	// http://blog.golang.org/json-and-go
	j := &NSQpush{}
	err = json.Unmarshal(m.Body, &j)
	if err != nil {
		h.L.Error("failed to parse JSON: "+err.Error(), "body", string(m.Body))
		atomic.AddInt32(&stat.drop, 1)
		m.Finish()
		return nil
	}

	if len(j.AppInfo.Token) < 64 {
		// fmt.Sprintf("token %s is less than 64 characters", j.AppInfo.Token)
		h.L.Error("token is less than 64 characters", "app_info", j.AppInfo)
		atomic.AddInt32(&stat.drop, 1)
		m.Finish()
		return nil
	}

	name := j.AppInfo.AppBundleName
	if j.AppInfo.Sandbox {
		name = name + "-dev"
	}

	// Check Application name
	conn, ok := h.Consumers[name]
	if !ok {
		m.Finish()
		atomic.AddInt32(&stat.skip, 1)
		h.L.Debug("appname not found, skip message", "appname", name)
		// MAYBE: add stat for unknown apps?
		return nil
	}
	cStat := conn.Stat

	// Prepare logging context
	msgCtx := []interface{}{
		"id", counter,
		"app", name,
		"token", j.AppInfo.Token,
	}
	h.L.Debug("start process message", msgCtx...)

	// Check TTL
	nowUnix := time.Now().UTC().Unix()
	jTs := j.DbRes.Timestamp
	delta := nowUnix - (jTs + h.ProducerTTL)
	if delta > 0 {
		m.Finish()

		atomic.AddInt32(&cStat.Drop, 1)
		atomic.AddInt32(&stat.drop, 1)

		msgTTLCtx := append(msgCtx, []interface{}{
			"TTL", h.ProducerTTL,
			"timestamp", jTs,
			"delta", delta,
			"body", string(m.Body),
		}...)
		h.L.Info("message TTL is over", msgTTLCtx...)
		return nil
	}

	// create Notify object
	notify := NewNotify()
	notify.PayloadLimit = h.Config.APNS.PayloadMaxSize

	message := &notify.Message

	// TODO: notify.SetAlert(j.Message)
	message.Alert = j.Message
	// TODO: notify.SetSound(j.PayloadApns.Sound)
	if len(j.PayloadAPNS.Sound) > 0 {
		message.Sound = j.PayloadAPNS.Sound
	}

	extra := &payloadExtraField{
		EventID:  j.Extra.EventID,
		FlagType: j.Extra.Type,
		SportID:  j.Extra.SportID,
		Version:  0,
	}
	// TODO: notify.SetExtra(extra)
	message.Custom["extra"] = extra
	notify.SetExpiry(jTs + h.ConsumerTTL)

	pStr, _ := notify.ToString()
	msgCtx = append(msgCtx, []interface{}{
		"seconds_left", delta,
		"payload", pStr,
	}...)

	msg := "message sent OK"
	if !*notSend {
		err = conn.SendTo(notify, j.AppInfo.Token)
		if err != nil {
			m.Finish()

			atomic.AddInt32(&cStat.Drop, 1)
			atomic.AddInt32(&stat.drop, 1)
			h.L.Error("connection send failed: "+err.Error(), msgCtx...)
			return nil
		}
		atomic.AddInt32(&cStat.Send, 1)
		atomic.AddInt32(&stat.send, 1)
	} else {
		atomic.AddInt32(&cStat.Skip, 1)
		atomic.AddInt32(&stat.skip, 1)
		msg = "message sent to /dev/null OK"
	}
	h.L.Info(msg, msgCtx...)

	m.Finish()
	return nil
}

// RunWithHandler configure and start NSQ handler
func (s *NSQSource) RunWithHandler(h nsq.Handler) *nsq.Consumer {
	var (
		err      error
		consumer *nsq.Consumer
	)
	hub, _ := h.(*Hub)
	// https://godoc.org/github.com/nsqio/go-nsq#Config
	cfg := nsq.NewConfig()

	cfg.UserAgent = fmt.Sprintf("mpush-apns-agent/%s", VERSION)
	cfg.DefaultRequeueDelay = time.Second * 5
	if s.MaxInFlight > 0 {
		cfg.MaxInFlight = s.MaxInFlight
	}

	consumer, err = nsq.NewConsumer(s.Topic, s.Channel, cfg)
	if err != nil {
		LogAndDie(hub.L, "NSQ consumer creation error", err, []interface{}{})
	}

	consumer.SetLogger(logger, s.LogLevel)

	// set NSQ handler
	consumer.AddConcurrentHandlers(h, s.Concurrency)
	hub.L.Debug("Add *hub to NSQ handlers", "config", spew.Sdump(cfg))

	var addrs []string
	if s.LookupAddrs != nil {
		addrs = s.LookupAddrs
		err = consumer.ConnectToNSQLookupds(addrs)
	} else if s.NsqdAddrs != nil {
		addrs = s.NsqdAddrs
		err = consumer.ConnectToNSQDs(addrs)
	} else {
		err = errors.New("You should set at least one nsqd or nsqlookupd address")
	}

	logCtx := []interface{}{"addrs", addrs}
	if err != nil {
		LogAndDie(hub.L, "NSQ connection error", err, logCtx)
	}
	hub.L.Debug("NSQ connected to", logCtx...)

	return consumer
}

func LogAndDieShort(l log15.Logger, err error) {
	l.Error(err.Error())
	panic(err)
}

func LogAndDie(l log15.Logger, msg string, err error, args []interface{}) {
	l.Error(msg+err.Error(), args...)
	panic(err)
}
