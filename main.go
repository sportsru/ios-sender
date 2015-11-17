package main

import (
	"encoding/json"
	"errors"
	"expvar"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/davecgh/go-spew/spew"
	nsq "github.com/nsqio/go-nsq"
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

	sync.RWMutex
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
	verbose     = flag.Bool("verbose", false, "verbose output")
	debug       = flag.Bool("debug", false, "debug mode (very verbose output)")
	notSend     = flag.Bool("null", false, "don't send messages (/dev/null mode)")
	httpAddr    = flag.String("http-stat", ":9090", "stat's http addr")
	showVersion = flag.Bool("version", false, "show version")
)

func main() {
	var err error

	flag.Parse()
	if *showVersion {
		fmt.Println(VERSION)
		os.Exit(0)
	}
	if *debug {
		*verbose = true
	}

	var config TomlConfig
	if _, err = toml.DecodeFile(*configPath, &config); err != nil {
		log.Fatal(err)
	}
	if config.APNS.PayloadMaxSize > PayloadMaxSize {
		config.APNS.PayloadMaxSize = PayloadMaxSize
	}

	if *verbose {
		log.Println("Config: ", spew.Sdump(&config))
	}

	hub := InitHubWithConfig(config)
	end := make(chan struct{})
	go func() {
		hub.Run()
		end <- struct{}{}
	}()

	server := &WebServer{nc: &hub.NsqConsumerL}
	err = server.Run(*httpAddr)
	if err != nil {
		log.Fatal("ERROR:", err)
	}
	<-end
}

// InitHubWithConfig create *Hub struct based on config and default values
func InitHubWithConfig(config TomlConfig) *Hub {
	var (
		err    error
		client *GatewayClient
	)

	connections := make(map[string]*GatewayClient)
	for nick, appCfg := range config.APNSapp {
		fmt.Println(nick)
		gateway := Gateway
		if appCfg.Sandbox {
			gateway = GatewaySandbox
		}
		client, err = testAPNS(appCfg.Name, gateway, appCfg.KeyOpen, appCfg.KeyPrivate)
		connections[appCfg.Name] = client
		if err != nil {
			log.Println("ERROR:", appCfg.Name, "connection failed:", err)
			continue
		}
		hubStat.Set(appCfg.Name, client.Stat)
	}

	// TODO: move source ini to separate func
	concurrency := config.NSQ.Concurrency
	if concurrency <= 0 {
		concurrency = 100 // FIXME: move to const
	}
	log.Println("Set Nsq consumer concurrency to", concurrency)

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
		log.Fatal("log_level parse failed", err)
	}

	if *verbose {
		log.Println("Nsq producer config")
		spew.Dump(&source)
	}

	hub := Hub{
		Consumers:    connections,
		Producer:     source,
		MessagesStat: &HubMessagesStat{},

		Config:      &config,
		ProducerTTL: parseTTLtoSeconds(config.NSQ.TTL),
		ConsumerTTL: parseTTLtoSeconds(config.APNS.TTL),
	}
	return &hub
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
			// TODO: disconnect all (connections cleanup)
			log.Println("MessagesStat:")
			spew.Dump(&h.MessagesStat)

			log.Println("hubStat:")
			log.Println(hubStat.String())
			//os.Exit(1)
			return
		case sig := <-sigChan:
			// TODO: ADD exit by timeout (if any problem with NSQ)
			log.Println("Got signal", sig.String(), "stop")
			nsqConsumer.Stop()
			log.Println("nsqConsumer.Stopped")
		}
	}
}

func testAPNS(name, addr, open, private string) (client *GatewayClient, err error) {
	client = NewGatewayClient(name, addr, open, private)
	err = client.Connect()
	if err != nil {
		//panic(name + " connection error:" + err.Error())
		return
	}

	log.Println(name, "wait for connection test")
	time.Sleep(50 * time.Millisecond)
	if !client.Connected {
		err = errors.New("disconnected unexpectedly")
		return
	}

	fmt.Printf("%v: connection to %v with keys %v, %v is OK\n",
		name, addr, open, private)

	// TODO: send in "Close()" method smth to stop Error handling
	err = client.Close()
	return
}

// HandleMessage process NSQ messages
// more info: https://godoc.org/github.com/nsqio/go-nsq#HandlerFunc.HandleMessage
func (h *Hub) HandleMessage(m *nsq.Message) error {
	var err error

	stat := h.MessagesStat
	counter := atomic.AddInt32(&stat.total, 1)

	if *debug {
		log.Println("Start NSQ HandleMessage with counter=", counter)
		defer log.Println("end NSQ HandleMessage with counter=", counter)
	}

	m.DisableAutoResponse()

	// http://blog.golang.org/json-and-go
	j := &NSQpush{}
	err = json.Unmarshal(m.Body, &j)

	if err != nil {
		log.Printf("ERROR: failed to parse JSON - %s: %s\n", err.Error(), string(m.Body))
		atomic.AddInt32(&stat.drop, 1)
		m.Finish()
		return nil
	}
	if len(j.AppInfo.Token) < 64 {
		log.Printf("ERROR: token %s is less than 64 characters\n", j.AppInfo.Token)
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
		// TODO: add stat by app for unknown apps by name
		// skip
		if *debug {
			// silent here, because other worker instance with different config
			// could serve this app
			log.Printf("Appname %s not found, skip message", name)
		}
		return nil
	}

	cStat := conn.Stat
	// hubStat.Get(name).(*expvar.Map)

	msgCommonInfo := fmt.Sprintf("id=%v, app=%v; token=%v", counter, name, j.AppInfo.Token)
	if *verbose {
		log.Printf("Message %v, attempt=%v; ", msgCommonInfo, m.Attempts)
	}

	// Check TTL
	nowUnix := time.Now().UTC().Unix()
	jTs := j.DbRes.Timestamp
	delta := nowUnix - (jTs + h.ProducerTTL)
	if delta > 0 {
		m.Finish()

		atomic.AddInt32(&cStat.Drop, 1)
		atomic.AddInt32(&stat.drop, 1)
		log.Printf("Message %v; TTL(%v) is over, timestamp=%v, delta=%vs, body=%v",
			msgCommonInfo, h.ProducerTTL, jTs, delta, string(m.Body))
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
	log.Printf("Message %v, seconds left=%v, with payload: %v", msgCommonInfo, delta, pStr)

	if !*notSend {
		err = conn.SendTo(notify, j.AppInfo.Token)
		if err != nil {
			m.Finish()

			atomic.AddInt32(&cStat.Drop, 1)
			atomic.AddInt32(&stat.drop, 1)
			log.Printf("ERROR: message %v, .SendTo() is fucked up: %s\n", msgCommonInfo, err)
			return nil
		}
		atomic.AddInt32(&cStat.Send, 1)
		atomic.AddInt32(&stat.send, 1)
	} else {
		atomic.AddInt32(&cStat.Skip, 1)
		atomic.AddInt32(&stat.skip, 1)
		log.Printf("Mesaage %v to /dev/null", msgCommonInfo)
	}

	m.Finish()
	return nil
}

// RunWithHandler configure and start NSQ handler
func (s *NSQSource) RunWithHandler(h nsq.Handler) *nsq.Consumer {
	var err error
	var consumer *nsq.Consumer
	// https://godoc.org/github.com/nsqio/go-nsq#Config
	cfg := nsq.NewConfig()

	//log.Println("Default nsq config:", spew.Sdump(cfg))

	cfg.UserAgent = fmt.Sprintf("mpush-apns-agent/%s", VERSION)
	cfg.DefaultRequeueDelay = time.Second * 5
	if s.MaxInFlight > 0 {
		cfg.MaxInFlight = s.MaxInFlight
	}

	consumer, err = nsq.NewConsumer(s.Topic, s.Channel, cfg)
	if err != nil {
		log.Fatalf(err.Error())
	}

	consumer.SetLogger(logger, s.LogLevel)

	// our messages consumer1 *hub added to NSQ handlers here
	consumer.AddConcurrentHandlers(h, s.Concurrency)
	log.Println("Add *hub to NSQ handlers with config:", spew.Sdump(cfg))

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

	if err != nil {
		log.Fatalf("connection error: to %v, %s", addrs, err.Error())
	}
	log.Println("connected to", addrs)

	return consumer
}
