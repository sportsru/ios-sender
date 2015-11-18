package main

import (
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/davecgh/go-spew/spew"
	"gopkg.in/tomb.v2"
)

const (
	// Gateway tcp addr of APNS gateway
	Gateway = "gateway.push.apple.com:2195"
	// GatewaySandbox tcp addr of APNS sandbox gateway
	GatewaySandbox = "gateway.sandbox.push.apple.com:2195"
	// Feedback tcp addr of APNS feedback service
	Feedback = "feedback.push.apple.com:2196"
	// FeedbackSandbox tcp addr of APNS sandbox feedback service
	FeedbackSandbox = "feedback.sandbox.push.apple.com:2196"
	// DefaultErrorTimeout not in use, but intention was to store
	//			sent messages for this time for async catching APNS errors
	DefaultErrorTimeout = 1 * time.Second
)

// GatewayClient mapped on one APNS tcp Gateway connection
type GatewayClient struct {
	Name string

	Gateway           string
	CertificateFile   string
	CertificateBase64 string
	KeyFile           string
	KeyBase64         string

	TLSConn *tls.Conn
	TLSConf *tls.Config

	NextIdentifier uint32
	ErrorTimeout   time.Duration
	LastUsageTime  time.Time
	Connected      bool

	ErrCh    chan error
	LoopDone chan struct{}

	t *tomb.Tomb
	//ctx    context.Context
	//cancel context.CancelFunc
	Stat *ClientStat

	sync.Mutex
}

// BareGatewayClient set TLS certificate and Private key values in Base64 format
func BareGatewayClient(gateway, certificateBase64, keyBase64 string) (c *GatewayClient) {
	c = new(GatewayClient)
	c.Gateway = gateway
	c.CertificateBase64 = certificateBase64
	c.KeyBase64 = keyBase64
	c.ErrorTimeout = DefaultErrorTimeout
	c.TLSConf = &tls.Config{} // {ClientSessionCache: tls.NewLRUClientSessionCache(0)}
	return c
}

// ClientStat stores GatewayClient on wire statistics
type ClientStat struct {
	Connected  bool
	Reconnects int32
	LastError  string
	Drop       int32
	Send       int32
	Skip       int32
	mu         sync.RWMutex
}

// SetConnected concurrent safe set Connected flag
func (v *ClientStat) SetConnected(b bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.Connected = b
}

// SetLastError concurrent safe set of last error
func (v *ClientStat) SetLastError(e error) {
	if e == nil {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.LastError = e.Error()
}

// String concurrent safe implementation of ClientStat serialization to json
func (v *ClientStat) String() string {
	v.mu.RLock()
	defer v.mu.RUnlock()

	j, _ := json.Marshal(v)
	return string(j)
}

// NewGatewayClient assumes you'll be passing in paths that
// point to your certificate and key.
func NewGatewayClient(name, gateway, certificateFile, keyFile string) (c *GatewayClient) {
	c = new(GatewayClient)
	c.Name = name
	c.Gateway = gateway
	c.CertificateFile = certificateFile
	c.KeyFile = keyFile
	c.ErrorTimeout = DefaultErrorTimeout
	c.TLSConf = &tls.Config{} // {ClientSessionCache: tls.NewLRUClientSessionCache(0)}

	c.Stat = &ClientStat{}
	return c
}

func (c *GatewayClient) connect() (err error) {
	var cert tls.Certificate
	_ = atomic.AddInt32(&c.Stat.Reconnects, 1)

	if *debug {
		log.Println("DEBUG: GatewayClient.Connect()")
	} else {
		// tmp debug
		log.Println("GatewayClient.Connect()")
	}
	if len(c.CertificateBase64) == 0 && len(c.KeyBase64) == 0 {
		// The user did not specify raw block contents, so check the filesystem.
		cert, err = tls.LoadX509KeyPair(c.CertificateFile, c.KeyFile)
	} else {
		// The user provided the raw block contents, so use that.
		cert, err = tls.X509KeyPair([]byte(c.CertificateBase64), []byte(c.KeyBase64))
	}

	if err != nil {
		return err
	}

	tlsConf := c.TLSConf
	tlsConf.Certificates = []tls.Certificate{cert}
	gatewayParts := strings.Split(c.Gateway, ":")
	tlsConf.ServerName = gatewayParts[0]

	// TODO: add timeout (DialWithDialer)
	// d := &net.Dialer{Timeout: 3 * time.Second}
	tlsConn, err := tls.Dial("tcp", c.Gateway, tlsConf)
	if err != nil {
		log.Println("tls.Deal error:", err)
		return err
	}

	c.TLSConn = tlsConn
	c.Connected = true
	c.Stat.SetConnected(true)
	c.Stat.SetLastError(errors.New(""))

	c.t = &tomb.Tomb{}
	c.t.Go(c.loop)

	return nil
}

const closeErrStr = "use of closed network connection"

func (c *GatewayClient) loop() error {
	defer log.Println("loop() defer called")

	c.LoopDone = make(chan struct{})
	c.ErrCh = make(chan error)
	go c.readLoop()
	// читаем пока канал не закроется в readLoop()
	var err error
	for readErr := range c.ErrCh {
		if nerr, ok := readErr.(*net.OpError); ok {
			if nerr.Op == "read" && nerr.Err.Error() == closeErrStr && !*debug {
				continue
			}
			err = nerr
		}
		log.Printf("App '%v': error=%v", c.Name, readErr)
	}
	log.Printf("App %v: stop", c.Name)
	c.t.Kill(nil)
	log.Println("connection->tomb->Kill() called successfully")

	c.Lock()
	c.Connected = false
	c.Stat.SetConnected(false)
	c.Stat.SetLastError(err)
	c.Unlock()

	return err
}

// Connect concurrent safe wrapper around connect() method
func (c *GatewayClient) Connect() error {
	// XXX: probably we can merge this code in SendTo
	c.Lock()
	defer c.Unlock()
	return c.connect()
}

// Close concurrent safe connection closer
// 1. set Connected flag to false
// 2. close TLS connecton
// 3. wait than Errors loop on this connection is over
// 4. save and return last error if any from Errors loop
// выставляем флаг, что это мы закрываем коннект,
// чтобы функция чтения ошибок могла это проверить
// ждем завершения горутины чтения ошибок
func (c *GatewayClient) Close() error {
	c.Lock()
	defer c.Unlock()

	if !c.Connected {
		return nil
	}
	c.Connected = false

	err := c.TLSConn.Close()
	dieStatus := <-c.t.Dying()

	if *debug {
		log.Println("dieStatus: ", dieStatus) // {}
	}
	c.Stat.SetLastError(err)
	return err
}

func (c *GatewayClient) write(payload []byte) error {
	state := c.TLSConn.ConnectionState()
	state.PeerCertificates = nil
	state.VerifiedChains = nil
	if *debug {
		log.Println("Try to write with state")
		spew.Dump(state)
	}

	_, err := c.TLSConn.Write(payload)
	if err != nil {
		// We probably disconnected. Reconnect and resend the message.
		// TODO: Might want to check the actual error returned?
		log.Printf("[APNS] Error writing data to socket: %v\n", err)
		log.Println("[APNS] *** Server disconnected unexpectedly. ***")
		err := c.Connect()
		if err != nil {
			log.Printf("[APNS] Could not reconnect to the server: %v\n", err)
			return err
		}
		log.Println("[APNS] Reconnected to the server successfully.")

		// TODO: This could cause an endless loop of errors.
		// 		If it's the connection failing, that would be caught above.
		// 		So why don't we add a counter to the payload itself?
		_, err = c.TLSConn.Write(payload)
		if err != nil {
			return err
		}
	}

	return nil
}

var ErrTokenWrongLength = errors.New("Token length < 64")

// SendTo sends APNS message on the wire
func (c *GatewayClient) SendTo(n *Notify, token string) error {
	if len(token) < 64 {
		return ErrTokenWrongLength
	}
	// Convert the hex string iOS returns into a device token.
	byteToken, err := hex.DecodeString(token)
	if err != nil {
		return err
	}

	id := atomic.AddUint32(&c.NextIdentifier, 1)

	// Converts this notification into the Enhanced Binary Format.
	message, err := n.ToBinary(id, byteToken)
	if err != nil {
		return err
	}

	// reconnect if needed
	// TODO: логировать реконнект
	var connErr error
	c.Lock()
	if !c.Connected {
		log.Println("SendTo() && !c.Connected: call c.connect()")
		connErr = c.connect()
	}
	c.Unlock()
	if connErr != nil {
		return connErr
	}

	// send payload
	c.Lock()
	err = c.write(message)
	c.Unlock()
	if err != nil {
		return err
	}

	return nil
}

// https://blog.golang.org/generate

// APNSErrorCode error wraps APNS cryptic codes:
// https://developer.apple.com/library/prerelease/ios/documentation/NetworkingInternet/Conceptual/RemoteNotificationsPG/Chapters/CommunicatingWIthAPS.html
//go:generate stringer -type=APNSErrorCode
type APNSErrorCode int

// stringer generate String() method on const type APNSErrorCode
const (
	ErrAPNSNoErrors APNSErrorCode = iota // "No errors encountered"
	ErrAPNSProcessing
	ErrAPNSMissingDeviceToken
	ErrAPNSMissingTopic
	ErrAPNSMissingPayload
	ErrAPNSInvalidTokenSize
	ErrAPNSInvalidTopicSize
	ErrAPNSInvalidPayloadSize
	ErrAPNSInvalidToken
	ErrAPNSShutdown
)

// ErrorAPNS stores APNS error as is (6 bytes) and implements error interface
type ErrorAPNS []byte

// NewErrorAPNSbuffer creates and return ErrorAPNS with underlined buffer
func NewErrorAPNSbuffer() ErrorAPNS {
	buffer := make([]byte, 6, 6)
	return ErrorAPNS(buffer)
}

// https://developer.apple.com/library/ios/documentation/NetworkingInternet/Conceptual/RemoteNotificationsPG/Chapters/CommunicatingWIthAPS.html
func (e ErrorAPNS) Error() string {
	code, status := int(e[0]), int(e[1])
	token := hex.EncodeToString(e[2:])
	if code != 8 {
		return fmt.Sprintf(
			"Wrong APNS response format, started with non 8 byte[0]=%v", code)
	}

	if status == 255 {
		return fmt.Sprintf("Unknown APNS error: status 255 (token=%v)", token)
	}
	if status < 11 {
		return fmt.Sprintf("APNS error %v: \"%s\" (token=%v)",
			status, APNSErrorCode(status), token)
	}
	return "Unknown error"
}

func (c *GatewayClient) readLoop() {
	for {
		c.Lock()
		isDisconnected := !c.Connected
		c.Unlock()
		if isDisconnected {
			return
		}
		// TODO: проверить что коннект еще числится в живых
		errorAPNS := NewErrorAPNSbuffer()
		_, err := c.TLSConn.Read([]byte(errorAPNS))

		if err != nil {
			c.ErrCh <- err
			close(c.ErrCh)
			return
		}

		// TODO: process if we close connection (to avoid errors on start)
		c.ErrCh <- errorAPNS
	}
}
