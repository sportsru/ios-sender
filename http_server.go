package main

import (
	_ "expvar"
	_ "net/http/pprof"

	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
)

// WebServer stores global state of web server
type WebServer struct {
	nc NsqConsumerLocked
}

// Run starts tcp listener for http server on addr
// inspired by https://github.com/hoisie/web/blob/master/server.go
func (s *WebServer) Run(addr string) error {
	initHandlers(s)
	sock, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		fmt.Println("HTTP now available at", addr)
		log.Fatal(http.Serve(sock, nil))
	}()
	return nil
}

func initHandlers(s *WebServer) {
	http.HandleFunc("/tools/nsq_loglevel", func(w http.ResponseWriter, r *http.Request) {
		level := r.URL.Query().Get("level")
		s.nc.Lock()
		defer s.nc.Unlock()

		if len(level) < 1 {
			info := fmt.Sprintf("NSQ Consumer log level is \"%s\"\n", strings.ToUpper(s.nc.loglevel))
			writeResponseWithErr(w, info)
			return
		}

		loglevel, err := GetNSQLogLevel(strings.ToLower(level))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		s.nc.consumer.SetLogger(logger, loglevel)
		s.nc.loglevel = level

		info := fmt.Sprintf("Set NSQ Consumer log level to \"%s\"\n", strings.ToUpper(level))
		log.Println(info)
		writeResponseWithErr(w, info)
	})
}
