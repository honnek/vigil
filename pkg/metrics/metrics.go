package metrics

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func Serve(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	server := http.Server{Addr: addr, Handler: mux}

	go func() {
		err := server.ListenAndServe()
		if err != nil {
			log.Printf("Failed to start metrics server: %v", err)
		}
	}()
}
