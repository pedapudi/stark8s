// Command exchange serves one workload's channels over HTTP.
package main

import (
	"log"
	"net/http"
	"os"

	"github.com/pedapudi/stark8s/pkg/exchange"
)

func main() {
	addr := os.Getenv("STARK8S_LISTEN")
	if addr == "" {
		addr = ":8080"
	}
	log.Printf("exchange listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, exchange.Handler(exchange.New())))
}
