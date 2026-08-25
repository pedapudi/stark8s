// Command coordinator serves one workload's control plane: topology, pod
// registry, partition ownership, segment index, seals, and loop epochs on
// the control port, and the segments it holds for external producers on
// the segment port.
//
// Environment:
//
//	STARK8S_SEGMENT_ADDR  host:port at which worker pods reach this
//	                      process's segment server (default
//	                      <hostname>:8090)
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/pedapudi/stark8s/pkg/coordinator"
)

func main() {
	self := os.Getenv("STARK8S_SEGMENT_ADDR")
	if self == "" {
		host, _ := os.Hostname()
		self = fmt.Sprintf("%s:%d", host, coordinator.SegmentPort)
	}
	co := coordinator.New(self)
	go func() {
		addr := fmt.Sprintf(":%d", coordinator.SegmentPort)
		log.Printf("coordinator segment server listening on %s (announced as %s)", addr, self)
		log.Fatal(http.ListenAndServe(addr, coordinator.SegmentHandler(co)))
	}()
	addr := fmt.Sprintf(":%d", coordinator.ControlPort)
	log.Printf("coordinator listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, coordinator.Handler(co)))
}
