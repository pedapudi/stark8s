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
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/pedapudi/stark8s/pkg/coordinator"
	"github.com/pedapudi/stark8s/pkg/storage"
)

func main() {
	host, _ := os.Hostname()
	self := os.Getenv("STARK8S_SEGMENT_ADDR")
	if self == "" {
		self = fmt.Sprintf("%s:%d", host, coordinator.SegmentPort)
	}
	co := coordinator.New(self)
	if dir := os.Getenv("STARK8S_COORDINATOR_STATE_DIR"); dir != "" {
		store, err := storage.NewLocal(dir)
		if err != nil {
			log.Fatal(err)
		}
		lock, err := store.LockWriter()
		if err != nil {
			log.Fatal(err)
		}
		defer lock.Close()
		co, err = coordinator.NewDurable(context.Background(), self, store, "coordinator.json", host)
		if err != nil {
			log.Fatal(err)
		}
	} else if endpoint := os.Getenv("STARK8S_OBJECT_STORE_ENDPOINT"); endpoint != "" {
		store, err := storage.NewS3(endpoint, storage.S3Credentials{
			AccessKey: os.Getenv("STARK8S_OBJECT_STORE_ACCESS_KEY"), SecretKey: os.Getenv("STARK8S_OBJECT_STORE_SECRET_KEY"),
			SessionToken: os.Getenv("STARK8S_OBJECT_STORE_SESSION_TOKEN"), Region: os.Getenv("STARK8S_OBJECT_STORE_REGION"),
		}, http.DefaultClient)
		if err != nil {
			log.Fatal(err)
		}
		key := "coordinator.json"
		if prefix := os.Getenv(coordinator.EnvObjectPrefix); prefix != "" {
			key = prefix + "/" + key
		}
		co, err = coordinator.NewDurable(context.Background(), self, store, key, host)
		if err != nil {
			log.Fatal(err)
		}
	}
	go func() {
		addr := fmt.Sprintf(":%d", coordinator.SegmentPort)
		log.Printf("coordinator segment server listening on %s (announced as %s)", addr, self)
		log.Fatal(http.ListenAndServe(addr, coordinator.SegmentHandler(co)))
	}()
	addr := fmt.Sprintf(":%d", coordinator.ControlPort)
	log.Printf("coordinator listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, coordinator.HandlerForGraph(co, os.Getenv(coordinator.EnvWorkload))))
}
