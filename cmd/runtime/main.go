package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	localruntime "github.com/pedapudi/stark8s/pkg/runtime"
	"github.com/pedapudi/stark8s/pkg/sdk"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	w, err := sdk.FromEnv()
	if err != nil {
		log.Fatal(err)
	}
	srv := localruntime.NewServer()
	srv.ConfigureCheckpointing(w.CheckpointStore != nil)
	httpServer := &http.Server{
		Addr:    ":8081",
		Handler: srv,
		BaseContext: func(net.Listener) context.Context {
			return ctx
		},
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- httpServer.ListenAndServe() }()
	ready := make(chan error, 1)
	go func() { ready <- srv.WaitReady(ctx) }()
	select {
	case err := <-serverErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
		return
	case err := <-ready:
		if err == nil {
			break
		}
		if !errors.Is(err, context.Canceled) {
			log.Fatal(err)
		}
		return
	}
	workerErrors := make(chan error, 1)
	go func() { workerErrors <- w.Run(ctx, srv.HandlersFor(w)) }()
	var runErr error
	select {
	case runErr = <-workerErrors:
	case runErr = <-srv.Failures():
		stop()
		<-workerErrors
	}
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		stop()
	}
	if err := <-serverErrors; err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		log.Fatal(runErr)
	}
}
