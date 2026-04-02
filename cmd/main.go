package main

import (
	"context"
	"fmt"
	server "gochatapp/internal/server"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var (
	WSPort = ":8081"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	srv := server.NewServer(ctx)

	http.HandleFunc("/", srv.HandleWS)

	httpServer := &http.Server{Addr: WSPort}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		srv.Run()
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("shutting down...")

	cancel()

	shutdownCtx, cancelCtx := context.WithTimeout(context.Background(), time.Second*5)
	defer cancelCtx()
	httpServer.Shutdown(shutdownCtx)

	wg.Wait()
	fmt.Printf("shutdown complete")

}
