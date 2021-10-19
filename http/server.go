package http

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/jmgilman/kv/btree"
	"github.com/jmgilman/kv/mock"
	"github.com/jmgilman/kv/service"
)

type Server struct {
	kvService service.KVService
	router    *mux.Router
	server    http.Server
}

func (s *Server) ListenAndServe() {
	done := make(chan struct{})

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		signal.Notify(sig, syscall.SIGTERM)

		<-sig

		fmt.Println("Gracefully shutting down server...")

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := s.server.Shutdown(ctx); err != nil {
			log.Fatal(err)
		}

		cancel()
		close(done)
	}()

	if err := s.server.ListenAndServe(); err != http.ErrServerClosed {
		log.Printf("error starting HTTP server: %v", err)
		return
	}

	<-done
}

func NewServer() (*Server, error) {
	// Create key/value service
	kvService := service.NewKVService(&btree.Tree{}, &mock.MockNVStore{})

	// Create server
	router := mux.NewRouter()
	server := &Server{
		kvService: kvService,
		router:    router,
		server:    http.Server{Addr: ":8080", Handler: router},
	}

	// Register routes
	server.routes()

	return server, nil
}
