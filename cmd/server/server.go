package main

import (
	"log"

	"github.com/jmgilman/kv/http"
)

func main() {
	server, err := http.NewServer()
	if err != nil {
		log.Fatal("creating server failed")
	}
	server.ListenAndServe()
}
