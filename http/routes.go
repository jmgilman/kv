package http

import (
	"errors"
	"io"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/jmgilman/kv"
)

func (s *Server) routes() {
	s.router.HandleFunc("/v1/{key}", s.handlePut()).Methods("PUT")
	s.router.HandleFunc("/v1/{key}", s.handleGet()).Methods("GET")
	s.router.HandleFunc("/v1/{key}", s.handleDelete()).Methods("DELETE")
}

func (s *Server) handleDelete() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		key := vars["key"]

		err := s.kvService.Delete(key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		w.WriteHeader(http.StatusOK)
	}
}

func (s *Server) handleGet() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		key := vars["key"]

		pair, err := s.kvService.Get(key)
		if err != nil {
			if errors.Is(err, kv.ErrorNoSuchKey) {
				http.Error(w, err.Error(), http.StatusNotFound)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}

			return
		}

		w.Write(pair.Value)
	}
}

func (s *Server) handlePut() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		key := vars["key"]

		value, err := io.ReadAll(r.Body)
		defer r.Body.Close()

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		err = s.kvService.Put(key, value)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
}
