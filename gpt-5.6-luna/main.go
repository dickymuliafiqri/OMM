// A small, single-file HTTP server exposing an in-memory visitor counter and
// a bounded text echo endpoint.
//
// For internet-facing deployments, provide CERT_FILE and KEY_FILE and run the
// server with TLS, or put it behind a TLS-terminating reverse proxy.
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"
)

const (
	maxMessageBytes = 4096
	maxConcurrent   = 256
	requestTimeout  = 10 * time.Second
)

var visitors uint64

type response struct {
	Total uint64 `json:"total,omitempty"`
	Msg   string `json:"msg,omitempty"`
	Error string `json:"error,omitempty"`
}

func main() {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}

	server := &http.Server{
		Addr:              addr,
		Handler:           withSecurityHeaders(withRequestLimits(http.HandlerFunc(route))),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       requestTimeout,
		WriteTimeout:      requestTimeout,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 * 1024,
	}

	// TLS is optional so the same file remains convenient for local use. Do
	// not expose plain HTTP to the public internet; use TLS or a trusted proxy.
	certFile, keyFile := os.Getenv("CERT_FILE"), os.Getenv("KEY_FILE")
	if (certFile == "") != (keyFile == "") {
		log.Fatal("CERT_FILE and KEY_FILE must be provided together")
	}
	if certFile != "" {
		server.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	serverErr := make(chan error, 1)
	go func() {
		if certFile != "" {
			serverErr <- server.ListenAndServeTLS(certFile, keyFile)
			return
		}
		log.Printf("WARNING: serving plain HTTP on %s; use TLS or a TLS reverse proxy in production", addr)
		serverErr <- server.ListenAndServe()
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-stop:
		log.Printf("shutting down after %s", sig)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("graceful shutdown failed: %v", err)
		}
	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}
}

func route(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/visit":
		if r.Method != http.MethodGet {
			methodNotAllowed(w)
			return
		}
		writeJSON(w, http.StatusOK, response{Total: atomic.AddUint64(&visitors, 1)})
	case "/echo":
		if r.Method != http.MethodGet {
			methodNotAllowed(w)
			return
		}
		echo(w, r)
	default:
		errorJSON(w, http.StatusNotFound, "not found")
	}
}

func echo(w http.ResponseWriter, r *http.Request) {
	// ParseQuery reports malformed percent-encoding instead of silently
	// discarding it. Requiring exactly one parameter avoids ambiguous input.
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || len(values) != 1 || len(values["msg"]) != 1 {
		errorJSON(w, http.StatusBadRequest, "query must contain exactly one msg parameter")
		return
	}
	msg := values["msg"][0]
	if msg == "" {
		errorJSON(w, http.StatusBadRequest, "msg must not be empty")
		return
	}
	if len(msg) > maxMessageBytes {
		errorJSON(w, http.StatusRequestEntityTooLarge, "msg is too long")
		return
	}
	if !utf8.ValidString(msg) {
		errorJSON(w, http.StatusBadRequest, "msg must be valid UTF-8")
		return
	}
	writeJSON(w, http.StatusOK, response{Msg: msg})
}

func writeJSON(w http.ResponseWriter, status int, value response) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	// All response fields are JSON-encoded. In particular, msg is never
	// interpolated into HTML, JavaScript, headers, or a log message.
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("response encode error: %v", err)
	}
}

func errorJSON(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, response{Error: message})
}

func methodNotAllowed(w http.ResponseWriter) {
	w.Header().Set("Allow", http.MethodGet)
	errorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
}

func withSecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'; base-uri 'none'")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
		h.Set("Cache-Control", "no-store")
		// Browsers honor HSTS only when received over HTTPS. Sending it on all
		// responses is harmless for local HTTP and safe for TLS deployments.
		h.Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		next.ServeHTTP(w, r)
	})
}

func withRequestLimits(next http.Handler) http.Handler {
	sem := make(chan struct{}, maxConcurrent)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
		default:
			errorJSON(w, http.StatusServiceUnavailable, "server busy")
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func init() { log.SetFlags(log.LstdFlags | log.LUTC) }
