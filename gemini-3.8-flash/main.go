package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

var totalVisitors uint64

const maxMsgLen = 2048

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", "default-src 'none'")
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

func visitHandler(w http.ResponseWriter, r *http.Request) {
	total := atomic.AddUint64(&totalVisitors, 1)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Total pengunjung: %d\n", total)
}

func echoHandler(w http.ResponseWriter, r *http.Request) {
	msg := r.URL.Query().Get("msg")
	if len(msg) > maxMsgLen {
		http.Error(w, "msg terlalu panjang (maks 2048 byte)", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(msg))
}

func setupRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /visit", visitHandler)
	mux.HandleFunc("GET /echo", echoHandler)
	return securityHeaders(mux)
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// ponytail: TLS dan rate limiting diserahkan ke reverse proxy (Nginx/Caddy/Cloudflare).
	// Tambah crypto/tls dan golang.org/x/time jika server ekspos langsung tanpa proxy.
	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           setupRoutes(),
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    1 << 20, // 1 MB
	}

	idleConnsClosed := make(chan struct{})
	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt, syscall.SIGTERM)
		<-sigint

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Printf("HTTP shutdown error: %v", err)
		}
		close(idleConnsClosed)
	}()

	log.Printf("Server jalan di port %s", port)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("HTTP server error: %v", err)
	}

	<-idleConnsClosed
	log.Println("Server selesai.")
}
