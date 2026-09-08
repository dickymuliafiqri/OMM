package server

import (
	"net/http"
	"net/url"
	"strings"

	"benchmark/pkg/logger"
)

// isLocalhostOrigin checks if an origin URL belongs to localhost or loopback.
func isLocalhostOrigin(origin string) bool {
	cleaned := strings.TrimRight(strings.TrimSpace(origin), "/")
	if cleaned == "" {
		return false
	}
	u, err := url.Parse(cleaned)
	if err != nil {
		return false
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return false
	}
	return isLoopbackHost(u.Hostname())
}

// CORSMiddleware provides strict origin whitelisting without wildcard support,
// and automatically allows localhost/loopback origins for local development.
func CORSMiddleware(allowedOrigins []string) func(http.Handler) http.Handler {
	// Build map for O(1) lookup
	originMap := make(map[string]struct{}, len(allowedOrigins))
	for _, o := range allowedOrigins {
		cleaned := strings.TrimRight(strings.TrimSpace(o), "/")
		if cleaned != "" && cleaned != "*" { // Explicitly reject wildcard
			originMap[cleaned] = struct{}{}
		}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := strings.TrimRight(strings.TrimSpace(r.Header.Get("Origin")), "/")

			// Only evaluate and set CORS headers if an Origin header is sent
			if origin != "" {
				_, isWhitelisted := originMap[origin]
				isLocalhost := isLocalhostOrigin(origin)
				allowed := isWhitelisted || isLocalhost
				logger.Debug("cors.eval", "origin=%q allowed=%v isLocalhost=%v isWhitelisted=%v method=%s path=%s",
					origin, allowed, isLocalhost, isWhitelisted, r.Method, r.URL.Path)

				if allowed {
					w.Header().Set("Access-Control-Allow-Origin", origin)
					w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
					w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Bench-Secret, X-Request-ID")
					w.Header().Set("Access-Control-Max-Age", "86400")
					w.Header().Set("Vary", "Origin")
				}
			}

			// Preflight request handling
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
