package server

import "net/http"

// SecurityHeadersMiddleware attaches strict security and no-cache headers to all responses.
func SecurityHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()

		// Prevent MIME-type sniffing
		h.Set("X-Content-Type-Options", "nosniff")

		// Prevent clickjacking / framing
		h.Set("X-Frame-Options", "DENY")

		// Modern browser XSS auditor control (disabled in favor of CSP, standard practice)
		h.Set("X-XSS-Protection", "0")

		// Strict caching restrictions for benchmark API responses
		h.Set("Cache-Control", "no-store, no-cache, must-revalidate")
		h.Set("Pragma", "no-cache")

		// Restrict referrer leakage
		h.Set("Referrer-Policy", "no-referrer")

		// Enforce HSTS when connection is HTTPS or via SSL termination proxy
		if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
			h.Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		}

		next.ServeHTTP(w, r)
	})
}
