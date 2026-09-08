package server

import (
	"crypto/subtle"
	"net/http"
	"strings"

	"benchmark/pkg/logger"
)

// AuthMiddleware enforces HMAC shared-secret authentication using constant-time comparison.
func AuthMiddleware(expectedSecret string) func(http.Handler) http.Handler {
	expectedBytes := []byte(expectedSecret)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			providedSecret := strings.TrimSpace(r.Header.Get("X-Bench-Secret"))
			if providedSecret == "" {
				logger.Warn("auth.rejected", "ip=%s reason=missing_secret", GetClientIP(r))
				writeJSON(w, http.StatusForbidden, map[string]string{
					"error": "forbidden: missing authentication secret",
				})
				return
			}

			// Constant-time comparison to prevent timing attacks
			if subtle.ConstantTimeCompare([]byte(providedSecret), expectedBytes) != 1 {
				logger.Warn("auth.rejected", "ip=%s reason=invalid_secret", GetClientIP(r))
				writeJSON(w, http.StatusForbidden, map[string]string{
					"error": "forbidden: invalid authentication secret",
				})
				return
			}

			logger.Debug("auth.success", "path=%s ip=%s", r.URL.Path, GetClientIP(r))
			next.ServeHTTP(w, r)
		})
	}
}
