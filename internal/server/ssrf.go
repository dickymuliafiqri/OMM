package server

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	"benchmark/pkg/logger"
)

var privateCIDRs []*net.IPNet

func init() {
	cidrStrings := []string{
		"127.0.0.0/8",    // IPv4 Loopback
		"10.0.0.0/8",     // RFC 1918
		"172.16.0.0/12",  // RFC 1918
		"192.168.0.0/16", // RFC 1918
		"169.254.0.0/16", // IPv4 Link-Local / AWS Metadata
		"0.0.0.0/8",      // Broadcast / Current network
		"::1/128",        // IPv6 Loopback
		"fc00::/7",       // IPv6 Unique Local Address
		"fe80::/10",      // IPv6 Link-Local
	}

	for _, cidr := range cidrStrings {
		_, ipNet, err := net.ParseCIDR(cidr)
		if err == nil {
			privateCIDRs = append(privateCIDRs, ipNet)
		}
	}
}

// SSRFValidator validates destination URLs to prevent SSRF vulnerabilities.
type SSRFValidator struct {
	AllowLocalhost bool // Allows localhost and 127.0.0.1 for local development/testing
}

// NewSSRFValidator creates a new SSRFValidator instance.
func NewSSRFValidator(allowLocalhost bool) *SSRFValidator {
	return &SSRFValidator{
		AllowLocalhost: allowLocalhost,
	}
}

// isLoopbackHost checks whether a host is localhost or a loopback address.
func isLoopbackHost(host string) bool {
	h := strings.ToLower(strings.TrimSpace(host))
	h = strings.Trim(h, "[]")
	if h == "localhost" || h == "127.0.0.1" || h == "::1" {
		return true
	}
	if ip := net.ParseIP(h); ip != nil && ip.IsLoopback() {
		return true
	}
	return false
}

// DefaultSSRFValidator is the standard strict validator for production.
var DefaultSSRFValidator = NewSSRFValidator(false)

// ValidateURL checks if a target URL is safe from SSRF attacks.
func (v *SSRFValidator) ValidateURL(rawURL string) error {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return errors.New("URL tidak boleh kosong")
	}

	parsed, err := url.ParseRequestURI(rawURL)
	if err != nil {
		return fmt.Errorf("format URL tidak valid: %w", err)
	}

	// 1. Enforce scheme: https only (http permitted solely for localhost/loopback if configured)
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "https" {
		if scheme == "http" && v.AllowLocalhost && isLoopbackHost(parsed.Hostname()) {
			// Permitted for localhost development
		} else {
			return fmt.Errorf("skema URL harus https, skema '%s' ditolak", scheme)
		}
	}

	// 2. Reject credentials in URL
	if parsed.User != nil {
		return errors.New("kredensial di dalam URL tidak diizinkan")
	}

	// 3. Validate hostname
	hostname := parsed.Hostname()
	if hostname == "" {
		return errors.New("hostname tidak boleh kosong")
	}

	// Reject internal metadata domains
	lowerHost := strings.ToLower(hostname)
	if lowerHost == "metadata.google.internal" || lowerHost == "instance-data" {
		return fmt.Errorf("akses ke host metadata internal '%s' diblokir", hostname)
	}

	// Check if hostname is localhost/loopback
	if isLoopbackHost(hostname) {
		if v.AllowLocalhost {
			logger.Debug("ssrf.loopback_allowed", "url=%s host=%s", rawURL, hostname)
			return nil
		}
		return errors.New("akses ke localhost diblokir")
	}

	// 4. Resolve IPs to check for private / internal addresses
	ips, err := net.LookupIP(hostname)
	if err != nil {
		return fmt.Errorf("gagal me-resolve host '%s': %w", hostname, err)
	}
	if len(ips) == 0 {
		return fmt.Errorf("tidak ada IP ditemukan untuk host '%s'", hostname)
	}

	for _, ip := range ips {
		if v.isPrivateIP(ip) {
			if v.AllowLocalhost && ip.IsLoopback() && isLoopbackHost(hostname) {
				continue
			}
			return fmt.Errorf("akses ke IP private/internal '%s' untuk host '%s' diblokir (SSRF guard)", ip.String(), hostname)
		}
	}

	logger.Debug("ssrf.validated", "url=%s host=%s ipCount=%d", rawURL, hostname, len(ips))
	return nil
}

func (v *SSRFValidator) isPrivateIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
		return true
	}

	for _, block := range privateCIDRs {
		if block.Contains(ip) {
			return true
		}
	}

	return false
}

// IsURLSafe is a convenience function using DefaultSSRFValidator.
func IsURLSafe(rawURL string) error {
	return DefaultSSRFValidator.ValidateURL(rawURL)
}
