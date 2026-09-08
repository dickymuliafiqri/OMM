package sandbox

import (
	"fmt"
	"net"
)

// GetFreePort finds and allocates an available local ephemeral TCP port.
// It binds to listener 127.0.0.1:0 and immediately closes it.
func GetFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("gagal resolve tcp addr: %w", err)
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, fmt.Errorf("gagal bind ke port ephemeral: %w", err)
	}
	defer listener.Close()

	tcpAddr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("gagal mendapatkan net.TCPAddr dari listener")
	}

	return tcpAddr.Port, nil
}

// IsPortAvailable checks whether a specific TCP port on localhost is currently not in use
func IsPortAvailable(port int) bool {
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return false
	}
	_ = listener.Close()
	return true
}
