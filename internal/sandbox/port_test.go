package sandbox

import (
	"testing"
)

func TestGetFreePort(t *testing.T) {
	port1, err := GetFreePort()
	if err != nil {
		t.Fatalf("GetFreePort gagal: %v", err)
	}

	if port1 <= 1024 || port1 > 65535 {
		t.Errorf("Port di luar rentang valid user/ephemeral port (1024-65535): %d", port1)
	}

	if !IsPortAvailable(port1) {
		t.Errorf("Port %d harusnya tersedia setelah listener ditutup", port1)
	}

	port2, err := GetFreePort()
	if err != nil {
		t.Fatalf("Panggilan kedua GetFreePort gagal: %v", err)
	}
	if port2 <= 1024 {
		t.Errorf("Port kedua tidak valid: %d", port2)
	}
}
