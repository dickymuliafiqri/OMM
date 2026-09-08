package bot

import (
	"context"
	"testing"

	"benchmark/internal/storage"
)

func TestBot_CheckAccess(t *testing.T) {
	db, err := storage.NewTursoDB(":memory:", "")
	if err != nil {
		t.Fatalf("Gagal init test db: %v", err)
	}
	repo := storage.NewRepository(db)
	defer repo.Close()

	if err := repo.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate db gagal: %v", err)
	}

	bot := &Bot{
		repo:          repo,
		adminIDs:      map[int64]bool{111: true},
		whitelistMode: true,
		whitelistIDs:  map[int64]bool{222: true},
	}

	ctx := context.Background()

	// 1. Admin must always pass whitelist
	if !bot.checkAccess(ctx, 111, 1000) {
		t.Errorf("Admin harus selalu lolos whitelist")
	}

	// 2. Whitelisted user must pass
	if !bot.checkAccess(ctx, 222, 1000) {
		t.Errorf("Whitelisted user harus lolos")
	}

	// 3. Non-whitelisted user must be rejected
	if bot.checkAccess(ctx, 333, 1000) {
		t.Errorf("User non-whitelisted harus ditolak saat WhitelistMode aktif")
	}

	// 4. Non-whitelist mode
	bot.whitelistMode = false
	if !bot.checkAccess(ctx, 333, 1000) {
		t.Errorf("User normal harus lolos jika WhitelistMode nonaktif")
	}

	// 5. Banned user must be rejected
	if err := repo.BanUser(ctx, 333); err != nil {
		t.Fatalf("BanUser gagal: %v", err)
	}
	if bot.checkAccess(ctx, 333, 1000) {
		t.Errorf("Banned user harus ditolak oleh checkAccess")
	}

	// 6. Unbanned user passes again
	if err := repo.UnbanUser(ctx, 333); err != nil {
		t.Fatalf("UnbanUser gagal: %v", err)
	}
	if !bot.checkAccess(ctx, 333, 1000) {
		t.Errorf("Setelah unban user harus kembali lolos")
	}
}
