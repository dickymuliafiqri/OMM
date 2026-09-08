# 🤖 AGENTS.md — AI Developer & Agent Architecture Guide

> **Target Audience:** Dokumen ini ditujukan khusus untuk AI coding assistant, autonomous agents, dan kontributor teknis yang membaca, memodifikasi, atau mengembangkan basis kode **OMM (On My Mark)**. Dokumen ini menjadi *single source of truth* mengenai arsitektur, filosofi evaluasi, batasan keamanan, dan konvensi rekayasa kode di proyek ini.

---

## 1. 🎯 Visi & Filosofi Proyek

**OMM (On My Mark)** adalah platform benchmarking otomatis untuk mengevaluasi kemampuan model AI (*Large Language Models*) dalam mendiagnosis, mengisolasi akar masalah, dan memperbaiki kode backend **Go 1.24+** nyata (*brownfield bug-fixing / SWE-bench*) berstandar produksi.

### Filosofi Utama:
1. **SWE-bench Brownfield vs. Greenfield Boilerplate:**
   AI tidak lagi diminta membuat server dari nol (yang rentan dihafal dari dataset pelatihan), melainkan **diberikan laporan bug riil (*GitHub issue*) dan basis kode Go yang rusak (*broken codebase*)**. AI ditantang untuk memperbaiki cacat logika/konkurensi tanpa merusak kontrak fungsi yang ada.
2. **Prinsip Fail-to-Pass (F2P) Deterministik:**
   Setiap task diverifikasi secara otomatis: kode awal **wajib gagal** pada test suite (`go test -race`), dan solusi perbaikan **wajib lolos 100% tanpa race condition**.
3. **Multi-Difficulty 4-Tier Ladder (20-25-30-25 Pts):**
   Membedakan secara tegas kapabilitas model baseline (8B–31B) dengan model penalaran tingkat tinggi (*flagship models*) melalui 4 tingkat kesulitan: **Junior**, **Mid-Level**, **Senior**, dan **Staff Engineer**.
4. **Toleransi Sintaks via Self-Healing (Compiler & Test Feedback):**
   Model diberikan kesempatan koreksi mandiri dengan 1 kali umpan balik (maksimal 2 percobaan) dengan penalti skor (-20% turn 2, gagal pada turn 2 = 0 pts).
5. **Penilaian Objektif & Mekanis (100% Native Go & Bebas Docker):**
   Semua penilaian dilakukan secara *live* di dalam sandbox terisolasi ephemeral menggunakan Go toolchain bawaan (`go test -race -v -count=1`).
6. **Stateless API Server Architecture:**
   Server benchmark (`omm-bench`) tidak menyimpan data permanen (stateless, zero-database). Persistence, session, dan leaderboard dikelola terpusat oleh frontend server (`omm-web`).

---

## 2. 🗺️ Peta Modul & Struktur Repositori

```text
OMM/
├── cmd/
│   └── bench/                      # Entrypoint: HTTP API benchmark server
│       └── main.go
├── internal/
│   ├── ai/                         # Klien AI, ekstraktor kode, dan prompt baku
│   │   ├── client.go               # Universal OpenAI/OpenRouter client, multi-turn Chat API
│   │   ├── extractor.go            # Pembersih markdown & ekstraksi blok kode Go
│   │   └── prompt.go               # Prompt standar evaluasi
│   ├── config/                     # Parser variabel lingkungan server (.env)
│   │   └── config.go               # Konfigurasi minimal benchmark server
│   ├── metrics/                    # Kolektor metrik performa in-memory
│   │   └── metrics.go              # Counter, Gauge, Timer metrics collector
│   ├── server/                     # Lapisan HTTP server & middleware keamanan
│   │   ├── router.go               # HTTP multiplexer (net/http standar)
│   │   ├── handler_bench.go        # Handler POST /api/bench
│   │   ├── handler_health.go       # Handler GET /healthz, /readyz
│   │   ├── handler_root.go         # Handler GET / (server discovery & metadata)
│   │   ├── handler_status.go       # Handler GET /api/bench/status
│   │   ├── middleware_auth.go      # Autentikasi HMAC shared secret
│   │   ├── middleware_ratelimit.go # Token bucket / sliding window rate limiter
│   │   ├── middleware_cors.go      # Kebijakan CORS whitelist ketat
│   │   ├── middleware_validate.go  # Validasi payload request & ukuran body
│   │   ├── middleware_security.go  # Security headers (nosniff, no-cache, DENY)
│   │   ├── middleware_logging.go   # Request logging & correlation ID middleware
│   │   └── ssrf.go                 # Validasi proteksi SSRF terhadap callback & base URL
│   ├── worker/                     # Worker pool & eksekusi evaluasi
│   │   ├── pool.go                 # Semaphore-based worker pool
│   │   ├── job.go                  # Definisi data model pekerjaan benchmark
│   │   ├── executor.go             # Orkestrasi siklus hidup evaluasi
│   │   ├── ladder.go               # 4-tier ladder runner
│   │   ├── callback.go             # Klien HTTP callback dengan retry
│   │   └── cost.go                 # Estimasi biaya token fallback
│   ├── swe/                        # Core SWE-bench engine & evaluator
│   │   ├── task.go                 # Definisi Task, Tier, EvalResult, LadderReport
│   │   ├── registry.go             # Registry task kurasi & fungsi default ladder
│   │   ├── evaluator.go            # Sandboxed test runner (go test -race) & F2P validator
│   │   ├── prompt.go               # Prompt builder (Issue Markdown + Broken Code) & SWE extractor
│   │   └── tasks/                  # 8 implementasi kasus uji bug riil
│   │       ├── t1_inventory_cache.go
│   │       ├── t1_order_validator.go
│   │       ├── t2_rate_limiter.go
│   │       ├── t2_worker_pool.go
│   │       ├── t3_connection_pool.go
│   │       ├── t3_distributed_cache.go
│   │       ├── t4_mpmc_ring.go
│   │       └── t4_saga_orchestrator.go
│   └── sandbox/                    # Lapisan isolasi keamanan eksekusi
│       ├── ast_guard.go            # Pemindaian statis AST: blokir RCE (os/exec, syscall, unsafe)
│       ├── port.go                 # Alokasi dinamis port bebas (:0)
│       └── runner.go               # Runner ephemeral sandbox
└── pkg/
    └── logger/                     # Structured 1-line logger dengan masking rahasia otomatis
```

---

## 3. 📐 Rubrik Penilaian 4-Tier Ladder (Total: 100 Pts)

Evaluasi SWE-bench OMM mengukur kapabilitas penyelesaian bug konkurensi dan sistem Go riil (*brownfield bug-fixing*) melalui **4-Tier Engineering Ladder**:

| Tier | Tingkat Kesulitan | Bobot Poin | Fokus Diagnostik & Karakteristik Bug | Contoh Task |
|:---|:---|:---:|:---|:---|
| **Tier 1** | **Junior** | **20 pts** | Bug logika fundamental, nil pointer dereference, map data race sederhana, slice bounds | Inventory cache nil check, order validator concurrency |
| **Tier 2** | **Mid-Level** | **25 pts** | Manajemen lifecycle goroutine, kebocoran goroutine, unbuffered channel hang, unstopped ticker | Distributed rate limiter, worker pool lifecycle |
| **Tier 3** | **Senior** | **30 pts** | Deadlock transfer antar akun (lock inversion AB-BA), context cancellation propagation failure | Connection pool leak, distributed LRU cache singleflight |
| **Tier 4** | **Staff** | **25 pts** | Lock-free concurrency, ABA/stale read pada atomic CAS, concurrent state machine race | MPMC ring buffer atomic CAS, financial saga orchestrator |

### Skala Penalti Self-Healing:
Tiap task memberikan kesempatan perbaikan mandiri dengan batas 1 kali umpan balik (maksimal 2 putaran / turns):
- **Turn 1 (First Attempt):** Multiplier **1.0** (100% poin maksimal, misal: Tier 3 = 30 pts)
- **Turn 2 (Second Attempt / 1x Feedback):** Multiplier **0.8** (80% poin maksimal, misal: Tier 3 = 24 pts)
- **Gagal setelah putaran ke-2:** **0 pts**

### Predikat Nilai (*Engineering Levels*):
- **Grade S (Staff Engineer):** 90 – 100 pts (Sangat Direkomendasikan / Menguasai Sistem & Lock-Free Concurrency)
- **Grade A (Senior Engineer):** 80 – 89 pts (Handal / Menyelesaikan Masalah Deadlock & Lifecycle Kompleks)
- **Grade B (Mid-Level Engineer):** 70 – 79 pts (Kompeten / Mampu Mengatasi Race Condition Standar)
- **Grade C (Junior Engineer):** 60 – 69 pts (Dasar / Memahami Sintaks dan Nil Check Dasar)
- **Grade F (Unqualified):** < 60 pts (Gagal / Tidak Mampu Menyelesaikan Bug Konkurensi Kritis)

---

## 4. 🔄 Protokol Self-Healing Feedback Loop

Fitur ini terletak pada [`internal/ai/client.go`](file:///C:/Users/Dicky%20Mulia%20Fiqri/go/src/github.com/dickymuliafiqri/omm/internal/ai/client.go) dan [`internal/swe/evaluator.go`](file:///C:/Users/Dicky%20Mulia%20Fiqri/go/src/github.com/dickymuliafiqri/omm/internal/swe/evaluator.go).

### Prinsip Operasional:
1. **Full-File Replacement:**
   * Model mengembalikan kode Go utuh (`package ...`) di dalam blok ` ```go ... ``` `. Model memiliki tingkat keberhasilan kompilasi dan perbaikan lebih tinggi saat menghasilkan file utuh dibandingkan diff/patch format.
2. **Umpan Balik Kompilator & Test Runner Riil:**
   * Jika evaluasi gagal (`go test -race` error, panic, data race, atau timeout), output pengujian mentah disertakan dalam prompt perbaikan turn 2.
3. **Akumulasi Token & Biaya:**
   * Token input dan output dari setiap percobaan dijumlahkan untuk kalkulasi biaya token yang akurat.

---

## 5. 🛠️ Aturan Pembuatan & Kurasi Task Baru (SWE-bench Tasks)

Setiap task di [`internal/swe/tasks/`](file:///C:/Users/Dicky%20Mulia%20Fiqri/go/src/github.com/dickymuliafiqri/omm/internal/swe/tasks/) wajib mematuhi protokol berikut:

1. **Fail-to-Pass (F2P) Wajib:**
   * `BrokenCode` **wajib gagal** saat dijalankan terhadap `TestSuite`.
   * `ReferenceSolution` **wajib lolos 100%** tanpa error dan **zero data race** (`-race`).
   * Verifikasi ini diuji secara otomatis pada unit test [`tasks_test.go`](file:///C:/Users/Dicky%20Mulia%20Fiqri/go/src/github.com/dickymuliafiqri/omm/internal/swe/tasks/tasks_test.go).
2. **Zero External Dependencies:**
   * Task hanya boleh menggunakan pustaka standar Go (`sync`, `sync/atomic`, `context`, `time`, dll.). Tidak diperkenankan modul pihak ketiga.
3. **Deterministik & Cepat:**
   * Eksekusi pengujian harus selesai dalam < 5 detik. Hindari `time.Sleep` yang terlalu panjang; gunakan `sync.WaitGroup`, sinyal channel, atau koordinasi atomic.
4. **Kepatuhan Keamanan AST:**
   * Task tidak boleh mengimpor paket terlarang (`os/exec`, `syscall`, `unsafe`, dll.).

---

## 6. 🛡️ Aturan Keamanan Sandbox & Isolasi (PENTING BAGI AGENT)

1. **AST Strict Mode (`ast_guard.go`):**
   * Dilarang keras mengizinkan impor paket `syscall`, `os/exec`, `unsafe`, `plugin`, `runtime/cgo`, atau `C`.
   * AST Guard membatalkan evaluasi sebelum kompilasi jika paket tersebut ditemukan.
2. **Ephemeral Workspace Cleanup:**
   * Setiap proses dijalankan di direktori sementara unik (`omm-sandbox-*`).
   * Selalu panggil `defer os.RemoveAll(tempDir)` untuk mencegah kebocoran disk.
3. **Strict Timeout & Resource Controls:**
   * Setiap task dieksekusi dengan timeout ketat (`context.WithTimeout`) untuk mencegah goroutine hang atau deadlock tak berujung.
4. **SSRF & Credential Protection:**
   * Validasi semua URL eksternal (`callbackUrl`, `baseUrl`) terhadap private IP dan metadata services.
   * Kredensial API Key tidak pernah dicatat ke dalam log dan wajib di-zero dari memori setelah selesai.

---

## 7. 🧪 Checklist Verifikasi Sebelum Menyelesaikan Tugas

Setiap agen AI yang memodifikasi repositori ini **WAJIB** menjalankan langkah verifikasi berikut di terminal:

```bash
# 1. Jalankan unit test
go test -count=1 ./...

# 2. Periksa kebersihan kode dari static analysis Go
go vet ./...

# 3. Pastikan entrypoint dapat dikompilasi tanpa error
go build ./cmd/bench
```

Jika salah satu perintah di atas gagal atau menghasilkan *warning*, perbaiki hingga seluruhnya lulus dengan status **Exit Code 0** sebelum menyerahkan hasil ke pengguna.
