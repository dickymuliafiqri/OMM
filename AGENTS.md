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
6. **Leaderboard Berkeadilan (Peak Score & Cost Efficiency):**
   Peringkat ditentukan oleh **Skor Tertinggi** dan **Efisiensi Biaya Riil** (akumulasi token di seluruh percobaan).

---

## 2. 🗺️ Peta Modul & Struktur Repositori

```text
OMM/
├── cmd/
│   ├── bot/
│   │   └── main.go                 # Entrypoint: Inisialisasi DB, worker pool, health server, dan Telegram bot
│   └── cli/
│       └── main.go                 # Entrypoint: Tool CLI SWE-bench & verifikasi F2P lokal
├── internal/
│   ├── ai/                         # Klien AI, ekstraktor kode, dan prompt baku
│   │   ├── client.go               # Universal OpenAI/OpenRouter client, multi-turn Chat API
│   │   ├── extractor.go            # Pembersih markdown & ekstraksi blok kode Go
│   │   └── prompt.go               # Prompt standar evaluasi
│   ├── swe/                        # Core SWE-bench engine & evaluator
│   │   ├── task.go                 # Definisi Task, Tier, EvalResult, LadderReport
│   │   ├── registry.go             # Registry task kurasi & fungsi default ladder
│   │   ├── evaluator.go            # Sandboxed test runner (go test -race) & F2P validator
│   │   ├── prompt.go               # Prompt builder (Issue Markdown + Broken Code) & SWE extractor
│   │   └── tasks/                  # 8 implementasi kasus uji bug riil
│   │       ├── t1_nil_pointer.go   # Tier 1: Nil pointer dereference pada session cache
│   │       ├── t1_map_race.go      # Tier 1: Concurrent map data race
│   │       ├── t2_goroutine_leak.go# Tier 2: Kebocoran goroutine & unstopped ticker
│   │       ├── t2_channel_block.go # Tier 2: Goroutine hang pada unbuffered channel
│   │       ├── t3_deadlock.go      # Tier 3: Deadlock transfer saldo (lock inversion)
│   │       ├── t3_ctx_propagate.go # Tier 3: Kegagalan propagasi pembatalan context
│   │       ├── t4_atomic_cas.go    # Tier 4: Race condition & stale read lock-free queue
│   │       └── t4_fsm_race.go      # Tier 4: Concurrent order state machine race
│   ├── bot/                        # Lapisan presentasi & interaksi Telegram
│   │   ├── bot.go                  # Router pesan Telegram & lifecycle bot
│   │   ├── state.go                # In-memory FSM session manager (TTL 15 menit)
│   │   ├── keyboards.go            # Definisi InlineKeyboardMarkup
│   │   └── views.go                # Formatter kartu skor ladder & leaderboard
│   ├── config/                     # Parser variabel lingkungan (.env)
│   ├── health/                     # HTTP server observabilitas internal (:8080)
│   ├── pricing/                    # Kalkulasi tarif token & sinkronisasi otomatis OpenRouter
│   ├── queue/                      # Worker pool, job deduplikasi per user, progress throttler
│   ├── sandbox/                    # Lapisan isolasi keamanan eksekusi
│   │   ├── ast_guard.go            # Pemindaian statis AST: blokir RCE (os/exec, syscall, unsafe)
│   │   ├── port.go                 # Alokasi dinamis port bebas (:0)
│   │   └── runner.go               # Runner ephemeral
│   └── storage/                    # Lapisan persistensi data
│       ├── turso.go                # Inisialisasi koneksi Turso LibSQL (driver database/sql)
│       ├── repository.go           # Interface kontrak CRUD database (termasuk SWE task runs)
│       └── sqlite_repo.go          # Implementasi query SQLite/LibSQL & analitik leaderboard
└── pkg/
    └── logger/                     # Structured 1-line logger dengan masking rahasia otomatis
```

---

## 3. 📐 Rubrik Penilaian 4-Tier Ladder (Total: 100 Pts)

Evaluasi SWE-bench OMM mengukur kapabilitas penyelesaian bug konkurensi dan sistem Go riil (*brownfield bug-fixing*) melalui **4-Tier Engineering Ladder**:

| Tier | Tingkat Kesulitan | Bobot Poin | Fokus Diagnostik & Karakteristik Bug | Contoh Task |
|:---|:---|:---:|:---|:---|
| **Tier 1** | **Junior** | **20 pts** | Bug logika fundamental, nil pointer dereference, map data race sederhana, slice bounds | Session cache nil check, concurrent map safety |
| **Tier 2** | **Mid-Level** | **25 pts** | Manajemen lifecycle goroutine, kebocoran goroutine, unbuffered channel hang, unstopped ticker | Background worker leak, unbuffered channel block |
| **Tier 3** | **Senior** | **30 pts** | Deadlock transfer antar akun (lock inversion AB-BA), context cancellation propagation failure | Balance transfer deadlock, fan-out aggregator context |
| **Tier 4** | **Staff** | **25 pts** | Lock-free concurrency, ABA/stale read pada atomic CAS, concurrent state machine race | Lock-free atomic CAS queue, order FSM double-transition |

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

Fitur ini terletak pada [`internal/ai/client.go`](file:///C:/Users/Dicky%20Mulia%20Fiqri/go/src/github.com/dickymuliafiqri/OMM/internal/ai/client.go), [`internal/swe/evaluator.go`](file:///C:/Users/Dicky%20Mulia%20Fiqri/go/src/github.com/dickymuliafiqri/OMM/internal/swe/evaluator.go), dan [`internal/queue/pool.go`](file:///C:/Users/Dicky%20Mulia%20Fiqri/go/src/github.com/dickymuliafiqri/OMM/internal/queue/pool.go).

### Prinsip Operasional:
1. **Full-File Replacement:**
   * Model mengembalikan kode Go utuh (`package ...`) di dalam blok ` ```go ... ``` `. Model 8B–31B memiliki tingkat keberhasilan kompilasi dan perbaikan di atas 85% ketika menghasilkan file utuh dibandingkan diff/patch format.
2. **Umpan Balik Kompilator & Test Runner Riil:**
   * Jika evaluasi gagal (`go test -race` error, panic, data race, atau timeout), output pengujian mentah disertakan dalam prompt perbaikan:
     ```text
     Your fix failed the test suite with the following output (go test -race):
     ```
     [test failure output / race stack trace]
     ```
     Please analyze the failure, fix the bugs, and return the COMPLETE updated Go code inside a single ```go ... ``` block.
     ```
3. **Akumulasi Token & Biaya:**
   * Token input dan output dari setiap percobaan dijumlahkan:
     `totalPromptTokens += promptTok`
     `totalCompletionTokens += compTok`
     `totalTokens += totTok`
   * Memastikan biaya yang tercatat di database mencerminkan konsumsi token yang sesungguhnya di seluruh putaran self-healing.

---

## 5. 🛠️ Aturan Pembuatan & Kurasi Task Baru (SWE-bench Tasks)

Setiap task di [`internal/swe/tasks/`](file:///C:/Users/Dicky%20Mulia%20Fiqri/go/src/github.com/dickymuliafiqri/OMM/internal/swe/tasks/) wajib mematuhi protokol berikut:

1. **Fail-to-Pass (F2P) Wajib:**
   * `BrokenCode` **wajib gagal** saat dijalankan terhadap `TestSuite`.
   * `ReferenceSolution` **wajib lolos 100%** tanpa error dan **zero data race** (`-race`).
   * Verifikasi ini diuji secara otomatis pada unit test [`tasks_test.go`](file:///C:/Users/Dicky%20Mulia%20Fiqri/go/src/github.com/dickymuliafiqri/OMM/internal/swe/tasks/tasks_test.go).
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
   * Setiap proses dijalankan di direktori sementara unik (`omm-swe-*`).
   * Selalu panggil `defer os.RemoveAll(tempDir)` untuk mencegah kebocoran disk VPS.
3. **Strict Timeout & Resource Controls:**
   * Setiap task dieksekusi dengan timeout ketat (`context.WithTimeout`) untuk mencegah goroutine hang atau deadlock tak berujung.
4. **Penanganan Context & Streaming AI:**
   * Selalu baca seluruh body HTTP (`resp.Body`) sebelum context ditutup.

---

## 7. 🏆 Aturan Leaderboard & Ranking

Logika leaderboard diatur di [`internal/storage/sqlite_repo.go`](file:///C:/Users/Dicky%20Mulia%20Fiqri/go/src/github.com/dickymuliafiqri/OMM/internal/storage/sqlite_repo.go):

1. **Pengurutan Berdasarkan Skor Tertinggi (*Highest/Peak Score*):**
   * Leaderboard diurutkan berdasarkan `MAX(total_score) DESC`, bukan `AVG(total_score)`.
2. **Metrik Efisiensi Biaya:**
   * Menampilkan estimasi biaya per run dan total token rata-rata (`prompt + completion`).
   * Tiering biaya: *Sangat Ekonomis* (< $0.001), *Ekonomis* (< $0.005), *Standar* (< $0.02), *Premium* (≥ $0.02).
3. **Max Tier Achieved:**
   * Menyimpan tingkat kesulitan tertinggi yang diselesaikan model (Tier 1–4).

---

## 8. 🧪 Checklist Verifikasi Sebelum Menyelesaikan Tugas

Setiap agen AI yang memodifikasi repositori ini **WAJIB** menjalankan langkah verifikasi berikut di terminal:

```bash
# 1. Jalankan seluruh unit test dengan race detector
go test -count=1 ./...

# 2. Periksa kebersihan kode dari static analysis Go
go vet ./...

# 3. Pastikan kedua entrypoint dapat dikompilasi tanpa error
go build ./cmd/bot ./cmd/cli
```

Jika salah satu perintah di atas gagal atau menghasilkan *warning*, perbaiki hingga seluruhnya lulus dengan status **Exit Code 0** sebelum menyerahkan hasil ke pengguna.
