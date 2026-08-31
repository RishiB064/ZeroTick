# ZeroTick

## Core System Features
* **High-Throughput TCP Ingestion:** A bounded `sync_channel` absorbs high-frequency micro-batches to provide backpressure between network handlers and persistent storage.
* **Gorilla Bit-Packing Compression:** Compresses time-series ticks using bit-level Delta-of-Delta timestamp encoding and XOR floating-point price encoding.
* **O(1) Quantitative Analytics:** Computes Welford's streaming variance, Z-scores, Bollinger Bands, and maximum drawdown in a single pass using a fixed 72-byte stack footprint.
* **POSIX Lock-Free Concurrency:** Utilizes `std::os::unix::fs::FileExt::read_at` to map queries directly to absolute disk byte offsets, eliminating read/write locking contention.
* **Automated Chaos Auto-Healing:** Simulates torn writes by injecting garbage bytes, then executes a modulo-based frame boundary repair to truncate corruption cleanly.
* **Native HTTP/SVG Multiplexer:** Dynamically routes Port 8080 traffic by parsing the first 4 bytes. Automatically serves raw, generated SVG charts to `GET` requests without HTTP frameworks.

## Hackathon Requirements
* **Single File:** The default Cargo `src/` directory tree was explicitly destroyed. The entire architecture—protocol, storage, quantitative engine, network gateway, ingest client, TUI, and chaos harness—is flattened into a single `engine.rs` root file.
* **Reproducible Build:** Built exclusively via the standard Rust toolchain. The `Cargo.toml` contains an empty `[dependencies]` block, guaranteeing zero third-party runtime supply-chain risk.
* **Package Killer:** Bypassed the popular `itoa` crate by implementing Rust 1.98's `core::fmt::NumBuffer` to execute zero-allocation integer formatting within the ANSI terminal dashboard.
* **STDLIB Log:** This ledger documents the exclusive reliance on `std::net`, `std::fs`, `std::sync`, and POSIX FFI bindings to replace the standard Rust ecosystem.

## The Zero-Dependency Matrix

| Ecosystem Crate | Zero-Dependency `std` Implementation |
| :--- | :--- |
| `tokio` / `axum` | `std::net::TcpListener` paired with raw byte-sniffing for dual TCP/HTTP routing. |
| `serde` / `bincode` | `#[repr(C)]` structs serialized directly via `to_le_bytes()` and `from_le_bytes()`. |
| `itoa` | `core::fmt::NumBuffer` executing zero-allocation formatting in the UI render loop. |
| `clap` / `structopt` | `std::env::args()` parsing with manual positional `match` routing. |
| `crossbeam-channel` | `std::sync::mpsc::sync_channel` enforcing fixed bounded backpressure. |
| `rand` | Hand-rolled Linear Congruential Generator (LCG) inside the synthetic ingestion client. |
| `askama` / `tera` | Native `format!` macros injecting IEEE-754 floats directly into raw SVG XML strings. |
| `gorilla` | Custom bit-reader/writer implementation operating directly on `Vec<u8>`. |

## Mechanical Sympathy (Navigating External Crates)

* **Bypassing `tokio` (The Multiplexer):** Instead of an async runtime to handle multiple protocols, the server directly sniffs the socket buffer's first 4 bytes. `b"GET "` manually triggers a string-parsing loop for HTTP routes, while `0x01` routes to the high-speed binary ingestion loop.
* **Eliminating Serialization Crates (Syscall Bottlenecks):** Naively writing bits individually to bypass serialization triggered overwhelming OS syscalls. The custom bit-writer was engineered to buffer into `u8` registers and execute `write_all()` on complete arrays, bridging the gap between bit-level compression and block-level storage.
* **Bypassing Database Engines (The `fsync` Death Spiral):** Forcing physical disk syncs stalled the storage worker at 250,000 ticks per second. Rather than pulling in an embedded database crate like RocksDB, a 256 KB Write-Once-Read-Many (WORM) buffer was built via `BufWriter`, absorbing micro-batches and syncing only when the OS page cache hits optimal capacity.
* **Removing Statistical Crates (Absorbing Floor Physics):** Simulating test data without `rand_distr` resulted in an absorbing floor effect where synthetic prices drifted to zero. A custom Ornstein-Uhlenbeck mean-reverting drift equation with reflective boundaries was written using pure IEEE-754 floating-point math to simulate institutional price action.
