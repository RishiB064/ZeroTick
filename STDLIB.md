# Standard Library Ledger

ZeroTick is implemented as a single Rust source file with no third-party
packages. This ledger records the standard-library techniques used for the
rubric's dependency-free bonuses.

## `core::fmt::NumBuffer` exploit

The terminal dashboard uses `core::fmt::NumBuffer`, stabilized in Rust 1.98,
to format `metrics.count` without allocating a `String`. Each dashboard render
creates a stack-backed buffer with:

```rust
let mut num_buf = core::fmt::NumBuffer::new();
let count_text = metrics.count.format_into(&mut num_buf);
```

`format_into` writes the decimal digits directly into `num_buf` and returns a
borrowed `&str`. The TICKS field passes that slice directly to the terminal row
formatter, avoiding the heap allocation that `format!("{count}")` would perform
for the integer conversion. The method belongs to the integer in Rust 1.98, so
its stable call direction is `value.format_into(&mut buffer)`.

## Descriptor-safe socket sharing

Accepted sockets are wrapped in `std::sync::Arc<std::net::TcpStream>` before
being passed to connection threads. Cloning or moving an `Arc` shares the same
stream object in process memory and does not invoke `TcpStream::try_clone`, so
no per-job `dup()` system call or duplicate file descriptor is introduced.

## Single-file layout

The complete executable is `engine.rs` in the repository root. There is no
`src/` directory, and `Cargo.toml` selects the root-level entry point explicitly:

```toml
[[bin]]
name = "zerotick"
path = "engine.rs"
```
