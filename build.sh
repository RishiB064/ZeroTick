#!/bin/bash
set -e

echo "==> Compiling ZeroTick Engine (Release Mode)..."
cargo build --release

echo "==> Initializing WORM storage directory..."
mkdir -p data

echo "==> Build successful."
echo "Execute the binary via: ./target/release/zerotick"
