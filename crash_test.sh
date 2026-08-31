#!/bin/bash
set -e

echo "==> Running ZeroTick Chaos & Crash Tests..."
cargo test --release -- --nocapture

echo "==> Simulating abrupt power loss during WORM flush..."
cargo run --release -- chaos ./data

echo "==> Verifying automated segment recovery..."
cargo run --release -- verify ./data

echo "==> All crash tests passed successfully."
