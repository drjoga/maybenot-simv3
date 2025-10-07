# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the **Maybenot** workspace - a framework for traffic analysis defenses that hide patterns in encrypted communication. Maybenot works by running probabilistic state machines within encrypted protocols (TLS, QUIC, WireGuard, Tor) to generate cover traffic and introduce delays that obscure communication patterns.

The project is currently on the `netsimv3` branch, with `main` being the primary development branch.

## Architecture

### Core Design
- **Framework**: Integrates with encrypted protocols and runs state machines that trigger padding and blocking actions
- **State Machines**: Lightweight probabilistic machines that determine defense actions based on network events
- **Simulator**: Evaluates defenses against network traces without real deployments
- **Integration Flow**: Events (packet sent/received) → Framework → Actions (send padding, block traffic) → Integration executes

### Workspace Crates

The workspace follows a topologically sorted dependency structure:

1. **maybenot** (`crates/maybenot/`) - Core framework library
   - Main types: `Framework`, `Machine`, `TriggerEvent`, `TriggerAction`
   - State machine execution engine
   - Machines are serialized/deserialized using base64-encoded strings

2. **maybenot-ffi** (`crates/maybenot-ffi/`) - C FFI wrapper for core framework
   - Enables integration with C/C++ projects like WireGuard

3. **maybenot-simulator** (`crates/maybenot-simulator/`) - v2 simulator
   - Simulates defenses against network traces
   - Key limitation: Cannot properly simulate blocking actions (see README)
   - Uses fixed static delay network model

4. **maybenot-simulatorv3** (`crates/maybenot-simulatorv3/`) - v3 simulator (NEW)
   - Fast and flexible simulator with configurable network topologies
   - Supports parallel simulation runs for trace links
   - Key modules: `integration`, `topology`, `linktrace`, `traffic_parse`
   - Binary: `linktrace_util` for trace utilities

5. **maybenot-machines** (`crates/maybenot-machines/`) - Pre-built defense machines
   - Hand-implemented machines based on academic literature
   - Common defenses for website fingerprinting

6. **maybenot-gen** (`crates/maybenot-gen/`) - Machine generation library
   - Generates defense machines programmatically

7. **maybenot-cli** (`crates/maybenot-cli/`) - Command-line tool
   - CLI for creating and working with defenses

### Key Concepts

- **Events**: Network activity (packet sent/received) reported to the framework
- **Actions**: Defense behaviors (send padding, block traffic, update timers) scheduled by machines
- **Machines**: Serialized as base64 strings (e.g., `"02eNpjYEAHjOgCAAA0AAI="`)
- **Limits**: Max fractions prevent machines from excessive overhead (can be bypassed with budgets)

## Development Commands

### Build and Test
```bash
# Build entire workspace
cargo build

# Build release (optimized)
cargo b -r

# Run all tests
cargo test
cargo t  # shorthand

# Test specific crate
cargo test -p maybenot
cargo test -p maybenot-simulatorv3

# Test specific integration test (simulatorv3)
cargo test --test trace_setup
cargo test --test v3test
```

### Code Quality (REQUIRED before committing)
```bash
# Run linter (REQUIRED)
cargo clippy --all-targets

# Format code (REQUIRED)
cargo fmt

# Check formatting without modifying
cargo fmt --check

# Full quality check sequence
cargo clippy --all-targets && cargo fmt && cargo t && cargo b -r
```

### Documentation and Debugging
```bash
# Build documentation
cargo doc

# Run with debug logging (simulator)
RUST_LOG=debug cargo test test_bypass_machine

# Run specific binary
cargo run --bin linktrace_util
```

## Important Testing Notes

### Simulatorv3 Test Dependencies
When checking out or testing `maybenot-simulatorv3`, you MUST run trace setup first:
```bash
cargo test --test trace_setup  # Run FIRST
cargo test                      # Then run other tests
```

This is explicitly mentioned in recent commit messages.

## Code Standards

### Linting Rules
- Warnings are denied (build fails on warnings)
- `unsafe_code` triggers warnings (must be justified)
- No non-ASCII identifiers
- Follow Rust 2018 idioms
- Document all unsafe blocks

### MSRV
- Minimum Supported Rust Version: **1.85.0**
- Follows Arti (Tor) project MSRV policy
- Check `.github/workflows/build-and-test.yml` for CI Rust versions

### Edition
- Uses Rust edition **2024**

## Real-World Usage

Maybenot is used in production by:
- **Mullvad VPN** - Implemented in their DAITA feature
- Integration: [Mullvad's WireGuard Go fork](https://github.com/mullvad/wireguard-go/)

## Debugging

The simulators support rich debug output:
```bash
RUST_LOG=debug cargo test <test_name>
```

## Academic Context

Based on Tor's Circuit Padding Framework (2019), which builds on WTF-PAD (2016) and Adaptive Padding (2006). See README for paper references and related work.
