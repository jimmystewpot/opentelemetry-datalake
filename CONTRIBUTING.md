# Contributing to opentelemetry-datalake

First off, thank you for considering contributing to `opentelemetry-datalake`! We appreciate your help in building a high-performance, reliable telemetry pipeline.

This guide will help you get started with the development process and explain our engineering standards.

## Table of Contents
- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Engineering Standards](#engineering-standards)
- [Development Workflow](#development-workflow)
- [Pull Request Process](#pull-request-process)
- [Reporting Issues](#reporting-issues)

---

## Code of Conduct

This project adheres to the [Rust Code of Conduct](https://www.rust-lang.org/policies/code-of-conduct). By participating, you are expected to uphold this code.

## Getting Started

### Prerequisites
To build and test this project, you will need:
- The latest stable version of **Rust** (installed via [rustup](https://rustup.rs/)).
- **Make** for running automated tasks.
- (Optional) **Docker** for running integration tests (if applicable).

### Setup
1. Fork the repository on GitHub.
2. Clone your fork locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/opentelemetry-datalake.git
   cd opentelemetry-datalake
   ```
3. Create a new branch for your work:
   ```bash
   git checkout -b feature/my-awesome-feature
   ```

## Engineering Standards

We hold `opentelemetry-datalake` to high engineering standards to ensure it remains the most efficient OTLP pipeline available.

### Zero-Panic Policy
We strive for a **zero-panic** production code path.
- Avoid `.unwrap()`, `.expect()`, and indexing that could fail (`array[i]`).
- Use `Result` and `Option` with proper error handling.
- Use the `?` operator to propagate errors to the top-level pipeline supervisor.
- For application-level errors, use the patterns defined in `crates/core/src/error.rs`.

### Performance & Memory
- **Zero-Cost Abstractions:** Prefer static dispatch (generics) over dynamic dispatch (`dyn Trait`) where possible.
- **Lock-Free Concurrency:** Use message passing and channels (MPSC) as described in [ARCHITECTURE.md](docs/ARCHITECTURE.md) instead of shared mutable state with heavy locking.
- **Arrow-First:** All telemetry data should be handled using Apache Arrow `RecordBatch`es for memory efficiency and vectorized processing.

### Documentation
- All public traits, structs, and functions should be documented with `///` comments.
- Major architectural changes must be reflected in the [ARCHITECTURE.md](docs/ARCHITECTURE.md) or other files in the `docs/` directory.

## Development Workflow

We use a root `Makefile` to simplify common development tasks.

### 1. Formatting
We use `rustfmt` to keep code style consistent.
```bash
cargo fmt
```

### 2. Linting
We use `clippy` with strict pedantic rules. Your code must pass all clippy checks.
```bash
make clippy
```

### 3. Testing
Ensure the full test suite passes:
```bash
make test
```

### 4. Benchmarking
If you are making performance-sensitive changes, run the micro-benchmarks:
```bash
make bench
```

### 5. All Gates
Run all quality gates at once:
```bash
make all
```

## Pull Request Process

1. **Keep it Focused:** Try to keep PRs small and focused on a single change or feature.
2. **Update Tests:** Include unit tests or integration tests for any new functionality or bug fixes.
3. **Commit Messages:** We recommend clear, descriptive commit messages.
4. **CI Verification:** Ensure that GitHub Actions pass for your PR.
5. **Review:** Be prepared to iterate on feedback from the maintainers.

## Reporting Issues

- **Bug Reports:** Use GitHub Issues. Please include your OS, Rust version, and a minimal reproducible example.
- **Security Issues:** For security vulnerabilities, please refer to our [SECURITY.md](SECURITY.md) and do **not** open a public issue.
- **Feature Requests:** Open an issue to discuss major features before implementation.

---

*Happy Hacking!*
