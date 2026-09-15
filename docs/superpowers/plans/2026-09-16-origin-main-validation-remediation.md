# Origin Main Health Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore `origin/main` to a 100% healthy, green state where all compilation, linting, tests, benchmarks, docs, and cargo-deny checks pass without errors or warnings.

**Architecture:** Revert the breaking Dependabot PR #23 (`arrow`/`parquet` 59 bump) which broke compatibility with `iceberg-rust`, configure Dependabot to pin Arrow/Parquet to major version 58 until upstream Iceberg is upgraded, fix `deny.toml` syntax for modern `cargo-deny` 0.18+, patch transitive security advisories via targeted lockfile updates, and add `lcov.info` to `.gitignore`.

**Tech Stack:** Rust 2024 edition, Apache Arrow (v58), Parquet (v58), Iceberg-Rust (v0.7/0.10 git rev), cargo-deny (v0.18+), Tokio, Clippy, cargo-llvm-cov.

**Spec:** Diagnostic review findings conducted during brainstorming spike (`origin/main` vs commit `37afadc`).

## Global Constraints

- Zero panic policy in production paths (`unwrap()` / `expect()` only in unit/integration tests).
- Zero compiler warnings or Clippy warnings under `-D warnings -W clippy::pedantic -A clippy::missing_errors_doc`.
- All workspace crates (`core`, `arrow-codec`, `storage`, `kafka-sink`, `starrocks-sink`, `otlp-receiver`, `noop-transformer`, root binary) must build and pass 100% of tests.
- Apache Iceberg integration in `storage` crate requires Apache Arrow 58 and Parquet 58 types until upstream `apache/iceberg-rust` supports Arrow 59.

---

### Task 1: Revert Breaking Dependabot PR #23 (Commit c65553e)

**Files:**
- Modify: `Cargo.toml:82-85`
- Modify: `Cargo.lock`

**Interfaces:**
- Consumes: None (reverts workspace `arrow` and `parquet` back to version `58.3.0`).
- Produces: Type compatibility between workspace `RecordBatch` and `iceberg` crate.

- [ ] **Step 1: Check git status and current HEAD commit**

Verify we are on `main` at `c65553e`:
```bash
git status
git log -n 1 --oneline
```
Expected: `c65553e Bump the arrow-iceberg group across 1 directory with 2 updates (#23)`

- [ ] **Step 2: Execute git revert for commit c65553e**

```bash
git revert --no-edit c65553ec43557ab461cd05dfcd56322d4521dbe8
```
Expected: Clean revert creating a new commit on `main`.

- [ ] **Step 3: Verify workspace compiles cleanly**

Run:
```bash
cargo check --workspace
```
Expected: Exit code 0, no compilation errors.

- [ ] **Step 4: Verify storage tests compile and pass**

Run:
```bash
cargo test -p storage
```
Expected: 22 passed, 0 failed.

---

### Task 2: Configure Dependabot to Prevent Premature Arrow/Parquet 59 Upgrades

**Files:**
- Modify: `.github/dependabot.yml:12-18`

**Interfaces:**
- Consumes: Dependabot package ecosystem configuration.
- Produces: Version constraint rules ignoring Arrow & Parquet `>= 59.0.0`.

- [ ] **Step 1: Edit .github/dependabot.yml**

Update the `arrow-iceberg` grouping or add an `ignore` block for Arrow and Parquet in `.github/dependabot.yml`:
```yaml
    ignore:
      - dependency-name: "arrow"
        versions: [">= 59.0.0"]
      - dependency-name: "parquet"
        versions: [">= 59.0.0"]
```

- [ ] **Step 2: Verify YAML syntax**

Run:
```bash
python3 -c "import yaml; yaml.safe_load(open('.github/dependabot.yml'))"
```
Expected: Exit code 0.

- [ ] **Step 3: Commit dependabot ignore rule**

```bash
git add .github/dependabot.yml
git commit -m "chore(dependabot): ignore arrow and parquet major version 59 until iceberg-rust supports it"
```

---

### Task 3: Fix deny.toml for Modern cargo-deny Compatibility

**Files:**
- Modify: `deny.toml:175-179`

**Interfaces:**
- Consumes: `deny.toml` configuration file.
- Produces: Valid configuration compatible with `cargo-deny` 0.18+.

- [ ] **Step 1: Verify current failure**

Run:
```bash
cargo deny check licenses
```
Expected: If `allow-workspace = false` is active, it errors with `error[unexpected-keys]: found 1 unexpected keys: ["allow-workspace"]`.

- [ ] **Step 2: Remove deprecated allow-workspace key**

In `deny.toml`, change:
```toml
# If true, workspace members are automatically allowed even when using deny-by-default
# This is useful for organizations that want to deny all external dependencies by default
# but allow their own workspace crates without having to explicitly list them
allow-workspace = false
```
to:
```toml
# If true, workspace members are automatically allowed even when using deny-by-default
# This is useful for organizations that want to deny all external dependencies by default
# but allow their own workspace crates without having to explicitly list them
# allow-workspace is deprecated in cargo-deny 0.18+
```

- [ ] **Step 3: Verify cargo deny parse and license check**

Run:
```bash
cargo deny check licenses
```
Expected: `licenses ok`.

- [ ] **Step 4: Commit deny.toml fix**

```bash
git add deny.toml
git commit -m "fix(security): remove deprecated allow-workspace key from deny.toml"
```

---

### Task 4: Resolve Transitive Security & Unsoundness Advisories

**Files:**
- Modify: `Cargo.lock`

**Interfaces:**
- Consumes: crates.io registry updates for `event-listener` and `rustls`.
- Produces: Zero security advisories in `cargo deny check advisories`.

- [ ] **Step 1: Verify advisories before update**

Run:
```bash
cargo deny check advisories
```
Expected: 2 errors (RUSTSEC-2026-0221 for `event-listener 5.4.1` and RUSTSEC-2026-0285 for `rustls 0.23.40`).

- [ ] **Step 2: Update vulnerable crates in Cargo.lock**

Run:
```bash
cargo update -p event-listener
cargo update -p rustls
```
Expected: `event-listener` upgraded to `>=5.4.2` and `rustls` upgraded to `>=0.23.45`.

- [ ] **Step 3: Verify zero advisories remaining**

Run:
```bash
cargo deny check advisories
```
Expected: `advisories ok`.

- [ ] **Step 4: Commit advisory fixes**

```bash
git add Cargo.lock
git commit -m "fix(deps): update event-listener and rustls to resolve security advisories"
```

---

### Task 5: Ignore lcov.info in .gitignore

**Files:**
- Modify: `.gitignore:30-31`

**Interfaces:**
- Consumes: Git repository status.
- Produces: Clean git status even after running `cargo llvm-cov` or `make coverage`.

- [ ] **Step 1: Add lcov.info to .gitignore**

Append `lcov.info` under `# Test Outputs & Logs`:
```gitignore
# Test Outputs & Logs
*.log
tests/e2e/receiver.log
lcov.info
```

- [ ] **Step 2: Verify git status is clean**

Run:
```bash
touch lcov.info
git status --porcelain
rm lcov.info
```
Expected: `lcov.info` does not appear as untracked.

- [ ] **Step 3: Commit .gitignore update**

```bash
git add .gitignore
git commit -m "chore: ignore lcov.info coverage output in gitignore"
```

---

### Task 6: Full Verification and Quality Gate Sweep

**Files:**
- Test: All workspace crates

**Interfaces:**
- Consumes: Whole codebase.
- Produces: Fully verified green status across all CI checks.

- [ ] **Step 1: Check code formatting**

Run:
```bash
cargo fmt --all -- --check
```
Expected: Exit code 0, 0 formatting errors.

- [ ] **Step 2: Run strict Pedantic Clippy**

Run:
```bash
cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc
```
Expected: Exit code 0, 0 warnings, 0 errors.

- [ ] **Step 3: Run full workspace unit & integration test suite**

Run:
```bash
cargo test --workspace
```
Expected: Exit code 0, all 124+ unit tests and integration tests pass.

- [ ] **Step 4: Run workspace benchmark smoke tests**

Run:
```bash
cargo bench --workspace -- --test
```
Expected: Exit code 0, all benchmarks execute and pass.

- [ ] **Step 5: Run documentation verification**

Run:
```bash
cargo doc --no-deps --workspace
```
Expected: Exit code 0, documentation builds cleanly.

- [ ] **Step 6: Run cargo deny checks**

Run:
```bash
cargo deny check advisories
cargo deny check licenses
```
Expected: Both exit with code 0 (`advisories ok`, `licenses ok`).
