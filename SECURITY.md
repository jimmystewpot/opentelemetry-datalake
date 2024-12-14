# Security Policy

We take the security of `opentelemetry-datalake` seriously. This document outlines our policy for reporting vulnerabilities and our commitment to maintaining a secure and reliable high-performance telemetry pipeline.

## Supported Versions

We currently support the following versions with security updates:

| Version | Supported          |
| ------- | ------------------ |
| Main    | :white_check_mark: |
| < 0.1   | :x:                |

As this project is in active development, we recommend always using the latest release or the `main` branch for the most up-to-date security patches.

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

If you believe you have found a security vulnerability, please report it privately through one of the following methods:

### 1. GitHub Private Vulnerability Reporting (Preferred)
The preferred way to report a vulnerability is to use the **"Report a vulnerability"** button on the **Security** tab of this repository. This creates a private communication channel between you and the maintainers.

### 2. Email
If you are unable to use the GitHub reporting workflow, please reach out to the project maintainers. (Note: Replace this with a specific email if one is designated for security).

### What to Include
To help us triage your report quickly, please include:
- A descriptive title.
- The affected version(s) or commit hash.
- A summary of the vulnerability and its potential impact.
- Step-by-step instructions to reproduce the issue (or a Proof of Concept).
- Any suggested mitigations.

## Our Process

Once a report is received, we will:
1. **Acknowledge:** Confirm receipt of the report within 48-72 hours.
2. **Triage:** Investigate the issue and determine its severity.
3. **Fix:** Develop a patch for all supported versions.
4. **Disclose:** Once a fix is ready and released, we will publish a Security Advisory to inform the community.

We ask that you keep the vulnerability confidential until we have had a chance to release a fix.

## Security Standards & Best Practices

`opentelemetry-datalake` is engineered for maximum reliability and performance:

- **Zero-Panic Policy:** We avoid `unwrap()`, `expect()`, and other panicking operations in production code paths to ensure the pipeline remains stable under all input conditions.
- **Memory Safety:** Leveraging Rust's ownership model and the Apache Arrow format to ensure efficient and safe memory management.
- **Dependency Auditing:** We regularly audit our dependencies for known vulnerabilities using tools like `cargo-audit`.
- **Automated Scanning:** Our CI pipeline includes static analysis and linting (Clippy) to catch common security pitfalls early.

## Security Model

`opentelemetry-datalake` is an infrastructure component. While we prioritize confidentiality and integrity, availability depends on the environment in which it is deployed. Users are responsible for:
- Implementing appropriate network security and access controls.
- Providing authentication and authorization for OTLP endpoints if exposed to untrusted networks.
- Managing rate limits and resource quotas to prevent Denial of Service (DoS) attacks.
