//! Command-line interface for the WASM guest transform developer toolchain.

#![allow(clippy::print_stdout)]

use anyhow::Result;
use clap::{Parser, Subcommand};
use datalake_wasm_tool::{bench, tester, validator};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "datalake-wasm", about = "WASM transformer dev toolchain")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Validate that a WASM module implements C-ABI v1 correctly.
    Validate {
        /// Path to the compiled .wasm binary.
        path: PathBuf,
    },
    /// Run the immutability and conformance test suite.
    Test {
        /// Path to the compiled .wasm binary.
        path: PathBuf,
    },
    /// Run local latency and throughput benchmarks.
    Bench {
        /// Path to the compiled .wasm binary.
        path: PathBuf,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Validate { path } => {
            let bytes = std::fs::read(&path)?;
            validator::validate_wasm_bytes(&bytes)?;
            println!("✓ {} is a valid ABI v1 module", path.display());
        }
        Commands::Test { path } => {
            let bytes = std::fs::read(&path)?;
            tester::run_immutability_suite(&bytes)?;
        }
        Commands::Bench { path } => {
            let bytes = std::fs::read(&path)?;
            bench::run_benchmark_with_disclaimer(&bytes)?;
        }
    }
    Ok(())
}
