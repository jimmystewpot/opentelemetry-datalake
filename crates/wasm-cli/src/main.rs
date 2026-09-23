//! Command-line interface for the WASM guest transform developer toolchain.

#![allow(clippy::print_stdout)]

use anyhow::Result;
use clap::{Parser, Subcommand};
use datalake_wasm_tool::{bench, helpers, tester, validator};
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
        /// Signal type to test: 'logs' (default), 'metrics', or 'traces'.
        #[arg(short, long, default_value = "logs")]
        signal: String,
        /// Optional path to a JSON configuration file, or inline JSON string for module init.
        #[arg(short, long)]
        config: Option<String>,
    },
    /// Run local latency and throughput benchmarks.
    Bench {
        /// Path to the compiled .wasm binary.
        path: PathBuf,
        /// Signal type to benchmark: 'logs' (default), 'metrics', or 'traces'.
        #[arg(short, long, default_value = "logs")]
        signal: String,
        /// Optional path to a JSON configuration file, or inline JSON string for module init.
        #[arg(short, long)]
        config: Option<String>,
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
        Commands::Test {
            path,
            signal,
            config,
        } => {
            let bytes = std::fs::read(&path)?;
            let signal_code = helpers::parse_signal(&signal)?;
            let config_payload = helpers::resolve_config_payload(config.as_deref(), signal_code)?;
            tester::run_immutability_suite_with_options(
                &bytes,
                signal_code,
                config_payload.as_deref(),
            )?;
        }
        Commands::Bench {
            path,
            signal,
            config,
        } => {
            let bytes = std::fs::read(&path)?;
            let signal_code = helpers::parse_signal(&signal)?;
            let config_payload = helpers::resolve_config_payload(config.as_deref(), signal_code)?;
            bench::run_benchmark_with_options(&bytes, signal_code, config_payload.as_deref())?;
        }
    }
    Ok(())
}
