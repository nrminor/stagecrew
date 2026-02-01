//! Stagecrew library for integration tests.
//!
//! This library exposes internal modules to enable integration testing of the
//! full workflow. The primary entry point is the binary in `main.rs`.
//!
//! # Stability
//!
//! This library is primarily for integration testing. The API is unstable
//! and may change between minor versions until 1.0.0. For stable usage,
//! use the CLI binary.

pub mod audit;
pub mod config;
pub mod db;
pub mod error;
pub mod removal;
pub mod scanner;

// TUI, CLI, and daemon are not exposed since they're binary-specific.
