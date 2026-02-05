# Stagecrew project justfile
# All repeating commands should be recipes here.
# Agents MUST read and use these recipes.

# Default recipe: show available commands
default:
    @just --list

# === Development Workflow ===

# Run all pre-commit checks (required before committing)
check: fmt-check lint test doc-check
    @echo "✅ All checks passed"

# Run checks on all files (required before pushing)
check-all: fmt-check lint-all test-all doc-check
    @echo "✅ All checks passed on full codebase"

# === Formatting ===

# Check formatting without modifying files
fmt-check:
    cargo fmt --all -- --check

# Apply formatting fixes
fmt:
    cargo fmt --all

# === Linting ===

# Run clippy with deny warnings (on changed files via cargo check)
lint:
    cargo clippy --all-targets --all-features -- -D warnings

# Run clippy on all files
lint-all:
    cargo clippy --all-targets --all-features -- -D warnings

# Lint shell scripts with shellcheck
lint-shell:
    shellcheck INSTALL.sh

# === Testing ===

# Run tests with nextest (--no-tests=pass allows empty test suites)
test:
    cargo nextest run --all-features --no-tests=pass

# Run all tests including ignored
test-all:
    cargo nextest run --all-features --run-ignored all --no-tests=pass

# Run tests with verbose output
test-verbose:
    cargo nextest run --all-features --no-capture --no-tests=pass

# === Building ===

# Build debug binary
build:
    cargo build

alias b := build

# Build release binary
build-release:
    cargo build --release

alias r := build-release

# Install a release binary on the system $PATH
install:
    cargo install --path=.

alias i := install

# Check compilation without building
check-compile:
    cargo check --all-targets --all-features

# === jj Workflow ===
# Since jj bypasses git hooks, use these recipes for enforcement.

# Prepare a commit: run all checks, then show status
prepare-commit: check
    @echo ""
    @echo "Ready to commit. Run: jj commit -m 'your message'"
    @jj status

# Prepare for push: run full checks
prepare-push: check-all
    @echo ""
    @echo "Ready to push. Run: jj git push"

# Show current jj status
status:
    jj status

# Show jj log
log:
    jj log

# === Utility ===

# Clean build artifacts
clean:
    cargo clean

# Update dependencies
update:
    cargo update

# === Documentation ===

# Check that documentation builds without errors
doc-check:
    cargo doc --no-deps --document-private-items

# Generate and open documentation
doc:
    cargo doc --no-deps --open

# Run the application (TUI mode)
run:
    cargo run

# Run the daemon
run-daemon:
    cargo run -- daemon

# Run status check (for shell hook)
run-status:
    cargo run -- status

# Initialize configuration
init:
    cargo run -- init

# Count source lines of code (excluding blanks and comments)
sloc:
    @tokei --types=Rust --compact
