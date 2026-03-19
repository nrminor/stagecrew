# Stagecrew

[![CI](https://github.com/nrminor/stagecrew/actions/workflows/ci.yml/badge.svg)](https://github.com/nrminor/stagecrew/actions/workflows/ci.yml)

Stagecrew is a disk usage management tool designed for shared HPC staging filesystems. It solves a common problem on research computing clusters: data accumulates on fast scratch or staging storage and never gets cleaned up, eventually filling the filesystem and causing problems for everyone.

Stagecrew enforces a removal-by-default policy. Files that haven't been modified within a configurable expiration period (default 90 days) are flagged for removal. Users can review pending removals through an interactive terminal interface, approve them, defer them to reset the clock, or permanently ignore specific paths. All actions are logged with timestamps and user identity for accountability.

## Installation

The easiest way to install stagecrew is with the install script:

```bash
curl -fsSL https://raw.githubusercontent.com/nrminor/stagecrew/main/INSTALL.sh | bash
```

This downloads a pre-built binary for your platform. If no binary is available, it falls back to building from source with cargo. The installer is aware of conda, mamba, and pixi environments and will install to the active environment's bin directory when appropriate.

You can also install from [crates.io](https://crates.io/crates/stagecrew):

```bash
cargo install stagecrew
```

Or clone and build locally:

```bash
git clone https://github.com/nrminor/stagecrew.git
cd stagecrew
cargo build --release
```

## Getting Started

Initialize stagecrew to create the configuration file and database:

```bash
stagecrew init
```

This creates a config file at `~/.config/stagecrew/config.toml` with a JSON schema reference for editor autocompletion. Edit it to add the paths you want to monitor:

```toml
tracked_paths = ["/scratch/myproject", "/staging/shared-data"]
expiration_days = 90
warning_days = 14
auto_remove = false
scan_interval_hours = 24
```

The `tracked_paths` array lists directories stagecrew will scan. Files within these directories that haven't been modified in `expiration_days` will be flagged. The `warning_days` setting controls when paths start showing up as "warning" in the interface. Setting `auto_remove` to true skips the approval step and removes expired paths automatically (use with caution).

Run an initial scan to populate the database:

```bash
stagecrew scan
```

Then launch the interactive interface:

```bash
stagecrew
```

## Using the TUI

The terminal interface shows all tracked directories with a live countdown timer at the top showing time until the next removal event. The main dashboard includes lifecycle statistics, a removal timeline sparkline, and a quota pie chart alongside the file browser.

Navigation uses vim-style keys: `j` and `k` move up and down, `g` jumps to the top, `G` to the bottom. Press `l` to drill into a directory and see individual files. Press `h` to go back up. `Tab` switches focus between the sidebar and main panel.

File management keybinds (from the main panel):

- `x` — toggle approval for removal (reversible)
- `r` — defer expiration (prompts for number of days)
- `i` — permanently ignore
- `I` — unignore (restore from ignored)
- `d` — delete file with confirmation
- `u` — undo last reversible action
- `Space` — toggle selection on current file
- `v` — enter visual selection mode
- `a` — select all entries
- `s` — cycle sort mode (due date, size, name, modified)
- `/` — search entries by name

Root and system management:

- `A` — add a new tracked path
- `X` — remove a tracked root (sidebar)
- `t` — set quota target for current root
- `T` — reset countdown timer for current root
- `F` — execute approved removals for current root
- `Y` — dry run (check if approved entries can be removed)
- `R` — refresh/rescan tracked paths

Views:

- `1` — main dashboard
- `2` — audit log
- `3` or `?` — help menu
- `E` — export audit log (from audit log view)

The TUI adapts to both light and dark terminal backgrounds.

## Running the Daemon

For automated management, run the daemon in the background:

```bash
stagecrew daemon
```

The daemon periodically scans tracked paths, transitions expired paths to pending (or approved if `auto_remove` is enabled), and removes any approved paths. It logs all actions to the audit trail and handles errors gracefully. Run it in a screen or tmux session, or set it up as a system service.

The daemon supports several flags for operational control:

```bash
stagecrew daemon --interval 12    # Override scan interval (hours)
stagecrew daemon --once           # Run one cycle and exit
stagecrew daemon --scan-only      # Scan without removing anything
stagecrew daemon --dry-run        # Report what would happen, change nothing
```

Verbosity can be controlled with `-v` (info), `-vv` (debug), or `-vvv` (trace), or with the `RUST_LOG` environment variable for precise per-module filtering.

## Configuration Management

Stagecrew provides a `config` subcommand for inspecting and managing configuration:

```bash
stagecrew config show       # Print effective configuration as TOML
stagecrew config path       # Print config file path
stagecrew config db-path    # Print database file path
stagecrew config log-path   # Print log file path
stagecrew config edit       # Open config in $VISUAL or $EDITOR
stagecrew config schema     # Print JSON schema for the config file
```

Per-root configuration is supported by placing a `stagecrew.toml` file in a tracked directory. Local configs can override `expiration_days`, `warning_days`, and `auto_remove` for that specific root.

## Shell Hook

Add a status check to your shell startup to get warnings about pending cleanups:

```bash
# In ~/.bashrc or ~/.zshrc
stagecrew status
```

This prints a one-line summary if there are paths needing attention, or stays silent if everything is clear.

## Audit Trail

Every action in stagecrew is recorded in an audit log with the user identity, timestamp, action type, affected path, and outcome. The audit log is viewable in the TUI (press `2`) and can be exported to JSONL or CSV format (press `E` in the audit log view).

The audit system tracks actions from both the TUI and the daemon, with structured events mirrored to the on-disk application log for complete accountability.

## Multi-User Setup

Stagecrew is designed for shared filesystems where multiple users need visibility into the same tracked paths. The database location can be configured explicitly in `config.toml` with the `database_path` setting, or it defaults to a `.stagecrew/stagecrew.db` file within the first tracked path's parent directory. This allows teams to share a single database.

The database uses SQLite with WAL mode for safe concurrent access. All actions record the username from the environment, so the audit log shows who approved or deferred each path.

## License

MIT License. See LICENSE file for details.
