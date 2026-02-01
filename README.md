# Stagecrew

[![CI](https://github.com/nrminor/stagecrew/actions/workflows/ci.yml/badge.svg)](https://github.com/nrminor/stagecrew/actions/workflows/ci.yml)

Stagecrew is a disk usage management tool designed for shared HPC staging filesystems. It solves a common problem on research computing clusters: data accumulates on fast scratch or staging storage and never gets cleaned up, eventually filling the filesystem and causing problems for everyone.

Stagecrew enforces a removal-by-default policy. Files that haven't been modified within a configurable expiration period (default 90 days) are flagged for removal. Users can review pending removals through an interactive terminal interface, approve them, defer them to reset the clock, or permanently ignore specific paths. All actions are logged with timestamps and user identity for accountability.

## Installation

The easiest way to install stagecrew is with the install script:

```bash
curl -fsSL https://raw.githubusercontent.com/nrminor/stagecrew/main/INSTALL.sh | bash
```

This downloads a pre-built binary for your platform. If no binary is available, it falls back to building from source with cargo.

You can also install directly with cargo:

```bash
cargo install --git https://github.com/nrminor/stagecrew.git
```

Or clone and build locally:

```bash
git clone https://github.com/nrminor/stagecrew.git
cd stagecrew
cargo build --release
```

## Getting Started

Initialize stagecrew to create the configuration file and database:

```bas
stagecrew init
```

This creates a config file at `~/.config/stagecrew/config.toml`. Edit it to add the paths you want to monitor:

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

The terminal interface shows all tracked directories sorted by urgency. Paths closest to expiration appear at the top. Color coding helps identify status at a glance: red for overdue or pending removal, yellow for paths within the warning period, green for safe paths with plenty of time remaining, and gray for ignored paths.

Navigation uses vim-style keys: `j` and `k` move up and down, `g` jumps to the top, `G` to the bottom. Press `Enter` or `l` to drill into a directory and see individual files sorted by size. Press `h`, `q`, or `Esc` to go back.

To manage paths, press `x` to approve a path for removal, `d` to defer it (which resets the expiration clock), or `i` to permanently ignore it. Each action requires confirmation. Press `p` to see only paths pending approval, `a` to view the audit log of past actions, and `?` for help.

## Running the Daemon

For automated management, run the daemon in the background:

```bash
stagecrew daemon
```

The daemon periodically scans tracked paths, transitions expired paths to pending (or approved if `auto_remove` is enabled), and removes any approved paths. It logs activity and handles errors gracefully. Run it in a screen or tmux session, or set it up as a system service.

## Shell Hook

Add a status check to your shell startup to get warnings about pending cleanups:

```bash
# In ~/.bashrc or ~/.zshrc
stagecrew status
```

This prints a one-line summary if there are paths needing attention, or stays silent if everything is clear.

## Multi-User Setup

Stagecrew is designed for shared filesystems where multiple users need visibility into the same tracked paths. The database location can be configured explicitly in `config.toml` with the `database_path` setting, or it defaults to a `.stagecrew/stagecrew.db` file within the first tracked path's parent directory. This allows teams to share a single database.

The database uses SQLite with WAL mode for safe concurrent access. All actions record the username from the environment, so the audit log shows who approved or deferred each path.

## License

MIT License. See LICENSE file for details.
