# Agent Instructions for Stagecrew

This document defines mandatory conventions for AI agents working on this project. Violations of these rules will result in failed commits and wasted iterations.

## Project Overview

Stagecrew is a disk usage management tool for shared HPC staging filesystems. It solves the problem of data accumulation on temporary storage by enforcing a removal-by-default policy with configurable expiration periods.

### Core Concepts

- **Tracked paths**: Directories on a shared CephFS mount that stagecrew monitors
- **Expiration**: Files expire 90 days after last modification (configurable)
- **Approval workflow**: By default, files require explicit approval before removal; auto-remove is opt-in
- **States**: Paths can be `tracked`, `pending` (awaiting approval), `approved`, `deferred` (clock reset), `ignored` (permanent exemption), `removed`, or `blocked` (removal failed)
- **Audit trail**: All actions (approvals, deferrals, removals) are logged with user identity and timestamp

### Components

- **Scanner**: Walks filesystem trees using jwalk, collects metadata (size, mtime), stores in SQLite
- **Daemon**: Background process that runs periodic scans and executes approved removals
- **TUI**: Ratatui-based interface for viewing tracked paths, approving/deferring/ignoring, and monitoring status
- **Shell hook**: Prints warnings on login about paths nearing expiration
- **CLI**: Subcommands for `tui`, `daemon`, `status`, `scan`, and `init`

### Key Design Decisions

- **SQLite with WAL mode** for shared state across multiple users
- **Directory-level tracking** as primary unit, with file-level drill-down
- **mtime-based expiration** (not ctime or atime)
- **No sudo** — the app runs with user permissions and handles permission errors gracefully
- **Symlinks are resolved** to track the actual file's mtime

## Before You Do Anything

1. **Read the justfile.** Run `just --list` to see available recipes. You must use these recipes for all repeating commands.
2. **Read `.agents/prd.json`** (if it exists) to find the current user story you should implement.
3. **Understand the project structure.** Read `src/main.rs` and the module files relevant to your task.

## Version Control: jj (Jujutsu)

This project uses `jj` with the git backend, not raw git.

### Critical: jj Bypasses Git Hooks

jj does not trigger git hooks. The justfile is our enforcement mechanism. You must run checks manually:

```bash
just check          # Required before every commit
just prepare-commit # Runs checks, then shows jj status
```

### Commit Workflow

1. Make your changes
2. Run `just check` — all checks must pass
3. Run `jj commit` with an appropriate message

### Commit Message Format

```text
Brief summary of what changed (one line)

Longer description in prose. Write in complete sentences. Do NOT hard-wrap lines at 72 or 80 characters — let the text flow naturally and allow the viewer to wrap it. Keep it simple and readable. Avoid bullets, headings, and other formatting that looks LLM-generated. Explain the why, not just the what.
```

**Important:** Do not insert manual line breaks in the commit body. Write each paragraph as a single long line. Git tools and viewers will wrap the text appropriately.

**Bad example:**

```text
Add feature X

## Changes
- Added foo.rs
- Modified bar.rs
- Updated tests

## Notes
- This implements feature X
- Related to issue #123
```

**Good example:**

```text
Add directory scanning with jwalk

This implements the core scanning functionality using jwalk for parallel filesystem traversal. The scanner collects file metadata including size and modification time, which will be used to calculate expiration dates. The scan runs in a background task to avoid blocking the TUI.
```

### Commit Size

Each commit should capture one complete feature or fix. Commits should be:

- Self-contained and functional
- Bisect-friendly (the project should build and pass tests at every commit)
- Aligned with a single PRD user story when using Ralph loops

## Quality Gates

### Pre-commit Checks (Mandatory)

Before every commit, run:

```bash
just check
```

This runs:

1. `cargo fmt --check` — formatting must be correct
2. `cargo clippy -- -D warnings` — no warnings allowed
3. `cargo nextest run` — all tests must pass
4. `cargo doc` — documentation must build without errors

If any check fails, fix the issues before committing. Do not skip checks.

### Documentation Requirements

Documentation must build successfully for a commit to be considered valid. Run `just doc-check` to verify.

**Doctests are preferred.** When writing documentation examples:

- **Prefer runnable examples** — examples that compile and run are the gold standard
- **Use `no_run` over `ignore`** — if an example can't run (e.g., requires filesystem or network), use `no_run` so it still gets type-checked
- **`ignore` is a last resort** — only use `ignore` when the code genuinely cannot be compiled (e.g., pseudocode, incomplete snippets)

````rust
/// Good: Runnable doctest (default)
/// ```
/// let x = 1 + 1;
/// assert_eq!(x, 2);
/// ```

/// Good: Compiles but doesn't run (e.g., has side effects)
/// ```no_run
/// std::fs::remove_file("/tmp/example")?;
/// # Ok::<(), std::io::Error>(())
/// ```

/// Acceptable only when necessary: Not compiled
/// ```ignore
/// // This is pseudocode showing the concept
/// magic_function_that_doesnt_exist();
/// ```
````

The goal is maximum compiler verification. Every doctest that compiles is a test that catches breakage.

### Clippy and `#[allow(...)]` Policy

This project uses stringent clippy lints (pedantic, perf, style, complexity, correctness). Warnings are denied.

**`#[allow(...)]` is treated like `unsafe`.** It requires:

1. A justification comment explaining why the allow is necessary
2. A `TODO(cleanup)` tag if the allow is temporary
3. Case-by-case approval — no blanket module-level allows in production code

**Acceptable (temporary, during development):**

```rust
// TODO(cleanup): Remove allow once TUI event loop is implemented.
// This async fn will use await when crossterm events are integrated.
#[allow(clippy::unused_async)]
pub async fn run(&mut self) -> Result<()> {
```

**Acceptable (permanent, with justification):**

```rust
// Allow: This match is clearer with explicit arms for documentation,
// even though they could be combined.
#[allow(clippy::match_same_arms)]
```

**Not acceptable:**

```rust
#![allow(dead_code)]  // No justification
#![allow(clippy::all)] // Blanket allow
```

## File Tracking: Deny-by-Default .gitignore

The `.gitignore` ignores everything by default (`*`). Files are only tracked if explicitly allowed.

**To track a new file:**

1. Add it to `.gitignore` with a `!` prefix
2. Place it in the appropriate section

Example:

```gitignore
# === Allowed: Source code ===
!/src/new_module
!/src/new_module/mod.rs
```

**No glob patterns.** Every tracked path must be explicit.

## Commands: Use the Justfile

Never run raw cargo commands for standard operations. Use justfile recipes:

| Task              | Command               |
| ----------------- | --------------------- |
| Run all checks    | `just check`          |
| Format code       | `just fmt`            |
| Check formatting  | `just fmt-check`      |
| Run clippy        | `just lint`           |
| Run tests         | `just test`           |
| Check docs build  | `just doc-check`      |
| Build debug       | `just build`          |
| Build release     | `just build-release`  |
| Prepare to commit | `just prepare-commit` |
| Prepare to push   | `just prepare-push`   |

Run `just --list` to see all available recipes.

## Prohibited Actions

Agents must never:

1. **Run `cargo install`** — Package installation is the user's responsibility
2. **Run `jj git push`** — Pushing is the user's responsibility
3. **Skip pre-commit checks** — Always run `just check` before committing
4. **Use blanket `#[allow(...)]`** — Each allow needs justification
5. **Add glob patterns to .gitignore** — Use explicit paths only
6. **Create documentation files** unless explicitly requested
7. **Modify Cargo.toml dependency versions** without using `cargo add`

## Ralph Loop Protocol

When working in Ralph loops (autonomous agent iterations):

1. **Read `.agents/prd.json`** to find the first story where `passes: false`
2. **Implement only that story** — do not look ahead or implement multiple stories
3. **Verify all acceptance criteria** before marking complete
4. **Run `just check`** — all checks must pass
5. **Commit with message** referencing the story: `US-XXX: Brief description`
6. **Update `.agents/prd.json`** — set `passes: true` and add notes if needed

### Story Completion Checklist

Before marking a story as complete:

- [ ] All acceptance criteria verified
- [ ] `just check` passes (fmt + clippy + tests)
- [ ] Changes committed with proper message
- [ ] No unrelated changes included
- [ ] New files added to `.gitignore` allow list

### If a Story Is Too Large

If a story cannot be completed in one iteration:

1. Implement as much as possible
2. Add a note to the story's `notes` field explaining what remains
3. Do not mark `passes: true`
4. The next iteration will continue the work

## Error Handling Conventions

- Use `thiserror` for defining error types in library code
- Use `color_eyre` for error reporting at the binary level
- Propagate errors with `?` and add context with `.context("description")`
- Never use `.unwrap()` or `.expect()` in library code without justification

## Testing Conventions

- Unit tests go in the same file as the code they test (inline `#[cfg(test)]` modules)
- Integration tests go in `tests/`
- Use `proptest` for property-based testing where appropriate
- Use `tempfile` for tests that need filesystem fixtures
- All tests must pass before committing

## Module Structure

The codebase is organized for minimal coupling to enable future workspace extraction:

```text
src/
├── main.rs       # Entry point, CLI dispatch
├── error.rs      # Error types (thiserror)
├── cli/          # CLI definitions
├── config/       # Configuration and paths
├── db/           # Database layer
├── scanner/      # Filesystem scanning
├── daemon/       # Background daemon
├── removal/      # File removal logic
├── audit/        # Audit trail
└── tui/          # Terminal UI
```

When adding code, respect module boundaries. Cross-module dependencies should flow downward (e.g., `tui` can depend on `db`, but `db` should not depend on `tui`).

## Getting Help

If you're unsure about a convention or encounter an ambiguous situation:

1. Check this document first
2. Look at existing code for patterns
3. Ask the user for clarification rather than guessing

**For documentation or research needs:** Delegate to `@documentation-nerd` rather than asking the user. This includes questions about external tools, libraries, APIs, or anything that requires looking up documentation. The user should not be your search engine.
