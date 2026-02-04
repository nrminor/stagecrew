//! Widget rendering for the TUI.

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::prelude::Stylize;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table};

use crate::audit::AuditService;
use crate::config::Config;
use crate::db::Database;
use crate::scanner::calculate_expiration;

use super::{App, FocusPanel, SortMode, View};

/// Render the current application state to the terminal.
///
/// This is the main rendering function that dispatches to view-specific
/// rendering based on the current `app.view` state.
pub(crate) fn render(app: &App, config: &Config, db: &Database, frame: &mut Frame) {
    // Create the main layout with a footer for keybinding hints
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(0),    // Main content area
            Constraint::Length(1), // Footer
        ])
        .split(frame.area());

    // Render the current view in the main area
    match app.view() {
        View::FileList => render_file_list_view(app, config, db, frame, chunks[0]),
        View::AuditLog => render_audit_log(app, db, frame, chunks[0]),
        View::Help => render_help(app, frame, chunks[0]),
    }

    // Render the footer
    render_footer(app, frame, chunks[1]);

    // Render entry deletion confirmation modal if pending entry delete
    if let Some(entries) = app.pending_entry_delete() {
        let count = entries.len();
        if count == 1 {
            render_entry_delete_modal(frame, &entries[0].path.to_string_lossy(), entries[0].is_dir);
        } else {
            render_entry_delete_modal_multi(frame, count);
        }
    }

    // Render entry deferral input modal if pending entry deferral
    if let Some(deferral) = app.pending_entry_deferral() {
        let count = deferral.entries.len();
        render_deferral_modal(
            frame,
            &deferral.entries[0].path.to_string_lossy(),
            &deferral.input,
            deferral.default_days,
            count,
        );
    }

    // Render entry ignore confirmation modal if pending entry ignore
    if let Some(entries) = app.pending_entry_ignore() {
        let count = entries.len();
        if count == 1 {
            render_ignore_modal(frame, &entries[0].path.to_string_lossy());
        } else {
            render_ignore_modal_multi(frame, count);
        }
    }

    // Render entry approval confirmation modal if pending entry approval
    if let Some(entries) = app.pending_entry_approval() {
        let count = entries.len();
        if count == 1 {
            render_confirmation_modal(frame, &entries[0].path.to_string_lossy());
        } else {
            render_confirmation_modal_multi(frame, count);
        }
    }

    // Render add path input modal if pending add path
    if let Some(input) = app.pending_add_path() {
        render_add_path_modal(frame, input);
    }

    // Render remove path confirmation modal if pending remove path
    if let Some(path) = app.pending_remove_path() {
        render_remove_path_modal(frame, path);
    }
}

/// Render the file list view with sidebar.
///
/// Displays tracked directories in left sidebar (20% width) and files from selected
/// directory in main panel (80% width). Shows header with stats for current view.
// Allow: This function orchestrates the two-panel layout and needs to handle
// sidebar, main panel, and header rendering. Breaking it up would make the layout
// coordination less clear.
#[allow(clippy::too_many_lines)]
fn render_file_list_view(
    app: &App,
    config: &Config,
    db: &Database,
    frame: &mut Frame,
    area: ratatui::layout::Rect,
) {
    // Split vertically: header | content area
    let v_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header with stats
            Constraint::Min(0),    // Content area (sidebar + main panel)
        ])
        .split(area);

    // Render header with stats
    render_file_view_header(app, config, db, frame, v_chunks[0]);

    if app.sidebar_visible() {
        // Split content area horizontally: sidebar | main panel
        let h_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(20), // Sidebar for directories
                Constraint::Percentage(80), // Main panel for files
            ])
            .split(v_chunks[1]);

        // Render sidebar with tracked directories
        render_sidebar(app, db, frame, h_chunks[0]);

        // Render main panel with entries from current path
        render_main_entry_panel(app, config, db, frame, h_chunks[1]);
    } else {
        // Sidebar hidden - main panel takes full width
        render_main_entry_panel(app, config, db, frame, v_chunks[1]);
    }
}

/// Render the header showing stats for the current file view.
fn render_file_view_header(
    app: &App,
    _config: &Config,
    _db: &Database,
    frame: &mut Frame,
    area: ratatui::layout::Rect,
) {
    let stats = app.cached_stats;

    // Allow: size values are guaranteed non-negative by schema, but stored as i64 for SQLite compatibility
    #[allow(clippy::cast_sign_loss)]
    let total_size_str = format_bytes(stats.total_size_bytes as u64);

    // Build header text, including status message if present
    let header_text = if let Some(status) = app.status_message.as_ref() {
        format!(
            "Total: {} files, {} | Pending: {} | Expiring soon: {} | Overdue: {} | {}",
            stats.total_files,
            total_size_str,
            stats.files_pending_approval,
            stats.files_within_warning,
            stats.files_overdue,
            status
        )
    } else {
        format!(
            "Total: {} files, {} | Pending: {} | Expiring soon: {} | Overdue: {}",
            stats.total_files,
            total_size_str,
            stats.files_pending_approval,
            stats.files_within_warning,
            stats.files_overdue
        )
    };

    let header = Paragraph::new(header_text)
        .block(Block::default().borders(Borders::ALL).title("Overview"))
        .style(Style::default());

    frame.render_widget(header, area);
}

/// Render the sidebar showing tracked roots.
fn render_sidebar(app: &App, db: &Database, frame: &mut Frame, area: ratatui::layout::Rect) {
    // Fetch roots from database
    let Ok(roots) = db.list_roots() else {
        let error_text = Paragraph::new("Error loading roots")
            .block(Block::default().borders(Borders::ALL).title("Roots"))
            .style(Style::default().fg(Color::Red));
        frame.render_widget(error_text, area);
        return;
    };

    // Update sidebar list length for navigation
    app.sidebar_len.set(roots.len());

    // Clamp selected index
    let selected_idx = if roots.is_empty() {
        0
    } else {
        app.sidebar_selected_index().min(roots.len() - 1)
    };

    // Set current root ID based on selection
    if let Some(root) = roots.get(selected_idx) {
        app.current_root_id.set(Some(root.id));
    }

    // Build sidebar rows
    let rows: Vec<Row> = roots
        .iter()
        .enumerate()
        .map(|(idx, root)| {
            // Extract just the directory name from full path
            let dir_name = std::path::Path::new(&root.path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(&root.path);

            let cell = Cell::from(dir_name);

            // Highlight selected row and show focus
            let style = if idx == selected_idx {
                if app.focus_panel() == FocusPanel::Sidebar {
                    Style::default()
                        .add_modifier(Modifier::REVERSED)
                        .fg(Color::Cyan)
                } else {
                    Style::default().add_modifier(Modifier::REVERSED)
                }
            } else {
                Style::default()
            };

            Row::new(vec![cell]).style(style)
        })
        .collect();

    // Empty state
    if rows.is_empty() {
        let empty_text = Paragraph::new("No tracked paths.\n\nRun 'stagecrew add PATH'")
            .block(Block::default().borders(Borders::ALL).title("Roots"))
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(empty_text, area);
        return;
    }

    let table = Table::new(rows, [Constraint::Percentage(100)]).block(
        Block::default()
            .title("Roots")
            .borders(Borders::ALL)
            .border_style(if app.focus_panel() == FocusPanel::Sidebar {
                Style::default().fg(Color::Cyan)
            } else {
                Style::default()
            }),
    );

    frame.render_widget(table, area);
}

/// Render the main panel showing entries from the current path.
// Allow: This function handles entry loading, sorting, and table rendering which are
// sequential operations that form a cohesive rendering pipeline.
#[allow(clippy::too_many_lines)]
fn render_main_entry_panel(
    app: &App,
    config: &Config,
    db: &Database,
    frame: &mut Frame,
    area: ratatui::layout::Rect,
) {
    // Get the current path for browsing
    let current_path = app.current_path();

    // If current_path is empty, show a message to select a root
    if current_path.is_empty() {
        let message = Paragraph::new(
            "Select a root from the sidebar\n\n(Use j/k to navigate, Tab to switch panels)",
        )
        .block(Block::default().borders(Borders::ALL).title("Entries"))
        .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(message, area);
        return;
    }

    // Fetch entries for this path
    let Ok(entries) = db.list_entries_by_parent(current_path) else {
        let error_text = Paragraph::new("Error loading entries from database")
            .block(Block::default().borders(Borders::ALL).title("Entries"))
            .style(Style::default().fg(Color::Red));
        frame.render_widget(error_text, area);
        return;
    };

    // Empty state
    if entries.is_empty() {
        let empty_text = Paragraph::new("No entries in this directory")
            .block(Block::default().borders(Borders::ALL).title("Entries"))
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(empty_text, area);
        return;
    }

    // Sort entries by expiration (most urgent first) by default
    // For directories (no mtime), use a large positive value so they sort to end
    let mut entry_rows: Vec<_> = entries
        .into_iter()
        .map(|entry| {
            // Directories have no mtime
            let days_remaining = entry.mtime.map_or(i64::MAX, |mtime| {
                calculate_expiration(mtime, config.expiration_days)
            });
            (entry, days_remaining)
        })
        .collect();

    // Sort based on current sort mode
    sort_entry_rows(&mut entry_rows, app.sort_mode());

    // Update entry list length for navigation
    app.entry_list_len.set(entry_rows.len());

    // Clamp selected index
    let selected_idx = if entry_rows.is_empty() {
        0
    } else {
        app.entry_selected_index().min(entry_rows.len() - 1)
    };

    // Compute search matches for highlighting
    let search_match_set: std::collections::HashSet<usize> = app
        .search_query
        .as_ref()
        .map(|q| {
            super::input::find_search_matches(&entry_rows, q)
                .into_iter()
                .collect()
        })
        .unwrap_or_default();

    // Build entry table rows
    let rows: Vec<Row> = entry_rows
        .iter()
        .enumerate()
        .map(|(idx, (entry, days_remaining))| {
            // Visual indicator showing attention status
            let (indicator_symbol, indicator_color) = expiration_indicator_entry(
                &entry.status,
                *days_remaining,
                config.warning_days,
                entry,
            );
            let indicator_cell = Cell::from(indicator_symbol).fg(indicator_color);

            // Extract filename from path with directory indicator
            let filename = std::path::Path::new(&entry.path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(&entry.path);
            let display_name = if entry.is_dir {
                format!("{filename}/")
            } else {
                filename.to_string()
            };
            let filename_cell = Cell::from(display_name);

            // Format size (directories show as "-")
            let size_str = if entry.is_dir {
                "-".to_string()
            } else {
                // Allow: size_bytes is guaranteed non-negative by schema, but stored as i64 for SQLite compatibility
                #[allow(clippy::cast_sign_loss)]
                format_bytes(entry.size_bytes as u64)
            };
            let size_cell = Cell::from(size_str);

            // Format expiration
            let expires_str = if *days_remaining >= 0 {
                format!("{days_remaining} days")
            } else {
                format!("{} days ago", -days_remaining)
            };
            let expires_cell = Cell::from(expires_str);

            // Display status from database, with expiration-based fallback for "tracked" entries.
            // Directories show the same status as files based on their oldest child's mtime.
            let status_str = match entry.status.as_str() {
                "tracked" => {
                    if *days_remaining <= 0 {
                        "overdue"
                    } else if *days_remaining <= i64::from(config.warning_days) {
                        "warning"
                    } else {
                        "tracked"
                    }
                }
                other => other,
            };
            let status_cell = Cell::from(status_str);

            // Determine row color based on actual entry status
            // For deferred entries, use the deferral end date instead of mtime-based expiration
            let effective_days = if entry.status == "deferred" {
                if let Some(deferred_until) = entry.deferred_until {
                    let now = jiff::Timestamp::now().as_second();
                    (deferred_until - now) / 86400
                } else {
                    *days_remaining
                }
            } else {
                *days_remaining
            };
            let row_style =
                determine_row_style(&entry.status, Some(effective_days), config.warning_days);

            // Check if this entry is selected (multi-select)
            let is_selected = app.selected_entries().contains(&entry.id);
            let is_search_match = search_match_set.contains(&idx);

            // Highlight selected row and show focus
            let is_cursor = idx == selected_idx && app.focus_panel() == FocusPanel::MainPanel;
            let mut style = if is_selected && is_cursor {
                // Cursor on a selected row: combine both indicators
                row_style
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD | Modifier::REVERSED)
                    .fg(Color::Cyan)
            } else if is_selected {
                // Selected entries get dark gray background for contrast with all text colors
                row_style.bg(Color::DarkGray).add_modifier(Modifier::BOLD)
            } else if idx == selected_idx {
                // Currently focused entry
                if app.focus_panel() == FocusPanel::MainPanel {
                    row_style.add_modifier(Modifier::REVERSED).fg(Color::Cyan)
                } else {
                    row_style.add_modifier(Modifier::REVERSED)
                }
            } else {
                row_style
            };

            // Underline search matches so they stand out
            if is_search_match {
                style = style.add_modifier(Modifier::UNDERLINED);
            }

            Row::new(vec![
                indicator_cell,
                filename_cell,
                size_cell,
                expires_cell,
                status_cell,
            ])
            .style(style)
        })
        .collect();

    // Build table
    let widths = [
        Constraint::Length(2),      // Visual indicator (●/⚠/✓)
        Constraint::Percentage(42), // Filename
        Constraint::Percentage(15), // Size
        Constraint::Percentage(20), // Expires
        Constraint::Percentage(20), // Status
    ];

    let sort_indicator = match app.sort_mode() {
        SortMode::Expiration => " (by expiration)",
        SortMode::Size => " (by size)",
        SortMode::Name => " (by name)",
        SortMode::Modified => " (by modified)",
    };

    let selection_info = if app.selected_entries().is_empty() {
        String::new()
    } else {
        format!(" | {} selected", app.selected_entries().len())
    };

    let search_info = if let Some(query) = &app.search_query {
        if query.is_empty() {
            String::new()
        } else {
            let match_count = search_match_set.len();
            format!(
                " | /{query} ({match_count} match{plural})",
                plural = if match_count == 1 { "" } else { "es" }
            )
        }
    } else {
        String::new()
    };

    let table = Table::new(rows, widths)
        .block(
            Block::default()
                .title(format!(
                    "Entries{sort_indicator}{selection_info}{search_info}"
                ))
                .borders(Borders::ALL)
                .border_style(if app.focus_panel() == FocusPanel::MainPanel {
                    Style::default().fg(Color::Cyan)
                } else {
                    Style::default()
                }),
        )
        .header(
            Row::new(entry_table_header_cells(app.sort_mode()))
                .style(Style::default().add_modifier(Modifier::BOLD))
                .bottom_margin(1),
        );

    frame.render_widget(table, area);
}

/// Sort entry rows according to the specified sort mode.
pub(super) fn sort_entry_rows(rows: &mut [(crate::db::Entry, i64)], sort_mode: SortMode) {
    match sort_mode {
        SortMode::Expiration => {
            // Ascending (most urgent first), but:
            // - Ignored entries sort to the end (they're not expiring)
            // - Deferred entries sort by their deferred_until date
            // - Directories sort by their oldest child's mtime
            rows.sort_by(|a, b| {
                let key_a = expiration_sort_key_entry(&a.0, a.1);
                let key_b = expiration_sort_key_entry(&b.0, b.1);
                key_a.cmp(&key_b)
            });
        }
        SortMode::Size => {
            // Descending (largest first), directories sort to end
            rows.sort_by(|a, b| {
                // Directories go to end
                match (a.0.is_dir, b.0.is_dir) {
                    (true, false) => std::cmp::Ordering::Greater,
                    (false, true) => std::cmp::Ordering::Less,
                    _ => b.0.size_bytes.cmp(&a.0.size_bytes),
                }
            });
        }
        SortMode::Name => {
            // Alphabetical ascending by filename, directories first
            rows.sort_by(|a, b| {
                // Directories first
                match (a.0.is_dir, b.0.is_dir) {
                    (true, false) => std::cmp::Ordering::Less,
                    (false, true) => std::cmp::Ordering::Greater,
                    _ => {
                        let name_a = std::path::Path::new(&a.0.path)
                            .file_name()
                            .and_then(|n| n.to_str())
                            .unwrap_or(&a.0.path);
                        let name_b = std::path::Path::new(&b.0.path)
                            .file_name()
                            .and_then(|n| n.to_str())
                            .unwrap_or(&b.0.path);
                        name_a.cmp(name_b)
                    }
                }
            });
        }
        SortMode::Modified => {
            // Descending (most recent first = highest mtime first), directories to end
            rows.sort_by(|a, b| {
                // Directories go to end (no mtime)
                match (a.0.mtime, b.0.mtime) {
                    (Some(mtime_a), Some(mtime_b)) => mtime_b.cmp(&mtime_a),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                }
            });
        }
    }
}

/// Compute a sort key for expiration-based sorting.
///
/// Returns an `i64` where lower values sort first (more urgent):
/// - Directories return `i64::MAX - 1` (sort near end, before ignored)
/// - Ignored entries return `i64::MAX` (sort to end)
/// - Deferred entries return days until deferral expires
/// - Other entries return their `days_remaining`
fn expiration_sort_key_entry(entry: &crate::db::Entry, days_remaining: i64) -> i64 {
    match entry.status.as_str() {
        "ignored" => i64::MAX, // Sort to end
        "deferred" => {
            // Sort by days until deferral expires
            if let Some(deferred_until) = entry.deferred_until {
                let now = jiff::Timestamp::now().as_second();
                let seconds_remaining = deferred_until - now;
                seconds_remaining / 86400 // Convert to days
            } else {
                // Deferred but no deferred_until set (shouldn't happen, but handle gracefully)
                days_remaining
            }
        }
        _ => days_remaining,
    }
}

/// Determine row style based on status and expiration.
///
/// For deferred files, the caller should pass the effective days remaining
/// based on `deferred_until`, not the mtime-based expiration.
fn determine_row_style(status: &str, days_remaining: Option<i64>, warning_days: u32) -> Style {
    // Ignored paths are gray
    if status == "ignored" {
        return Style::default().fg(Color::DarkGray);
    }

    // Pending or approved paths are red (require attention)
    if status == "pending" || status == "approved" {
        return Style::default().fg(Color::Red);
    }

    // Check expiration status (applies to tracked and deferred)
    match days_remaining {
        None => Style::default(), // No mtime, use default
        Some(days) if days <= 0 => Style::default().fg(Color::Red), // Overdue
        Some(days) if days <= i64::from(warning_days) => Style::default().fg(Color::Yellow), // Warning
        _ => Style::default().fg(Color::Green),                                              // Safe
    }
}

/// Generate a visual indicator (symbol + color) for attention status (for entries).
///
/// Returns a tuple of (symbol, color) that signals when attention is needed:
/// - `●` RED: Overdue (expired, requires immediate attention)
/// - `⚠` YELLOW: Warning period (approaching expiration)
/// - `—` GRAY: Ignored (won't expire, no action needed)
/// - `📁` BLUE: Directory (no expiration)
/// - ` ` (space): Safe, no attention needed
///
/// For deferred entries, the indicator is based on the deferral end date.
fn expiration_indicator_entry(
    status: &str,
    days_remaining: i64,
    warning_days: u32,
    entry: &crate::db::Entry,
) -> (&'static str, Color) {
    // Ignored entries show a dash — they won't expire
    if status == "ignored" {
        return ("—", Color::DarkGray);
    }

    // Deferred entries: calculate days until deferral expires
    let effective_days = if status == "deferred" {
        if let Some(deferred_until) = entry.deferred_until {
            let now = jiff::Timestamp::now().as_second();
            (deferred_until - now) / 86400
        } else {
            days_remaining
        }
    } else {
        days_remaining
    };

    if effective_days <= 0 {
        ("●", Color::Red) // Overdue - filled circle
    } else if effective_days <= i64::from(warning_days) {
        ("⚠", Color::Yellow) // Warning - warning triangle
    } else {
        (" ", Color::Reset) // Safe - no indicator needed
    }
}

/// Generate header cells for the entry table with sort indicators.
///
/// The currently sorted column gets a triangle indicator:
/// - `▲` for ascending sort (Name, Expiration)
/// - `▼` for descending sort (Size, Modified)
fn entry_table_header_cells(sort_mode: SortMode) -> Vec<Cell<'static>> {
    let indicator_asc = " ▲";
    let indicator_desc = " ▼";

    let filename_header = match sort_mode {
        SortMode::Name => format!("Filename{indicator_asc}"),
        _ => "Filename".to_string(),
    };

    let size_header = match sort_mode {
        SortMode::Size => format!("Size{indicator_desc}"),
        _ => "Size".to_string(),
    };

    let expires_header = match sort_mode {
        SortMode::Expiration => format!("Expires{indicator_asc}"),
        _ => "Expires".to_string(),
    };

    // Modified isn't a visible column, but if we're sorting by it, show in Status column
    let status_header = match sort_mode {
        SortMode::Modified => format!("Status (by mtime{indicator_desc})"),
        _ => "Status".to_string(),
    };

    vec![
        Cell::from(""), // No header for indicator column
        Cell::from(filename_header),
        Cell::from(size_header),
        Cell::from(expires_header),
        Cell::from(status_header),
    ]
}

/// Format bytes as human-readable string (e.g., "1.2 KB", "523 MB").
fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB"];
    const THRESHOLD: f64 = 1000.0;

    if bytes == 0 {
        return "0 B".to_string();
    }

    // Allow: Casting u64 to f64 for display purposes. Precision loss above 2^53 is acceptable
    // for human-readable sizes (that's ~9 PB, well beyond typical file sizes).
    #[allow(clippy::cast_precision_loss)]
    let bytes_f = bytes as f64;

    // Allow: Log-based calculation always produces non-negative results for positive inputs,
    // and floor() ensures the result fits in usize range [0..5] which is then clamped.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let unit_idx = (bytes_f.log10() / THRESHOLD.log10()).floor() as usize;
    let unit_idx = unit_idx.min(UNITS.len() - 1);

    // Allow: unit_idx is clamped to [0..5], which always fits in i32.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let value = bytes_f / THRESHOLD.powi(unit_idx as i32);

    if unit_idx == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit_idx])
    }
}

/// Render the audit log view.
///
/// Displays recent audit entries showing timestamp, user, action, and path.
/// The view is scrollable and shows the most recent entries first.
fn render_audit_log(app: &App, db: &Database, frame: &mut Frame, area: ratatui::layout::Rect) {
    // Fetch recent audit entries (limit to 1000 for now)
    let audit = AuditService::new(db);
    let Ok(entries) = audit.list_recent(1000) else {
        // Error handling: show error message
        let error_text = Paragraph::new("Error loading audit log from database")
            .block(Block::default().borders(Borders::ALL).title("Error"))
            .style(Style::default().fg(Color::Red));
        frame.render_widget(error_text, area);
        return;
    };

    // Update list length for navigation
    app.sidebar_len.set(entries.len());

    // Clamp selected index to valid range
    let selected_idx = if entries.is_empty() {
        0
    } else {
        app.sidebar_selected_index().min(entries.len() - 1)
    };

    // Handle empty state
    if entries.is_empty() {
        let empty_text = Paragraph::new("No audit entries found.\n\nPress 'q' or Esc to go back")
            .block(Block::default().borders(Borders::ALL).title("Audit Log"))
            .style(Style::default());
        frame.render_widget(empty_text, area);
        return;
    }

    // Build table rows
    let rows: Vec<Row> = entries
        .iter()
        .enumerate()
        .map(|(idx, entry)| {
            // Format timestamp as human-readable in local timezone
            let timestamp_str = format_timestamp(entry.timestamp);
            let timestamp_cell = Cell::from(timestamp_str);

            let user_cell = Cell::from(entry.user.as_str());
            let action_cell = Cell::from(entry.action.as_str());

            let path_str = entry.target_path.as_deref().unwrap_or("<system-wide>");
            let path_cell = Cell::from(path_str);

            // Highlight selected row
            let style = if idx == selected_idx {
                Style::default().add_modifier(Modifier::REVERSED)
            } else {
                Style::default()
            };

            Row::new(vec![timestamp_cell, user_cell, action_cell, path_cell]).style(style)
        })
        .collect();

    // Build table
    let widths = [
        Constraint::Percentage(20), // Timestamp
        Constraint::Percentage(15), // User
        Constraint::Percentage(15), // Action
        Constraint::Percentage(50), // Path
    ];

    let table = Table::new(rows, widths)
        .block(
            Block::default()
                .title("Audit Log (Most Recent First)")
                .borders(Borders::ALL),
        )
        .header(
            Row::new(vec![
                Cell::from("Timestamp"),
                Cell::from("User"),
                Cell::from("Action"),
                Cell::from("Path"),
            ])
            .style(Style::default().add_modifier(Modifier::BOLD))
            .bottom_margin(1),
        );

    frame.render_widget(table, area);
}

/// Format a Unix timestamp as a human-readable string in local timezone.
///
/// Formats as "YYYY-MM-DD HH:MM:SS" in local time.
fn format_timestamp(timestamp: i64) -> String {
    // Convert Unix timestamp to jiff::Timestamp
    let ts = jiff::Timestamp::from_second(timestamp).unwrap_or(jiff::Timestamp::UNIX_EPOCH);

    // Format in local timezone as "YYYY-MM-DD HH:MM:SS"
    ts.to_zoned(jiff::tz::TimeZone::system())
        .strftime("%Y-%m-%d %H:%M:%S")
        .to_string()
}

/// Render the help view with keybinding reference.
///
/// Displays all available keybindings organized by category (Navigation, Views,
/// Actions, Other). Any key press dismisses this view and returns to the directory list.
/// Render the help view with keybinding reference.
///
/// Displays all available keybindings organized by sections: File-Centric Workflow,
/// Navigation, Selection, Actions, Views, Sorting, and Other. Any key press dismisses
/// this view and returns to the file list.
fn render_help(_app: &App, frame: &mut Frame, area: ratatui::layout::Rect) {
    let block = Block::default()
        .title("Stagecrew - Keybinding Reference")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan))
        .style(Style::default());

    let help_text = r"File-Centric Workflow:
  The main panel shows files from the currently selected directory.
  The left sidebar shows tracked directories for filtering.
  Navigate the sidebar with j/k to change which directory's files are shown.

Navigation:
  j / ↓       Move selection down in focused panel
  k / ↑       Move selection up in focused panel
  g           Jump to top of focused panel
  G           Jump to bottom of focused panel
  Tab         Switch focus between sidebar and main panel
  h           Switch focus to sidebar (shows sidebar if hidden)
  l           Switch focus to main panel
  B           Toggle sidebar visibility
  
Selection (main panel only):
  Space       Toggle selection on current file and advance cursor
  v           Enter/exit visual mode (range select from anchor)
  Esc         Exit visual mode / clear search / clear selection

Actions (main panel only - on focused file or all selected files):
  d           Delete file(s) with confirmation
  r           Defer file(s) expiration (reset clock, prompt for days)
  i           Permanently ignore file(s)
  x           Approve file(s) for daemon removal

Views:
  a           Show audit log
  ?           Show this help screen
  
Sorting:
  s           Cycle sort mode (Expiration → Size → Name → Modified)
  
Other:
  R           Refresh tracked paths
  q           Quit application
  Ctrl+C      Quit application

Press any key to close this help screen";

    let text = Paragraph::new(help_text).block(block);

    frame.render_widget(text, area);
}

/// Render the footer with mode badge and context-sensitive keybinding hints.
fn render_footer(app: &App, frame: &mut Frame, area: ratatui::layout::Rect) {
    // Determine current mode for the badge
    let (mode_label, mode_style) = if app.search_input_active || app.search_query.is_some() {
        (
            " SEARCH ",
            Style::default()
                .fg(Color::Black)
                .bg(Color::Blue)
                .add_modifier(Modifier::BOLD),
        )
    } else if app.is_visual_mode() {
        (
            " VISUAL ",
            Style::default()
                .fg(Color::Black)
                .bg(Color::Green)
                .add_modifier(Modifier::BOLD),
        )
    } else {
        (
            " NORMAL ",
            Style::default()
                .fg(Color::White)
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
    };

    // Split footer: fixed-width badge on left, hints fill the rest
    let badge_width: u16 = 9; // " VISUAL " + 1 space separator
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(badge_width), Constraint::Min(0)])
        .split(area);

    // Render mode badge
    let badge = Paragraph::new(Span::styled(mode_label, mode_style));
    frame.render_widget(badge, chunks[0]);

    // Search input bar replaces hints when typing
    if app.search_input_active {
        let query = app.search_query.as_deref().unwrap_or("");
        let search_bar = Paragraph::new(Line::from(vec![Span::styled(
            format!("/{query}█"),
            Style::default().fg(Color::White).bg(Color::DarkGray),
        )]));
        frame.render_widget(search_bar, chunks[1]);
        return;
    }

    // Check if any modal is open (takes precedence over normal view hints)
    let modal_open = app.pending_entry_delete().is_some()
        || app.pending_entry_deferral().is_some()
        || app.pending_entry_ignore().is_some()
        || app.pending_entry_approval().is_some()
        || app.pending_add_path().is_some()
        || app.pending_remove_path().is_some();

    let hints = if modal_open {
        if app.pending_entry_deferral().is_some() {
            "[0-9] Enter days [Backspace] Delete [Enter] Confirm [Esc] Cancel".to_string()
        } else if app.pending_add_path().is_some() {
            "[Type path] (supports ~) [Backspace] Delete [Enter] Add [Esc] Cancel".to_string()
        } else {
            "[y] Yes [n] No [Esc] Cancel".to_string()
        }
    } else {
        match app.view() {
            View::FileList => {
                let selection_count = app.selected_entries().len();

                if app.search_query.is_some() {
                    "[n] Next match [N] Prev match [/] New search [Esc] Clear search [q] Quit"
                        .to_string()
                } else if app.is_visual_mode() {
                    format!(
                        "[j/k] Extend [g/G] Extend to top/bottom [Esc] Keep & exit [v] Keep & exit [d/r/i/x] Act on {selection_count} [q] Quit"
                    )
                } else if selection_count > 0 {
                    format!(
                        "[d] Delete {selection_count} [r] Defer {selection_count} [i] Ignore {selection_count} [x] Approve {selection_count} [Esc] Clear [q] Quit"
                    )
                } else {
                    match app.focus_panel() {
                        FocusPanel::Sidebar => {
                            "[j/k] Navigate [g/G] Top/Bottom [X] Remove [A] Add [Tab/h/l] Switch panel [s] Sort [a] Audit [?] Help [q] Quit"
                        }
                        FocusPanel::MainPanel => {
                            "[j/k] Navigate [g/G] Top/Bottom [d] Delete [r] Defer [i] Ignore [x] Approve [Space] Select [v] Visual [A] Add path [Tab/h/l] Switch panel [s] Sort [a] Audit [?] Help [q] Quit"
                        }
                    }
                    .to_string()
                }
            }
            View::AuditLog => "[j/k] Navigate [g/G] Top/Bottom [Esc] Back [q] Quit".to_string(),
            View::Help => "[Any key] Close".to_string(),
        }
    };

    let footer = Paragraph::new(hints).style(Style::default().fg(Color::Black).bg(Color::Gray));
    frame.render_widget(footer, chunks[1]);
}

/// Render a confirmation modal for approval actions.
///
/// Displays a centered modal asking the user to confirm removal approval.
fn render_confirmation_modal(frame: &mut Frame, path: &str) {
    use ratatui::layout::{Alignment, Rect};

    // Calculate centered rectangle for modal (50% width, 7 lines height)
    let area = frame.area();
    let modal_width = area.width / 2;
    let modal_height = 7;
    let modal_x = (area.width.saturating_sub(modal_width)) / 2;
    let modal_y = (area.height.saturating_sub(modal_height)) / 2;

    let modal_area = Rect {
        x: modal_x,
        y: modal_y,
        width: modal_width,
        height: modal_height,
    };

    // Create the modal content
    let title = "Approve Removal";
    let message = format!("Approve removal of:\n\n{path}\n\n(y/n)");

    let modal = Paragraph::new(message)
        .block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Yellow)),
        )
        .alignment(Alignment::Center)
        .style(Style::default().bg(Color::Black).fg(Color::White));

    // Clear the area behind the modal
    frame.render_widget(Clear, modal_area);
    frame.render_widget(modal, modal_area);
}

/// Render a deferral input modal for entering days to defer.
///
/// Displays a centered modal prompting the user to enter the number of days
/// to defer expiration. Shows the current input and the default value.
/// The count parameter indicates how many files will be deferred.
fn render_deferral_modal(
    frame: &mut Frame,
    path: &str,
    input: &str,
    default_days: u32,
    count: usize,
) {
    use ratatui::layout::{Alignment, Rect};

    // Calculate centered rectangle for modal (60% width, 9 lines height)
    let area = frame.area();
    let modal_width = (area.width * 3) / 5;
    let modal_height = 9;
    let modal_x = (area.width.saturating_sub(modal_width)) / 2;
    let modal_y = (area.height.saturating_sub(modal_height)) / 2;

    let modal_area = Rect {
        x: modal_x,
        y: modal_y,
        width: modal_width,
        height: modal_height,
    };

    // Create the modal content
    let title = if count > 1 {
        format!("Defer Expiration ({count} files)")
    } else {
        "Defer Expiration".to_string()
    };
    let display_input = if input.is_empty() {
        format!("[{default_days}]")
    } else {
        input.to_string()
    };
    let path_display = if count > 1 {
        format!("{count} selected files")
    } else {
        path.to_string()
    };
    let message = format!(
        "Defer expiration for:\n{path_display}\n\nDays to defer: {display_input}\n\n(Enter to confirm, Esc to cancel)"
    );

    let modal = Paragraph::new(message)
        .block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan)),
        )
        .alignment(Alignment::Center)
        .style(Style::default().bg(Color::Black).fg(Color::White));

    // Clear the area behind the modal
    frame.render_widget(Clear, modal_area);
    frame.render_widget(modal, modal_area);
}

/// Render an entry deletion confirmation modal.
///
/// Displays a centered modal prompting the user to confirm deletion
/// of the selected entry (file or directory).
fn render_entry_delete_modal(frame: &mut Frame, path: &str, is_dir: bool) {
    use ratatui::layout::{Alignment, Rect};

    // Calculate centered rectangle for modal (50% width, 7 lines height)
    let area = frame.area();
    let modal_width = area.width / 2;
    let modal_height = 7;
    let modal_x = (area.width.saturating_sub(modal_width)) / 2;
    let modal_y = (area.height.saturating_sub(modal_height)) / 2;

    let modal_area = Rect {
        x: modal_x,
        y: modal_y,
        width: modal_width,
        height: modal_height,
    };

    // Create the modal content
    let (title, type_name) = if is_dir {
        ("Delete Directory", "directory")
    } else {
        ("Delete File", "file")
    };
    let message = format!("Delete {type_name}:\n\n{path}\n\n(y/n)");

    let modal = Paragraph::new(message)
        .block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Red)),
        )
        .alignment(Alignment::Center)
        .style(Style::default().bg(Color::Black).fg(Color::White));

    // Clear the area behind the modal
    frame.render_widget(Clear, modal_area);
    frame.render_widget(modal, modal_area);
}

/// Render a multi-entry deletion confirmation modal.
fn render_entry_delete_modal_multi(frame: &mut Frame, count: usize) {
    use ratatui::layout::{Alignment, Rect};

    let area = frame.area();
    let modal_width = area.width / 2;
    let modal_height = 7;
    let modal_x = (area.width.saturating_sub(modal_width)) / 2;
    let modal_y = (area.height.saturating_sub(modal_height)) / 2;

    let modal_area = Rect {
        x: modal_x,
        y: modal_y,
        width: modal_width,
        height: modal_height,
    };

    let title = format!("Delete {count} Entries");
    let message = format!("Delete {count} entries?\n\n(y/n)");

    let modal = Paragraph::new(message)
        .block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Red)),
        )
        .alignment(Alignment::Center)
        .style(Style::default().bg(Color::Black).fg(Color::White));

    // Clear the area behind the modal
    frame.render_widget(Clear, modal_area);
    frame.render_widget(modal, modal_area);
}

/// Render an ignore confirmation modal for permanently exempting a path.
///
/// Displays a centered modal prompting the user to confirm permanent exemption
/// of the selected directory from auto-removal.
fn render_ignore_modal(frame: &mut Frame, path: &str) {
    use ratatui::layout::{Alignment, Rect};

    // Calculate centered rectangle for modal (50% width, 7 lines height)
    let area = frame.area();
    let modal_width = area.width / 2;
    let modal_height = 7;
    let modal_x = (area.width.saturating_sub(modal_width)) / 2;
    let modal_y = (area.height.saturating_sub(modal_height)) / 2;

    let modal_area = Rect {
        x: modal_x,
        y: modal_y,
        width: modal_width,
        height: modal_height,
    };

    // Create the modal content
    let title = "Ignore Path Permanently";
    let message = format!("Permanently ignore:\n\n{path}\n\n(y/n)");

    let modal = Paragraph::new(message)
        .block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan)),
        )
        .alignment(Alignment::Center)
        .style(Style::default().bg(Color::Black).fg(Color::White));

    // Clear the area behind the modal
    frame.render_widget(Clear, modal_area);
    frame.render_widget(modal, modal_area);
}

/// Render a multi-file ignore confirmation modal.
fn render_ignore_modal_multi(frame: &mut Frame, count: usize) {
    use ratatui::layout::{Alignment, Rect};

    let area = frame.area();
    let modal_width = area.width / 2;
    let modal_height = 7;
    let modal_x = (area.width.saturating_sub(modal_width)) / 2;
    let modal_y = (area.height.saturating_sub(modal_height)) / 2;

    let modal_area = Rect {
        x: modal_x,
        y: modal_y,
        width: modal_width,
        height: modal_height,
    };

    let title = format!("Ignore {count} Files Permanently");
    let message = format!("Permanently ignore {count} files?\n\n(y/n)");

    let modal = Paragraph::new(message)
        .block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan)),
        )
        .alignment(Alignment::Center)
        .style(Style::default().bg(Color::Black).fg(Color::White));

    // Clear the area behind the modal
    frame.render_widget(Clear, modal_area);
    frame.render_widget(modal, modal_area);
}

/// Render a multi-file approval confirmation modal.
fn render_confirmation_modal_multi(frame: &mut Frame, count: usize) {
    use ratatui::layout::{Alignment, Rect};

    let area = frame.area();
    let modal_width = area.width / 2;
    let modal_height = 7;
    let modal_x = (area.width.saturating_sub(modal_width)) / 2;
    let modal_y = (area.height.saturating_sub(modal_height)) / 2;

    let modal_area = Rect {
        x: modal_x,
        y: modal_y,
        width: modal_width,
        height: modal_height,
    };

    let title = format!("Approve {count} Files for Removal");
    let message = format!("Approve {count} files for removal?\n\n(y/n)");

    let modal = Paragraph::new(message)
        .block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Yellow)),
        )
        .alignment(Alignment::Center)
        .style(Style::default().bg(Color::Black).fg(Color::White));

    // Clear the area behind the modal
    frame.render_widget(Clear, modal_area);
    frame.render_widget(modal, modal_area);
}

/// Render add path text input modal.
///
/// Displays a centered modal prompting the user to enter a path to add to `tracked_paths`.
/// Supports tilde expansion (~).
fn render_add_path_modal(frame: &mut Frame, input: &str) {
    use ratatui::layout::{Alignment, Rect};

    // Calculate centered rectangle for modal (60% width, 7 lines height)
    let area = frame.area();
    let modal_width = (area.width * 3) / 5;
    let modal_height = 7;
    let modal_x = (area.width.saturating_sub(modal_width)) / 2;
    let modal_y = (area.height.saturating_sub(modal_height)) / 2;

    let modal_area = Rect {
        x: modal_x,
        y: modal_y,
        width: modal_width,
        height: modal_height,
    };

    // Create the modal content
    let display_input = if input.is_empty() { "_" } else { input };
    let message = format!(
        "Add tracked path:\n\n{display_input}\n\n(Supports ~) (Enter to add, Esc to cancel)"
    );

    let modal = Paragraph::new(message)
        .block(
            Block::default()
                .title("Add Tracked Path")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Green)),
        )
        .alignment(Alignment::Center)
        .style(Style::default().bg(Color::Black).fg(Color::White));

    // Clear the area behind the modal
    frame.render_widget(Clear, modal_area);
    frame.render_widget(modal, modal_area);
}

/// Render remove path confirmation modal.
///
/// Displays a centered modal prompting the user to confirm removal of a tracked path.
fn render_remove_path_modal(frame: &mut Frame, path: &str) {
    use ratatui::layout::{Alignment, Rect};

    // Calculate centered rectangle for modal (50% width, 7 lines height)
    let area = frame.area();
    let modal_width = area.width / 2;
    let modal_height = 7;
    let modal_x = (area.width.saturating_sub(modal_width)) / 2;
    let modal_y = (area.height.saturating_sub(modal_height)) / 2;

    let modal_area = Rect {
        x: modal_x,
        y: modal_y,
        width: modal_width,
        height: modal_height,
    };

    // Create the modal content
    let message = format!("Remove tracked path:\n{path}\n\n(y/n)");

    let modal = Paragraph::new(message)
        .block(
            Block::default()
                .title("Remove Tracked Path")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Red)),
        )
        .alignment(Alignment::Center)
        .style(Style::default().bg(Color::Black).fg(Color::White));

    // Clear the area behind the modal
    frame.render_widget(Clear, modal_area);
    frame.render_widget(modal, modal_area);
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create a minimal Entry for testing (file)
    fn test_entry(path: &str, size_bytes: i64, mtime: Option<i64>) -> crate::db::Entry {
        let now = jiff::Timestamp::now().as_second();
        crate::db::Entry {
            id: 0,
            root_id: 1,
            path: path.to_string(),
            parent_path: "/".to_string(),
            is_dir: false,
            size_bytes,
            mtime,
            tracked_since: Some(now),
            status: "tracked".to_string(),
            deferred_until: None,
            created_at: now,
            updated_at: now,
        }
    }

    // Helper to create a directory Entry for testing
    fn test_entry_dir(path: &str) -> crate::db::Entry {
        let now = jiff::Timestamp::now().as_second();
        crate::db::Entry {
            id: 0,
            root_id: 1,
            path: path.to_string(),
            parent_path: "/".to_string(),
            is_dir: true,
            size_bytes: 0,
            mtime: None,
            tracked_since: Some(now),
            status: "tracked".to_string(),
            deferred_until: None,
            created_at: now,
            updated_at: now,
        }
    }

    // Tests for sort_entry_rows

    #[test]
    fn sort_entries_by_expiration_most_urgent_first() {
        let mut rows = vec![
            (test_entry("/c.txt", 100, Some(1000)), 30), // 30 days remaining
            (test_entry("/a.txt", 200, Some(5000)), 5),  // 5 days remaining (most urgent)
            (test_entry("/b.txt", 150, Some(3000)), 15), // 15 days remaining
        ];

        sort_entry_rows(&mut rows, SortMode::Expiration);

        assert_eq!(
            rows[0].0.path, "/a.txt",
            "Most urgent (5 days) should be first"
        );
        assert_eq!(
            rows[1].0.path, "/b.txt",
            "Middle urgency (15 days) should be second"
        );
        assert_eq!(
            rows[2].0.path, "/c.txt",
            "Least urgent (30 days) should be last"
        );
    }

    #[test]
    fn sort_entries_directories_sort_by_expiration_like_files() {
        let mut rows = vec![
            (test_entry("/a.txt", 100, Some(1000)), 10),
            (test_entry_dir("/subdir"), 3),
            (test_entry("/b.txt", 100, Some(1000)), 20),
        ];

        sort_entry_rows(&mut rows, SortMode::Expiration);

        assert_eq!(
            rows[0].0.path, "/subdir",
            "Most urgent (dir with 3 days) first"
        );
        assert_eq!(rows[1].0.path, "/a.txt", "File with 10 days second");
        assert_eq!(rows[2].0.path, "/b.txt", "Least urgent (20 days) last");
    }

    #[test]
    fn sort_entries_by_size_largest_first() {
        let mut rows = vec![
            (test_entry("/a.txt", 100, Some(1000)), 10),
            (test_entry("/b.txt", 500, Some(1000)), 10),
            (test_entry("/c.txt", 250, Some(1000)), 10),
        ];

        sort_entry_rows(&mut rows, SortMode::Size);

        assert_eq!(rows[0].0.path, "/b.txt", "Largest (500) should be first");
        assert_eq!(rows[1].0.path, "/c.txt", "Middle (250) should be second");
        assert_eq!(rows[2].0.path, "/a.txt", "Smallest (100) should be last");
    }

    #[test]
    fn sort_entries_by_name_alphabetical_dirs_first() {
        let mut rows = vec![
            (test_entry("/zebra.txt", 100, Some(1000)), 10),
            (test_entry_dir("/alpha_dir"), 15),
            (test_entry("/mango.txt", 100, Some(1000)), 10),
        ];

        sort_entry_rows(&mut rows, SortMode::Name);

        assert_eq!(rows[0].0.path, "/alpha_dir", "Directory should come first");
        assert_eq!(rows[1].0.path, "/mango.txt", "Mango should be second");
        assert_eq!(rows[2].0.path, "/zebra.txt", "Zebra should be last");
    }

    #[test]
    fn sort_entries_empty_list_does_not_panic() {
        let mut rows: Vec<(crate::db::Entry, i64)> = vec![];

        // Should not panic for any sort mode
        sort_entry_rows(&mut rows, SortMode::Expiration);
        sort_entry_rows(&mut rows, SortMode::Size);
        sort_entry_rows(&mut rows, SortMode::Name);
        sort_entry_rows(&mut rows, SortMode::Modified);

        assert_eq!(rows.len(), 0, "Empty list should remain empty");
    }

    #[test]
    fn sort_entries_by_modified_most_recent_first() {
        let mut rows = vec![
            (test_entry("/a.txt", 100, Some(1000)), 10), // Oldest
            (test_entry("/b.txt", 100, Some(5000)), 10), // Most recent
            (test_entry("/c.txt", 100, Some(3000)), 10), // Middle
        ];

        sort_entry_rows(&mut rows, SortMode::Modified);

        assert_eq!(
            rows[0].0.path, "/b.txt",
            "Most recent (5000) should be first"
        );
        assert_eq!(rows[1].0.path, "/c.txt", "Middle (3000) should be second");
        assert_eq!(rows[2].0.path, "/a.txt", "Oldest (1000) should be last");
    }

    // Tests for determine_row_style

    #[test]
    fn determine_row_style_ignored_is_gray() {
        let style = determine_row_style("ignored", Some(30), 14);
        assert_eq!(style.fg, Some(Color::DarkGray));
    }

    #[test]
    fn determine_row_style_pending_is_red() {
        let style = determine_row_style("pending", Some(30), 14);
        assert_eq!(style.fg, Some(Color::Red));
    }

    #[test]
    fn determine_row_style_approved_is_red() {
        let style = determine_row_style("approved", Some(30), 14);
        assert_eq!(style.fg, Some(Color::Red));
    }

    #[test]
    fn determine_row_style_overdue_is_red() {
        let style = determine_row_style("tracked", Some(0), 14);
        assert_eq!(style.fg, Some(Color::Red));

        let style = determine_row_style("tracked", Some(-5), 14);
        assert_eq!(style.fg, Some(Color::Red));
    }

    #[test]
    fn determine_row_style_within_warning_is_yellow() {
        let style = determine_row_style("tracked", Some(14), 14);
        assert_eq!(style.fg, Some(Color::Yellow));

        let style = determine_row_style("tracked", Some(7), 14);
        assert_eq!(style.fg, Some(Color::Yellow));

        let style = determine_row_style("tracked", Some(1), 14);
        assert_eq!(style.fg, Some(Color::Yellow));
    }

    #[test]
    fn determine_row_style_safe_is_green() {
        let style = determine_row_style("tracked", Some(15), 14);
        assert_eq!(style.fg, Some(Color::Green));

        let style = determine_row_style("tracked", Some(90), 14);
        assert_eq!(style.fg, Some(Color::Green));
    }

    #[test]
    fn determine_row_style_no_mtime_is_default() {
        let style = determine_row_style("tracked", None, 14);
        assert_eq!(style.fg, None);
    }

    #[test]
    fn determine_row_style_status_takes_precedence_over_expiration() {
        // Even if days remaining is safe, pending status should show red
        let style = determine_row_style("pending", Some(90), 14);
        assert_eq!(style.fg, Some(Color::Red));

        // Ignored should be gray regardless of expiration
        let style = determine_row_style("ignored", Some(-10), 14);
        assert_eq!(style.fg, Some(Color::DarkGray));
    }

    // Tests for format_timestamp

    #[test]
    fn format_timestamp_formats_correctly() {
        // 2024-01-15 14:32:45 UTC
        let ts = 1_705_329_165;
        let result = format_timestamp(ts);

        // Verify it contains expected date components
        // (exact time depends on local timezone, so we check for date)
        assert!(
            result.contains("2024"),
            "Should contain year 2024, got: {result}"
        );
        assert!(
            result.contains("01") || result.contains("1-"),
            "Should contain month 01, got: {result}"
        );
        assert!(
            result.contains("15") || result.contains("14") || result.contains("16"),
            "Should contain day 14-16 (timezone variance), got: {result}"
        );
    }

    #[test]
    fn format_timestamp_handles_unix_epoch() {
        let result = format_timestamp(0);
        // Unix epoch is 1970-01-01 00:00:00 UTC
        // Local timezone may shift this slightly, but should contain 1969 or 1970
        assert!(
            result.contains("1970") || result.contains("1969"),
            "Should contain year 1970 or 1969, got: {result}"
        );
    }

    #[test]
    fn format_timestamp_handles_invalid_timestamp_gracefully() {
        // Timestamp that would fail conversion - should fall back to UNIX_EPOCH
        let result = format_timestamp(i64::MIN);
        // Should not panic and should return something reasonable
        assert!(
            !result.is_empty(),
            "Should return non-empty string even for invalid timestamp"
        );
    }

    #[test]
    fn format_timestamp_includes_time_components() {
        // 2024-06-15 09:30:45 UTC
        let ts = 1_718_443_845;
        let result = format_timestamp(ts);

        // Verify format includes time separator ':'
        assert!(
            result.contains(':'),
            "Should include time separator ':', got: {result}"
        );

        // Verify format roughly matches "YYYY-MM-DD HH:MM:SS"
        // We check for two colons (HH:MM:SS)
        assert_eq!(
            result.matches(':').count(),
            2,
            "Should have exactly 2 colons for HH:MM:SS format, got: {result}"
        );
    }

    // Tests for expiration_indicator_entry

    fn make_test_entry(status: &str, deferred_until: Option<i64>) -> crate::db::Entry {
        crate::db::Entry {
            id: 1,
            root_id: 1,
            path: "/test/file.txt".to_string(),
            parent_path: "/test".to_string(),
            is_dir: false,
            size_bytes: 100,
            mtime: Some(0),
            tracked_since: None,
            status: status.to_string(),
            deferred_until,
            created_at: 0,
            updated_at: 0,
        }
    }

    #[test]
    fn expiration_indicator_entry_overdue_is_red_circle() {
        let entry = make_test_entry("tracked", None);
        let (symbol, color) = expiration_indicator_entry("tracked", 0, 14, &entry);
        assert_eq!(symbol, "●", "Overdue (0 days) should show filled circle");
        assert_eq!(color, Color::Red, "Overdue should be red");

        let (symbol, color) = expiration_indicator_entry("tracked", -5, 14, &entry);
        assert_eq!(symbol, "●", "Negative days should show filled circle");
        assert_eq!(color, Color::Red, "Overdue should be red");
    }

    #[test]
    fn expiration_indicator_entry_within_warning_is_yellow_triangle() {
        let entry = make_test_entry("tracked", None);
        let (symbol, color) = expiration_indicator_entry("tracked", 1, 14, &entry);
        assert_eq!(symbol, "⚠", "1 day remaining should show warning triangle");
        assert_eq!(color, Color::Yellow, "Warning should be yellow");

        let (symbol, color) = expiration_indicator_entry("tracked", 14, 14, &entry);
        assert_eq!(symbol, "⚠", "At warning threshold should show warning");
        assert_eq!(color, Color::Yellow, "Warning should be yellow");
    }

    #[test]
    fn expiration_indicator_entry_safe_is_empty() {
        let entry = make_test_entry("tracked", None);
        let (symbol, color) = expiration_indicator_entry("tracked", 15, 14, &entry);
        assert_eq!(symbol, " ", "Safe entries should show no indicator");
        assert_eq!(color, Color::Reset, "Safe should use reset color");

        let (symbol, _) = expiration_indicator_entry("tracked", 90, 14, &entry);
        assert_eq!(symbol, " ", "Very safe entries should show no indicator");
    }

    #[test]
    fn expiration_indicator_entry_ignored_is_gray_dash() {
        let entry = make_test_entry("ignored", None);
        let (symbol, color) = expiration_indicator_entry("ignored", -30, 14, &entry);
        assert_eq!(symbol, "—", "Ignored entries should show dash");
        assert_eq!(color, Color::DarkGray, "Ignored should be gray");

        // Even if "overdue", ignored entries show dash
        let (symbol, _) = expiration_indicator_entry("ignored", 0, 14, &entry);
        assert_eq!(
            symbol, "—",
            "Ignored overdue entries should still show dash"
        );
    }

    #[test]
    fn expiration_indicator_entry_deferred_uses_deferred_until() {
        // Deferred entry with plenty of time on deferral
        let future = jiff::Timestamp::now().as_second() + (30 * 86400); // 30 days from now
        let entry = make_test_entry("deferred", Some(future));
        let (symbol, _) = expiration_indicator_entry("deferred", -100, 14, &entry);
        assert_eq!(
            symbol, " ",
            "Deferred with time remaining should show no indicator"
        );

        // Deferred entry with deferral expiring soon
        let soon = jiff::Timestamp::now().as_second() + (5 * 86400); // 5 days from now
        let entry = make_test_entry("deferred", Some(soon));
        let (symbol, color) = expiration_indicator_entry("deferred", -100, 14, &entry);
        assert_eq!(symbol, "⚠", "Deferred expiring soon should show warning");
        assert_eq!(color, Color::Yellow);
    }
}
