//! Widget rendering for the TUI.

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

use crate::config::Config;
use crate::db::Database;
use crate::scanner::calculate_expiration;

use super::{App, SortMode, View};

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
        View::DirectoryList => render_directory_list(app, config, db, frame, chunks[0]),
        View::DirectoryDetail => render_directory_detail(app, frame, chunks[0]),
        View::PendingApprovals => render_pending_approvals(app, frame, chunks[0]),
        View::AuditLog => render_audit_log(app, frame, chunks[0]),
        View::Help => render_help(app, frame, chunks[0]),
    }

    // Render the footer
    render_footer(app, frame, chunks[1]);
}

/// Render the directory list view.
///
/// Displays all tracked directories with aggregate stats in header and sortable table.
// Allow: This function orchestrates rendering and needs to handle multiple concerns:
// fetching data, sorting, styling, and building the table. Extracting helpers would
// make the code less readable as the rendering logic is inherently sequential.
#[allow(clippy::too_many_lines)]
fn render_directory_list(
    app: &App,
    config: &Config,
    db: &Database,
    frame: &mut Frame,
    area: ratatui::layout::Rect,
) {
    // Split into header area and table area
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header with stats
            Constraint::Min(0),    // Table
        ])
        .split(area);

    // Fetch directories from database
    let Ok(directories) = db.list_directories(None) else {
        // Error handling: show error message
        let error_text = Paragraph::new("Error loading directories from database")
            .block(Block::default().borders(Borders::ALL).title("Error"))
            .style(Style::default().fg(Color::Red));
        frame.render_widget(error_text, area);
        return;
    };

    // Fetch stats from database
    let stats = match db.get_stats() {
        Ok(s) => s,
        Err(_) => {
            // Use zeros if stats aren't available
            crate::db::Stats {
                total_tracked_paths: 0,
                total_size_bytes: 0,
                paths_within_warning: 0,
                paths_pending_approval: 0,
                paths_overdue: 0,
            }
        }
    };

    // Render header with stats
    render_header(&stats, config, app, frame, chunks[0]);

    // Prepare directory rows with calculated expiration
    let mut dir_rows: Vec<_> = directories
        .into_iter()
        .map(|dir| {
            let days_remaining = dir
                .oldest_mtime
                .map(|mtime| calculate_expiration(mtime, config.expiration_days));
            (dir, days_remaining)
        })
        .collect();

    // Sort based on current sort mode
    match app.sort_mode() {
        SortMode::Expiration => {
            // Ascending (most urgent first) - None sorts to beginning
            dir_rows.sort_by(|a, b| a.1.cmp(&b.1));
        }
        SortMode::Size => {
            // Descending (largest first)
            dir_rows.sort_by(|a, b| b.0.size_bytes.cmp(&a.0.size_bytes));
        }
        SortMode::Name => {
            // Alphabetical ascending
            dir_rows.sort_by(|a, b| a.0.path.cmp(&b.0.path));
        }
    }

    // Update list length for navigation
    app.list_len.set(dir_rows.len());

    // Clamp selected index to valid range
    let selected_idx = if dir_rows.is_empty() {
        0
    } else {
        app.selected_index().min(dir_rows.len() - 1)
    };

    // Build table rows
    let rows: Vec<Row> = dir_rows
        .iter()
        .enumerate()
        .map(|(idx, (dir, days_remaining))| {
            let path_cell = Cell::from(dir.path.as_str());
            // Allow: size_bytes is guaranteed non-negative by schema, but stored as i64 for SQLite compatibility
            #[allow(clippy::cast_sign_loss)]
            let size_cell = Cell::from(format_bytes(dir.size_bytes as u64));

            let expires_str = days_remaining.map_or_else(
                || "N/A".to_string(),
                |days| {
                    if days >= 0 {
                        format!("{days} days")
                    } else {
                        format!("{} days ago", -days)
                    }
                },
            );
            let expires_cell = Cell::from(expires_str);

            let status_cell = Cell::from(dir.status.as_str());

            // Determine row color based on status and expiration
            let row_style = determine_row_style(&dir.status, *days_remaining, config.warning_days);

            // Highlight selected row
            let style = if idx == selected_idx {
                row_style.add_modifier(Modifier::REVERSED)
            } else {
                row_style
            };

            Row::new(vec![path_cell, size_cell, expires_cell, status_cell]).style(style)
        })
        .collect();

    // Build table
    let widths = [
        Constraint::Percentage(50), // Path
        Constraint::Percentage(15), // Size
        Constraint::Percentage(20), // Expires
        Constraint::Percentage(15), // Status
    ];

    let sort_indicator = match app.sort_mode() {
        SortMode::Expiration => " (by expiration)",
        SortMode::Size => " (by size)",
        SortMode::Name => " (by name)",
    };

    let table = Table::new(rows, widths)
        .block(
            Block::default()
                .title(format!("Directories{sort_indicator}"))
                .borders(Borders::ALL),
        )
        .header(
            Row::new(vec![
                Cell::from("Path"),
                Cell::from("Size"),
                Cell::from("Expires"),
                Cell::from("Status"),
            ])
            .style(Style::default().add_modifier(Modifier::BOLD))
            .bottom_margin(1),
        );

    frame.render_widget(table, chunks[1]);
}

/// Render the header with aggregate statistics.
fn render_header(
    stats: &crate::db::Stats,
    _config: &Config,
    _app: &App,
    frame: &mut Frame,
    area: ratatui::layout::Rect,
) {
    // Allow: size values are guaranteed non-negative by schema, but stored as i64 for SQLite compatibility
    #[allow(clippy::cast_sign_loss)]
    let total_size_str = format_bytes(stats.total_size_bytes as u64);

    let header_text = format!(
        "Total: {} paths, {} | Pending: {} | Within warning: {} | Overdue: {}",
        stats.total_tracked_paths,
        total_size_str,
        stats.paths_pending_approval,
        stats.paths_within_warning,
        stats.paths_overdue
    );

    let header = Paragraph::new(header_text)
        .block(Block::default().borders(Borders::ALL).title("Overview"))
        .style(Style::default());

    frame.render_widget(header, area);
}

/// Determine row style based on status and expiration.
fn determine_row_style(status: &str, days_remaining: Option<i64>, warning_days: u32) -> Style {
    // Ignored paths are gray
    if status == "ignored" {
        return Style::default().fg(Color::DarkGray);
    }

    // Pending or approved paths are red (require attention)
    if status == "pending" || status == "approved" {
        return Style::default().fg(Color::Red);
    }

    // Check expiration status
    match days_remaining {
        None => Style::default(), // No mtime, use default
        Some(days) if days <= 0 => Style::default().fg(Color::Red), // Overdue
        Some(days) if days <= i64::from(warning_days) => Style::default().fg(Color::Yellow), // Warning
        _ => Style::default().fg(Color::Green),                                              // Safe
    }
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

/// Render the directory detail view.
///
/// This is a placeholder implementation that will be expanded in US-016.
fn render_directory_detail(_app: &App, frame: &mut Frame, area: ratatui::layout::Rect) {
    let block = Block::default()
        .title("Directory Details")
        .borders(Borders::ALL)
        .style(Style::default());

    let text =
        Paragraph::new("Directory detail view (US-016)\n\nPress 'q' or 'h' or Esc to go back")
            .block(block);

    frame.render_widget(text, area);
}

/// Render the pending approvals view.
///
/// This is a placeholder implementation that will be expanded in US-020.
fn render_pending_approvals(_app: &App, frame: &mut Frame, area: ratatui::layout::Rect) {
    let block = Block::default()
        .title("Pending Approvals")
        .borders(Borders::ALL)
        .style(Style::default());

    let text = Paragraph::new("Pending approvals view (US-020)\n\nPress 'q' or Esc to go back")
        .block(block);

    frame.render_widget(text, area);
}

/// Render the audit log view.
///
/// This is a placeholder implementation that will be expanded in US-021.
fn render_audit_log(_app: &App, frame: &mut Frame, area: ratatui::layout::Rect) {
    let block = Block::default()
        .title("Audit Log")
        .borders(Borders::ALL)
        .style(Style::default());

    let text =
        Paragraph::new("Audit log view (US-021)\n\nPress 'q' or Esc to go back").block(block);

    frame.render_widget(text, area);
}

/// Render the help view.
///
/// This is a placeholder implementation that will be expanded in US-022.
fn render_help(_app: &App, frame: &mut Frame, area: ratatui::layout::Rect) {
    let block = Block::default()
        .title("Help")
        .borders(Borders::ALL)
        .style(Style::default());

    let help_text = r"Keybindings:

Navigation:
  j/k or ↓/↑  - Move selection down/up
  g/G         - Go to top/bottom
  
Actions:
  Enter or l  - View directory details
  q           - Quit
  Ctrl+C      - Quit
  
Views:
  p           - Pending approvals
  a           - Audit log
  ?           - This help screen

Press any key to close";

    let text = Paragraph::new(help_text).block(block);

    frame.render_widget(text, area);
}

/// Render the footer with context-sensitive keybinding hints.
fn render_footer(app: &App, frame: &mut Frame, area: ratatui::layout::Rect) {
    let hints = match app.view() {
        View::DirectoryList => {
            "[j/k] Navigate [g/G] Top/Bottom [s] Sort [Enter] Details [p] Pending [a] Audit [?] Help [q] Quit"
        }
        View::DirectoryDetail => "[j/k] Navigate [h/Esc] Back [q] Quit",
        View::PendingApprovals | View::AuditLog => "[j/k] Navigate [Esc] Back [q] Quit",
        View::Help => "[Any key] Close",
    };

    let footer = Paragraph::new(hints).style(Style::default().fg(Color::Black).bg(Color::Gray));

    frame.render_widget(footer, area);
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
