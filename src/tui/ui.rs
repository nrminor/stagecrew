//! Widget rendering for the TUI.

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

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

    // Render confirmation modal if pending approval
    if let Some((_, path)) = app.pending_approval() {
        render_confirmation_modal(frame, path);
    }

    // Render deferral input modal if pending deferral
    if let Some(deferral) = app.pending_deferral() {
        render_deferral_modal(
            frame,
            &deferral.path,
            &deferral.input,
            deferral.default_days,
        );
    }

    // Render ignore confirmation modal if pending ignore
    if let Some((_, path)) = app.pending_ignore() {
        render_ignore_modal(frame, path);
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

    // Split content area horizontally: sidebar | main panel
    let h_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(20), // Sidebar for directories
            Constraint::Percentage(80), // Main panel for files
        ])
        .split(v_chunks[1]);

    // Render header with stats
    render_file_view_header(config, db, frame, v_chunks[0]);

    // Render sidebar with tracked directories
    render_sidebar(app, db, frame, h_chunks[0]);

    // Render main panel with files from selected directory
    render_main_file_panel(app, config, db, frame, h_chunks[1]);
}

/// Render the header showing stats for the current file view.
fn render_file_view_header(
    _config: &Config,
    db: &Database,
    frame: &mut Frame,
    area: ratatui::layout::Rect,
) {
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
                last_scan_completed: None,
            }
        }
    };

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

/// Render the sidebar showing tracked directories.
fn render_sidebar(app: &App, db: &Database, frame: &mut Frame, area: ratatui::layout::Rect) {
    // Fetch directories from database
    let Ok(directories) = db.list_directories(None) else {
        let error_text = Paragraph::new("Error loading directories")
            .block(Block::default().borders(Borders::ALL).title("Directories"))
            .style(Style::default().fg(Color::Red));
        frame.render_widget(error_text, area);
        return;
    };

    // Update sidebar list length for navigation
    app.sidebar_len.set(directories.len());

    // Clamp selected index
    let selected_idx = if directories.is_empty() {
        0
    } else {
        app.sidebar_selected_index().min(directories.len() - 1)
    };

    // Set current directory ID based on selection
    if let Some(dir) = directories.get(selected_idx) {
        app.current_directory_id.set(Some(dir.id));
    }

    // Build sidebar rows
    let rows: Vec<Row> = directories
        .iter()
        .enumerate()
        .map(|(idx, dir)| {
            // Extract just the directory name from full path
            let dir_name = std::path::Path::new(&dir.path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(&dir.path);

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
            .block(Block::default().borders(Borders::ALL).title("Directories"))
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(empty_text, area);
        return;
    }

    let table = Table::new(rows, [Constraint::Percentage(100)]).block(
        Block::default()
            .title("Directories")
            .borders(Borders::ALL)
            .border_style(if app.focus_panel() == FocusPanel::Sidebar {
                Style::default().fg(Color::Cyan)
            } else {
                Style::default()
            }),
    );

    frame.render_widget(table, area);
}

/// Render the main panel showing files from the selected directory.
// Allow: This function handles file loading, sorting, and table rendering which are
// sequential operations that form a cohesive rendering pipeline.
#[allow(clippy::too_many_lines)]
fn render_main_file_panel(
    app: &App,
    config: &Config,
    db: &Database,
    frame: &mut Frame,
    area: ratatui::layout::Rect,
) {
    // Get the current directory ID from sidebar selection
    let Some(directory_id) = app.current_directory_id() else {
        let message = Paragraph::new(
            "Select a directory from the sidebar\n\n(Use j/k to navigate, Tab to switch panels)",
        )
        .block(Block::default().borders(Borders::ALL).title("Files"))
        .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(message, area);
        return;
    };

    // Fetch files for this directory
    let Ok(files) = db.list_files_by_directory(directory_id) else {
        let error_text = Paragraph::new("Error loading files from database")
            .block(Block::default().borders(Borders::ALL).title("Files"))
            .style(Style::default().fg(Color::Red));
        frame.render_widget(error_text, area);
        return;
    };

    // Empty state
    if files.is_empty() {
        let empty_text = Paragraph::new("No files in this directory")
            .block(Block::default().borders(Borders::ALL).title("Files"))
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(empty_text, area);
        return;
    }

    // Sort files by expiration (most urgent first) by default
    let mut file_rows: Vec<_> = files
        .into_iter()
        .map(|file| {
            let days_remaining = calculate_expiration(file.mtime, config.expiration_days);
            (file, days_remaining)
        })
        .collect();

    // Sort based on current sort mode
    sort_file_rows(&mut file_rows, app.sort_mode());

    // Update file list length for navigation
    app.file_list_len.set(file_rows.len());

    // Clamp selected index
    let selected_idx = if file_rows.is_empty() {
        0
    } else {
        app.file_selected_index().min(file_rows.len() - 1)
    };

    // Build file table rows
    let rows: Vec<Row> = file_rows
        .iter()
        .enumerate()
        .map(|(idx, (file, days_remaining))| {
            // Extract filename from path
            let filename = std::path::Path::new(&file.path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(&file.path);
            let filename_cell = Cell::from(filename);

            // Format size
            // Allow: size_bytes is guaranteed non-negative by schema, but stored as i64 for SQLite compatibility
            #[allow(clippy::cast_sign_loss)]
            let size_cell = Cell::from(format_bytes(file.size_bytes as u64));

            // Format expiration
            let expires_str = if *days_remaining >= 0 {
                format!("{days_remaining} days")
            } else {
                format!("{} days ago", -days_remaining)
            };
            let expires_cell = Cell::from(expires_str);

            // Status (files don't have individual status yet, so show based on expiration)
            let status_str = if *days_remaining <= 0 {
                "overdue"
            } else if *days_remaining <= i64::from(config.warning_days) {
                "warning"
            } else {
                "tracked"
            };
            let status_cell = Cell::from(status_str);

            // Determine row color based on expiration
            let row_style = if *days_remaining <= 0 {
                Style::default().fg(Color::Red)
            } else if *days_remaining <= i64::from(config.warning_days) {
                Style::default().fg(Color::Yellow)
            } else {
                Style::default().fg(Color::Green)
            };

            // Highlight selected row and show focus
            let style = if idx == selected_idx {
                if app.focus_panel() == FocusPanel::MainPanel {
                    row_style.add_modifier(Modifier::REVERSED).fg(Color::Cyan)
                } else {
                    row_style.add_modifier(Modifier::REVERSED)
                }
            } else {
                row_style
            };

            Row::new(vec![filename_cell, size_cell, expires_cell, status_cell]).style(style)
        })
        .collect();

    // Build table
    let widths = [
        Constraint::Percentage(45), // Filename
        Constraint::Percentage(15), // Size
        Constraint::Percentage(20), // Expires
        Constraint::Percentage(20), // Status
    ];

    let sort_indicator = match app.sort_mode() {
        SortMode::Expiration => " (by expiration)",
        SortMode::Size => " (by size)",
        SortMode::Name => " (by name)",
    };

    let table = Table::new(rows, widths)
        .block(
            Block::default()
                .title(format!("Files{sort_indicator}"))
                .borders(Borders::ALL)
                .border_style(if app.focus_panel() == FocusPanel::MainPanel {
                    Style::default().fg(Color::Cyan)
                } else {
                    Style::default()
                }),
        )
        .header(
            Row::new(vec![
                Cell::from("Filename"),
                Cell::from("Size"),
                Cell::from("Expires"),
                Cell::from("Status"),
            ])
            .style(Style::default().add_modifier(Modifier::BOLD))
            .bottom_margin(1),
        );

    frame.render_widget(table, area);
}

/// Sort file rows according to the specified sort mode.
fn sort_file_rows(rows: &mut [(crate::db::File, i64)], sort_mode: SortMode) {
    match sort_mode {
        SortMode::Expiration => {
            // Ascending (most urgent first)
            rows.sort_by(|a, b| a.1.cmp(&b.1));
        }
        SortMode::Size => {
            // Descending (largest first)
            rows.sort_by(|a, b| b.0.size_bytes.cmp(&a.0.size_bytes));
        }
        SortMode::Name => {
            // Alphabetical ascending by filename
            rows.sort_by(|a, b| {
                let name_a = std::path::Path::new(&a.0.path)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or(&a.0.path);
                let name_b = std::path::Path::new(&b.0.path)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or(&b.0.path);
                name_a.cmp(name_b)
            });
        }
    }
}

/// LEGACY: Old header function - superseded by `render_file_view_header` in US-027.
#[allow(dead_code)]
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

/// LEGACY: Old directory sorting function - superseded by `sort_file_rows` in US-027.
#[allow(dead_code)]
fn sort_directory_rows(rows: &mut [(crate::db::Directory, Option<i64>)], sort_mode: SortMode) {
    match sort_mode {
        SortMode::Expiration => {
            // Ascending (most urgent first) - None sorts to beginning
            rows.sort_by(|a, b| a.1.cmp(&b.1));
        }
        SortMode::Size => {
            // Descending (largest first)
            rows.sort_by(|a, b| b.0.size_bytes.cmp(&a.0.size_bytes));
        }
        SortMode::Name => {
            // Alphabetical ascending
            rows.sort_by(|a, b| a.0.path.cmp(&b.0.path));
        }
    }
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

/// LEGACY: Old directory detail view - superseded by file-centric layout in US-027.
// Allow: Function kept for reference during transition. Will be removed in cleanup pass.
#[allow(dead_code, clippy::too_many_lines)]
fn render_directory_detail(
    app: &App,
    _config: &Config,
    db: &Database,
    frame: &mut Frame,
    area: ratatui::layout::Rect,
) {
    // Split into breadcrumb area and table area
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Breadcrumb header
            Constraint::Min(0),    // File table
        ])
        .split(area);

    // Get the current directory ID from app state
    let Some(directory_id) = app.current_directory_id() else {
        // No directory selected - show error message
        let error_text = Paragraph::new("No directory selected. Press 'h' to go back.")
            .block(Block::default().borders(Borders::ALL).title("Error"))
            .style(Style::default().fg(Color::Red));
        frame.render_widget(error_text, area);
        return;
    };

    // Fetch files for this directory
    let Ok(files) = db.list_files_by_directory(directory_id) else {
        // Error loading files
        let error_text = Paragraph::new("Error loading files from database")
            .block(Block::default().borders(Borders::ALL).title("Error"))
            .style(Style::default().fg(Color::Red));
        frame.render_widget(error_text, area);
        return;
    };

    // Get directory info for breadcrumb (using first file's path or fallback)
    let directory_path = if let Some(first_file) = files.first() {
        // Extract directory path from file path
        std::path::Path::new(&first_file.path).parent().map_or_else(
            || "Unknown".to_string(),
            |p| p.to_string_lossy().to_string(),
        )
    } else {
        // Try to get directory info from database
        if let Ok(directories) = db.list_directories(None) {
            directories
                .iter()
                .find(|d| d.id == directory_id)
                .map_or_else(|| "Unknown".to_string(), |d| d.path.clone())
        } else {
            "Unknown".to_string()
        }
    };

    // Render breadcrumb header
    let breadcrumb = Paragraph::new(format!("Viewing: {directory_path}"))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Directory Details"),
        )
        .style(Style::default());
    frame.render_widget(breadcrumb, chunks[0]);

    // Sort files by size descending (largest first)
    let mut sorted_files = files;
    sorted_files.sort_by(|a, b| b.size_bytes.cmp(&a.size_bytes));

    // Update list length for navigation
    app.file_list_len.set(sorted_files.len());

    // Clamp selected index to valid range
    let selected_idx = if sorted_files.is_empty() {
        0
    } else {
        app.file_selected_index().min(sorted_files.len() - 1)
    };

    // Build table rows
    let rows: Vec<Row> = sorted_files
        .iter()
        .enumerate()
        .map(|(idx, file)| {
            // Extract filename from path
            let filename = std::path::Path::new(&file.path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(&file.path);
            let filename_cell = Cell::from(filename);

            // Format size
            // Allow: size_bytes is guaranteed non-negative by schema, but stored as i64 for SQLite compatibility
            #[allow(clippy::cast_sign_loss)]
            let size_cell = Cell::from(format_bytes(file.size_bytes as u64));

            // Format modified time
            let modified_str = match jiff::Timestamp::from_second(file.mtime) {
                Ok(timestamp) => {
                    // Format as YYYY-MM-DD HH:MM
                    timestamp
                        .to_zoned(jiff::tz::TimeZone::system())
                        .strftime("%Y-%m-%d %H:%M")
                        .to_string()
                }
                Err(_) => "Invalid date".to_string(),
            };
            let modified_cell = Cell::from(modified_str);

            // Highlight selected row
            let style = if idx == selected_idx {
                Style::default().add_modifier(Modifier::REVERSED)
            } else {
                Style::default()
            };

            Row::new(vec![filename_cell, size_cell, modified_cell]).style(style)
        })
        .collect();

    // Empty state message
    if rows.is_empty() {
        let empty_text = Paragraph::new("No files in this directory")
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Files (by size)"),
            )
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(empty_text, chunks[1]);
        return;
    }

    // Build table
    let widths = [
        Constraint::Percentage(50), // Filename
        Constraint::Percentage(20), // Size
        Constraint::Percentage(30), // Modified
    ];

    let table = Table::new(rows, widths)
        .block(
            Block::default()
                .title("Files (by size)")
                .borders(Borders::ALL),
        )
        .header(
            Row::new(vec![
                Cell::from("Filename"),
                Cell::from("Size"),
                Cell::from("Modified"),
            ])
            .style(Style::default().add_modifier(Modifier::BOLD))
            .bottom_margin(1),
        );

    frame.render_widget(table, chunks[1]);
}

/// LEGACY: Old pending approvals view - superseded by file-centric layout in US-027.
// Allow: Function kept for reference during transition. Will be removed in cleanup pass.
#[allow(dead_code, clippy::too_many_lines)]
fn render_pending_approvals(
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
            Constraint::Length(3), // Header
            Constraint::Min(0),    // Table
        ])
        .split(area);

    // Fetch only pending directories from database
    let Ok(directories) = db.list_directories(Some("pending")) else {
        // Error handling: show error message
        let error_text = Paragraph::new("Error loading directories from database")
            .block(Block::default().borders(Borders::ALL).title("Error"))
            .style(Style::default().fg(Color::Red));
        frame.render_widget(error_text, area);
        return;
    };

    // Render header showing pending count
    render_pending_header(directories.len(), frame, chunks[0]);

    // Handle empty state
    if directories.is_empty() {
        let empty_text = Paragraph::new(
            "No pending directories.\n\nPress 'q' or Esc to return to directory list.",
        )
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Pending Approvals"),
        )
        .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(empty_text, chunks[1]);
        return;
    }

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
    sort_directory_rows(&mut dir_rows, app.sort_mode());

    // Update list length for navigation
    app.sidebar_len.set(dir_rows.len());

    // Clamp selected index to valid range
    let selected_idx = app.sidebar_selected_index().min(dir_rows.len() - 1);

    // Set current directory ID for potential actions
    if let Some((dir, _)) = dir_rows.get(selected_idx) {
        app.current_directory_id.set(Some(dir.id));
    }

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
                .title(format!("Pending Approvals{sort_indicator}"))
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

/// LEGACY: Header for old pending approvals view - no longer used.
#[allow(dead_code)]
fn render_pending_header(pending_count: usize, frame: &mut Frame, area: ratatui::layout::Rect) {
    let header_text = format!("Pending directories awaiting approval: {pending_count}");

    let header = Paragraph::new(header_text)
        .block(Block::default().borders(Borders::ALL).title("Overview"))
        .style(Style::default());

    frame.render_widget(header, area);
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
fn render_help(_app: &App, frame: &mut Frame, area: ratatui::layout::Rect) {
    let block = Block::default()
        .title("Stagecrew - Keybinding Reference")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan))
        .style(Style::default());

    let help_text = r"Navigation:
  j / ↓       Move selection down
  k / ↑       Move selection up
  g           Jump to top of list
  G           Jump to bottom of list
  
Views:
  Enter / l   View directory details (from directory list)
  h / Esc     Return to previous view
  p           Show pending approvals
  a           Show audit log
  ?           Show this help screen
  
Actions (Directory List & Pending Approvals):
  x           Approve directory for removal
  d           Defer directory expiration (reset clock)
  i           Permanently ignore directory
  s           Cycle sort mode (Expiration → Size → Name)
  
Other:
  q           Quit application
  Ctrl+C      Quit application

Press any key to close this help screen";

    let text = Paragraph::new(help_text).block(block);

    frame.render_widget(text, area);
}

/// Render the footer with context-sensitive keybinding hints.
fn render_footer(app: &App, frame: &mut Frame, area: ratatui::layout::Rect) {
    let hints = match app.view() {
        View::FileList => {
            "[j/k] Navigate [g/G] Top/Bottom [Tab/h/l] Switch panel [s] Sort [a] Audit [?] Help [q] Quit"
        }
        View::AuditLog => "[j/k] Navigate [g/G] Top/Bottom [Esc] Back [q] Quit",
        View::Help => "[Any key] Close",
    };

    let footer = Paragraph::new(hints).style(Style::default().fg(Color::Black).bg(Color::Gray));

    frame.render_widget(footer, area);
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

    // Clear the area behind the modal (create a simple background)
    let background = Block::default()
        .style(Style::default().bg(Color::Black))
        .borders(Borders::NONE);

    frame.render_widget(background, modal_area);
    frame.render_widget(modal, modal_area);
}

/// Render a deferral input modal for entering days to defer.
///
/// Displays a centered modal prompting the user to enter the number of days
/// to defer expiration. Shows the current input and the default value.
fn render_deferral_modal(frame: &mut Frame, path: &str, input: &str, default_days: u32) {
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
    let title = "Defer Expiration";
    let display_input = if input.is_empty() {
        format!("[{default_days}]")
    } else {
        input.to_string()
    };
    let message = format!(
        "Defer expiration for:\n{path}\n\nDays to defer: {display_input}\n\n(Enter to confirm, Esc to cancel)"
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

    // Clear the area behind the modal (create a simple background)
    let background = Block::default()
        .style(Style::default().bg(Color::Black))
        .borders(Borders::NONE);

    frame.render_widget(background, modal_area);
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

    // Clear the area behind the modal (create a simple background)
    let background = Block::default()
        .style(Style::default().bg(Color::Black))
        .borders(Borders::NONE);

    frame.render_widget(background, modal_area);
    frame.render_widget(modal, modal_area);
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create a minimal Directory for testing
    fn test_directory(path: &str, size_bytes: i64) -> crate::db::Directory {
        let now = jiff::Timestamp::now().as_second();
        crate::db::Directory {
            id: 0,
            path: path.to_string(),
            size_bytes,
            file_count: 0,
            oldest_mtime: None,
            last_scanned: Some(now),
            status: "tracked".to_string(),
            deferred_until: None,
            created_at: now,
            updated_at: now,
        }
    }

    // Tests for sort_directory_rows

    #[test]
    fn sort_by_expiration_most_urgent_first() {
        let mut rows = vec![
            (test_directory("/c", 100), Some(30)), // 30 days remaining
            (test_directory("/a", 200), Some(5)),  // 5 days remaining (most urgent)
            (test_directory("/b", 150), Some(15)), // 15 days remaining
        ];

        sort_directory_rows(&mut rows, SortMode::Expiration);

        assert_eq!(rows[0].0.path, "/a", "Most urgent (5 days) should be first");
        assert_eq!(
            rows[1].0.path, "/b",
            "Middle urgency (15 days) should be second"
        );
        assert_eq!(
            rows[2].0.path, "/c",
            "Least urgent (30 days) should be last"
        );
    }

    #[test]
    fn sort_by_expiration_none_sorts_to_beginning() {
        let mut rows = vec![
            (test_directory("/c", 100), Some(30)),
            (test_directory("/a", 200), Some(5)),
            (test_directory("/d", 50), None), // No mtime
        ];

        sort_directory_rows(&mut rows, SortMode::Expiration);

        assert_eq!(
            rows[0].0.path, "/d",
            "Directory with no mtime (None) should sort to beginning"
        );
        assert_eq!(rows[1].0.path, "/a", "Most urgent should be second");
        assert_eq!(rows[2].0.path, "/c", "Least urgent should be last");
    }

    #[test]
    fn sort_by_expiration_handles_negative_values() {
        let mut rows = vec![
            (test_directory("/a", 100), Some(-10)), // Overdue by 10 days
            (test_directory("/b", 100), Some(5)),   // 5 days remaining
            (test_directory("/c", 100), Some(-30)), // Overdue by 30 days
        ];

        sort_directory_rows(&mut rows, SortMode::Expiration);

        assert_eq!(
            rows[0].0.path, "/c",
            "Most overdue (-30) should be first (most urgent)"
        );
        assert_eq!(rows[1].0.path, "/a", "Less overdue (-10) should be second");
        assert_eq!(rows[2].0.path, "/b", "Not overdue (5) should be last");
    }

    #[test]
    fn sort_by_size_largest_first() {
        let mut rows = vec![
            (test_directory("/a", 100), Some(10)),
            (test_directory("/b", 500), Some(10)),
            (test_directory("/c", 250), Some(10)),
        ];

        sort_directory_rows(&mut rows, SortMode::Size);

        assert_eq!(rows[0].0.path, "/b", "Largest (500) should be first");
        assert_eq!(rows[1].0.path, "/c", "Middle (250) should be second");
        assert_eq!(rows[2].0.path, "/a", "Smallest (100) should be last");
    }

    #[test]
    fn sort_by_name_alphabetical() {
        let mut rows = vec![
            (test_directory("/zebra", 100), Some(10)),
            (test_directory("/alpha", 100), Some(10)),
            (test_directory("/mango", 100), Some(10)),
        ];

        sort_directory_rows(&mut rows, SortMode::Name);

        assert_eq!(rows[0].0.path, "/alpha", "Alpha should be first");
        assert_eq!(rows[1].0.path, "/mango", "Mango should be second");
        assert_eq!(rows[2].0.path, "/zebra", "Zebra should be last");
    }

    #[test]
    fn sort_empty_list_does_not_panic() {
        let mut rows: Vec<(crate::db::Directory, Option<i64>)> = vec![];

        // Should not panic for any sort mode
        sort_directory_rows(&mut rows, SortMode::Expiration);
        sort_directory_rows(&mut rows, SortMode::Size);
        sort_directory_rows(&mut rows, SortMode::Name);

        assert_eq!(rows.len(), 0, "Empty list should remain empty");
    }

    #[test]
    fn sort_single_item_is_trivial() {
        let mut rows = vec![(test_directory("/only", 100), Some(10))];

        sort_directory_rows(&mut rows, SortMode::Expiration);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0.path, "/only");

        sort_directory_rows(&mut rows, SortMode::Size);
        assert_eq!(rows[0].0.path, "/only");

        sort_directory_rows(&mut rows, SortMode::Name);
        assert_eq!(rows[0].0.path, "/only");
    }

    #[test]
    fn sort_by_expiration_with_equal_values() {
        let mut rows = vec![
            (test_directory("/c", 100), Some(10)),
            (test_directory("/a", 200), Some(10)),
            (test_directory("/b", 150), Some(10)),
        ];

        sort_directory_rows(&mut rows, SortMode::Expiration);

        // All have same expiration, so order is stable (depends on Rust's stable sort)
        // Just verify they're all still present
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().any(|(d, _)| d.path == "/a"));
        assert!(rows.iter().any(|(d, _)| d.path == "/b"));
        assert!(rows.iter().any(|(d, _)| d.path == "/c"));
    }

    #[test]
    fn sort_by_size_with_equal_values() {
        let mut rows = vec![
            (test_directory("/c", 100), Some(10)),
            (test_directory("/a", 100), Some(10)),
            (test_directory("/b", 100), Some(10)),
        ];

        sort_directory_rows(&mut rows, SortMode::Size);

        // All have same size, so order is stable
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().any(|(d, _)| d.path == "/a"));
        assert!(rows.iter().any(|(d, _)| d.path == "/b"));
        assert!(rows.iter().any(|(d, _)| d.path == "/c"));
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
}
