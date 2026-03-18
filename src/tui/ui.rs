//! Widget rendering for the TUI.

use std::sync::OnceLock;

use ratatui::Frame;
use ratatui::layout::Margin;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, BorderType, Borders, Cell, Clear, HighlightSpacing, Padding, Paragraph, Row, Scrollbar,
    ScrollbarOrientation, ScrollbarState, Table, Tabs, Wrap,
};

use crate::audit::AuditService;
use crate::config::Config;
use crate::db::Database;
use crate::removal::{DryRunResult, RemovalMethod};
use crate::scanner::calculate_expiration;

use super::TuiContext;

use super::{
    App, FocusPanel, PendingAuditExport, PendingQuotaTarget, QuotaTargetFocus, SortMode, View,
};

/// Semantic color palette for consistent styling across the TUI.
/// Using RGB values ensures colors look the same regardless of terminal theme,
/// and enables smooth gradient interpolation for the row fade effect.
mod palette {
    use ratatui::style::Color;

    /// Safe/healthy state - files with plenty of time remaining
    /// Uses Terminal.app's green for a mellower appearance than pure bright green
    pub const GREEN: Color = Color::Rgb(0, 166, 0);
    /// Warning state - files approaching expiration
    pub const YELLOW: Color = Color::Rgb(220, 200, 0);
    /// Danger/overdue state - files past expiration or pending removal
    pub const RED: Color = Color::Rgb(220, 80, 80);
    /// Deferred state - files with extended deadline
    pub const CYAN: Color = Color::Rgb(0, 200, 200);
    /// Shared modal surface background.
    ///
    /// Use terminal-default background so modal surfaces blend with existing pane
    /// backgrounds instead of creating a dark rectangular cutout.
    pub const MODAL_BG: Color = Color::Reset;
    /// Shared modal primary text color. Uses terminal default for light/dark adaptivity.
    pub const MODAL_FG: Color = Color::Reset;
    /// Shared modal secondary text color.
    pub const MODAL_MUTED: Color = Color::DarkGray;
}

/// Whether the terminal background is light. Detected once before the TUI
/// takes over the terminal, then used to choose fade direction.
static LIGHT_THEME: OnceLock<bool> = OnceLock::new();

/// Detect whether the terminal uses a light background.
/// Must be called before ratatui enters alternate screen mode.
pub(crate) fn detect_terminal_theme() {
    let is_light = std::time::Duration::from_millis(100);
    let light = matches!(termbg::theme(is_light), Ok(termbg::Theme::Light));
    let _ = LIGHT_THEME.set(light);
}

fn is_light_theme() -> bool {
    LIGHT_THEME.get().copied().unwrap_or(false)
}

/// Pre-computed gradient for row fading effect.
/// Contains 101 colors (indices 0-100) from full brightness to fully faded.
/// Index 0 = cursor row (full brightness), index 100 = maximally faded.
struct FadeGradient {
    text: [Color; 101],
    green: [Color; 101],
    yellow: [Color; 101],
    red: [Color; 101],
    gray: [Color; 101],
}

impl FadeGradient {
    /// Create a new gradient set for row fading.
    /// Fades toward a muted endpoint that recedes on the detected background.
    fn new() -> Self {
        // In dark mode, fade toward a dim gray (less visible against dark bg).
        // In light mode, fade toward a light gray (less visible against light bg).
        let fade_end = if is_light_theme() {
            (200, 200, 200)
        } else {
            (130, 130, 130)
        };

        // Text start color also adapts: dark text on light bg, light text on dark bg.
        let text_start = if is_light_theme() {
            (40, 40, 40)
        } else {
            (220, 220, 220)
        };

        // RGB values must match palette constants
        let green_rgb = (0, 166, 0);
        let yellow_rgb = (220, 200, 0);
        let red_rgb = (220, 80, 80);

        Self {
            text: Self::generate(text_start, fade_end),
            green: Self::generate(green_rgb, fade_end),
            yellow: Self::generate(yellow_rgb, fade_end),
            red: Self::generate(red_rgb, fade_end),
            gray: Self::generate((128, 128, 128), fade_end),
        }
    }

    /// Generate a 101-color gradient via linear RGB interpolation.
    fn generate(start: (u8, u8, u8), end: (u8, u8, u8)) -> [Color; 101] {
        let mut gradient = [Color::Reset; 101];
        for (i, slot) in gradient.iter_mut().enumerate() {
            let r = Self::lerp_channel(start.0, end.0, i);
            let g = Self::lerp_channel(start.1, end.1, i);
            let b = Self::lerp_channel(start.2, end.2, i);
            *slot = Color::Rgb(r, g, b);
        }
        gradient
    }

    /// Linear interpolation for a single color channel.
    /// `t` is the step index (0-100), representing t/100 of the way from start to end.
    #[allow(clippy::cast_possible_truncation)]
    // Allow: t is clamped to 0-100 before cast, and result is mathematically
    // bounded to 0-255 (interpolation between two u8 values).
    fn lerp_channel(start: u8, end: u8, t: usize) -> u8 {
        let start_16 = u16::from(start);
        let end_16 = u16::from(end);
        let t_16 = t.min(100) as u16;

        let result = if end_16 >= start_16 {
            start_16 + (end_16 - start_16) * t_16 / 100
        } else {
            start_16 - (start_16 - end_16) * t_16 / 100
        };

        result as u8
    }

    /// Get the fade percentage for a row based on distance from cursor.
    /// Returns 0 for cursor row, up to 100 for rows at the edge of visibility.
    fn fade_percent(row_idx: usize, cursor_idx: usize, visible_rows: usize) -> usize {
        if visible_rows == 0 {
            return 0;
        }
        let distance = row_idx.abs_diff(cursor_idx);
        (distance * 100 / visible_rows.max(1)).min(100)
    }
}

fn fade_gradient() -> &'static FadeGradient {
    static GRADIENT: OnceLock<FadeGradient> = OnceLock::new();
    GRADIENT.get_or_init(FadeGradient::new)
}

/// Render the current application state to the terminal.
///
/// This is the main rendering function that dispatches to view-specific
/// rendering based on the current `app.view` state.
pub(crate) fn render(app: &mut App, ctx: &TuiContext, frame: &mut Frame) {
    // Create the main layout with header, tabs, content, and footer.
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5), // Header (logo + countdown dial)
            Constraint::Length(3), // View tabs block
            Constraint::Min(0),    // Main content area
            Constraint::Length(1), // Footer
        ])
        .split(frame.area());

    // Render header banner with logo and countdown.
    render_header(app, frame, chunks[0]);

    // Render tabs.
    render_view_tabs(app, frame, chunks[1]);

    // Render the current view in the main area
    match app.view() {
        View::FileList => {
            let config = ctx.config(app);
            render_file_list_view(app, config, ctx.db, frame, chunks[2]);
        }
        View::AuditLog => render_audit_log(app, ctx.db, frame, chunks[2]),
        View::Help => render_help(app, frame, chunks[2]),
    }

    // Render footer
    render_footer(app, frame, chunks[3]);

    // Render entry deletion confirmation modal if pending entry delete
    if let Some(deletion) = app.pending_entry_delete() {
        let count = deletion.entries.len();
        if count == 1 {
            render_entry_delete_modal(
                frame,
                &deletion.entries[0].path.to_string_lossy(),
                deletion.entries[0].is_dir,
                deletion.method,
            );
        } else {
            render_entry_delete_modal_multi(frame, count, deletion.method);
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
        render_remove_path_modal(frame, &path.display().to_string());
    }

    // Render quota target input modal if pending
    if let Some(target) = app.pending_quota_target() {
        render_quota_target_modal(frame, target);
    }

    // Render audit export modal if pending
    if let Some(export) = app.pending_audit_export() {
        render_audit_export_modal(frame, export);
    }

    // Render dry run results modal if pending
    if let Some(result) = app.pending_dry_run() {
        render_dry_run_modal(frame, result);
    }
}

/// The stagecrew ASCII logo, trimmed to the 3 letter rows.
const LOGO: &str = "\
┏━┓╺┳╸┏━┓┏━╸┏━╸┏━╸┏━┓┏━╸╻ ╻
┗━┓ ┃ ┣━┫┃╺┓┣╸ ┃  ┣┳┛┣╸ ┃╻┃
┗━┛ ╹ ╹ ╹┗━┛┗━╸┗━╸╹┗╸┗━╸┗┻┛";

fn render_header(app: &App, frame: &mut Frame, area: Rect) {
    let block = Block::default()
        .borders(Borders::ALL)
        .padding(Padding::symmetric(1, 0));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let now = jiff::Timestamp::now().as_second();
    let has_countdown = app.nearest_expiration.is_some();

    if has_countdown {
        let dial_width: u16 = 16;
        let h_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(30),         // Logo
                Constraint::Length(2),          // Gap
                Constraint::Min(10),            // Context text
                Constraint::Length(1),          // Gap before dial
                Constraint::Length(dial_width), // Timer dial
            ])
            .split(inner);

        let logo = Paragraph::new(LOGO);
        frame.render_widget(logo, h_chunks[0]);

        let expiration_ts = app.nearest_expiration.unwrap_or(now);
        let remaining = expiration_ts - now;
        let (context_lines, time_top, time_bottom, color) =
            build_countdown_display(remaining, &app.cached_stats);

        let context = Paragraph::new(context_lines).alignment(Alignment::Right);
        frame.render_widget(context, h_chunks[2]);

        render_timer_dial(frame, h_chunks[4], &time_top, &time_bottom, color);
    } else {
        let h_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(30), // Logo
                Constraint::Length(2),  // Gap
                Constraint::Min(10),    // Status text
            ])
            .split(inner);

        let logo = Paragraph::new(LOGO);
        frame.render_widget(logo, h_chunks[0]);

        let status_lines = build_idle_display(&app.cached_stats);
        let status = Paragraph::new(status_lines).alignment(Alignment::Right);
        frame.render_widget(status, h_chunks[2]);
    }
}

fn build_countdown_display(
    remaining_seconds: i64,
    stats: &crate::db::Stats,
) -> (Vec<Line<'static>>, String, String, Color) {
    let (label, time_top, time_bottom, color) = if remaining_seconds <= 0 {
        let overdue = remaining_seconds.unsigned_abs();
        let days = overdue / 86400;
        let hours = (overdue % 86400) / 3600;
        let mins = (overdue % 3600) / 60;
        let secs = overdue % 60;

        let label = if days > 0 {
            format!(
                "overdue by {days} day{}, {hours} hour{}",
                if days == 1 { "" } else { "s" },
                if hours == 1 { "" } else { "s" }
            )
        } else {
            format!(
                "overdue by {hours} hour{}, {mins} min{}",
                if hours == 1 { "" } else { "s" },
                if mins == 1 { "" } else { "s" }
            )
        };

        (
            label,
            format!("-{days}d {hours:02}h"),
            format!("{mins:02}:{secs:02}"),
            palette::RED,
        )
    } else {
        let r = remaining_seconds.unsigned_abs();
        let days = r / 86400;
        let hours = (r % 86400) / 3600;
        let mins = (r % 3600) / 60;
        let secs = r % 60;

        let color = if days == 0 && hours < 6 {
            palette::RED
        } else if remaining_seconds <= 14 * 86400 {
            palette::YELLOW
        } else {
            palette::GREEN
        };

        let label = if days > 0 {
            format!(
                "next removal in {days} day{}, {hours} hour{}",
                if days == 1 { "" } else { "s" },
                if hours == 1 { "" } else { "s" }
            )
        } else {
            format!(
                "next removal in {hours} hour{}, {mins} min{}",
                if hours == 1 { "" } else { "s" },
                if mins == 1 { "" } else { "s" }
            )
        };

        (
            label,
            format!("{days}d {hours:02}h"),
            format!("{mins:02}:{secs:02}"),
            color,
        )
    };

    let is_overdue = remaining_seconds <= 0;
    let context = build_context_lines(&label, stats, is_overdue);
    (context, time_top, time_bottom, color)
}

fn build_context_lines(
    label: &str,
    stats: &crate::db::Stats,
    is_overdue: bool,
) -> Vec<Line<'static>> {
    let mut lines = vec![Line::from(Span::styled(
        label.to_string(),
        Style::default().fg(Color::DarkGray),
    ))];

    let overdue = stats.files_overdue;
    let warning = stats.files_within_warning;

    let mut summary_spans = Vec::new();
    if overdue > 0 || is_overdue {
        let count = overdue.max(1);
        summary_spans.push(Span::styled(
            format!("{count} file{} overdue", if count == 1 { "" } else { "s" }),
            Style::default().fg(palette::RED),
        ));
    }
    if warning > 0 {
        if !summary_spans.is_empty() {
            summary_spans.push(Span::styled(" · ", Style::default().fg(Color::DarkGray)));
        }
        summary_spans.push(Span::styled(
            format!(
                "{warning} file{} due soon",
                if warning == 1 { "" } else { "s" }
            ),
            Style::default().fg(palette::YELLOW),
        ));
    }
    if summary_spans.is_empty() {
        summary_spans.push(Span::styled(
            "all clear",
            Style::default().fg(palette::GREEN),
        ));
    }
    lines.push(Line::from(summary_spans));

    #[allow(clippy::cast_sign_loss)]
    let total_bytes = stats.total_size_bytes.max(0) as u64;
    lines.push(Line::from(Span::styled(
        format!(
            "tracking {} files ({})",
            stats.total_files,
            format_bytes(total_bytes)
        ),
        Style::default().fg(Color::DarkGray),
    )));

    lines
}

fn build_idle_display(stats: &crate::db::Stats) -> Vec<Line<'static>> {
    if stats.total_files == 0 {
        vec![
            Line::from(""),
            Line::from(Span::styled(
                "no directories tracked",
                Style::default().fg(Color::DarkGray),
            )),
        ]
    } else {
        #[allow(clippy::cast_sign_loss)]
        let total_bytes = stats.total_size_bytes.max(0) as u64;
        vec![
            Line::from(""),
            Line::from(Span::styled(
                "all clear",
                Style::default().fg(palette::GREEN),
            )),
            Line::from(Span::styled(
                format!(
                    "tracking {} files ({})",
                    stats.total_files,
                    format_bytes(total_bytes)
                ),
                Style::default().fg(Color::DarkGray),
            )),
        ]
    }
}

fn render_timer_dial(
    frame: &mut Frame,
    area: Rect,
    time_top: &str,
    time_bottom: &str,
    color: Color,
) {
    if area.height < 3 || area.width < 12 {
        return;
    }

    let border_style = Style::default().fg(color);
    let time_style = Style::default().fg(color).add_modifier(Modifier::BOLD);

    let combined = format!("{time_top} {time_bottom}");
    let width = area
        .width
        .min(u16::try_from(combined.len() + 4).unwrap_or(20));
    let x = area.right().saturating_sub(width);
    let y = area.top();

    let buffer = frame.buffer_mut();

    // Top arc
    buffer[(x, y)].set_symbol("╭").set_style(border_style);
    for dx in 1..width.saturating_sub(1) {
        buffer[(x + dx, y)].set_symbol("─").set_style(border_style);
    }
    buffer[(x + width - 1, y)]
        .set_symbol("╮")
        .set_style(border_style);

    // Middle row with combined time
    buffer[(x, y + 1)].set_symbol("│").set_style(border_style);
    buffer[(x + width - 1, y + 1)]
        .set_symbol("│")
        .set_style(border_style);
    let padded = format!(
        "{combined:^width$}",
        width = usize::from(width).saturating_sub(2)
    );
    write_dial_text(buffer, x, y + 1, width, &padded, time_style);

    // Bottom arc
    buffer[(x, y + 2)].set_symbol("╰").set_style(border_style);
    for dx in 1..width.saturating_sub(1) {
        buffer[(x + dx, y + 2)]
            .set_symbol("─")
            .set_style(border_style);
    }
    buffer[(x + width - 1, y + 2)]
        .set_symbol("╯")
        .set_style(border_style);
}

fn write_dial_text(
    buffer: &mut ratatui::buffer::Buffer,
    x: u16,
    y: u16,
    width: u16,
    text: &str,
    style: Style,
) {
    for (i, ch) in text.chars().enumerate() {
        if let Ok(dx) = u16::try_from(i + 1)
            && dx < width - 1
        {
            buffer[(x + dx, y)]
                .set_symbol(&ch.to_string())
                .set_style(style);
        }
    }
}

fn render_view_tabs(app: &App, frame: &mut Frame, area: Rect) {
    let selected = match app.view() {
        View::FileList => Some(0),
        View::AuditLog => Some(1),
        View::Help => Some(2),
    };

    let tab_block = Block::default().borders(Borders::ALL);
    let tabs_inner = tab_block.inner(area);
    frame.render_widget(tab_block, area);

    let Some(selected) = selected else {
        frame.render_widget(Paragraph::new(""), tabs_inner);
        return;
    };

    let titles = vec![
        Line::from(vec![
            Span::styled("MAIN DASHBOARD", Style::default()),
            Span::raw(" [1]"),
        ]),
        Line::from(vec![
            Span::styled("AUDIT LOG", Style::default()),
            Span::raw(" [2]"),
        ]),
        Line::from(vec![
            Span::styled("HELP MENU", Style::default()),
            Span::raw(" [3]"),
        ]),
    ];

    let tabs = Tabs::new(titles)
        .select(selected)
        .style(Style::default().fg(Color::DarkGray))
        .highlight_style(Style::default().fg(Color::Reset))
        .divider(Span::styled("│", Style::default().fg(Color::DarkGray)));

    frame.render_widget(tabs, tabs_inner);
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
    app: &mut App,
    config: &Config,
    db: &Database,
    frame: &mut Frame,
    area: ratatui::layout::Rect,
) {
    // Split vertically: top widgets row | content area
    let v_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(7), // Top row: lifecycle + timeline widgets
            Constraint::Min(0),    // Content area (sidebar + main panel)
        ])
        .split(area);

    // Split top row horizontally: lifecycle widget | expiration timeline
    let top_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(v_chunks[0]);

    // Render both top widgets
    render_lifecycle_widget(app, config, db, frame, top_chunks[0]);
    render_expiration_timeline(app, config, db, frame, top_chunks[1]);

    let content_area = v_chunks[1];

    if app.sidebar_visible() {
        // Split content area horizontally: sidebar | main panel
        let h_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(20), // Sidebar for directories
                Constraint::Percentage(80), // Main panel for files
            ])
            .split(content_area);

        // Render sidebar with tracked directories
        render_sidebar(app, config, db, frame, h_chunks[0]);

        // Render main panel with entries from current path
        render_main_entry_panel(app, config, db, frame, h_chunks[1]);
    } else {
        // Sidebar hidden - main panel takes full width
        render_main_entry_panel(app, config, db, frame, content_area);
    }
}

/// A single lifecycle category's file count and byte total, converted to unsigned.
struct LifecycleTally {
    files: u64,
    bytes: u64,
}

/// Pre-computed unsigned view of `Stats` for the overview widget, with pending
/// and overdue merged into a single "overdue" bucket for display purposes.
struct LifecycleView {
    total_files: u64,
    total_bytes: u64,
    healthy: LifecycleTally,
    warning: LifecycleTally,
    overdue: LifecycleTally,
}

impl From<&crate::db::Stats> for LifecycleView {
    fn from(stats: &crate::db::Stats) -> Self {
        // Clamp to zero before casting — the schema guarantees non-negative values,
        // but defensive conversion avoids wrapping on corrupt data. The max(0) makes
        // the cast safe; clippy can't prove this statically.
        #[allow(clippy::cast_sign_loss)]
        let clamp = |v: i64| -> u64 { v.max(0) as u64 };

        Self {
            total_files: clamp(stats.total_files),
            total_bytes: clamp(stats.total_size_bytes),
            healthy: LifecycleTally {
                files: clamp(stats.files_healthy),
                bytes: clamp(stats.bytes_healthy),
            },
            warning: LifecycleTally {
                files: clamp(stats.files_within_warning),
                bytes: clamp(stats.bytes_within_warning),
            },
            overdue: LifecycleTally {
                files: clamp(stats.files_overdue) + clamp(stats.files_pending_approval),
                bytes: clamp(stats.bytes_overdue) + clamp(stats.bytes_pending_approval),
            },
        }
    }
}

impl LifecycleView {
    /// Compute lifecycle view from a list of entries.
    ///
    /// Categorizes entries by lifecycle status based on their expiration state:
    /// - Healthy: tracked entries with more than `warning_days` until expiration
    /// - Warning: tracked entries within the warning period
    /// - Overdue: entries past expiration
    ///
    /// Directories and ignored/removed entries are excluded from the tally.
    fn from_entries(entries: &[crate::db::Entry], config: &Config) -> Self {
        let mut healthy = LifecycleTally { files: 0, bytes: 0 };
        let mut warning = LifecycleTally { files: 0, bytes: 0 };
        let mut overdue = LifecycleTally { files: 0, bytes: 0 };

        for entry in entries
            .iter()
            .filter(|e| !e.is_dir && e.status != "removed" && e.status != "ignored")
        {
            // Clamp size to non-negative for safe casting
            #[allow(clippy::cast_sign_loss)]
            let size = entry.size_bytes.max(0) as u64;

            // Calculate days remaining from the active countdown timestamp.
            let days_remaining = if entry.status == "deferred" {
                entry.deferred_until.map(|until| {
                    let now = jiff::Timestamp::now().as_second();
                    (until - now) / 86400
                })
            } else {
                entry
                    .countdown_start
                    .map(|cs| calculate_expiration(cs, config.expiration_days))
                    .or_else(|| {
                        if entry.status == "pending" || entry.status == "approved" {
                            // Defensive fallback for historical rows missing countdown_start.
                            Some(0)
                        } else {
                            None
                        }
                    })
            };

            match days_remaining {
                Some(d) if d <= 0 => {
                    overdue.files += 1;
                    overdue.bytes += size;
                }
                Some(d) if d <= i64::from(config.warning_days) => {
                    warning.files += 1;
                    warning.bytes += size;
                }
                // None (no countdown_start) or Some with days > warning_days: assume healthy
                _ => {
                    healthy.files += 1;
                    healthy.bytes += size;
                }
            }
        }

        Self {
            total_files: healthy.files + warning.files + overdue.files,
            total_bytes: healthy.bytes + warning.bytes + overdue.bytes,
            healthy,
            warning,
            overdue,
        }
    }
}

/// Render the lifecycle widget showing stats for the selected root.
///
/// Layout (7 rows total = border + 5 content + border):
///   Row 0: Top border with "LIFECYCLE" title
///   Row 1: Summary — total in white, then colored lifecycle tallies
///   Row 2: Thin divider line
///   Row 3: Files lifecycle bar
///   Row 4: Thin divider line
///   Row 5: Bytes lifecycle bar
///   Row 6: Bottom border
///
/// If no root is selected, shows a placeholder message.
fn render_lifecycle_widget(
    app: &App,
    config: &Config,
    db: &Database,
    frame: &mut Frame,
    area: ratatui::layout::Rect,
) {
    let block = Block::default().borders(Borders::ALL).title("LIFECYCLE");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    // Get the selected root
    let Some(root_id) = app.current_root_id else {
        let msg = Paragraph::new("Select a root from the sidebar")
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(msg, inner);
        return;
    };

    // Get entries for this root
    let Ok(entries) = db.list_entries_by_root(root_id) else {
        let msg = Paragraph::new("Error loading entries").style(Style::default().fg(palette::RED));
        frame.render_widget(msg, inner);
        return;
    };

    let view = LifecycleView::from_entries(&entries, config);

    // Split inner area: summary + divider on top (2 rows), bars below (3 rows)
    let v_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Length(3)])
        .split(inner);

    let summary_area = v_chunks[0];
    let bars_area = v_chunks[1];

    let summary_line = build_summary_line(&view, app.status_message.as_deref());

    let full_divider = Line::from(Span::styled(
        "─".repeat(usize::from(summary_area.width)),
        Style::default().fg(Color::DarkGray),
    ));

    let summary_widget = Paragraph::new(vec![summary_line, full_divider]);
    frame.render_widget(summary_widget, summary_area);

    // Build lifecycle bars sized to full width. The "Files " / "Bytes " prefix is 6 chars.
    let prefix_width: u16 = 6;
    let bar_width = bars_area.width.saturating_sub(prefix_width);

    let files_segments = [
        BarSegment {
            value: view.healthy.files,
            label: view.healthy.files.to_string(),
            color: palette::GREEN,
        },
        BarSegment {
            value: view.warning.files,
            label: view.warning.files.to_string(),
            color: palette::YELLOW,
        },
        BarSegment {
            value: view.overdue.files,
            label: view.overdue.files.to_string(),
            color: palette::RED,
        },
    ];
    let bytes_segments = [
        BarSegment {
            value: view.healthy.bytes,
            label: format_bytes(view.healthy.bytes),
            color: palette::GREEN,
        },
        BarSegment {
            value: view.warning.bytes,
            label: format_bytes(view.warning.bytes),
            color: palette::YELLOW,
        },
        BarSegment {
            value: view.overdue.bytes,
            label: format_bytes(view.overdue.bytes),
            color: palette::RED,
        },
    ];

    let files_bar = build_lifecycle_bar(&files_segments, bar_width);
    let bytes_bar = build_lifecycle_bar(&bytes_segments, bar_width);

    let mut files_spans = vec![Span::styled(
        "Files ",
        Style::default().add_modifier(Modifier::DIM),
    )];
    files_spans.extend(files_bar);
    let files_line = Line::from(files_spans);

    let bar_divider = Line::from(Span::styled(
        "─".repeat(usize::from(bars_area.width)),
        Style::default().fg(Color::DarkGray),
    ));

    let mut bytes_spans = vec![Span::styled(
        "Bytes ",
        Style::default().add_modifier(Modifier::DIM),
    )];
    bytes_spans.extend(bytes_bar);
    let bytes_line = Line::from(bytes_spans);

    let bars_widget = Paragraph::new(vec![files_line, bar_divider, bytes_line]);
    frame.render_widget(bars_widget, bars_area);
}

/// Render the expiration timeline showing when files expire over the next 30 days.
///
/// The timeline is a horizontal strip with markers indicating when files will expire.
/// Files are bucketed by day, with the count shown below each marker.
// Allow: This function handles timeline rendering with multiple layout stages (axis labels,
// timeline, markers, summary) that form a cohesive visualization pipeline.
#[allow(clippy::too_many_lines)]
fn render_expiration_timeline(
    app: &App,
    config: &Config,
    db: &Database,
    frame: &mut Frame,
    area: ratatui::layout::Rect,
) {
    const TIMELINE_DAYS: usize = 30;

    let block = Block::default()
        .borders(Borders::ALL)
        .title("REMOVAL TIMELINE");
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let content = inner.inner(Margin {
        horizontal: 1,
        vertical: 0,
    });

    // Get the selected root
    let Some(root_id) = app.current_root_id else {
        let msg = Paragraph::new("Select a root from the sidebar")
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(msg, content);
        return;
    };

    // Get entries for this root
    let Ok(entries) = db.list_entries_by_root(root_id) else {
        let msg = Paragraph::new("Error loading entries").style(Style::default().fg(palette::RED));
        frame.render_widget(msg, content);
        return;
    };

    // Separate overdue backlog from the upcoming 30-day distribution.
    let mut future_buckets: [u64; TIMELINE_DAYS + 1] = [0; TIMELINE_DAYS + 1];
    let mut overdue_count: u64 = 0;
    let mut overdue_bytes: i64 = 0;
    let mut future_count: u64 = 0;
    let mut future_bytes: i64 = 0;

    for entry in entries
        .iter()
        .filter(|e| !e.is_dir && e.status != "removed" && e.status != "ignored")
    {
        // Calculate days until expiration using the effective countdown timestamp.
        // Approved/pending entries still belong on their true due date in the timeline.
        let days_remaining = if entry.status == "deferred" {
            entry.deferred_until.map(|until| {
                let now = jiff::Timestamp::now().as_second();
                (until - now) / 86400
            })
        } else {
            entry
                .countdown_start
                .map(|cs| calculate_expiration(cs, config.expiration_days))
                .or_else(|| {
                    if entry.status == "pending" || entry.status == "approved" {
                        // Defensive fallback when historical records are missing countdown_start.
                        Some(0)
                    } else {
                        None
                    }
                })
        };

        if let Some(days) = days_remaining {
            if days < 0 {
                overdue_count += 1;
                overdue_bytes += entry.size_bytes;
            } else if days <= i64::try_from(TIMELINE_DAYS).unwrap_or(i64::MAX) {
                #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
                let bucket_idx = days as usize;
                future_buckets[bucket_idx] += 1;
                future_count += 1;
                future_bytes += entry.size_bytes;
            }
        }
    }

    // If nothing is overdue or expiring soon, show a calm message.
    if overdue_count == 0 && future_count == 0 {
        let msg = Paragraph::new("No files expiring in the next 30 days")
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(msg, content);
        return;
    }

    // Layout: labels, compact chart, metric row, and summary.
    let v_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Min(1),
        ])
        .split(content);

    let labels_area = v_chunks[0];
    let chart_area = v_chunks[1];
    let metrics_area = v_chunks[2];
    let summary_area = v_chunks[3];

    let overdue_width = 22;
    let h_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(overdue_width),
            Constraint::Length(2),
            Constraint::Min(10),
        ])
        .split(chart_area);

    let overdue_area = h_chunks[0];
    let future_area = h_chunks[2];

    let label_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(overdue_width),
            Constraint::Length(2),
            Constraint::Min(10),
        ])
        .split(labels_area);

    frame.render_widget(
        Paragraph::new(Line::from(Span::styled(
            "overdue backlog",
            Style::default().fg(Color::DarkGray),
        ))),
        label_chunks[0],
    );
    frame.render_widget(
        Paragraph::new(build_future_axis_labels(future_area.width))
            .style(Style::default().fg(Color::DarkGray)),
        label_chunks[2],
    );

    render_overdue_block(
        overdue_area,
        frame,
        overdue_count,
        overdue_bytes,
        overdue_count + future_count,
    );

    let sparkline = build_future_sparkline(&future_buckets, future_area.width, config.warning_days);
    frame.render_widget(Paragraph::new(sparkline), future_area);

    let metric_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(overdue_width),
            Constraint::Length(2),
            Constraint::Min(10),
        ])
        .split(metrics_area);

    let overdue_metric = if overdue_count == 0 {
        String::new()
    } else {
        format!(
            "{} overdue",
            format_bytes(overdue_bytes.max(0).cast_unsigned())
        )
    };
    let future_metric = if future_count == 0 {
        "nothing due soon".to_string()
    } else {
        format!(
            "{} due soon",
            format_bytes(future_bytes.max(0).cast_unsigned())
        )
    };
    frame.render_widget(
        Paragraph::new(Line::from(Span::styled(
            overdue_metric,
            Style::default().fg(Color::DarkGray),
        ))),
        metric_chunks[0],
    );
    frame.render_widget(
        Paragraph::new(Line::from(Span::styled(
            future_metric,
            Style::default().fg(Color::DarkGray),
        )))
        .alignment(Alignment::Left),
        metric_chunks[2],
    );

    let summary_text = match (overdue_count, future_count) {
        (0, upcoming) => format!(
            "{} due in next 30 days ({})",
            pluralize_files(upcoming),
            format_bytes(future_bytes.max(0).cast_unsigned())
        ),
        (overdue, 0) => format!(
            "{} overdue ({})",
            pluralize_files(overdue),
            format_bytes(overdue_bytes.max(0).cast_unsigned())
        ),
        (overdue, upcoming) => format!(
            "{} overdue | {} due in next 30 days",
            pluralize_files(overdue),
            pluralize_files(upcoming)
        ),
    };

    let summary_paragraph = Paragraph::new(Line::from(Span::styled(
        summary_text,
        Style::default().add_modifier(Modifier::BOLD),
    )))
    .alignment(Alignment::Center);
    let summary_rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0), Constraint::Length(1)])
        .split(summary_area);
    frame.render_widget(summary_paragraph, summary_rows[1]);
}

fn build_future_axis_labels(width: u16) -> Line<'static> {
    let axis_width = usize::from(width);
    let mut axis_label = String::with_capacity(axis_width);
    axis_label.push_str("today");
    let mid_label = "+15d";
    let end_label = "+30d";

    let mid_pos = axis_width / 2;
    let end_pos = axis_width.saturating_sub(end_label.len());

    let padding_to_mid = mid_pos
        .saturating_sub(axis_label.len())
        .saturating_sub(mid_label.len() / 2);
    for _ in 0..padding_to_mid {
        axis_label.push(' ');
    }
    axis_label.push_str(mid_label);

    let padding_to_end = end_pos.saturating_sub(axis_label.len());
    for _ in 0..padding_to_end {
        axis_label.push(' ');
    }
    axis_label.push_str(end_label);

    Line::from(axis_label)
}

fn render_overdue_block(
    area: Rect,
    frame: &mut Frame,
    overdue_count: u64,
    _overdue_bytes: i64,
    total_count: u64,
) {
    let inner_width = area.width.saturating_sub(1);
    let blocks = u16::try_from(
        overdue_count
            .saturating_mul(u64::from(inner_width))
            .saturating_add(total_count.saturating_sub(1))
            / total_count.max(1),
    )
    .unwrap_or(inner_width)
    .max(if overdue_count > 0 { 4 } else { 0 })
    .min(inner_width);
    let bar = "█".repeat(usize::from(blocks));
    if overdue_count == 0 {
        frame.render_widget(
            Paragraph::new(Line::from(Span::styled(
                "✓ all clear",
                Style::default().fg(palette::GREEN),
            ))),
            area,
        );
        return;
    }
    let count_text = pluralize_files(overdue_count);
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(bar, Style::default().fg(palette::RED)),
            Span::raw(" "),
            Span::styled(count_text, Style::default().add_modifier(Modifier::BOLD)),
        ])),
        area,
    );
}

fn build_future_sparkline(buckets: &[u64], width: u16, warning_days: u32) -> Line<'static> {
    const BLOCKS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

    if width == 0 || buckets.is_empty() {
        return Line::from(String::new());
    }

    let samples = sample_future_buckets(buckets, usize::from(width));
    let max_value = samples.iter().map(|(count, _)| *count).max().unwrap_or(0);
    if max_value == 0 {
        return Line::from(Span::styled(
            "·".repeat(usize::from(width)),
            Style::default().fg(Color::DarkGray),
        ));
    }

    let spans: Vec<Span<'static>> = samples
        .into_iter()
        .map(|(count, day)| {
            let idx = usize::try_from(
                count
                    .saturating_mul(u64::try_from(BLOCKS.len()).unwrap_or(8))
                    .saturating_sub(1)
                    / max_value,
            )
            .unwrap_or(0)
            .min(BLOCKS.len().saturating_sub(1));
            let color = if day == 0 {
                palette::RED
            } else if day <= usize::try_from(warning_days).unwrap_or(0) {
                palette::YELLOW
            } else {
                palette::GREEN
            };
            Span::styled(BLOCKS[idx].to_string(), Style::default().fg(color))
        })
        .collect();

    Line::from(spans)
}

fn sample_future_buckets(buckets: &[u64], width: usize) -> Vec<(u64, usize)> {
    (0..width)
        .map(|column| {
            let start = column.saturating_mul(buckets.len()) / width;
            let end = ((column + 1).saturating_mul(buckets.len()) / width).max(start + 1);
            let slice = &buckets[start..end.min(buckets.len())];
            let count = slice.iter().copied().sum::<u64>();
            let day = slice
                .iter()
                .enumerate()
                .find(|(_, count)| **count > 0)
                .map_or(start, |(offset, _)| start + offset);
            (count, day.min(buckets.len().saturating_sub(1)))
        })
        .collect()
}

fn pluralize_files(count: u64) -> String {
    format!("{count} file{}", if count == 1 { "" } else { "s" })
}

/// Render the quota target widget (pie chart with text above, or placeholder message).
// Allow: This function handles multiple early-return cases for different states (no root,
// error, no target) before the main rendering logic. Breaking it up would obscure the flow.
#[allow(clippy::too_many_lines)]
fn render_quota_widget(
    app: &App,
    config: &Config,
    db: &Database,
    frame: &mut Frame,
    area: ratatui::layout::Rect,
) {
    // Render a bordered block for the quota widget
    let block = Block::default().borders(Borders::ALL).title("QUOTA");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    // Get the current root ID
    let Some(root_id) = app.current_root_id else {
        let msg = Paragraph::new("Select a root")
            .alignment(ratatui::layout::Alignment::Center)
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(msg, inner);
        return;
    };

    // Get the root from the database to check for target
    let Ok(roots) = db.list_roots() else {
        let msg = Paragraph::new("Error")
            .alignment(ratatui::layout::Alignment::Center)
            .style(Style::default().fg(palette::RED));
        frame.render_widget(msg, inner);
        return;
    };

    let Some(root) = roots.iter().find(|r| r.id == root_id) else {
        let msg = Paragraph::new("Not found")
            .alignment(ratatui::layout::Alignment::Center)
            .style(Style::default().fg(palette::RED));
        frame.render_widget(msg, inner);
        return;
    };

    let Some(target_bytes) = root.target_bytes else {
        let msg = Paragraph::new("Press t to set")
            .alignment(ratatui::layout::Alignment::Center)
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(msg, inner);
        return;
    };

    // Categorize bytes using exactly the same lifecycle logic as the top widget.
    // This keeps the quota pie and lifecycle bars semantically aligned.
    let (healthy_bytes, warning_bytes, overdue_bytes) = match db.list_entries_by_root(root_id) {
        Ok(entries) => {
            let view = LifecycleView::from_entries(&entries, config);
            let clamp_i64 = |v: u64| i64::try_from(v).unwrap_or(i64::MAX);
            (
                clamp_i64(view.healthy.bytes),
                clamp_i64(view.warning.bytes),
                clamp_i64(view.overdue.bytes),
            )
        }
        Err(_) => (0, 0, 0),
    };

    let used_bytes = healthy_bytes + warning_bytes + overdue_bytes;

    // Calculate values for display. Precision loss is acceptable since we only
    // display integer percentages.
    #[allow(clippy::cast_precision_loss)]
    let target_f64 = target_bytes.max(1) as f64;
    #[allow(clippy::cast_precision_loss)]
    let used_f64 = used_bytes.max(0) as f64;

    // Split inner area: text on top (2 rows), pie chart below
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(1)])
        .split(inner);

    let text_area = chunks[0];
    let chart_area = chunks[1];

    // Build the text label above the chart
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let pct_display = (used_f64 / target_f64 * 100.0).round() as u32;

    #[allow(clippy::cast_sign_loss)]
    let used_display = crate::format_bytes(used_bytes.max(0) as u64);
    #[allow(clippy::cast_sign_loss)]
    let target_display = crate::format_bytes(target_bytes.max(0) as u64);

    // Text color: red if over quota, otherwise terminal default
    let text_color = if used_bytes > target_bytes {
        palette::RED
    } else {
        Color::Reset
    };

    let text_content = if used_bytes > target_bytes {
        #[allow(clippy::cast_sign_loss)]
        let overage = crate::format_bytes((used_bytes - target_bytes).max(0) as u64);
        vec![
            Line::from(Span::styled(
                format!("{pct_display}% ({overage} over)"),
                Style::default().fg(text_color),
            )),
            Line::from(format!("{used_display} / {target_display}")),
        ]
    } else {
        vec![
            Line::from(Span::styled(
                format!("{pct_display}%"),
                Style::default().fg(text_color),
            )),
            Line::from(format!("{used_display} / {target_display}")),
        ]
    };

    let text_widget = Paragraph::new(text_content)
        .alignment(ratatui::layout::Alignment::Center)
        .style(Style::default().fg(text_color));
    frame.render_widget(text_widget, text_area);

    // Constrain chart area to be square (use height as the limiting dimension,
    // accounting for braille's 2:1 aspect ratio where each cell is 2 wide x 4 tall)
    let chart_height = chart_area.height;
    // Each braille cell is roughly 2 chars wide for 4 dots tall, so multiply height by 2
    let ideal_width = chart_height.saturating_mul(2);
    let actual_width = chart_area.width.min(ideal_width);
    let x_offset = (chart_area.width.saturating_sub(actual_width)) / 2;
    let square_chart_area = ratatui::layout::Rect {
        x: chart_area.x + x_offset,
        y: chart_area.y,
        width: actual_width,
        height: chart_height,
    };

    // Create pie slices based on lifecycle status and quota headroom.
    let slices = if used_bytes > target_bytes {
        // Over quota: show solid red. We use 99.99% + 0.01% because tui-piechart
        // doesn't render single-slice charts correctly (shows as a thin line).
        vec![
            tui_piechart::PieSlice::new("", 99.99, palette::RED),
            tui_piechart::PieSlice::new("", 0.01, palette::RED),
        ]
    } else {
        // Under quota: show healthy (green), warning (yellow), overdue (red), remaining (gray)
        let remaining_bytes = (target_bytes - used_bytes).max(0);

        #[allow(clippy::cast_precision_loss)]
        let healthy_pct = healthy_bytes.max(0) as f64 / target_f64 * 100.0;
        #[allow(clippy::cast_precision_loss)]
        let warning_pct = warning_bytes.max(0) as f64 / target_f64 * 100.0;
        #[allow(clippy::cast_precision_loss)]
        let overdue_pct = overdue_bytes.max(0) as f64 / target_f64 * 100.0;
        #[allow(clippy::cast_precision_loss)]
        let remaining_pct = remaining_bytes as f64 / target_f64 * 100.0;

        let mut slices = Vec::new();
        if healthy_pct > 0.0 {
            slices.push(tui_piechart::PieSlice::new("", healthy_pct, palette::GREEN));
        }
        if warning_pct > 0.0 {
            slices.push(tui_piechart::PieSlice::new(
                "",
                warning_pct,
                palette::YELLOW,
            ));
        }
        if overdue_pct > 0.0 {
            slices.push(tui_piechart::PieSlice::new("", overdue_pct, palette::RED));
        }
        if remaining_pct > 0.0 {
            slices.push(tui_piechart::PieSlice::new(
                "",
                remaining_pct,
                Color::DarkGray,
            ));
        }

        // If no slices (empty root with target), show all remaining
        if slices.is_empty() {
            slices.push(tui_piechart::PieSlice::new("", 100.0, Color::DarkGray));
        }

        slices
    };

    let chart = tui_piechart::PieChart::new(slices)
        .show_legend(false)
        .high_resolution(true);

    frame.render_widget(chart, square_chart_area);
}

/// Build the summary line showing total files and bytes, plus optional status message.
fn build_summary_line<'a>(view: &LifecycleView, status_message: Option<&str>) -> Line<'a> {
    let mut spans = vec![Span::styled(
        format!(
            "Total: {} files, {}",
            view.total_files,
            format_bytes(view.total_bytes)
        ),
        Style::default().add_modifier(Modifier::BOLD),
    )];

    if let Some(status) = status_message {
        spans.push(Span::raw("  "));
        spans.push(Span::styled(
            status.to_owned(),
            Style::default().add_modifier(Modifier::DIM),
        ));
    }

    Line::from(spans)
}

/// Build a proportional lifecycle bar as colored spans.
///
/// Each segment is rendered as a colored background band with the label centered
/// in dark text. When a segment is too narrow for its label, it shows as a solid
/// colored band. An empty bar (all zeros) renders as a solid dark gray band.
///
/// A single segment of a lifecycle bar: a proportional value, display label, and color.
struct BarSegment {
    value: u64,
    label: String,
    color: Color,
}

/// Build a proportional lifecycle bar as colored spans from a slice of segments.
///
/// Each segment is rendered as a colored background band with the label centered
/// in dark text. When a segment is too narrow for its label, it shows as a solid
/// colored band. An empty bar (all zeros) renders as a solid dark gray band.
fn build_lifecycle_bar(segments: &[BarSegment], width: u16) -> Vec<Span<'static>> {
    let w = usize::from(width);
    let total: u64 = segments.iter().map(|s| s.value).sum();

    if total == 0 || w == 0 {
        return vec![Span::styled(
            " ".repeat(w),
            Style::default().bg(Color::DarkGray),
        )];
    }

    let values: Vec<u64> = segments.iter().map(|s| s.value).collect();
    let widths = proportional_widths(&values, total, w);

    let mut spans = Vec::new();
    for (seg, &seg_width) in segments.iter().zip(&widths) {
        if seg_width > 0 {
            spans.push(build_bar_segment(seg_width, seg.color, &seg.label));
        }
    }

    spans
}

/// Build a single colored bar segment as a background-colored band with centered text.
///
/// The segment is spaces with a colored background. If the segment is wide enough
/// for the label, the label is centered within it in dark text. Otherwise the
/// segment is a solid colored band with no text.
fn build_bar_segment(width: usize, color: Color, label: &str) -> Span<'static> {
    let label_len = label.len();
    let content = if width >= label_len {
        // Center the label within spaces
        let total_padding = width - label_len;
        let left_pad = total_padding / 2;
        let right_pad = total_padding - left_pad;
        format!("{}{}{}", " ".repeat(left_pad), label, " ".repeat(right_pad),)
    } else {
        " ".repeat(width)
    };

    Span::styled(content, Style::default().bg(color).fg(Color::Black))
}

/// Distribute `width` characters proportionally across segments using integer math.
///
/// Non-zero values get at least 1 character. The returned widths sum to exactly
/// `width`. Uses u128 intermediates to avoid overflow on large byte totals.
fn proportional_widths(values: &[u64], total: u64, width: usize) -> Vec<usize> {
    let total_128 = u128::from(total);
    let width_128 = width as u128;

    // Initial proportional assignment via integer division with rounding
    let mut widths: Vec<usize> = values
        .iter()
        .map(|&v| {
            if total_128 == 0 {
                0
            } else {
                // Round to nearest: (v * width + total/2) / total.
                // Result is bounded by width (a usize), so truncation is safe.
                #[allow(clippy::cast_possible_truncation)]
                let w = (u128::from(v) * width_128 + total_128 / 2) / total_128;
                w as usize
            }
        })
        .collect();

    // Guarantee at least 1 char for non-zero values
    for (i, &v) in values.iter().enumerate() {
        if v > 0 && widths[i] == 0 {
            widths[i] = 1;
        }
    }

    // Adjust to sum to exactly width by modifying the largest segment
    let sum: usize = widths.iter().sum();
    if sum != width
        && let Some(max_idx) = widths
            .iter()
            .enumerate()
            .max_by_key(|&(_, &w)| w)
            .map(|(i, _)| i)
    {
        if sum > width {
            widths[max_idx] = widths[max_idx].saturating_sub(sum - width);
        } else {
            widths[max_idx] += width - sum;
        }
    }

    widths
}

/// Render the sidebar showing tracked roots and quota widget.
fn render_sidebar(
    app: &mut App,
    config: &Config,
    db: &Database,
    frame: &mut Frame,
    area: ratatui::layout::Rect,
) {
    // Split sidebar: roots list on top, quota widget on bottom (fixed 14 rows)
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(14)])
        .split(area);

    let roots_area = chunks[0];
    let quota_area = chunks[1];

    // Render roots list
    render_roots_list(app, db, frame, roots_area);

    // Render quota widget
    render_quota_widget(app, config, db, frame, quota_area);
}

/// Render the roots list in the sidebar.
fn render_roots_list(app: &mut App, db: &Database, frame: &mut Frame, area: ratatui::layout::Rect) {
    // Fetch roots from database
    let Ok(roots) = db.list_roots() else {
        let error_text = Paragraph::new("Error loading roots")
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("TRACKED DIRECTORIES"),
            )
            .style(Style::default().fg(palette::RED));
        frame.render_widget(error_text, area);
        return;
    };

    // Update sidebar list length for navigation
    app.sidebar_len = roots.len();

    // Clamp selected index
    let selected_idx = if roots.is_empty() {
        0
    } else {
        app.sidebar_selected_index().min(roots.len() - 1)
    };

    // Set current root ID based on selection
    if let Some(root) = roots.get(selected_idx) {
        app.current_root_id = Some(root.id);
    }

    // Build sidebar rows
    let rows: Vec<Row> = roots
        .iter()
        .enumerate()
        .map(|(idx, root)| {
            // Extract just the directory name from full path
            let path_str = root.path.to_string_lossy();
            let dir_name = root
                .path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(&path_str);

            let cell = Cell::from(dir_name.to_owned());

            // Highlight selected row and show focus
            let style = if idx == selected_idx {
                if app.focus_panel() == FocusPanel::Sidebar {
                    Style::default()
                        .add_modifier(Modifier::REVERSED)
                        .fg(palette::CYAN)
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
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("TRACKED DIRECTORIES"),
            )
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(empty_text, area);
        return;
    }

    let table = Table::new(rows, [Constraint::Percentage(100)]).block(
        Block::default()
            .title("TRACKED DIRECTORIES")
            .borders(Borders::ALL)
            .border_style(if app.focus_panel() == FocusPanel::Sidebar {
                Style::default().fg(palette::CYAN)
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
    app: &mut App,
    config: &Config,
    db: &Database,
    frame: &mut Frame,
    area: ratatui::layout::Rect,
) {
    // Get the current path for browsing
    let current_path = app.current_path();

    // If current_path is empty, show a message to select a root
    if current_path.as_os_str().is_empty() {
        let message = Paragraph::new(
            "Select a root from the sidebar\n\n(Use j/k to navigate, Tab to switch panels)",
        )
        .block(Block::default().borders(Borders::ALL).title("ENTRIES"))
        .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(message, area);
        return;
    }

    // Fetch entries for this path
    let Ok(entries) = db.list_entries_by_parent(current_path) else {
        let error_text = Paragraph::new("Error loading entries from database")
            .block(Block::default().borders(Borders::ALL).title("ENTRIES"))
            .style(Style::default().fg(palette::RED));
        frame.render_widget(error_text, area);
        return;
    };

    // Empty state
    if entries.is_empty() {
        let empty_text = Paragraph::new("No entries in this directory")
            .block(Block::default().borders(Borders::ALL).title("ENTRIES"))
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(empty_text, area);
        return;
    }

    // Sort entries by expiration (most urgent first) by default
    // For directories (no countdown_start), use a large positive value so they sort to end
    let mut entry_rows: Vec<_> = entries
        .into_iter()
        .map(|entry| {
            // Directories have no countdown_start
            let days_remaining = entry.countdown_start.map_or(i64::MAX, |cs| {
                calculate_expiration(cs, config.expiration_days)
            });
            (entry, days_remaining)
        })
        .collect();

    // Sort based on current sort mode
    sort_entry_rows(&mut entry_rows, app.sort_mode());

    // Update entry list length for navigation
    app.entry_list_len = entry_rows.len();

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

    // Calculate viewport height for gradient fade effect
    // This determines how aggressively rows fade based on distance from cursor
    let viewport_height = area.height.saturating_sub(4) as usize; // borders + header + margin
    let gradient = fade_gradient();

    // Build entry table rows
    let rows: Vec<Row> = entry_rows
        .iter()
        .enumerate()
        .map(|(idx, (entry, days_remaining))| {
            // Calculate fade percentage based on distance from cursor
            // Cursor row = 0% fade (full brightness), edge rows = up to 100% fade
            let fade_pct = FadeGradient::fade_percent(idx, selected_idx, viewport_height);

            // Countdown indicator + workflow marker (pending/approved)
            let (indicator_symbol, indicator_color) = expiration_indicator_entry(
                &entry.status,
                *days_remaining,
                config.warning_days,
                entry,
            );
            let (workflow_symbol, workflow_color) = workflow_indicator(&entry.status);
            let indicator_cell = Cell::from(Line::from(vec![
                Span::styled(
                    indicator_symbol.to_string(),
                    Style::default().fg(indicator_color),
                ),
                Span::styled(
                    workflow_symbol.to_string(),
                    Style::default().fg(workflow_color),
                ),
            ]));

            // Extract filename from path with directory indicator
            let path_str = entry.path.to_string_lossy();
            let filename = entry
                .path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(&path_str);
            let display_name = if entry.is_dir {
                format!("{filename}/")
            } else {
                filename.to_string()
            };
            // Format size (directories show as "-")
            let size_str = if entry.is_dir {
                "-".to_string()
            } else {
                // Allow: size_bytes is guaranteed non-negative by schema, but stored as i64 for SQLite compatibility
                #[allow(clippy::cast_sign_loss)]
                format_bytes(entry.size_bytes as u64)
            };

            // Calculate effective days for the Due column
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

            // Format the Due column value
            // Ignored entries show "—" since they're exempt from removal
            let due_str = if entry.status == "ignored" {
                "—".to_string()
            } else if effective_days >= 0 {
                format!("{effective_days} days")
            } else {
                format!("{} days ago", -effective_days)
            };

            // Check if this entry is selected (multi-select)
            let is_selected = app.selected_entries().contains(&entry.id);
            let is_search_match = search_match_set.contains(&idx);

            // Determine if this is the cursor row - affects styling strategy
            // When REVERSED modifier is used, cell foreground colors become backgrounds,
            // creating visual artifacts. So cursor rows use plain text (no cell colors).
            let is_cursor = idx == selected_idx && app.focus_panel() == FocusPanel::MainPanel;
            let uses_reversed = is_cursor; // REVERSED swaps fg/bg, breaking cell colors

            // Build cells with gradient-faded colors based on distance from cursor
            // Cursor row and selected rows don't fade; other rows fade progressively
            let should_fade = !is_cursor && !is_selected;

            let filename_cell = if uses_reversed {
                Cell::from(display_name)
            } else if entry.status == "ignored" {
                let color = if should_fade {
                    gradient.gray[fade_pct]
                } else {
                    Color::DarkGray
                };
                Cell::from(display_name).style(Style::default().fg(color))
            } else {
                let color = if should_fade {
                    gradient.text[fade_pct]
                } else {
                    Color::Reset // Default terminal color for cursor/selected
                };
                Cell::from(display_name).style(Style::default().fg(color))
            };

            let size_cell = if uses_reversed {
                Cell::from(size_str)
            } else {
                let color = if should_fade {
                    gradient.gray[fade_pct]
                } else {
                    Color::DarkGray
                };
                Cell::from(size_str).style(Style::default().fg(color))
            };

            // Due column is countdown-driven.
            // Ignored entries are always gray.
            // The gradient fades each color toward dark gray.
            let due_color = if uses_reversed {
                Color::Reset
            } else {
                let base_gradient = match entry.status.as_str() {
                    "ignored" => &gradient.gray,
                    _ => {
                        // For tracked/pending/approved/deferred entries, color by urgency.
                        if effective_days <= 0 {
                            &gradient.red // Overdue
                        } else if effective_days <= i64::from(config.warning_days) {
                            &gradient.yellow // Warning period
                        } else {
                            &gradient.green // Safe
                        }
                    }
                };
                if should_fade {
                    base_gradient[fade_pct]
                } else {
                    base_gradient[0] // Full brightness for cursor/selected
                }
            };
            let due_cell = Cell::from(due_str).style(Style::default().fg(due_color));

            // Row-level styling for selection and cursor
            let mut row_style = if is_selected && is_cursor {
                // Cursor on a selected row: combine both indicators
                Style::default().add_modifier(Modifier::BOLD | Modifier::REVERSED)
            } else if is_selected {
                // Selected entries get underline to distinguish from cursor
                Style::default().add_modifier(Modifier::BOLD | Modifier::UNDERLINED)
            } else if is_cursor {
                // Currently focused entry
                Style::default().add_modifier(Modifier::REVERSED)
            } else {
                Style::default()
            };

            // Underline search matches so they stand out
            if is_search_match {
                row_style = row_style.add_modifier(Modifier::UNDERLINED);
            }

            Row::new(vec![indicator_cell, filename_cell, size_cell, due_cell]).style(row_style)
        })
        .collect();

    // Build table
    let widths = [
        Constraint::Length(3),      // Countdown + workflow indicator (e.g. "⚠✓")
        Constraint::Percentage(54), // Filename
        Constraint::Percentage(15), // Size
        Constraint::Percentage(28), // Due
    ];

    let sort_indicator = match app.sort_mode() {
        SortMode::Expiration => " (by due)",
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
                    "ENTRIES{sort_indicator}{selection_info}{search_info}"
                ))
                .borders(Borders::ALL)
                .border_style(if app.focus_panel() == FocusPanel::MainPanel {
                    Style::default().fg(palette::CYAN)
                } else {
                    Style::default()
                }),
        )
        .header(
            Row::new(entry_table_header_cells(app.sort_mode()))
                .style(Style::default().add_modifier(Modifier::BOLD))
                .bottom_margin(1),
        )
        .highlight_spacing(HighlightSpacing::Never);

    // Don't set selection on TableState - we handle row highlighting manually.
    // This prevents ratatui from auto-scrolling to the selected row,
    // allowing mouse scroll to control the viewport independently.
    // We only use TableState for scroll offset management.

    // Only scroll to follow cursor when keyboard navigation requested it.
    // This allows mouse scrolling to move the viewport independently.
    if app.ensure_cursor_visible {
        let viewport_height = area.height.saturating_sub(4) as usize; // borders + header + margin
        let current_offset = app.entry_table_state.offset();
        if selected_idx < current_offset {
            // Selection is above viewport - scroll up to show it
            *app.entry_table_state.offset_mut() = selected_idx;
        } else if selected_idx >= current_offset + viewport_height && viewport_height > 0 {
            // Selection is below viewport - scroll down to show it
            *app.entry_table_state.offset_mut() = selected_idx.saturating_sub(viewport_height) + 1;
        }
        app.ensure_cursor_visible = false;
    }

    // Store area for mouse hit-testing
    app.entry_table_area = area;

    // Render the table
    frame.render_stateful_widget(table, area, &mut app.entry_table_state);

    // Render the scrollbar (inside the border, on the right edge)
    // Only render if there's content to scroll
    let viewport_height = area.height.saturating_sub(4) as usize; // borders + header + margin
    if entry_rows.len() > viewport_height {
        // content_length is the scrollable range: max_offset + 1 so position 0..=max_offset maps correctly
        let max_offset = entry_rows.len().saturating_sub(viewport_height);
        app.entry_scrollbar_state =
            ScrollbarState::new(max_offset + 1).position(app.entry_table_state.offset());

        let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
            .begin_symbol(Some("▲"))
            .end_symbol(Some("▼"))
            .track_symbol(Some("│"));

        frame.render_stateful_widget(
            scrollbar,
            area.inner(Margin {
                vertical: 1,
                horizontal: 0,
            }),
            &mut app.entry_scrollbar_state,
        );
    }
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
                        let str_a = a.0.path.to_string_lossy();
                        let name_a =
                            a.0.path
                                .file_name()
                                .and_then(|n| n.to_str())
                                .unwrap_or(&str_a);
                        let str_b = b.0.path.to_string_lossy();
                        let name_b =
                            b.0.path
                                .file_name()
                                .and_then(|n| n.to_str())
                                .unwrap_or(&str_b);
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
        ("●", palette::RED) // Overdue - filled circle
    } else if effective_days <= i64::from(warning_days) {
        ("⚠", palette::YELLOW) // Warning - warning triangle
    } else {
        (" ", Color::Reset) // Safe - no indicator needed
    }
}

/// Workflow-state marker shown alongside the countdown indicator.
///
/// Keeps approval workflow state visible without overloading countdown colors.
fn workflow_indicator(status: &str) -> (&'static str, Color) {
    match status {
        "pending" => ("!", palette::YELLOW),
        "approved" => ("✓", Color::Reset),
        _ => (" ", Color::Reset),
    }
}

/// Generate header cells for the entry table with sort indicators.
///
/// The currently sorted column gets a triangle indicator:
/// - `▲` for ascending sort (Name, Due)
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

    // Due column shows sort indicator for both Expiration and Modified sorts
    // (Modified sorts by mtime, which determines the due date)
    let due_header = match sort_mode {
        SortMode::Expiration => format!("Due{indicator_asc}"),
        SortMode::Modified => format!("Due (mtime){indicator_desc}"),
        _ => "Due".to_string(),
    };

    vec![
        Cell::from(""), // No header for indicator column
        Cell::from(filename_header),
        Cell::from(size_header),
        Cell::from(due_header),
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
// Allow: This view coordinates data loading, row styling, scroll state, and
// scrollbar rendering in one place for readability, similar to the main table
// renderer pattern.
#[allow(clippy::too_many_lines)]
fn render_audit_log(app: &mut App, db: &Database, frame: &mut Frame, area: ratatui::layout::Rect) {
    // Fetch recent audit entries (limit to 1000 for now)
    let audit = AuditService::new(db);
    let Ok(entries) = audit.list_recent(1000) else {
        // Error handling: show error message
        let error_text = Paragraph::new("Error loading audit log from database")
            .block(Block::default().borders(Borders::ALL).title("Error"))
            .style(Style::default().fg(palette::RED));
        frame.render_widget(error_text, area);
        return;
    };

    // Update list length for navigation
    app.sidebar_len = entries.len();

    // Clamp selected index to valid range
    let selected_idx = if entries.is_empty() {
        0
    } else {
        app.sidebar_selected_index().min(entries.len() - 1)
    };

    // Handle empty state
    if entries.is_empty() {
        let empty_text = Paragraph::new("No audit entries found.\n\nPress 'q' or Esc to go back")
            .block(Block::default().borders(Borders::ALL).title("AUDIT LOG"))
            .style(Style::default());
        frame.render_widget(empty_text, area);
        return;
    }

    // Build table rows with distance-based fade similar to the main panel.
    let gradient = fade_gradient();
    let max_dist = entries.len().saturating_sub(1).max(1);
    let rows: Vec<Row> = entries
        .iter()
        .enumerate()
        .map(|(idx, entry)| {
            let is_selected = idx == selected_idx;
            let distance = idx.abs_diff(selected_idx);
            let fade_pct = ((distance * 100) / max_dist).min(100);
            let should_fade = !is_selected;

            // Format timestamp as human-readable in local timezone
            let timestamp_str = format_timestamp(entry.timestamp);
            let timestamp_cell = if is_selected {
                Cell::from(timestamp_str)
            } else {
                let color = if should_fade {
                    gradient.text[fade_pct]
                } else {
                    Color::DarkGray
                };
                Cell::from(timestamp_str).style(Style::default().fg(color))
            };

            let user_cell = if is_selected {
                Cell::from(entry.user.as_str())
            } else {
                let color = if should_fade {
                    gradient.text[fade_pct]
                } else {
                    Color::Gray
                };
                Cell::from(entry.user.as_str()).style(Style::default().fg(color))
            };

            let action_style = if is_selected {
                Style::default().add_modifier(Modifier::BOLD)
            } else {
                let base = match entry.action.as_str() {
                    "remove" => &gradient.red,
                    "defer" => &gradient.green,
                    "ignore" => &gradient.yellow,
                    "approve" | "unapprove" | "unignore" | "undo" => &gradient.text,
                    _ => &gradient.gray,
                };
                let color = if should_fade { base[fade_pct] } else { base[0] };
                Style::default().fg(color).add_modifier(Modifier::BOLD)
            };
            let action_cell = Cell::from(entry.action.as_str()).style(action_style);

            let path_str = entry.target_path.as_deref().unwrap_or("<system-wide>");
            let path_cell = if is_selected {
                Cell::from(path_str)
            } else {
                let color = if should_fade {
                    gradient.text[fade_pct]
                } else {
                    Color::Gray
                };
                Cell::from(path_str).style(Style::default().fg(color))
            };

            // Highlight selected row
            let style = if is_selected {
                Style::default().add_modifier(Modifier::BOLD | Modifier::REVERSED)
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
                .title(format!(
                    "AUDIT LOG (Most Recent First | {} entries | E export)",
                    entries.len()
                ))
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
        )
        .highlight_spacing(HighlightSpacing::Never);

    // Don't set selection on TableState - we handle row highlighting manually.
    // This prevents ratatui from auto-scrolling to the selected row,
    // allowing mouse scroll to control the viewport independently.

    // Only scroll to follow cursor when keyboard navigation requested it.
    if app.ensure_cursor_visible {
        let viewport_height = area.height.saturating_sub(4) as usize; // borders + header + margin
        let current_offset = app.audit_table_state.offset();
        if selected_idx < current_offset {
            // Selection is above viewport - scroll up to show it
            *app.audit_table_state.offset_mut() = selected_idx;
        } else if selected_idx >= current_offset + viewport_height && viewport_height > 0 {
            // Selection is below viewport - scroll down to show it
            *app.audit_table_state.offset_mut() = selected_idx.saturating_sub(viewport_height) + 1;
        }
        // Note: flag is cleared by entry panel render, which runs first in FileList view.
        // For AuditLog view, we clear it here.
        if app.view() == super::View::AuditLog {
            app.ensure_cursor_visible = false;
        }
    }

    // Store area for mouse hit-testing
    app.audit_table_area = area;

    // Render the table
    frame.render_stateful_widget(table, area, &mut app.audit_table_state);

    // Render the scrollbar (inside the border, on the right edge)
    // Only render if there's content to scroll
    let viewport_height = area.height.saturating_sub(4) as usize; // borders + header + margin
    if entries.len() > viewport_height {
        // content_length is the scrollable range: max_offset + 1 so position 0..=max_offset maps correctly
        let max_offset = entries.len().saturating_sub(viewport_height);
        app.audit_scrollbar_state =
            ScrollbarState::new(max_offset + 1).position(app.audit_table_state.offset());

        let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
            .begin_symbol(Some("▲"))
            .end_symbol(Some("▼"))
            .track_symbol(Some("│"));

        frame.render_stateful_widget(
            scrollbar,
            area.inner(Margin {
                vertical: 1,
                horizontal: 0,
            }),
            &mut app.audit_scrollbar_state,
        );
    }
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
const HELP_LEFT_TEXT: &str = r"File-Centric Workflow:
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
  l           Switch focus to main panel / enter directory
  B           Toggle sidebar visibility
  /           Search entries (Enter to confirm, Esc to cancel)
  n / N       Jump to next / previous search match

Selection (main panel only):
  Space       Toggle selection on current file and advance cursor
  v           Enter/exit visual mode (range select from anchor)
  a           Select all entries in current directory
  Esc         Exit visual mode / clear search / clear selection

Actions (on focused file or all selected files):
  d           Delete file(s) with confirmation
  r           Defer file(s) expiration (reset clock, prompt for days)
  i           Permanently ignore file(s)
  x           Approve file(s) for daemon removal
  I           Unignore file(s) (restore from ignored)
  u           Undo last reversible action
  e           Open in $VISUAL/$EDITOR (suspends TUI)
  o           Open with system viewer (fire-and-forget)";

const HELP_RIGHT_TEXT: &str = r"Root Management:
  A           Add a new tracked path
  X           Remove selected root (sidebar only)
  t           Set quota target for current root

Views:
  1           Main dashboard (file list)
  2           Audit log
  3 / ?       Show this help screen

Sorting:
  s           Cycle sort mode (Due → Size → Name → Modified)

Other:
  E           Export audit log (from Audit Log view)
  F           Execute approved removals for current root
  R           Refresh tracked paths (rescan filesystem)
  T           Reset countdown timer for current root
  Y           Dry run: check if approved entries can be removed
  q           Quit application (or return from audit log)
  Ctrl+C      Quit application";

fn styled_help_lines(text: &str) -> Vec<Line<'static>> {
    text.lines()
        .map(|line| {
            let is_header = line.ends_with(':') && !line.starts_with("  ");
            if is_header {
                Line::from(vec![Span::styled(
                    line.to_string(),
                    Style::default()
                        .fg(palette::CYAN)
                        .add_modifier(Modifier::BOLD),
                )])
            } else {
                Line::from(line.to_string())
            }
        })
        .collect()
}

fn help_legend_lines() -> Vec<Line<'static>> {
    vec![
        Line::from(""),
        Line::from(vec![Span::styled(
            "Legend:",
            Style::default()
                .fg(palette::CYAN)
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from(vec![
            Span::styled("  ■", Style::default().fg(palette::GREEN)),
            Span::raw(" Healthy (beyond warning window)"),
        ]),
        Line::from(vec![
            Span::styled("  ■", Style::default().fg(palette::YELLOW)),
            Span::raw(" Warning (within warning window)"),
        ]),
        Line::from(vec![
            Span::styled("  ■", Style::default().fg(palette::RED)),
            Span::raw(" Overdue (due now or in the past)"),
        ]),
        Line::from(vec![
            Span::styled("  ⚠ / ●", Style::default().fg(palette::YELLOW)),
            Span::raw(" Countdown glyphs (warning / overdue)"),
        ]),
        Line::from(vec![
            Span::styled("  !", Style::default().fg(palette::YELLOW)),
            Span::raw(" Pending workflow state"),
        ]),
        Line::from(vec![
            Span::styled("  ✓", Style::default().fg(Color::Reset)),
            Span::raw(" Approved workflow state"),
        ]),
    ]
}

fn render_help(_app: &App, frame: &mut Frame, area: ratatui::layout::Rect) {
    let block = Block::default()
        .title("KEYBIND REFERENCE")
        .borders(Borders::ALL)
        .style(Style::default());

    let left_lines = styled_help_lines(HELP_LEFT_TEXT);
    let mut right_lines = styled_help_lines(HELP_RIGHT_TEXT);
    right_lines.extend(help_legend_lines());
    right_lines.push(Line::from(""));
    right_lines.push(Line::from("Press any key to close this help screen"));

    frame.render_widget(block, area);
    let inner = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Fill(1),
            Constraint::Length(2),
            Constraint::Fill(1),
        ])
        .split(area.inner(Margin {
            horizontal: 1,
            vertical: 1,
        }));

    let left = Paragraph::new(left_lines).wrap(Wrap { trim: true });
    let right = Paragraph::new(right_lines).wrap(Wrap { trim: true });

    frame.render_widget(left, inner[0]);
    frame.render_widget(right, inner[2]);
}

/// Build a `Line` of hint spans that fits within `max_width`.
///
/// Left-side hints are rendered in priority order until space runs out.
/// Right-side (pinned) hints are always shown if they fit, separated from
/// the left group by flexible padding. Each hint is rendered as `[key] Label`
/// with a single space between hints.
fn build_hint_line(left: &[(&str, &str)], right: &[(&str, &str)], max_width: u16) -> Line<'static> {
    let style = Style::default().add_modifier(Modifier::REVERSED);
    let width = usize::from(max_width);

    // Format a single hint as "[key] Label"
    let fmt = |key: &str, label: &str| -> String { format!("[{key}] {label}") };

    // Measure the pinned right-side hints (with separating spaces)
    let right_parts: Vec<String> = right.iter().map(|(k, l)| fmt(k, l)).collect();
    let right_total: usize =
        right_parts.iter().map(String::len).sum::<usize>() + right_parts.len().saturating_sub(1); // spaces between

    // If even the pinned hints don't fit, just render what we can
    if right_total >= width {
        let joined = right_parts.join(" ");
        return Line::from(Span::styled(
            joined[..width.min(joined.len())].to_string(),
            style,
        ));
    }

    // Budget for left-side hints: total width minus right hints minus a gap
    let min_gap: usize = 2;
    let left_budget = width.saturating_sub(right_total).saturating_sub(min_gap);

    // Greedily add left-side hints in priority order
    let mut left_rendered: Vec<String> = Vec::new();
    let mut left_used: usize = 0;
    for (key, label) in left {
        let part = fmt(key, label);
        let needed = if left_rendered.is_empty() {
            part.len()
        } else {
            part.len() + 1 // space separator
        };
        if left_used + needed > left_budget {
            break;
        }
        left_used += needed;
        left_rendered.push(part);
    }

    let left_str = left_rendered.join(" ");
    let gap = width
        .saturating_sub(left_str.len())
        .saturating_sub(right_total);
    let right_str = right_parts.join(" ");

    Line::from(Span::styled(
        format!("{left_str}{:>gap$}{right_str}", "", gap = gap),
        style,
    ))
}

/// A keybind hint: `(key_label, description)`, rendered as `[key] description`.
type Hint = (&'static str, &'static str);

/// Return priority-ordered hint pairs for the current non-modal context.
///
/// Hints are ordered by importance: the most essential bindings come first
/// so they survive truncation on narrow terminals. The returned tuple is
/// `(left_hints, right_pinned_hints)`.
fn context_hints(app: &App) -> (Vec<Hint>, Vec<Hint>) {
    let right = vec![("?", "Help"), ("q", "Quit")];

    let left = match app.view() {
        View::FileList => {
            let selection_count = app.selected_entries().len();

            if app.search_query.is_some() {
                vec![
                    ("n", "Next match"),
                    ("N", "Prev match"),
                    ("/", "New search"),
                    ("Esc", "Clear search"),
                ]
            } else if app.is_visual_mode() {
                vec![
                    ("j/k", "Extend"),
                    ("d/r/i/x", "Act on selection"),
                    ("Esc", "Keep & exit"),
                    ("g/G", "Top/Bottom"),
                ]
            } else if selection_count > 0 {
                vec![
                    ("d", "Delete"),
                    ("r", "Defer"),
                    ("i", "Ignore"),
                    ("x", "Approve"),
                    ("Esc", "Clear"),
                ]
            } else {
                match app.focus_panel() {
                    FocusPanel::Sidebar => vec![
                        ("j/k", "Navigate"),
                        ("h/l", "Switch panel"),
                        ("d/r/i/x", "Actions"),
                        ("Space", "Select"),
                        ("g/G", "Top/Bottom"),
                        ("s", "Sort"),
                    ],
                    FocusPanel::MainPanel => vec![
                        ("j/k", "Navigate"),
                        ("h/l", "Switch panel"),
                        ("d", "Delete"),
                        ("r", "Defer"),
                        ("i", "Ignore"),
                        ("x", "Approve"),
                        ("I", "Unignore"),
                        ("u", "Undo"),
                        ("F", "Remove approved"),
                        ("T", "Reset timer"),
                        ("Y", "Dry run"),
                        ("Space", "Select"),
                        ("v", "Visual"),
                        ("s", "Sort"),
                        ("a", "All"),
                    ],
                }
            }
        }
        View::AuditLog => vec![
            ("j/k", "Navigate"),
            ("g/G", "Top/Bottom"),
            ("E", "Export"),
            ("Esc", "Back"),
        ],
        View::Help => {
            // Help view is simple enough to not need pinned right hints
            return (vec![("Any key", "Close")], vec![]);
        }
    };

    (left, right)
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
                .bg(palette::GREEN)
                .add_modifier(Modifier::BOLD),
        )
    } else {
        (
            " NORMAL ",
            Style::default().add_modifier(Modifier::BOLD | Modifier::REVERSED),
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
            Style::default().add_modifier(Modifier::REVERSED),
        )]));
        frame.render_widget(search_bar, chunks[1]);
        return;
    }

    // Check if any modal is open (takes precedence over normal view hints).
    // Modal hints are short enough to never overflow, so they bypass truncation.
    let modal_open = app.pending_entry_delete().is_some()
        || app.pending_entry_deferral().is_some()
        || app.pending_entry_ignore().is_some()
        || app.pending_entry_approval().is_some()
        || app.pending_add_path().is_some()
        || app.pending_remove_path().is_some()
        || app.pending_audit_export().is_some()
        || app.pending_dry_run().is_some();

    if modal_open {
        let hints = if app.pending_dry_run().is_some() {
            "[Any key] Close"
        } else if app.pending_entry_deferral().is_some() {
            "[0-9] Enter days [Backspace] Delete [Enter] Confirm [Esc] Cancel"
        } else if app.pending_add_path().is_some() {
            "[Type path] (supports ~) [Backspace] Delete [Enter] Add [Esc] Cancel"
        } else if app.pending_audit_export().is_some() {
            "[Type path] [Tab] Format [Backspace] Delete [Enter] Export [Esc] Cancel"
        } else {
            "[y] Yes [n] No [Esc] Cancel"
        };
        let footer = Paragraph::new(hints).style(Style::default().add_modifier(Modifier::REVERSED));
        frame.render_widget(footer, chunks[1]);
        return;
    }

    let (left, right) = context_hints(app);
    let line = build_hint_line(&left, &right, chunks[1].width);
    let footer = Paragraph::new(line);
    frame.render_widget(footer, chunks[1]);
}

fn render_modal_shell(
    frame: &mut Frame,
    title: &str,
    border_color: Color,
    desired_width: u16,
    desired_height: u16,
) -> Rect {
    let area = frame.area();

    // Dim existing UI so modal content has clear visual priority.
    let buffer = frame.buffer_mut();
    for y in area.top()..area.bottom() {
        for x in area.left()..area.right() {
            let cell = &mut buffer[(x, y)];
            cell.set_style(cell.style().add_modifier(Modifier::DIM));
        }
    }

    let width = desired_width.clamp(44, area.width.saturating_sub(4));
    let height = desired_height.clamp(8, area.height.saturating_sub(2));
    let modal_area = Rect {
        x: area.left() + area.width.saturating_sub(width) / 2,
        y: area.top() + area.height.saturating_sub(height) / 2,
        width,
        height,
    };

    let block = Block::default()
        .title(title)
        .title_style(
            Style::default()
                .fg(border_color)
                .add_modifier(Modifier::BOLD),
        )
        .title_alignment(Alignment::Center)
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(border_color))
        .padding(Padding::symmetric(2, 1))
        .style(Style::default().bg(palette::MODAL_BG).fg(palette::MODAL_FG));
    let inner = block.inner(modal_area);

    // Clear modal cells so dim modifier and old symbols do not bleed through.
    frame.render_widget(Clear, modal_area);
    frame.render_widget(block, modal_area);

    inner
}

fn render_modal_body(frame: &mut Frame, area: Rect, lines: Vec<Line<'_>>) {
    let body = Paragraph::new(lines)
        .alignment(Alignment::Left)
        .wrap(Wrap { trim: true })
        .style(Style::default().fg(palette::MODAL_FG).bg(palette::MODAL_BG));
    frame.render_widget(body, area);
}

const MODAL_WIDTH_CONFIRM: u16 = 64;
const MODAL_WIDTH_INPUT: u16 = 74;
const MODAL_WIDTH_FORM: u16 = 82;

/// Render a confirmation modal for approval actions.
///
/// Displays a centered modal asking the user to confirm removal approval.
fn render_confirmation_modal(frame: &mut Frame, path: &str) {
    let inner = render_modal_shell(
        frame,
        "Approve Removal",
        palette::YELLOW,
        MODAL_WIDTH_CONFIRM,
        10,
    );
    render_modal_body(
        frame,
        inner,
        vec![
            Line::from(vec![Span::styled(
                "Action",
                Style::default().fg(palette::MODAL_MUTED),
            )]),
            Line::from("Approve removal for:"),
            Line::from(path.to_string()),
            Line::from(""),
            Line::from(vec![Span::styled(
                "[y] confirm   [n] cancel",
                Style::default().fg(palette::MODAL_MUTED),
            )]),
        ],
    );
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

    let inner = render_modal_shell(frame, &title, palette::CYAN, MODAL_WIDTH_INPUT, 12);
    render_modal_body(
        frame,
        inner,
        vec![
            Line::from(vec![Span::styled(
                "Target",
                Style::default().fg(palette::MODAL_MUTED),
            )]),
            Line::from(path_display),
            Line::from(""),
            Line::from(vec![
                Span::styled("Days to defer: ", Style::default().fg(palette::MODAL_MUTED)),
                Span::raw(display_input),
            ]),
            Line::from(""),
            Line::from(vec![Span::styled(
                "[Enter] confirm   [Esc] cancel",
                Style::default().fg(palette::MODAL_MUTED),
            )]),
        ],
    );
}

/// Render an entry deletion confirmation modal.
///
/// Displays a centered modal prompting the user to confirm deletion
/// of the selected entry (file or directory). The modal text varies
/// based on the removal method (trash vs permanent delete).
fn render_entry_delete_modal(frame: &mut Frame, path: &str, is_dir: bool, method: RemovalMethod) {
    let type_name = if is_dir { "directory" } else { "file" };
    let (title, action_verb, border_color) = match method {
        RemovalMethod::Trash => (
            if is_dir {
                "Move Directory to Trash"
            } else {
                "Move File to Trash"
            },
            "Move to trash",
            palette::YELLOW,
        ),
        RemovalMethod::PermanentDelete => (
            if is_dir {
                "Permanently Delete Directory"
            } else {
                "Permanently Delete File"
            },
            "Permanently delete",
            palette::RED,
        ),
    };
    let inner = render_modal_shell(frame, title, border_color, MODAL_WIDTH_CONFIRM, 10);
    render_modal_body(
        frame,
        inner,
        vec![
            Line::from(vec![Span::styled(
                "Action",
                Style::default().fg(palette::MODAL_MUTED),
            )]),
            Line::from(format!("{action_verb} {type_name}:")),
            Line::from(path.to_string()),
            Line::from(""),
            Line::from(vec![Span::styled(
                "[y] confirm   [n] cancel",
                Style::default().fg(palette::MODAL_MUTED),
            )]),
        ],
    );
}

/// Render a multi-entry deletion confirmation modal.
///
/// The modal text varies based on the removal method (trash vs permanent delete).
fn render_entry_delete_modal_multi(frame: &mut Frame, count: usize, method: RemovalMethod) {
    let (title, message, border_color) = match method {
        RemovalMethod::Trash => (
            format!("Move {count} Entries to Trash"),
            format!("Move {count} entries to trash?"),
            palette::YELLOW,
        ),
        RemovalMethod::PermanentDelete => (
            format!("Permanently Delete {count} Entries"),
            format!("Permanently delete {count} entries?"),
            palette::RED,
        ),
    };

    let inner = render_modal_shell(frame, &title, border_color, 58, 9);
    render_modal_body(
        frame,
        inner,
        vec![
            Line::from(message),
            Line::from(""),
            Line::from(vec![Span::styled(
                "[y] confirm   [n] cancel",
                Style::default().fg(palette::MODAL_MUTED),
            )]),
        ],
    );
}

/// Render an ignore confirmation modal for permanently exempting a path.
///
/// Displays a centered modal prompting the user to confirm permanent exemption
/// of the selected directory from auto-removal.
fn render_ignore_modal(frame: &mut Frame, path: &str) {
    let inner = render_modal_shell(
        frame,
        "Ignore Path Permanently",
        palette::CYAN,
        MODAL_WIDTH_CONFIRM,
        10,
    );
    render_modal_body(
        frame,
        inner,
        vec![
            Line::from("Permanently ignore:"),
            Line::from(path.to_string()),
            Line::from(""),
            Line::from(vec![Span::styled(
                "[y] confirm   [n] cancel",
                Style::default().fg(palette::MODAL_MUTED),
            )]),
        ],
    );
}

/// Render a multi-file ignore confirmation modal.
fn render_ignore_modal_multi(frame: &mut Frame, count: usize) {
    let title = format!("Ignore {count} Files Permanently");
    let inner = render_modal_shell(frame, &title, palette::CYAN, 58, 9);
    render_modal_body(
        frame,
        inner,
        vec![
            Line::from(format!("Permanently ignore {count} files?")),
            Line::from(""),
            Line::from(vec![Span::styled(
                "[y] confirm   [n] cancel",
                Style::default().fg(palette::MODAL_MUTED),
            )]),
        ],
    );
}

/// Render a multi-file approval confirmation modal.
fn render_confirmation_modal_multi(frame: &mut Frame, count: usize) {
    let title = format!("Approve {count} Files for Removal");
    let inner = render_modal_shell(frame, &title, palette::YELLOW, 60, 9);
    render_modal_body(
        frame,
        inner,
        vec![
            Line::from(format!("Approve {count} files for removal?")),
            Line::from(""),
            Line::from(vec![Span::styled(
                "[y] confirm   [n] cancel",
                Style::default().fg(palette::MODAL_MUTED),
            )]),
        ],
    );
}

/// Render add path text input modal.
///
/// Displays a centered modal prompting the user to enter a path to add to `tracked_paths`.
/// Supports tilde expansion (~).
fn render_add_path_modal(frame: &mut Frame, input: &str) {
    let display_input = if input.is_empty() { "_" } else { input };

    let inner = render_modal_shell(
        frame,
        "Add Tracked Path",
        palette::GREEN,
        MODAL_WIDTH_INPUT,
        11,
    );
    render_modal_body(
        frame,
        inner,
        vec![
            Line::from(vec![Span::styled(
                "Path",
                Style::default().fg(palette::MODAL_MUTED),
            )]),
            Line::from(display_input.to_string()),
            Line::from(""),
            Line::from(vec![Span::styled(
                "Supports ~ expansion",
                Style::default().fg(palette::MODAL_MUTED),
            )]),
            Line::from(vec![Span::styled(
                "[Enter] add   [Esc] cancel",
                Style::default().fg(palette::MODAL_MUTED),
            )]),
        ],
    );
}

/// Render remove path confirmation modal.
///
/// Displays a centered modal prompting the user to confirm removal of a tracked path.
fn render_remove_path_modal(frame: &mut Frame, path: &str) {
    let inner = render_modal_shell(
        frame,
        "Remove Tracked Path",
        palette::RED,
        MODAL_WIDTH_CONFIRM,
        10,
    );
    render_modal_body(
        frame,
        inner,
        vec![
            Line::from("Remove tracked path:"),
            Line::from(path.to_string()),
            Line::from(""),
            Line::from(vec![Span::styled(
                "[y] confirm   [n] cancel",
                Style::default().fg(palette::MODAL_MUTED),
            )]),
        ],
    );
}

/// Render audit export modal.
fn render_audit_export_modal(frame: &mut Frame, export: &PendingAuditExport) {
    let inner = render_modal_shell(frame, "Export Audit Log", palette::CYAN, 86, 12);
    let display_input = if export.path_input.is_empty() {
        "_".to_string()
    } else {
        export.path_input.clone()
    };

    render_modal_body(
        frame,
        inner,
        vec![
            Line::from(vec![Span::styled(
                "Output path",
                Style::default().fg(palette::MODAL_MUTED),
            )]),
            Line::from(display_input),
            Line::from(""),
            Line::from(vec![
                Span::styled("Format: ", Style::default().fg(palette::MODAL_MUTED)),
                Span::styled(
                    export.format.label(),
                    Style::default()
                        .fg(palette::CYAN)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    "  (Tab to switch)",
                    Style::default().fg(palette::MODAL_MUTED),
                ),
            ]),
            Line::from(""),
            Line::from(vec![Span::styled(
                "[Enter] export   [Esc] cancel",
                Style::default().fg(palette::MODAL_MUTED),
            )]),
        ],
    );
}

/// Render the quota target input modal.
///
/// Displays a centered modal with two fields: a numeric input for the size value
/// and a unit selector (MB/GB/TB). Tab switches focus between fields.
fn render_quota_target_modal(frame: &mut Frame, target: &PendingQuotaTarget) {
    let inner = render_modal_shell(
        frame,
        "Set Quota Target",
        palette::CYAN,
        MODAL_WIDTH_FORM,
        13,
    );

    // Format the current target for display
    let current_display = match target.current_target {
        // Allow: bytes is guaranteed non-negative by the guard
        #[allow(clippy::cast_sign_loss)]
        Some(bytes) if bytes >= 0 => crate::format_bytes(bytes as u64),
        Some(_) => "invalid".to_string(),
        None => "not set".to_string(),
    };

    // Format the size input with cursor indicator
    let size_display = if target.input.is_empty() {
        "_".to_string()
    } else {
        target.input.clone()
    };

    // Format the unit selector with highlight on selected
    let unit_display = format!(
        "{}  {}  {}",
        if target.unit == super::ByteUnit::MB {
            "[MB]"
        } else {
            " MB "
        },
        if target.unit == super::ByteUnit::GB {
            "[GB]"
        } else {
            " GB "
        },
        if target.unit == super::ByteUnit::TB {
            "[TB]"
        } else {
            " TB "
        },
    );

    // Highlight the focused field
    let (size_style, unit_style) = match target.focus {
        QuotaTargetFocus::Size => (
            Style::default()
                .fg(palette::CYAN)
                .add_modifier(Modifier::BOLD),
            Style::default().fg(palette::MODAL_FG),
        ),
        QuotaTargetFocus::Unit => (
            Style::default().fg(palette::MODAL_FG),
            Style::default()
                .fg(palette::CYAN)
                .add_modifier(Modifier::BOLD),
        ),
    };

    // Build the content with styled spans
    let path_display = target.root_path.display().to_string();
    let content = vec![
        Line::from(vec![
            Span::styled("Root: ", Style::default().fg(palette::MODAL_MUTED)),
            Span::raw(path_display),
        ]),
        Line::from(vec![
            Span::styled("Current: ", Style::default().fg(palette::MODAL_MUTED)),
            Span::raw(current_display),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("Size: ", Style::default().fg(palette::MODAL_MUTED)),
            Span::styled(size_display, size_style),
            Span::styled("    Unit: ", Style::default().fg(palette::MODAL_MUTED)),
            Span::styled(unit_display, unit_style),
        ]),
        Line::from(""),
        Line::from(vec![Span::styled(
            "[Tab] switch fields   [Enter] confirm",
            Style::default().fg(palette::MODAL_MUTED),
        )]),
        Line::from(vec![Span::styled(
            "Empty or 0 clears target   [Esc] cancel",
            Style::default().fg(palette::MODAL_MUTED),
        )]),
    ];

    render_modal_body(frame, inner, content);
}

/// Render the dry run results modal showing which approved entries would fail removal.
fn render_dry_run_modal(frame: &mut Frame, result: &DryRunResult) {
    let title = format!(
        "Dry Run: {} of {} removable",
        result.removable_count, result.total_count
    );

    let failure_count = result.failures.len();
    let modal_height = u16::try_from(failure_count)
        .unwrap_or(u16::MAX)
        .saturating_add(6)
        .min(24);

    let inner = render_modal_shell(frame, &title, palette::YELLOW, 78, modal_height);

    let mut lines = vec![
        Line::from(vec![Span::styled(
            format!(
                "{} entr{} would fail removal:",
                failure_count,
                if failure_count == 1 { "y" } else { "ies" }
            ),
            Style::default().fg(palette::RED),
        )]),
        Line::from(""),
    ];

    for failure in &result.failures {
        let path_display = failure.path.to_string_lossy();
        lines.push(Line::from(vec![
            Span::styled("  ", Style::default()),
            Span::styled(
                path_display.into_owned(),
                Style::default().fg(palette::MODAL_FG),
            ),
        ]));
        lines.push(Line::from(vec![Span::styled(
            format!("    {}", failure.reason),
            Style::default().fg(palette::MODAL_MUTED),
        )]));
    }

    lines.push(Line::from(""));
    lines.push(Line::from(vec![Span::styled(
        "[Any key] close",
        Style::default().fg(palette::MODAL_MUTED),
    )]));

    render_modal_body(frame, inner, lines);
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    // Helper to create a minimal Entry for testing (file)
    fn test_entry(path: &str, size_bytes: i64, mtime: Option<i64>) -> crate::db::Entry {
        let now = jiff::Timestamp::now().as_second();
        crate::db::Entry {
            id: 0,
            root_id: 1,
            path: PathBuf::from(path),
            parent_path: PathBuf::from("/"),
            is_dir: false,
            size_bytes,
            mtime,
            tracked_since: Some(now),
            countdown_start: Some(now),
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
            path: PathBuf::from(path),
            parent_path: PathBuf::from("/"),
            is_dir: true,
            size_bytes: 0,
            mtime: None,
            tracked_since: Some(now),
            countdown_start: None,
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
            rows[0].0.path,
            PathBuf::from("/a.txt"),
            "Most urgent (5 days) should be first"
        );
        assert_eq!(
            rows[1].0.path,
            PathBuf::from("/b.txt"),
            "Middle urgency (15 days) should be second"
        );
        assert_eq!(
            rows[2].0.path,
            PathBuf::from("/c.txt"),
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
            rows[0].0.path,
            PathBuf::from("/subdir"),
            "Most urgent (dir with 3 days) first"
        );
        assert_eq!(
            rows[1].0.path,
            PathBuf::from("/a.txt"),
            "File with 10 days second"
        );
        assert_eq!(
            rows[2].0.path,
            PathBuf::from("/b.txt"),
            "Least urgent (20 days) last"
        );
    }

    #[test]
    fn sort_entries_by_size_largest_first() {
        let mut rows = vec![
            (test_entry("/a.txt", 100, Some(1000)), 10),
            (test_entry("/b.txt", 500, Some(1000)), 10),
            (test_entry("/c.txt", 250, Some(1000)), 10),
        ];

        sort_entry_rows(&mut rows, SortMode::Size);

        assert_eq!(
            rows[0].0.path,
            PathBuf::from("/b.txt"),
            "Largest (500) should be first"
        );
        assert_eq!(
            rows[1].0.path,
            PathBuf::from("/c.txt"),
            "Middle (250) should be second"
        );
        assert_eq!(
            rows[2].0.path,
            PathBuf::from("/a.txt"),
            "Smallest (100) should be last"
        );
    }

    #[test]
    fn sort_entries_by_name_alphabetical_dirs_first() {
        let mut rows = vec![
            (test_entry("/zebra.txt", 100, Some(1000)), 10),
            (test_entry_dir("/alpha_dir"), 15),
            (test_entry("/mango.txt", 100, Some(1000)), 10),
        ];

        sort_entry_rows(&mut rows, SortMode::Name);

        assert_eq!(
            rows[0].0.path,
            PathBuf::from("/alpha_dir"),
            "Directory should come first"
        );
        assert_eq!(
            rows[1].0.path,
            PathBuf::from("/mango.txt"),
            "Mango should be second"
        );
        assert_eq!(
            rows[2].0.path,
            PathBuf::from("/zebra.txt"),
            "Zebra should be last"
        );
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
            rows[0].0.path,
            PathBuf::from("/b.txt"),
            "Most recent (5000) should be first"
        );
        assert_eq!(
            rows[1].0.path,
            PathBuf::from("/c.txt"),
            "Middle (3000) should be second"
        );
        assert_eq!(
            rows[2].0.path,
            PathBuf::from("/a.txt"),
            "Oldest (1000) should be last"
        );
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
            path: PathBuf::from("/test/file.txt"),
            parent_path: PathBuf::from("/test"),
            is_dir: false,
            size_bytes: 100,
            mtime: Some(0),
            tracked_since: None,
            countdown_start: Some(0),
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
        assert_eq!(color, palette::RED, "Overdue should be red");

        let (symbol, color) = expiration_indicator_entry("tracked", -5, 14, &entry);
        assert_eq!(symbol, "●", "Negative days should show filled circle");
        assert_eq!(color, palette::RED, "Overdue should be red");
    }

    #[test]
    fn expiration_indicator_entry_within_warning_is_yellow_triangle() {
        let entry = make_test_entry("tracked", None);
        let (symbol, color) = expiration_indicator_entry("tracked", 1, 14, &entry);
        assert_eq!(symbol, "⚠", "1 day remaining should show warning triangle");
        assert_eq!(color, palette::YELLOW, "Warning should be yellow");

        let (symbol, color) = expiration_indicator_entry("tracked", 14, 14, &entry);
        assert_eq!(symbol, "⚠", "At warning threshold should show warning");
        assert_eq!(color, palette::YELLOW, "Warning should be yellow");
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
        assert_eq!(color, palette::YELLOW);
    }

    #[test]
    fn workflow_indicator_pending_and_approved_have_markers() {
        let (pending_symbol, pending_color) = workflow_indicator("pending");
        assert_eq!(pending_symbol, "!");
        assert_eq!(pending_color, palette::YELLOW);

        let (approved_symbol, approved_color) = workflow_indicator("approved");
        assert_eq!(approved_symbol, "✓");
        assert_eq!(approved_color, Color::Reset);
    }

    #[test]
    fn workflow_indicator_other_states_are_blank() {
        let (tracked_symbol, tracked_color) = workflow_indicator("tracked");
        assert_eq!(tracked_symbol, " ");
        assert_eq!(tracked_color, Color::Reset);

        let (deferred_symbol, deferred_color) = workflow_indicator("deferred");
        assert_eq!(deferred_symbol, " ");
        assert_eq!(deferred_color, Color::Reset);
    }
}
