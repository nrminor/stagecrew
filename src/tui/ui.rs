//! Widget rendering for the TUI.

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Style};
use ratatui::widgets::{Block, Borders, Paragraph};

use super::{App, View};

/// Render the current application state to the terminal.
///
/// This is the main rendering function that dispatches to view-specific
/// rendering based on the current `app.view` state.
pub(crate) fn render(app: &App, frame: &mut Frame) {
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
        View::DirectoryList => render_directory_list(app, frame, chunks[0]),
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
/// This is a placeholder implementation that will be expanded in US-014.
fn render_directory_list(_app: &App, frame: &mut Frame, area: ratatui::layout::Rect) {
    let block = Block::default()
        .title("Stagecrew - Directory List")
        .borders(Borders::ALL)
        .style(Style::default());

    let text = Paragraph::new("Directory list view (US-014)").block(block);

    frame.render_widget(text, area);
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
            "[j/k] Navigate [Enter] Details [p] Pending [a] Audit [?] Help [q] Quit"
        }
        View::DirectoryDetail => "[j/k] Navigate [h/Esc] Back [q] Quit",
        View::PendingApprovals | View::AuditLog => "[j/k] Navigate [Esc] Back [q] Quit",
        View::Help => "[Any key] Close",
    };

    let footer = Paragraph::new(hints).style(Style::default().fg(Color::Black).bg(Color::Gray));

    frame.render_widget(footer, area);
}
