//! Widget rendering for the TUI.

use ratatui::Frame;

use super::App;

/// Render the current application state to the terminal.
pub fn render(_app: &App, _frame: &mut Frame) {
    // TODO: Implement rendering for each view:
    // - DirectoryList: Table with path, size, days until expiration, status
    // - DirectoryDetail: File list within selected directory
    // - PendingApprovals: Items awaiting approval
    // - AuditLog: Scrollable audit history
    // - Help: Keybinding reference
}
