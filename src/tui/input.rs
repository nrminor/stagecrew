//! Vim-style keybinding handling.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::{App, FocusPanel, SortMode, View};
use crate::audit::{AuditAction, AuditService};
use crate::config::Config;
use crate::db::Database;

/// Handles keyboard input with vim-style bindings.
pub(crate) struct InputHandler;

impl InputHandler {
    /// Process a key event and update app state.
    ///
    /// Takes a config reference for reading default values and a database reference
    /// for actions that modify state (approve, defer, ignore).
    pub fn handle(app: &mut App, config: &Config, db: &Database, key: KeyEvent) {
        match app.view {
            View::FileList => Self::handle_file_list(app, config, db, key),
            View::AuditLog => Self::handle_audit_log(app, key),
            View::Help => Self::handle_help(app, key),
        }
    }

    // Allow: This function coordinates input handling for the file list view, including
    // modal dispatch, focus management, navigation, actions, and view switching. Breaking
    // it into smaller functions would obscure the input handling flow.
    #[allow(clippy::too_many_lines)]
    fn handle_file_list(app: &mut App, config: &Config, db: &Database, key: KeyEvent) {
        // Check for pending confirmations/inputs first
        if app.pending_add_path.is_some() {
            Self::handle_add_path_input(app, config, db, key);
            return;
        }
        if app.pending_remove_path.is_some() {
            Self::handle_remove_path_confirmation(app, config, key);
            return;
        }
        if app.pending_file_delete.is_some() {
            Self::handle_file_delete_confirmation(app, db, key);
            return;
        }
        if app.pending_file_deferral.is_some() {
            Self::handle_file_deferral_input(app, config, db, key);
            return;
        }
        if app.pending_file_ignore.is_some() {
            Self::handle_file_ignore_confirmation(app, db, key);
            return;
        }
        if app.pending_file_approval.is_some() {
            Self::handle_file_approval_confirmation(app, db, key);
            return;
        }

        match key.code {
            // Quit
            KeyCode::Char('q') => app.should_quit = true,
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                app.should_quit = true;
            }

            // Switch focus between sidebar and main panel
            KeyCode::Tab | KeyCode::Char('\t') => {
                app.focus_panel = match app.focus_panel {
                    FocusPanel::Sidebar => FocusPanel::MainPanel,
                    FocusPanel::MainPanel => FocusPanel::Sidebar,
                };
            }
            KeyCode::Char('h') => {
                app.focus_panel = FocusPanel::Sidebar;
            }
            KeyCode::Char('l') => {
                app.focus_panel = FocusPanel::MainPanel;
            }

            // Navigation (vim-style) - operates on focused panel
            KeyCode::Char('j') | KeyCode::Down => match app.focus_panel {
                FocusPanel::Sidebar => {
                    app.sidebar_selected_index = app.sidebar_selected_index.saturating_add(1);
                }
                FocusPanel::MainPanel => {
                    app.file_selected_index = app.file_selected_index.saturating_add(1);
                }
            },
            KeyCode::Char('k') | KeyCode::Up => match app.focus_panel {
                FocusPanel::Sidebar => {
                    app.sidebar_selected_index = app.sidebar_selected_index.saturating_sub(1);
                }
                FocusPanel::MainPanel => {
                    app.file_selected_index = app.file_selected_index.saturating_sub(1);
                }
            },
            KeyCode::Char('g') => {
                // Go to top of focused panel
                match app.focus_panel {
                    FocusPanel::Sidebar => app.sidebar_selected_index = 0,
                    FocusPanel::MainPanel => app.file_selected_index = 0,
                }
            }
            KeyCode::Char('G') => {
                // Go to bottom of focused panel
                match app.focus_panel {
                    FocusPanel::Sidebar => {
                        app.select_last_sidebar(app.sidebar_len.get());
                    }
                    FocusPanel::MainPanel => {
                        app.select_last_file(app.file_list_len.get());
                    }
                }
            }

            // Sort modes (applies to file list)
            KeyCode::Char('s') => {
                app.sort_mode = match app.sort_mode {
                    SortMode::Expiration => SortMode::Size,
                    SortMode::Size => SortMode::Name,
                    SortMode::Name => SortMode::Modified,
                    SortMode::Modified => SortMode::Expiration,
                };
            }

            // Selection mode (only when main panel is focused)
            KeyCode::Char(' ') if app.focus_panel == FocusPanel::MainPanel => {
                Self::toggle_file_selection(app, db);
            }
            KeyCode::Char('v') if app.focus_panel == FocusPanel::MainPanel => {
                Self::enter_visual_mode(app, db);
            }
            KeyCode::Esc if app.focus_panel == FocusPanel::MainPanel => {
                app.clear_selection();
            }

            // File-level actions (only when main panel is focused)
            KeyCode::Char('d') if app.focus_panel == FocusPanel::MainPanel => {
                Self::initiate_file_delete(app, db);
            }
            KeyCode::Char('r') if app.focus_panel == FocusPanel::MainPanel => {
                Self::initiate_file_defer(app, config, db);
            }
            KeyCode::Char('i') if app.focus_panel == FocusPanel::MainPanel => {
                Self::initiate_file_ignore(app, db);
            }
            KeyCode::Char('x') if app.focus_panel == FocusPanel::MainPanel => {
                Self::initiate_file_approve(app, db);
            }

            // Views
            KeyCode::Char('a') => app.view = View::AuditLog,
            KeyCode::Char('?') => app.view = View::Help,

            // Path management (A = add, X = remove from sidebar)
            KeyCode::Char('A') => {
                // Initiate add path modal
                app.pending_add_path = Some(String::new());
            }
            KeyCode::Char('X') if app.focus_panel == FocusPanel::Sidebar => {
                Self::initiate_remove_path(app, config, db);
            }

            _ => {}
        }
    }

    /// Toggle selection of the currently focused file.
    fn toggle_file_selection(app: &mut App, db: &Database) {
        // Get the currently selected directory ID
        let Some(directory_id) = app.current_directory_id.get() else {
            tracing::warn!("Cannot toggle selection: no directory selected");
            return;
        };

        // Query files for this directory
        let files = match db.list_files_by_directory(directory_id) {
            Ok(files) => files,
            Err(e) => {
                tracing::warn!("Failed to query files: {}", e);
                return;
            }
        };

        // Get the selected file based on file_selected_index
        let Some(file) = files.get(app.file_selected_index) else {
            tracing::warn!("No file selected (index out of bounds)");
            return;
        };

        // Toggle selection
        app.toggle_file_selection(file.id);
    }

    /// Enter visual mode by selecting all visible files in the current directory.
    fn enter_visual_mode(app: &mut App, db: &Database) {
        // Get the currently selected directory ID
        let Some(directory_id) = app.current_directory_id.get() else {
            tracing::warn!("Cannot enter visual mode: no directory selected");
            return;
        };

        // Query files for this directory
        let files = match db.list_files_by_directory(directory_id) {
            Ok(files) => files,
            Err(e) => {
                tracing::warn!("Failed to query files: {}", e);
                return;
            }
        };

        // Select all file IDs
        for file in files {
            app.selected_files.insert(file.id);
        }
    }

    /// Handle confirmation prompt (y/n/Esc) for ignore action.
    // Allow: Will be used for file-level actions in US-030.
    #[allow(dead_code)]
    fn handle_ignore_confirmation(app: &mut App, db: &Database, key: KeyEvent) {
        match key.code {
            KeyCode::Char('y' | 'Y') => {
                // User confirmed - perform the ignore
                if let Some((dir_id, path)) = &app.pending_ignore {
                    // Update directory status to 'ignored'
                    if let Err(e) = db.update_directory_status(*dir_id, "ignored") {
                        // Log error but continue - user will see status unchanged
                        tracing::warn!("Failed to ignore directory {}: {}", path, e);
                    } else {
                        // Record audit entry
                        let audit = AuditService::new(db);
                        let user = AuditService::current_user();
                        if let Err(e) = audit.record(
                            &user,
                            AuditAction::Ignore,
                            Some(path),
                            None,
                            Some(*dir_id),
                        ) {
                            tracing::warn!("Failed to record audit entry for ignore: {}", e);
                        }
                    }
                }
                // Clear pending ignore
                app.pending_ignore = None;
            }
            KeyCode::Char('n' | 'N') | KeyCode::Esc => {
                // Cancel confirmation
                app.pending_ignore = None;
            }
            _ => {
                // Ignore other keys during confirmation
            }
        }
    }

    /// Handle confirmation prompt (y/n/Esc) for approval action.
    // Allow: Will be used for file-level actions in US-030.
    #[allow(dead_code)]
    fn handle_confirmation(app: &mut App, db: &Database, key: KeyEvent) {
        match key.code {
            KeyCode::Char('y' | 'Y') => {
                // User confirmed - perform the approval
                if let Some((dir_id, path)) = &app.pending_approval {
                    // Update directory status to 'approved'
                    if let Err(e) = db.update_directory_status(*dir_id, "approved") {
                        // Log error but continue - user will see status unchanged
                        tracing::warn!("Failed to approve directory {}: {}", path, e);
                    } else {
                        // Record audit entry
                        let audit = AuditService::new(db);
                        let user = AuditService::current_user();
                        if let Err(e) = audit.record(
                            &user,
                            AuditAction::Approve,
                            Some(path),
                            None,
                            Some(*dir_id),
                        ) {
                            tracing::warn!("Failed to record audit entry for approval: {}", e);
                        }
                    }
                }
                // Clear pending approval
                app.pending_approval = None;
            }
            KeyCode::Char('n' | 'N') | KeyCode::Esc => {
                // Cancel confirmation
                app.pending_approval = None;
            }
            _ => {
                // Ignore other keys during confirmation
            }
        }
    }

    /// Handle deferral input prompt for numeric days input.
    ///
    /// Accepts digit characters to build up the input string, Enter to confirm,
    /// Backspace to delete digits, and Esc to cancel.
    // Allow: Will be used for file-level actions in US-030.
    #[allow(dead_code)]
    fn handle_deferral_input(app: &mut App, _config: &Config, db: &Database, key: KeyEvent) {
        match key.code {
            KeyCode::Char(c) if c.is_ascii_digit() => {
                // Append digit to input buffer
                if let Some(ref mut deferral) = app.pending_deferral {
                    deferral.input.push(c);
                }
            }
            KeyCode::Backspace => {
                // Remove last digit from input buffer
                if let Some(ref mut deferral) = app.pending_deferral {
                    deferral.input.pop();
                }
            }
            KeyCode::Enter => {
                // User confirmed - process the deferral
                if let Some(deferral) = &app.pending_deferral {
                    // Parse the input as days (or use default if empty)
                    let days: u32 = if deferral.input.is_empty() {
                        // Use default from config
                        deferral.default_days
                    } else if let Ok(parsed_days) = deferral.input.parse::<u32>() {
                        // Validate that days > 0
                        if parsed_days == 0 {
                            // Invalid input - clear and return (user can try again)
                            tracing::warn!("Invalid deferral period: must be > 0");
                            app.pending_deferral = None;
                            return;
                        }
                        parsed_days
                    } else {
                        // Parse error - clear and return
                        tracing::warn!("Invalid deferral input: {}", deferral.input);
                        app.pending_deferral = None;
                        return;
                    };

                    // Calculate deferred_until timestamp
                    let now = jiff::Timestamp::now();
                    let days_i64 = i64::from(days);
                    let deferred_until = now.as_second() + (days_i64 * 86400);

                    // Update directory status to 'deferred' and set deferred_until
                    if let Err(e) = db.update_directory_status(deferral.directory_id, "deferred") {
                        tracing::warn!("Failed to defer directory {}: {}", deferral.path, e);
                    } else {
                        // Update deferred_until timestamp using raw SQL
                        // Note: This is a limitation of the current CRUD interface which doesn't
                        // expose deferred_until updates. Future work: add update_directory_deferral() method.
                        let conn = db.conn();
                        if let Err(e) = conn.execute(
                            "UPDATE directories SET deferred_until = ? WHERE id = ?",
                            rusqlite::params![deferred_until, deferral.directory_id],
                        ) {
                            tracing::warn!(
                                "Failed to set deferred_until for {}: {}",
                                deferral.path,
                                e
                            );
                        } else {
                            // Record audit entry
                            let audit = AuditService::new(db);
                            let user = AuditService::current_user();
                            let details = Some(format!("Deferred for {days} days"));
                            if let Err(e) = audit.record(
                                &user,
                                AuditAction::Defer,
                                Some(&deferral.path),
                                details.as_deref(),
                                Some(deferral.directory_id),
                            ) {
                                tracing::warn!("Failed to record audit entry for deferral: {}", e);
                            }
                        }
                    }
                }
                // Clear pending deferral
                app.pending_deferral = None;
            }
            KeyCode::Esc => {
                // Cancel deferral input
                app.pending_deferral = None;
            }
            _ => {
                // Ignore other keys during input
            }
        }
    }

    fn handle_audit_log(app: &mut App, key: KeyEvent) {
        match key.code {
            KeyCode::Char('q' | 'h') | KeyCode::Esc => {
                app.view = View::FileList;
            }
            KeyCode::Char('j') | KeyCode::Down => {
                app.sidebar_selected_index = app.sidebar_selected_index.saturating_add(1);
            }
            KeyCode::Char('k') | KeyCode::Up => {
                app.sidebar_selected_index = app.sidebar_selected_index.saturating_sub(1);
            }
            KeyCode::Char('g') => app.sidebar_selected_index = 0, // Go to top
            KeyCode::Char('G') => app.select_last_sidebar(app.sidebar_len.get()), // Go to bottom
            _ => {}
        }
    }

    fn handle_help(app: &mut App, key: KeyEvent) {
        // Any key closes the help view
        let _ = key;
        app.view = View::FileList;
    }

    /// Initiate file deletion by querying the database for the selected file(s).
    ///
    /// If files are selected via multi-select, delete all selected files.
    /// Otherwise, delete the currently focused file.
    fn initiate_file_delete(app: &mut App, db: &Database) {
        // Get the currently selected directory ID
        let Some(directory_id) = app.current_directory_id.get() else {
            tracing::warn!("Cannot delete file: no directory selected");
            return;
        };

        // Query files for this directory
        let files = match db.list_files_by_directory(directory_id) {
            Ok(files) => files,
            Err(e) => {
                tracing::warn!("Failed to query files: {}", e);
                return;
            }
        };

        // Determine which files to delete
        let files_to_delete: Vec<(i64, String)> = if app.selected_files.is_empty() {
            // No selection - use currently focused file
            if let Some(file) = files.get(app.file_selected_index) {
                vec![(file.id, file.path.clone())]
            } else {
                tracing::warn!("No file selected (index out of bounds)");
                return;
            }
        } else {
            // Use selected files
            files
                .into_iter()
                .filter(|f| app.selected_files.contains(&f.id))
                .map(|f| (f.id, f.path.clone()))
                .collect()
        };

        if files_to_delete.is_empty() {
            tracing::warn!("No files to delete");
            return;
        }

        // Set pending deletion state
        app.pending_file_delete = Some(files_to_delete);
    }

    /// Handle file deletion confirmation (y/n/Esc).
    fn handle_file_delete_confirmation(app: &mut App, db: &Database, key: KeyEvent) {
        match key.code {
            KeyCode::Char('y' | 'Y') => {
                // User confirmed - perform the deletion for all pending files
                if let Some(files) = &app.pending_file_delete {
                    let audit = AuditService::new(db);
                    let user = AuditService::current_user();
                    let dir_id = app.current_directory_id.get();

                    for (file_id, path) in files {
                        if let Err(e) = db.delete_file(*file_id, path) {
                            tracing::warn!("Failed to delete file {}: {}", path, e);
                        } else {
                            // Record audit entry for each file
                            if let Err(e) = audit.record(
                                &user,
                                AuditAction::Remove,
                                Some(path),
                                Some("File deleted by user"),
                                dir_id,
                            ) {
                                tracing::warn!(
                                    "Failed to record audit entry for file deletion: {}",
                                    e
                                );
                            }
                        }
                    }
                }
                // Clear pending deletion and selection
                app.pending_file_delete = None;
                app.clear_selection();
            }
            KeyCode::Char('n' | 'N') | KeyCode::Esc => {
                // Cancel deletion
                app.pending_file_delete = None;
            }
            _ => {
                // Ignore other keys during confirmation
            }
        }
    }

    /// Initiate file deferral by setting up the input state.
    ///
    /// If files are selected via multi-select, defer all selected files with the same number of days.
    /// Otherwise, defer the currently focused file.
    fn initiate_file_defer(app: &mut App, config: &Config, db: &Database) {
        // Get the currently selected directory ID
        let Some(directory_id) = app.current_directory_id.get() else {
            tracing::warn!("Cannot defer file: no directory selected");
            return;
        };

        // Query files for this directory
        let files = match db.list_files_by_directory(directory_id) {
            Ok(files) => files,
            Err(e) => {
                tracing::warn!("Failed to query files: {}", e);
                return;
            }
        };

        // Determine which files to defer
        let files_to_defer: Vec<i64> = if app.selected_files.is_empty() {
            // No selection - use currently focused file
            if let Some(file) = files.get(app.file_selected_index) {
                vec![file.id]
            } else {
                tracing::warn!("No file selected (index out of bounds)");
                return;
            }
        } else {
            // Use selected files
            files
                .iter()
                .filter(|f| app.selected_files.contains(&f.id))
                .map(|f| f.id)
                .collect()
        };

        if files_to_defer.is_empty() {
            tracing::warn!("No files to defer");
            return;
        }

        // Get first file path for display in modal
        let first_file_path = files
            .iter()
            .find(|f| files_to_defer.contains(&f.id))
            .map_or_else(|| "unknown".to_string(), |f| f.path.clone());

        // Set pending deferral state (using first file_id in directory_id field, rest in additional_file_ids)
        app.pending_file_deferral = Some(super::PendingDeferral {
            directory_id: files_to_defer[0], // Note: reusing PendingDeferral struct, directory_id field holds file_id
            path: first_file_path,
            input: String::new(),
            default_days: config.expiration_days,
            additional_file_ids: files_to_defer[1..].to_vec(),
        });
    }

    /// Handle file deferral input (digits/backspace/enter/esc).
    fn handle_file_deferral_input(app: &mut App, _config: &Config, db: &Database, key: KeyEvent) {
        match key.code {
            KeyCode::Char(c) if c.is_ascii_digit() => {
                // Append digit to input buffer
                if let Some(ref mut deferral) = app.pending_file_deferral {
                    deferral.input.push(c);
                }
            }
            KeyCode::Backspace => {
                // Remove last digit from input buffer
                if let Some(ref mut deferral) = app.pending_file_deferral {
                    deferral.input.pop();
                }
            }
            KeyCode::Enter => {
                // User confirmed - process the deferral
                if let Some(deferral) = &app.pending_file_deferral {
                    // Parse the input as days (or use default if empty)
                    let days: u32 = if deferral.input.is_empty() {
                        deferral.default_days
                    } else if let Ok(parsed_days) = deferral.input.parse::<u32>() {
                        if parsed_days == 0 {
                            tracing::warn!("Invalid deferral period: must be > 0");
                            app.pending_file_deferral = None;
                            return;
                        }
                        parsed_days
                    } else {
                        tracing::warn!("Invalid deferral input: {}", deferral.input);
                        app.pending_file_deferral = None;
                        return;
                    };

                    // Calculate deferred_until timestamp
                    let now = jiff::Timestamp::now();
                    let days_i64 = i64::from(days);
                    let deferred_until = now.as_second() + (days_i64 * 86400);

                    // Update file status to 'deferred' and set deferred_until for all files
                    let file_ids_to_defer: Vec<i64> = std::iter::once(deferral.directory_id)
                        .chain(deferral.additional_file_ids.iter().copied())
                        .collect();

                    let audit = AuditService::new(db);
                    let user = AuditService::current_user();
                    let details = Some(format!("Deferred for {days} days"));
                    let dir_id = app.current_directory_id.get();

                    for file_id in file_ids_to_defer {
                        if let Err(e) = db.defer_file(file_id, deferred_until) {
                            tracing::warn!("Failed to defer file with id {}: {}", file_id, e);
                        } else {
                            // Record audit entry for each file (path is not available for all, using first file's path)
                            if let Err(e) = audit.record(
                                &user,
                                AuditAction::Defer,
                                Some(&deferral.path),
                                details.as_deref(),
                                dir_id,
                            ) {
                                tracing::warn!(
                                    "Failed to record audit entry for file deferral: {}",
                                    e
                                );
                            }
                        }
                    }
                }
                // Clear pending deferral and selection
                app.pending_file_deferral = None;
                app.clear_selection();
            }
            KeyCode::Esc => {
                // Cancel deferral input
                app.pending_file_deferral = None;
            }
            _ => {
                // Ignore other keys during input
            }
        }
    }

    /// Initiate file ignore by setting up the confirmation state.
    ///
    /// If files are selected via multi-select, ignore all selected files.
    /// Otherwise, ignore the currently focused file.
    fn initiate_file_ignore(app: &mut App, db: &Database) {
        // Get the currently selected directory ID
        let Some(directory_id) = app.current_directory_id.get() else {
            tracing::warn!("Cannot ignore file: no directory selected");
            return;
        };

        // Query files for this directory
        let files = match db.list_files_by_directory(directory_id) {
            Ok(files) => files,
            Err(e) => {
                tracing::warn!("Failed to query files: {}", e);
                return;
            }
        };

        // Determine which files to ignore
        let files_to_ignore: Vec<(i64, String)> = if app.selected_files.is_empty() {
            // No selection - use currently focused file
            if let Some(file) = files.get(app.file_selected_index) {
                vec![(file.id, file.path.clone())]
            } else {
                tracing::warn!("No file selected (index out of bounds)");
                return;
            }
        } else {
            // Use selected files
            files
                .into_iter()
                .filter(|f| app.selected_files.contains(&f.id))
                .map(|f| (f.id, f.path.clone()))
                .collect()
        };

        if files_to_ignore.is_empty() {
            tracing::warn!("No files to ignore");
            return;
        }

        // Set pending ignore state
        app.pending_file_ignore = Some(files_to_ignore);
    }

    /// Handle file ignore confirmation (y/n/Esc).
    fn handle_file_ignore_confirmation(app: &mut App, db: &Database, key: KeyEvent) {
        match key.code {
            KeyCode::Char('y' | 'Y') => {
                // User confirmed - perform the ignore for all pending files
                if let Some(files) = &app.pending_file_ignore {
                    let audit = AuditService::new(db);
                    let user = AuditService::current_user();
                    let dir_id = app.current_directory_id.get();

                    for (file_id, path) in files {
                        if let Err(e) = db.update_file_status(*file_id, "ignored") {
                            tracing::warn!("Failed to ignore file {}: {}", path, e);
                        } else {
                            // Record audit entry for each file
                            if let Err(e) =
                                audit.record(&user, AuditAction::Ignore, Some(path), None, dir_id)
                            {
                                tracing::warn!(
                                    "Failed to record audit entry for file ignore: {}",
                                    e
                                );
                            }
                        }
                    }
                }
                // Clear pending ignore and selection
                app.pending_file_ignore = None;
                app.clear_selection();
            }
            KeyCode::Char('n' | 'N') | KeyCode::Esc => {
                // Cancel ignore
                app.pending_file_ignore = None;
            }
            _ => {
                // Ignore other keys during confirmation
            }
        }
    }

    /// Initiate file approval by setting up the confirmation state.
    ///
    /// If files are selected via multi-select, approve all selected files.
    /// Otherwise, approve the currently focused file.
    fn initiate_file_approve(app: &mut App, db: &Database) {
        // Get the currently selected directory ID
        let Some(directory_id) = app.current_directory_id.get() else {
            tracing::warn!("Cannot approve file: no directory selected");
            return;
        };

        // Query files for this directory
        let files = match db.list_files_by_directory(directory_id) {
            Ok(files) => files,
            Err(e) => {
                tracing::warn!("Failed to query files: {}", e);
                return;
            }
        };

        // Determine which files to approve
        let files_to_approve: Vec<(i64, String)> = if app.selected_files.is_empty() {
            // No selection - use currently focused file
            if let Some(file) = files.get(app.file_selected_index) {
                vec![(file.id, file.path.clone())]
            } else {
                tracing::warn!("No file selected (index out of bounds)");
                return;
            }
        } else {
            // Use selected files
            files
                .into_iter()
                .filter(|f| app.selected_files.contains(&f.id))
                .map(|f| (f.id, f.path.clone()))
                .collect()
        };

        if files_to_approve.is_empty() {
            tracing::warn!("No files to approve");
            return;
        }

        // Set pending approval state
        app.pending_file_approval = Some(files_to_approve);
    }

    /// Handle file approval confirmation (y/n/Esc).
    fn handle_file_approval_confirmation(app: &mut App, db: &Database, key: KeyEvent) {
        match key.code {
            KeyCode::Char('y' | 'Y') => {
                // User confirmed - perform the approval for all pending files
                if let Some(files) = &app.pending_file_approval {
                    let audit = AuditService::new(db);
                    let user = AuditService::current_user();
                    let dir_id = app.current_directory_id.get();

                    for (file_id, path) in files {
                        if let Err(e) = db.update_file_status(*file_id, "approved") {
                            tracing::warn!("Failed to approve file {}: {}", path, e);
                        } else {
                            // Record audit entry for each file
                            if let Err(e) =
                                audit.record(&user, AuditAction::Approve, Some(path), None, dir_id)
                            {
                                tracing::warn!(
                                    "Failed to record audit entry for file approval: {}",
                                    e
                                );
                            }
                        }
                    }
                }
                // Clear pending approval and selection
                app.pending_file_approval = None;
                app.clear_selection();
            }
            KeyCode::Char('n' | 'N') | KeyCode::Esc => {
                // Cancel approval
                app.pending_file_approval = None;
            }
            _ => {
                // Ignore other keys during confirmation
            }
        }
    }

    /// Handle add path text input (characters/backspace/enter/esc).
    fn handle_add_path_input(app: &mut App, config: &Config, _db: &Database, key: KeyEvent) {
        match key.code {
            KeyCode::Char(c) if !c.is_control() => {
                // Append character to input buffer
                if let Some(ref mut input) = app.pending_add_path {
                    input.push(c);
                }
            }
            KeyCode::Backspace => {
                // Remove last character from input buffer
                if let Some(ref mut input) = app.pending_add_path {
                    input.pop();
                }
            }
            KeyCode::Enter => {
                // User confirmed - add the path
                if let Some(input) = &app.pending_add_path {
                    if input.trim().is_empty() {
                        tracing::warn!("Cannot add empty path");
                        app.pending_add_path = None;
                        return;
                    }

                    // Expand tilde
                    let expanded_path = shellexpand::tilde(input.trim());
                    let path = std::path::PathBuf::from(expanded_path.as_ref());

                    // Validate path exists and is a directory
                    if !path.exists() {
                        tracing::warn!("Path does not exist: {}", path.display());
                        app.pending_add_path = None;
                        return;
                    }

                    if !path.is_dir() {
                        tracing::warn!("Path is not a directory: {}", path.display());
                        app.pending_add_path = None;
                        return;
                    }

                    // Canonicalize to prevent duplicates
                    let canonical_path = match path.canonicalize() {
                        Ok(p) => p,
                        Err(e) => {
                            tracing::warn!("Failed to canonicalize path {}: {}", path.display(), e);
                            app.pending_add_path = None;
                            return;
                        }
                    };

                    // Check if already tracked
                    if config.tracked_paths.contains(&canonical_path) {
                        tracing::warn!("Path already tracked: {}", canonical_path.display());
                        app.pending_add_path = None;
                        return;
                    }

                    // Add to config
                    let mut new_config = config.clone();
                    new_config.tracked_paths.push(canonical_path.clone());

                    // Save config
                    let paths = crate::config::AppPaths::new();
                    if let Err(e) = new_config.save(&paths) {
                        tracing::warn!("Failed to save config: {}", e);
                        app.pending_add_path = None;
                        return;
                    }

                    tracing::info!(
                        "Added tracked path: {} (will be scanned on next daemon cycle or manual scan)",
                        canonical_path.display()
                    );
                }

                // Clear pending add path
                app.pending_add_path = None;
            }
            KeyCode::Esc => {
                // Cancel add path input
                app.pending_add_path = None;
            }
            _ => {
                // Ignore other keys during input
            }
        }
    }

    /// Initiate path removal by querying config for the selected sidebar directory.
    fn initiate_remove_path(app: &mut App, _config: &Config, db: &Database) {
        // Get list of tracked directories from database
        let directories = match db.list_directories(None) {
            Ok(dirs) => dirs,
            Err(e) => {
                tracing::warn!("Failed to query directories: {}", e);
                return;
            }
        };

        // Get the selected directory path
        if let Some(dir) = directories.get(app.sidebar_selected_index) {
            app.pending_remove_path = Some(dir.path.clone());
        } else {
            tracing::warn!("No directory selected for removal");
        }
    }

    /// Handle remove path confirmation (y/n/Esc).
    fn handle_remove_path_confirmation(app: &mut App, config: &Config, key: KeyEvent) {
        match key.code {
            KeyCode::Char('y' | 'Y') => {
                // User confirmed - remove the path
                if let Some(path_to_remove) = &app.pending_remove_path {
                    let mut new_config = config.clone();

                    // Remove from tracked_paths
                    new_config.tracked_paths.retain(|p| p != path_to_remove);

                    // Save config
                    let paths = crate::config::AppPaths::new();
                    if let Err(e) = new_config.save(&paths) {
                        tracing::warn!("Failed to save config: {}", e);
                    } else {
                        tracing::info!("Removed tracked path: {}", path_to_remove);
                    }
                }

                // Clear pending remove path
                app.pending_remove_path = None;
            }
            KeyCode::Char('n' | 'N') | KeyCode::Esc => {
                // Cancel removal
                app.pending_remove_path = None;
            }
            _ => {
                // Ignore other keys during confirmation
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::db::Database;
    use crate::tui::PendingDeferral;
    use tempfile::tempdir;

    fn make_key_event(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::empty())
    }

    fn make_key_event_with_mods(code: KeyCode, modifiers: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, modifiers)
    }

    fn temp_database() -> (Database, tempfile::TempDir) {
        let dir = tempdir().expect("Failed to create temp dir");
        let db_path = dir.path().join("test.db");
        let db = Database::open(&db_path).expect("Failed to create test database");
        (db, dir)
    }

    fn test_config() -> Config {
        Config::default()
    }

    // ===== Navigation and Focus Tests =====

    #[test]
    fn tab_switches_focus_between_panels() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Start with sidebar focused
        assert_eq!(app.focus_panel, FocusPanel::Sidebar);

        // Tab to main panel
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Tab));
        assert_eq!(app.focus_panel, FocusPanel::MainPanel);

        // Tab back to sidebar
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Tab));
        assert_eq!(app.focus_panel, FocusPanel::Sidebar);
    }

    #[test]
    fn h_focuses_sidebar() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        app.focus_panel = FocusPanel::MainPanel;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('h')));
        assert_eq!(app.focus_panel, FocusPanel::Sidebar);
    }

    #[test]
    fn l_focuses_main_panel() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        app.focus_panel = FocusPanel::Sidebar;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('l')));
        assert_eq!(app.focus_panel, FocusPanel::MainPanel);
    }

    #[test]
    fn j_navigates_down_in_focused_panel() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Navigate down in sidebar
        app.focus_panel = FocusPanel::Sidebar;
        app.sidebar_selected_index = 0;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('j')));
        assert_eq!(app.sidebar_selected_index, 1);
        assert_eq!(app.file_selected_index, 0, "File index should not change");

        // Navigate down in main panel
        app.focus_panel = FocusPanel::MainPanel;
        app.file_selected_index = 0;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('j')));
        assert_eq!(app.file_selected_index, 1);
        assert_eq!(
            app.sidebar_selected_index, 1,
            "Sidebar index should not change"
        );
    }

    #[test]
    fn k_navigates_up_in_focused_panel() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Navigate up in sidebar
        app.focus_panel = FocusPanel::Sidebar;
        app.sidebar_selected_index = 5;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('k')));
        assert_eq!(app.sidebar_selected_index, 4);

        // Navigate up in main panel
        app.focus_panel = FocusPanel::MainPanel;
        app.file_selected_index = 5;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('k')));
        assert_eq!(app.file_selected_index, 4);
    }

    #[test]
    fn g_goes_to_top_of_focused_panel() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Go to top in sidebar
        app.focus_panel = FocusPanel::Sidebar;
        app.sidebar_selected_index = 10;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('g')));
        assert_eq!(app.sidebar_selected_index, 0);

        // Go to top in main panel
        app.focus_panel = FocusPanel::MainPanel;
        app.file_selected_index = 10;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('g')));
        assert_eq!(app.file_selected_index, 0);
    }

    #[test]
    fn capital_g_goes_to_bottom_of_focused_panel() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Simulate list lengths (normally set by render)
        app.sidebar_len.set(10);
        app.file_list_len.set(20);

        // Go to bottom in sidebar
        app.focus_panel = FocusPanel::Sidebar;
        app.sidebar_selected_index = 0;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('G')));
        assert_eq!(app.sidebar_selected_index, 9); // len - 1

        // Go to bottom in main panel
        app.focus_panel = FocusPanel::MainPanel;
        app.file_selected_index = 0;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('G')));
        assert_eq!(app.file_selected_index, 19); // len - 1
    }

    // ===== Sort Mode Tests =====

    #[test]
    fn s_cycles_sort_modes() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        assert_eq!(app.sort_mode, SortMode::Expiration);

        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('s')));
        assert_eq!(app.sort_mode, SortMode::Size);

        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('s')));
        assert_eq!(app.sort_mode, SortMode::Name);

        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('s')));
        assert_eq!(app.sort_mode, SortMode::Modified);

        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('s')));
        assert_eq!(app.sort_mode, SortMode::Expiration);
    }

    // ===== View Switching Tests =====

    #[test]
    fn a_switches_to_audit_log_view() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        assert_eq!(app.view, View::FileList);
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('a')));
        assert_eq!(app.view, View::AuditLog);
    }

    #[test]
    fn question_mark_switches_to_help_view() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        assert_eq!(app.view, View::FileList);
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('?')));
        assert_eq!(app.view, View::Help);
    }

    #[test]
    fn help_view_closes_on_any_key() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        app.view = View::Help;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('x')));
        assert_eq!(app.view, View::FileList);
    }

    #[test]
    fn audit_log_view_returns_to_file_list_on_q() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        app.view = View::AuditLog;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('q')));
        assert_eq!(app.view, View::FileList);
    }

    // ===== Quit Tests =====

    #[test]
    fn q_quits_application() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        assert!(!app.should_quit);
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('q')));
        assert!(app.should_quit);
    }

    #[test]
    fn ctrl_c_quits_application() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        assert!(!app.should_quit);
        InputHandler::handle(
            &mut app,
            &config,
            &db,
            make_key_event_with_mods(KeyCode::Char('c'), KeyModifiers::CONTROL),
        );
        assert!(app.should_quit);
    }

    // ===== File-Level Action Tests =====

    /// Helper to set up test with a directory and files in the database
    fn setup_with_files(db: &Database) -> (i64, Vec<i64>) {
        // Create a test directory
        let dir_id = db
            .insert_or_update_directory("/test/dir", 1000, 2, Some(100), 200)
            .expect("Failed to create test directory");

        // Insert two test files
        let file1_id = db
            .insert_or_update_file(dir_id, "/test/dir/file1.txt", 500, 100)
            .expect("Failed to create file1");
        let file2_id = db
            .insert_or_update_file(dir_id, "/test/dir/file2.txt", 500, 150)
            .expect("Failed to create file2");

        (dir_id, vec![file1_id, file2_id])
    }

    #[test]
    fn d_key_initiates_file_delete_confirmation() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Set up database with files
        let (dir_id, file_ids) = setup_with_files(&db);

        // Set up app state to simulate viewing directory with first file selected
        app.current_directory_id.set(Some(dir_id));
        app.focus_panel = FocusPanel::MainPanel;
        app.file_selected_index = 0;

        // Press 'd' key
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('d')));

        // Should set pending_file_delete with first file
        assert!(
            app.pending_file_delete.is_some(),
            "pending_file_delete should be set"
        );
        let files = app
            .pending_file_delete
            .as_ref()
            .expect("Expected pending delete");
        assert_eq!(files.len(), 1, "Should have one file pending deletion");
        assert_eq!(files[0].0, file_ids[0], "Should be first file");
        assert_eq!(
            files[0].1, "/test/dir/file1.txt",
            "Path should match first file"
        );
    }

    #[test]
    fn d_key_ignored_when_sidebar_focused() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Set up database with files
        let (dir_id, _file_ids) = setup_with_files(&db);

        // Focus sidebar (not main panel)
        app.current_directory_id.set(Some(dir_id));
        app.focus_panel = FocusPanel::Sidebar;

        // Press 'd' key
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('d')));

        // Should NOT set pending_file_delete when sidebar is focused
        assert!(
            app.pending_file_delete.is_none(),
            "pending_file_delete should not be set when sidebar focused"
        );
    }

    #[test]
    fn file_delete_confirmation_y_deletes_file() {
        let (db, _db_dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Create a real temporary file for deletion
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test_file.txt");
        std::fs::write(&file_path, b"test content").expect("Failed to create temp file");

        // Set up database with the real file path
        let dir_id = db
            .insert_or_update_directory(
                temp_dir.path().to_str().expect("Invalid path"),
                13,
                1,
                Some(100),
                200,
            )
            .expect("Failed to create test directory");
        let file_id = db
            .insert_or_update_file(dir_id, file_path.to_str().expect("Invalid path"), 13, 100)
            .expect("Failed to create file");

        app.current_directory_id.set(Some(dir_id));

        // Manually set pending delete (simulating 'd' key press)
        app.pending_file_delete = Some(vec![(
            file_id,
            file_path.to_str().expect("Invalid path").to_string(),
        )]);

        // Press 'y' to confirm
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('y')));

        // Should clear pending delete
        assert!(
            app.pending_file_delete.is_none(),
            "pending_file_delete should be cleared"
        );

        // File should be marked as removed in database
        let files = db
            .list_files_by_directory(dir_id)
            .expect("Failed to list files");
        let file = files
            .iter()
            .find(|f| f.id == file_id)
            .expect("File should still exist in DB");
        assert_eq!(file.status, "removed", "File status should be 'removed'");

        // File should be deleted from filesystem
        assert!(
            !file_path.exists(),
            "File should be deleted from filesystem"
        );
    }

    #[test]
    fn file_delete_confirmation_n_cancels() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Set up database with files
        let (dir_id, file_ids) = setup_with_files(&db);
        app.current_directory_id.set(Some(dir_id));

        // Manually set pending delete
        app.pending_file_delete = Some(vec![(file_ids[0], "/test/dir/file1.txt".to_string())]);

        // Press 'n' to cancel
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('n')));

        // Should clear pending delete
        assert!(
            app.pending_file_delete.is_none(),
            "pending_file_delete should be cleared"
        );

        // File should still be in tracked status
        let files = db
            .list_files_by_directory(dir_id)
            .expect("Failed to list files");
        let file = files
            .iter()
            .find(|f| f.id == file_ids[0])
            .expect("File should exist");
        assert_eq!(
            file.status, "tracked",
            "File status should remain 'tracked'"
        );
    }

    #[test]
    fn r_key_initiates_file_deferral() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Set up database with files
        let (dir_id, _file_ids) = setup_with_files(&db);

        // Set up app state
        app.current_directory_id.set(Some(dir_id));
        app.focus_panel = FocusPanel::MainPanel;
        app.file_selected_index = 0;

        // Press 'r' key
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('r')));

        // Should set pending_file_deferral
        assert!(
            app.pending_file_deferral.is_some(),
            "pending_file_deferral should be set"
        );
        let deferral = app
            .pending_file_deferral
            .as_ref()
            .expect("Expected deferral");
        assert_eq!(deferral.path, "/test/dir/file1.txt");
        assert_eq!(deferral.default_days, 90); // from test_config
    }

    #[test]
    fn file_deferral_enter_confirms_with_default_days() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Set up database with files
        let (dir_id, file_ids) = setup_with_files(&db);
        app.current_directory_id.set(Some(dir_id));

        // Manually set pending deferral (empty input means use default)
        app.pending_file_deferral = Some(PendingDeferral {
            directory_id: file_ids[0], // Note: field name is misleading, it holds file_id
            path: "/test/dir/file1.txt".to_string(),
            input: String::new(),
            default_days: 90,
            additional_file_ids: Vec::new(),
        });

        // Press Enter to confirm
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Enter));

        // Should clear pending deferral
        assert!(
            app.pending_file_deferral.is_none(),
            "pending_file_deferral should be cleared"
        );

        // File should be marked as deferred
        let files = db
            .list_files_by_directory(dir_id)
            .expect("Failed to list files");
        let file = files
            .iter()
            .find(|f| f.id == file_ids[0])
            .expect("File should exist");
        assert_eq!(file.status, "deferred", "File status should be 'deferred'");
        assert!(
            file.deferred_until.is_some(),
            "deferred_until should be set"
        );
    }

    #[test]
    fn file_deferral_enter_confirms_with_custom_days() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Set up database with files
        let (dir_id, file_ids) = setup_with_files(&db);
        app.current_directory_id.set(Some(dir_id));

        // Manually set pending deferral with input
        app.pending_file_deferral = Some(PendingDeferral {
            directory_id: file_ids[0],
            path: "/test/dir/file1.txt".to_string(),
            input: "30".to_string(),
            default_days: 90,
            additional_file_ids: Vec::new(),
        });

        // Press Enter to confirm
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Enter));

        // Should clear pending deferral
        assert!(
            app.pending_file_deferral.is_none(),
            "pending_file_deferral should be cleared"
        );

        // File should be deferred with calculated timestamp
        let files = db
            .list_files_by_directory(dir_id)
            .expect("Failed to list files");
        let file = files
            .iter()
            .find(|f| f.id == file_ids[0])
            .expect("File should exist");
        assert_eq!(file.status, "deferred");

        // Verify deferred_until is approximately 30 days in the future
        let now = jiff::Timestamp::now().as_second();
        let days_30_secs = 30 * 86400;
        let expected_until = now + days_30_secs;
        let actual_until = file.deferred_until.expect("deferred_until should be set");
        let diff = (actual_until - expected_until).abs();
        assert!(
            diff < 10,
            "deferred_until should be approximately 30 days from now (diff: {diff})"
        );
    }

    #[test]
    fn i_key_initiates_file_ignore() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Set up database with files
        let (dir_id, file_ids) = setup_with_files(&db);

        // Set up app state
        app.current_directory_id.set(Some(dir_id));
        app.focus_panel = FocusPanel::MainPanel;
        app.file_selected_index = 0;

        // Press 'i' key
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('i')));

        // Should set pending_file_ignore
        assert!(
            app.pending_file_ignore.is_some(),
            "pending_file_ignore should be set"
        );
        let files = app
            .pending_file_ignore
            .as_ref()
            .expect("Expected pending ignore");
        assert_eq!(files.len(), 1, "Should have one file pending ignore");
        assert_eq!(files[0].0, file_ids[0]);
        assert_eq!(files[0].1, "/test/dir/file1.txt");
    }

    #[test]
    fn file_ignore_confirmation_y_ignores_file() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Set up database with files
        let (dir_id, file_ids) = setup_with_files(&db);
        app.current_directory_id.set(Some(dir_id));

        // Manually set pending ignore
        app.pending_file_ignore = Some(vec![(file_ids[0], "/test/dir/file1.txt".to_string())]);

        // Press 'y' to confirm
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('y')));

        // Should clear pending ignore
        assert!(
            app.pending_file_ignore.is_none(),
            "pending_file_ignore should be cleared"
        );

        // File should be marked as ignored
        let files = db
            .list_files_by_directory(dir_id)
            .expect("Failed to list files");
        let file = files
            .iter()
            .find(|f| f.id == file_ids[0])
            .expect("File should exist");
        assert_eq!(file.status, "ignored", "File status should be 'ignored'");
    }

    #[test]
    fn x_key_initiates_file_approval() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Set up database with files
        let (dir_id, file_ids) = setup_with_files(&db);

        // Set up app state
        app.current_directory_id.set(Some(dir_id));
        app.focus_panel = FocusPanel::MainPanel;
        app.file_selected_index = 0;

        // Press 'x' key
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('x')));

        // Should set pending_file_approval
        assert!(
            app.pending_file_approval.is_some(),
            "pending_file_approval should be set"
        );
        let files = app
            .pending_file_approval
            .as_ref()
            .expect("Expected pending approval");
        assert_eq!(files.len(), 1, "Should have one file pending approval");
        assert_eq!(files[0].0, file_ids[0]);
        assert_eq!(files[0].1, "/test/dir/file1.txt");
    }

    #[test]
    fn file_approval_confirmation_y_approves_file() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Set up database with files
        let (dir_id, file_ids) = setup_with_files(&db);
        app.current_directory_id.set(Some(dir_id));

        // Manually set pending approval
        app.pending_file_approval = Some(vec![(file_ids[0], "/test/dir/file1.txt".to_string())]);

        // Press 'y' to confirm
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('y')));

        // Should clear pending approval
        assert!(
            app.pending_file_approval.is_none(),
            "pending_file_approval should be cleared"
        );

        // File should be marked as approved
        let files = db
            .list_files_by_directory(dir_id)
            .expect("Failed to list files");
        let file = files
            .iter()
            .find(|f| f.id == file_ids[0])
            .expect("File should exist");
        assert_eq!(file.status, "approved", "File status should be 'approved'");
    }
}
