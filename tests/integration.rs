//! Integration tests for stagecrew.
//!
//! This module contains end-to-end tests that verify the full workflow
//! of stagecrew from initialization through scanning, expiration, approval,
//! and removal.

use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::time::SystemTime;

use tempfile::TempDir;

use stagecrew::audit::{AuditAction, AuditService};
use stagecrew::config::Config;
use stagecrew::db::Database;
use stagecrew::removal::remove_approved;
use stagecrew::scanner::{Scanner, scan_and_persist, transition_expired_paths};

/// Test the complete workflow: initialize, scan, transition, approve, remove, audit.
///
/// This test verifies:
/// 1. Database initialization with schema
/// 2. Scanning directory trees with files of varying ages
/// 3. Roots and entries are correctly stored in the database
/// 4. Expiration calculation identifies old files
/// 5. State transitions move expired entries to pending
/// 6. Manual approval changes status to approved
/// 7. Removal actually deletes files from filesystem
/// 8. Audit log records all actions
// Allow: This is an integration test that verifies the full workflow end-to-end.
// The length is justified by comprehensive coverage of all acceptance criteria.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn test_full_workflow() {
    // 1. Set up test environment with temporary directories
    let temp_root = TempDir::with_prefix("stagecrew-integration-").expect("failed to create integration test temp directory - check disk space and system temp directory permissions");
    let db_path = temp_root.path().join("test.db");
    let tracked_dir = temp_root.path().join("staging");
    fs::create_dir_all(&tracked_dir).expect("failed to create staging directory for integration test - check disk space and write permissions");

    // Create files with varying ages
    // - old_file: 95 days old (will expire with 90 day policy)
    // - recent_file: 10 days old (safe)
    // - middle_file: 80 days old (within warning period)
    let old_file = tracked_dir.join("old_data.txt");
    let recent_file = tracked_dir.join("recent_data.txt");
    let middle_file = tracked_dir.join("middle_data.txt");

    // Create files and set their modification times
    create_file_with_age(&old_file, 95, 1024);
    create_file_with_age(&recent_file, 10, 512);
    create_file_with_age(&middle_file, 80, 4096);

    // 2. Initialize database and config
    let db = Database::open(&db_path).expect("database should initialize");

    let mut config = Config::default();
    config.tracked_paths = vec![tracked_dir.clone()];
    config.expiration_days = 90;
    config.warning_days = 14;
    config.auto_remove = false;

    // 3. Perform initial scan
    let scanner = Scanner::new();
    let scan_summary = scan_and_persist(
        &db,
        &scanner,
        &config.tracked_paths,
        config.expiration_days,
        config.warning_days,
    )
    .await
    .expect("scan should succeed");

    // Verify scan results
    assert_eq!(
        scan_summary.total_directories, 1,
        "should scan 1 directory (the tracked root)"
    );
    assert_eq!(scan_summary.total_files, 3, "should scan 3 files");
    let expected_total_bytes = 1024 + 512 + 4096;
    assert_eq!(
        scan_summary.total_size_bytes, expected_total_bytes,
        "should count all bytes"
    );

    // 4. Verify root is in database
    let roots = db.list_roots().expect("should list roots");
    assert_eq!(roots.len(), 1, "should have 1 root in database");
    assert_eq!(
        roots[0].path,
        tracked_dir.to_string_lossy(),
        "root path should match"
    );

    // 5. Verify entries are in database
    let all_entries = db
        .list_entries_by_parent(&tracked_dir.to_string_lossy())
        .expect("should list entries");
    assert_eq!(
        all_entries.len(),
        3,
        "should have 3 file entries in database"
    );

    // All entries should initially be 'tracked'
    for entry in &all_entries {
        assert_eq!(
            entry.status, "tracked",
            "entries should start with tracked status"
        );
    }

    // Find the old file entry in database
    let old_file_path = old_file.to_string_lossy().to_string();
    let old_file_entry = all_entries
        .iter()
        .find(|e| e.path == old_file_path)
        .expect("old_file should be in database");

    // Verify old file entry has correct metadata
    assert_eq!(
        old_file_entry.size_bytes, 1024,
        "old_file should have correct size"
    );
    assert!(old_file_entry.mtime.is_some(), "old_file should have mtime");

    // 5b. Manually backdate tracked_since for old file to simulate long-tracked file
    // With the tracked_since logic, newly-scanned old files get tracked_since = now,
    // giving them a full expiration period. To test expiration, we need to backdate tracked_since.
    let now = jiff::Timestamp::now();
    let ninetyfive_days_ago = now
        .checked_sub(jiff::SignedDuration::from_secs(95 * 86400))
        .expect("timestamp arithmetic");

    // Update tracked_since for old_file
    db.conn()
        .execute(
            "UPDATE entries SET tracked_since = ?1 WHERE path = ?2",
            (ninetyfive_days_ago.as_second(), &old_file_path),
        )
        .expect("failed to backdate tracked_since for old_file");

    // 6. Transition expired paths (should move old_file to pending)
    let transition_summary = transition_expired_paths(&db, config.expiration_days, false)
        .expect("transition should succeed");

    assert_eq!(
        transition_summary.expired_to_pending, 1,
        "should transition 1 entry to pending"
    );
    assert_eq!(
        transition_summary.expired_to_approved, 0,
        "should not auto-approve (auto_remove=false)"
    );

    // Verify old_file is now pending
    let pending_entries = db
        .list_entries(Some("pending"))
        .expect("should list pending entries");
    assert_eq!(
        pending_entries.len(),
        1,
        "should have 1 pending entry after transition"
    );
    assert_eq!(
        pending_entries[0].path, old_file_path,
        "old_file should be pending"
    );

    // 7. Manually approve the old file for removal
    let audit = AuditService::new(&db);
    let user = AuditService::current_user();

    db.update_entry_status(old_file_entry.id, "approved")
        .expect("should update status to approved");

    audit
        .record(
            &user,
            AuditAction::Approve,
            Some(&old_file_path),
            Some("Manual approval for removal"),
            Some(old_file_entry.id),
        )
        .expect("should record approval in audit log");

    // Verify status change
    let approved_entries = db
        .list_entries(Some("approved"))
        .expect("should list approved entries");
    assert_eq!(approved_entries.len(), 1, "should have 1 approved entry");
    assert_eq!(
        approved_entries[0].path, old_file_path,
        "old_file should be approved"
    );

    // 8. Perform removal
    let removal_summary = remove_approved(&db).expect("removal should succeed");

    assert_eq!(removal_summary.removed_count(), 1, "should remove 1 entry");
    assert_eq!(
        removal_summary.blocked_count(),
        0,
        "should have 0 blocked removals"
    );
    assert_eq!(
        removal_summary.total_bytes_freed(),
        1024,
        "should free correct number of bytes"
    );

    // Verify file was actually deleted from filesystem
    assert!(
        !old_file.exists(),
        "old_file should no longer exist on filesystem"
    );

    // Verify database shows removed status
    let removed_entries = db
        .list_entries(Some("removed"))
        .expect("should list removed entries");
    assert_eq!(
        removed_entries.len(),
        1,
        "should have 1 removed entry in database"
    );
    assert_eq!(
        removed_entries[0].path, old_file_path,
        "old_file should have removed status"
    );

    // 9. Verify audit trail contains all actions
    let audit_entries = audit
        .list_recent(10)
        .expect("should list recent audit entries");

    // Should have at least 3 entries: scan, approve, remove
    assert!(
        audit_entries.len() >= 3,
        "should have at least 3 audit entries (scan, approve, remove), got {}",
        audit_entries.len()
    );

    // Check for specific audit actions
    let actions: Vec<String> = audit_entries.iter().map(|e| e.action.clone()).collect();
    assert!(
        actions.contains(&"scan".to_string()),
        "should have scan action"
    );
    assert!(
        actions.contains(&"approve".to_string()),
        "should have approve action"
    );
    assert!(
        actions.contains(&"remove".to_string()),
        "should have remove action"
    );

    // Verify approve action recorded correct details
    let approve_entry = audit_entries
        .iter()
        .find(|e| e.action == "approve")
        .expect("should find approve entry");
    assert_eq!(
        approve_entry.target_path,
        Some(old_file_path.clone()),
        "approve entry should reference old_file"
    );
    assert_eq!(
        approve_entry.entry_id,
        Some(old_file_entry.id),
        "approve entry should have entry_id"
    );

    // Verify remove action recorded bytes freed
    let remove_entry = audit_entries
        .iter()
        .find(|e| e.action == "remove")
        .expect("should find remove entry");
    assert!(
        remove_entry
            .details
            .as_ref()
            .expect("remove audit entry should have details field populated with bytes freed - check removal service records details correctly")
            .contains("1024"),
        "remove entry should mention bytes freed"
    );

    // 10. Verify recent and middle files are untouched
    assert!(
        recent_file.exists(),
        "recent_file should still exist (not expired)"
    );
    assert!(
        middle_file.exists(),
        "middle_file should still exist (within warning period)"
    );

    let tracked_entries: Vec<_> = db
        .list_entries(Some("tracked"))
        .expect("should list tracked entries")
        .into_iter()
        .filter(|e| !e.is_dir)
        .collect();
    assert_eq!(
        tracked_entries.len(),
        2,
        "should still have 2 tracked file entries (recent and middle files)"
    );

    // Verify stats were updated
    let stats = db.get_stats().expect("should get stats");
    assert_eq!(stats.total_files, 3, "stats should show 3 total files");
    assert!(
        stats.last_scan_completed.is_some(),
        "stats should have last_scan_completed timestamp"
    );
}

/// Helper function to create a file with a specific age (days old) and size.
///
/// Sets the file's modification time to `days_ago` days in the past.
fn create_file_with_age(path: &Path, days_ago: u64, size_bytes: usize) {
    let mut file = File::create(path).expect("should create file");

    // Write data to achieve desired size
    let data = vec![b'X'; size_bytes];
    file.write_all(&data).expect("should write data");
    file.flush().expect("should flush file");

    // Set modification time
    let now = SystemTime::now();
    let age_seconds = days_ago * 86400;
    let mtime = now
        .checked_sub(std::time::Duration::from_secs(age_seconds))
        .expect("should calculate past time");

    filetime::set_file_mtime(path, filetime::FileTime::from_system_time(mtime))
        .expect("should set mtime");
}
