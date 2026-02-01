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
use stagecrew::scanner::{
    Scanner, recalculate_directory_oldest_mtime, scan_and_persist, transition_expired_paths,
};

/// Test the complete workflow: initialize, scan, transition, approve, remove, audit.
///
/// This test verifies:
/// 1. Database initialization with schema
/// 2. Scanning directory trees with files of varying ages
/// 3. Directories and files are correctly stored in the database
/// 4. Expiration calculation identifies old files
/// 5. State transitions move expired paths to pending
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
    // - old_dir: 95 days old (will expire with 90 day policy)
    // - recent_dir: 10 days old (safe)
    // - middle_dir: 80 days old (within warning period)
    let old_dir = tracked_dir.join("old_data");
    let recent_dir = tracked_dir.join("recent_data");
    let middle_dir = tracked_dir.join("middle_data");

    fs::create_dir_all(&old_dir)
        .expect("failed to create old_data test directory - check disk space and permissions");
    fs::create_dir_all(&recent_dir)
        .expect("failed to create recent_data test directory - check disk space and permissions");
    fs::create_dir_all(&middle_dir)
        .expect("failed to create middle_data test directory - check disk space and permissions");

    // Create files and set their modification times
    create_file_with_age(&old_dir.join("file1.txt"), 95, 1024);
    create_file_with_age(&old_dir.join("file2.txt"), 95, 2048);
    create_file_with_age(&recent_dir.join("file3.txt"), 10, 512);
    create_file_with_age(&middle_dir.join("file4.txt"), 80, 4096);

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
        scan_summary.total_directories, 3,
        "should scan 3 directories"
    );
    assert_eq!(scan_summary.total_files, 4, "should scan 4 files");
    let expected_total_bytes = 1024 + 2048 + 512 + 4096;
    assert_eq!(
        scan_summary.total_size_bytes, expected_total_bytes,
        "should count all bytes"
    );

    // 4. Verify directories are in database
    let all_dirs = db.list_directories(None).expect("should list directories");
    assert_eq!(all_dirs.len(), 3, "should have 3 directories in database");

    // All directories should initially be 'tracked'
    for dir in &all_dirs {
        assert_eq!(
            dir.status, "tracked",
            "directories should start with tracked status"
        );
    }

    // Find the old directory in database
    let old_dir_path = old_dir.to_string_lossy().to_string();
    let old_dir_record = all_dirs
        .iter()
        .find(|d| d.path == old_dir_path)
        .expect("old_dir should be in database");

    // Verify old directory has correct metadata
    assert_eq!(old_dir_record.file_count, 2, "old_dir should have 2 files");
    assert_eq!(
        old_dir_record.size_bytes,
        1024 + 2048,
        "old_dir should have correct size"
    );
    assert!(
        old_dir_record.oldest_mtime.is_some(),
        "old_dir should have oldest_mtime"
    );

    // 5. Verify files are in database
    let old_dir_files = db
        .list_files_by_directory(old_dir_record.id)
        .expect("should list files");
    assert_eq!(
        old_dir_files.len(),
        2,
        "old_dir should have 2 files in database"
    );

    // 5b. Manually backdate tracked_since for old files to simulate long-tracked files
    // With the new tracked_since logic, newly-scanned old files get tracked_since = now,
    // giving them a full expiration period. To test expiration, we need to backdate tracked_since.
    let now = jiff::Timestamp::now();
    let ninetyfive_days_ago = now
        .checked_sub(jiff::SignedDuration::from_secs(95 * 86400))
        .expect("timestamp arithmetic");
    let eighty_days_ago = now
        .checked_sub(jiff::SignedDuration::from_secs(80 * 86400))
        .expect("timestamp arithmetic");
    // Update tracked_since for old_dir files (to 95 days ago)
    db.conn()
        .execute(
            "UPDATE files SET tracked_since = ?1 WHERE directory_id = ?2",
            (ninetyfive_days_ago.as_second(), old_dir_record.id),
        )
        .expect("failed to backdate tracked_since for old_dir files");
    // Update tracked_since for middle_dir files (to 80 days ago)
    let middle_dir_path = middle_dir.to_string_lossy().to_string();
    let middle_dir_record = db
        .get_directory_by_path(&middle_dir_path)
        .expect("should query middle_dir")
        .expect("middle_dir should exist");
    db.conn()
        .execute(
            "UPDATE files SET tracked_since = ?1 WHERE directory_id = ?2",
            (eighty_days_ago.as_second(), middle_dir_record.id),
        )
        .expect("failed to backdate tracked_since for middle_dir files");
    // Recalculate oldest_mtime for affected directories
    recalculate_directory_oldest_mtime(&db, old_dir_record.id)
        .expect("failed to recalculate old_dir oldest_mtime");
    recalculate_directory_oldest_mtime(&db, middle_dir_record.id)
        .expect("failed to recalculate middle_dir oldest_mtime");

    // 6. Transition expired paths (should move old_dir to pending)
    let transition_summary = transition_expired_paths(&db, config.expiration_days, false)
        .expect("transition should succeed");

    assert_eq!(
        transition_summary.expired_to_pending, 1,
        "should transition 1 path to pending"
    );
    assert_eq!(
        transition_summary.expired_to_approved, 0,
        "should not auto-approve (auto_remove=false)"
    );

    // Verify old_dir is now pending
    let pending_dirs = db
        .list_directories(Some("pending"))
        .expect("should list pending directories");
    assert_eq!(
        pending_dirs.len(),
        1,
        "should have 1 pending directory after transition"
    );
    assert_eq!(
        pending_dirs[0].path, old_dir_path,
        "old_dir should be pending"
    );

    // 7. Manually approve the old directory for removal
    let audit = AuditService::new(&db);
    let user = AuditService::current_user();

    db.update_directory_status(old_dir_record.id, "approved")
        .expect("should update status to approved");

    audit
        .record(
            &user,
            AuditAction::Approve,
            Some(&old_dir_path),
            Some("Manual approval for removal"),
            Some(old_dir_record.id),
        )
        .expect("should record approval in audit log");

    // Verify status change
    let approved_dirs = db
        .list_directories(Some("approved"))
        .expect("should list approved directories");
    assert_eq!(approved_dirs.len(), 1, "should have 1 approved directory");
    assert_eq!(
        approved_dirs[0].path, old_dir_path,
        "old_dir should be approved"
    );

    // 8. Perform removal
    let removal_summary = remove_approved(&db).expect("removal should succeed");

    assert_eq!(
        removal_summary.removed_count(),
        1,
        "should remove 1 directory"
    );
    assert_eq!(
        removal_summary.blocked_count(),
        0,
        "should have 0 blocked removals"
    );
    assert_eq!(
        removal_summary.total_bytes_freed(),
        1024 + 2048,
        "should free correct number of bytes"
    );

    // Verify directory was actually deleted from filesystem
    assert!(
        !old_dir.exists(),
        "old_dir should no longer exist on filesystem"
    );

    // Verify database shows removed status
    let removed_dirs = db
        .list_directories(Some("removed"))
        .expect("should list removed directories");
    assert_eq!(
        removed_dirs.len(),
        1,
        "should have 1 removed directory in database"
    );
    assert_eq!(
        removed_dirs[0].path, old_dir_path,
        "old_dir should have removed status"
    );

    // 9. Verify audit trail contains all actions
    let audit_entries = audit
        .list_recent(10)
        .expect("should list recent audit entries");

    // Should have at least 3 entries: scan, transition, approve, remove
    assert!(
        audit_entries.len() >= 4,
        "should have at least 4 audit entries (scan, transition, approve, remove)"
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
        Some(old_dir_path.clone()),
        "approve entry should reference old_dir"
    );
    assert_eq!(
        approve_entry.directory_id,
        Some(old_dir_record.id),
        "approve entry should have directory_id"
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
            .contains(&(1024 + 2048).to_string()),
        "remove entry should mention bytes freed"
    );

    // 10. Verify recent and middle directories are untouched
    assert!(
        recent_dir.exists(),
        "recent_dir should still exist (not expired)"
    );
    assert!(
        middle_dir.exists(),
        "middle_dir should still exist (within warning period)"
    );

    let tracked_dirs = db
        .list_directories(Some("tracked"))
        .expect("should list tracked directories");
    assert_eq!(
        tracked_dirs.len(),
        2,
        "should still have 2 tracked directories (recent and middle)"
    );

    // Verify stats were updated
    let stats = db.get_stats().expect("should get stats");
    assert_eq!(
        stats.total_tracked_paths, 3,
        "stats should show 3 total paths"
    );
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
