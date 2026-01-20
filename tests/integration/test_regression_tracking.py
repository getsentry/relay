"""
Test regression tracking with orphaned RegressionGroups.

Fixes SENTRY-2DG5: Handles the case where RegressionGroup records exist
without corresponding GroupHash/Issue records.
"""

import logging
import pytest


def test_orphaned_regression_group_detection(mini_sentry, caplog):
    """
    Test that orphaned RegressionGroups (without corresponding Issue groups)
    are properly detected and logged.
    
    Scenario:
    1. Create a RegressionGroup (regression detected)
    2. Corresponding issue creation fails (e.g., rate limiting)
    3. Query for the issue group via bulk_get_groups_from_fingerprints
    4. Verify proper logging of "Missing issue group for regression issue"
    """
    project_id = 42
    fingerprint = "test-fingerprint-123"
    
    # Step 1: Create a RegressionGroup (regression is detected)
    regression_id = mini_sentry.create_regression_group(
        project_id=project_id,
        fingerprint=fingerprint,
        regression_data={
            "previous_release": "1.0.0",
            "current_release": "1.0.1"
        }
    )
    
    assert regression_id in mini_sentry.regression_groups
    
    # Step 2: Issue creation fails (simulated by NOT creating issue group)
    # In production this could be due to:
    # - Rate limiting
    # - Database errors
    # - Timing issues
    
    # Step 3: Run escalation check - query for the missing issue group
    with caplog.at_level(logging.WARNING):
        result = mini_sentry.bulk_get_groups_from_fingerprints(
            project_id=project_id,
            fingerprints=[fingerprint]
        )
    
    # Step 4: Verify proper handling
    assert result[fingerprint] is None
    assert any(
        "Missing issue group for regression issue" in record.message
        for record in caplog.records
    )
    
    # Verify the warning includes relevant details
    warning_msg = next(
        record.message for record in caplog.records
        if "Missing issue group for regression issue" in record.message
    )
    assert f"project_id={project_id}" in warning_msg
    assert f"fingerprint={fingerprint}" in warning_msg
    assert f"regression_id={regression_id}" in warning_msg


def test_successful_regression_with_issue_group(mini_sentry):
    """
    Test the normal case where RegressionGroup has a corresponding Issue group.
    """
    project_id = 42
    fingerprint = "test-fingerprint-456"
    
    # Create RegressionGroup
    regression_id = mini_sentry.create_regression_group(
        project_id=project_id,
        fingerprint=fingerprint
    )
    
    # Create corresponding Issue group (normal flow)
    group_id = mini_sentry.create_issue_group(
        project_id=project_id,
        fingerprint=fingerprint
    )
    
    # Query should succeed
    result = mini_sentry.bulk_get_groups_from_fingerprints(
        project_id=project_id,
        fingerprints=[fingerprint]
    )
    
    assert result[fingerprint] == group_id


def test_deleted_issue_group_creates_orphan(mini_sentry, caplog):
    """
    Test that deleting an Issue group creates an orphaned RegressionGroup.
    
    Scenario:
    1. Create RegressionGroup and Issue group (normal flow)
    2. Issue group is deleted (user action or cleanup)
    3. RegressionGroup remains orphaned
    4. Query detects the orphaned state
    """
    project_id = 42
    fingerprint = "test-fingerprint-789"
    
    # Normal flow: both regression and issue created
    regression_id = mini_sentry.create_regression_group(
        project_id=project_id,
        fingerprint=fingerprint
    )
    group_id = mini_sentry.create_issue_group(
        project_id=project_id,
        fingerprint=fingerprint
    )
    
    # Verify initial state is correct
    result = mini_sentry.bulk_get_groups_from_fingerprints(
        project_id=project_id,
        fingerprints=[fingerprint]
    )
    assert result[fingerprint] == group_id
    
    # Issue group is deleted (e.g., user merges/deletes issue)
    mini_sentry.delete_issue_group(group_id)
    
    # Query now detects orphaned regression
    with caplog.at_level(logging.WARNING):
        result = mini_sentry.bulk_get_groups_from_fingerprints(
            project_id=project_id,
            fingerprints=[fingerprint]
        )
    
    assert result[fingerprint] is None
    assert any(
        "Missing issue group for regression issue" in record.message
        for record in caplog.records
    )


def test_cleanup_orphaned_regressions(mini_sentry, caplog):
    """
    Test the cleanup utility that removes orphaned RegressionGroups.
    """
    project_id = 42
    
    # Create orphaned regression
    fingerprint1 = "orphaned-1"
    regression_id1 = mini_sentry.create_regression_group(
        project_id=project_id,
        fingerprint=fingerprint1
    )
    
    # Create regression with issue (not orphaned)
    fingerprint2 = "not-orphaned"
    regression_id2 = mini_sentry.create_regression_group(
        project_id=project_id,
        fingerprint=fingerprint2
    )
    mini_sentry.create_issue_group(
        project_id=project_id,
        fingerprint=fingerprint2
    )
    
    assert len(mini_sentry.regression_groups) == 2
    
    # Run cleanup
    with caplog.at_level(logging.INFO):
        orphaned = mini_sentry.cleanup_orphaned_regressions()
    
    # Verify orphaned regression was removed
    assert regression_id1 in orphaned
    assert regression_id2 not in orphaned
    assert len(mini_sentry.regression_groups) == 1
    assert regression_id1 not in mini_sentry.regression_groups
    assert regression_id2 in mini_sentry.regression_groups
    
    # Verify logging
    assert any(
        "Cleaning up orphaned RegressionGroup" in record.message
        for record in caplog.records
    )


def test_multiple_fingerprints_bulk_query(mini_sentry):
    """
    Test bulk query with mix of valid, invalid, and orphaned fingerprints.
    """
    project_id = 42
    
    # Fingerprint 1: Has issue group
    fp1 = "valid-fingerprint"
    group_id1 = mini_sentry.create_issue_group(project_id, fp1)
    
    # Fingerprint 2: Orphaned regression
    fp2 = "orphaned-fingerprint"
    mini_sentry.create_regression_group(project_id, fp2)
    
    # Fingerprint 3: Doesn't exist at all
    fp3 = "non-existent-fingerprint"
    
    # Bulk query
    result = mini_sentry.bulk_get_groups_from_fingerprints(
        project_id=project_id,
        fingerprints=[fp1, fp2, fp3]
    )
    
    assert result[fp1] == group_id1  # Valid
    assert result[fp2] is None  # Orphaned
    assert result[fp3] is None  # Non-existent


def test_group_hash_exists_but_issue_deleted(mini_sentry, caplog):
    """
    Test edge case where GroupHash exists but Issue group is missing.
    This could happen due to database inconsistency.
    """
    project_id = 42
    fingerprint = "inconsistent-state"
    
    # Manually create inconsistent state
    fake_group_id = "fake-group-123"
    key = (project_id, fingerprint)
    mini_sentry.group_hashes[key] = fake_group_id
    # Note: NOT creating the actual issue group
    
    with caplog.at_level(logging.WARNING):
        result = mini_sentry.bulk_get_groups_from_fingerprints(
            project_id=project_id,
            fingerprints=[fingerprint]
        )
    
    assert result[fingerprint] is None
    assert any(
        "GroupHash exists but Issue group missing" in record.message
        for record in caplog.records
    )
