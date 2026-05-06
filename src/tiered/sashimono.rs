//! Compact Sashimono contract vocabulary.
//!
//! This module is intentionally small and side-effect free. It pins the object
//! identity/validation rules that higher integration layers need before the
//! live VFS path starts publishing page deltas.

use serde::{Deserialize, Serialize};
use std::fmt;

pub const SASHIMONO_FORMAT_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectKind {
    RootPointer,
    Checkpoint,
    Delta,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommittedBoundary {
    AfterSqliteCommit,
    AfterWalFlush,
    AfterCheckpoint,
    ExplicitBarrier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootPointerV1 {
    pub kind: ObjectKind,
    pub format_version: u16,
    pub tenant_scope: String,
    pub database_id: String,
    pub database_incarnation_id: String,
    pub object_prefix: String,
    pub root_generation: u64,
    pub writer_id: String,
    pub lease_epoch: u64,
    pub checkpoint_id: String,
    pub latest_delta_sequence: u64,
    pub latest_database_version: u64,
    pub state_checksum: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointV1 {
    pub kind: ObjectKind,
    pub format_version: u16,
    pub tenant_scope: String,
    pub database_id: String,
    pub database_incarnation_id: String,
    pub object_prefix: String,
    pub writer_id: String,
    pub lease_epoch: u64,
    pub checkpoint_id: String,
    pub checkpoint_delta_sequence: u64,
    pub database_version: u64,
    pub page_size: u32,
    pub database_page_count: u64,
    pub page_objects: Vec<PageObjectRefV1>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PageObjectRefV1 {
    pub first_page: u64,
    pub page_count: u64,
    pub key: String,
    pub checksum: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeltaV1 {
    pub kind: ObjectKind,
    pub format_version: u16,
    pub tenant_scope: String,
    pub database_id: String,
    pub database_incarnation_id: String,
    pub object_prefix: String,
    pub writer_id: String,
    pub lease_epoch: u64,
    pub base_database_version: u64,
    pub end_database_version: u64,
    pub sequence: u64,
    pub page_size: u32,
    pub base_database_page_count: u64,
    pub end_database_page_count: u64,
    pub committed_boundary: CommittedBoundary,
    pub changed_pages: Vec<DeltaPageV1>,
    pub checksum: u64,
    pub replay_idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeltaPageV1 {
    pub page_number: u64,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContractError {
    WrongKind {
        expected: ObjectKind,
        actual: ObjectKind,
    },
    UnsupportedFormatVersion(u16),
    EmptyField(&'static str),
    ZeroField(&'static str),
    RootRollback,
    StaleLeaseEpoch,
    RootIdentityMismatch,
    WrongObjectPrefix,
    NonContiguousDelta {
        expected: u64,
        actual: u64,
    },
    VersionMismatch {
        expected: u64,
        actual: u64,
    },
    PageSizeMismatch {
        expected: u32,
        actual: u32,
    },
    DatabaseSizeMismatch {
        expected: u64,
        actual: u64,
    },
    MissingCheckpoint(String),
    MissingDelta(u64),
    DuplicateDelta(u64),
    ReplayChainTooLong {
        max: u64,
        actual: u64,
    },
    DeltaPageOutOfRange {
        page_number: u64,
        end_page_count: u64,
    },
    PageObjectOutOfRange {
        first_page: u64,
        page_count: u64,
        database_page_count: u64,
    },
    DeltaPageWrongSize {
        page_number: u64,
        expected: usize,
        actual: usize,
    },
    DeltaChecksumMismatch {
        expected: u64,
        actual: u64,
    },
}

impl fmt::Display for ContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ContractError {}

pub type ContractResult<T> = Result<T, ContractError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryPlanV1 {
    pub checkpoint: CheckpointV1,
    pub deltas: Vec<DeltaV1>,
}

impl RootPointerV1 {
    pub fn validate(&self) -> ContractResult<()> {
        require_kind(self.kind, ObjectKind::RootPointer)?;
        validate_common(
            self.format_version,
            &self.tenant_scope,
            &self.database_id,
            &self.database_incarnation_id,
            &self.object_prefix,
            &self.writer_id,
            self.lease_epoch,
        )?;
        require_nonempty("checkpoint_id", &self.checkpoint_id)?;
        require_nonzero("root_generation", self.root_generation)?;
        require_nonzero("state_checksum", self.state_checksum)?;
        Ok(())
    }

    pub fn validate_update_from(&self, previous: &RootPointerV1) -> ContractResult<()> {
        self.validate()?;
        previous.validate()?;
        if self.tenant_scope != previous.tenant_scope
            || self.database_id != previous.database_id
            || self.database_incarnation_id != previous.database_incarnation_id
            || self.object_prefix != previous.object_prefix
            || self.checkpoint_id.is_empty()
        {
            return Err(ContractError::RootIdentityMismatch);
        }
        if self.root_generation <= previous.root_generation {
            return Err(ContractError::RootRollback);
        }
        if self.latest_delta_sequence < previous.latest_delta_sequence
            || self.latest_database_version < previous.latest_database_version
        {
            return Err(ContractError::RootRollback);
        }
        if self.lease_epoch < previous.lease_epoch {
            return Err(ContractError::StaleLeaseEpoch);
        }
        Ok(())
    }
}

impl CheckpointV1 {
    pub fn validate(&self) -> ContractResult<()> {
        require_kind(self.kind, ObjectKind::Checkpoint)?;
        validate_common(
            self.format_version,
            &self.tenant_scope,
            &self.database_id,
            &self.database_incarnation_id,
            &self.object_prefix,
            &self.writer_id,
            self.lease_epoch,
        )?;
        require_nonempty("checkpoint_id", &self.checkpoint_id)?;
        require_nonzero("database_version", self.database_version)?;
        require_nonzero("page_size", self.page_size as u64)?;
        require_nonzero("database_page_count", self.database_page_count)?;
        if self.page_objects.is_empty() {
            return Err(ContractError::EmptyField("page_objects"));
        }
        for page_object in &self.page_objects {
            page_object.validate(self.database_page_count, &self.object_prefix)?;
        }
        Ok(())
    }
}

pub fn select_recovery_plan(
    root: &RootPointerV1,
    checkpoints: &[CheckpointV1],
    deltas: &[DeltaV1],
    max_delta_replay: u64,
) -> ContractResult<RecoveryPlanV1> {
    root.validate()?;
    let checkpoint = checkpoints
        .iter()
        .find(|checkpoint| checkpoint.checkpoint_id == root.checkpoint_id)
        .ok_or_else(|| ContractError::MissingCheckpoint(root.checkpoint_id.clone()))?;
    validate_checkpoint_for_root(checkpoint, root)?;

    if checkpoint.checkpoint_delta_sequence > root.latest_delta_sequence {
        return Err(ContractError::NonContiguousDelta {
            expected: checkpoint.checkpoint_delta_sequence,
            actual: root.latest_delta_sequence,
        });
    }
    let replay_len = root.latest_delta_sequence - checkpoint.checkpoint_delta_sequence;
    if replay_len > max_delta_replay {
        return Err(ContractError::ReplayChainTooLong {
            max: max_delta_replay,
            actual: replay_len,
        });
    }

    let mut planned = Vec::with_capacity(replay_len as usize);
    let mut expected_version = checkpoint.database_version;
    let mut expected_page_count = checkpoint.database_page_count;
    let expected_page_size = checkpoint.page_size;

    for expected_sequence in checkpoint.checkpoint_delta_sequence + 1..=root.latest_delta_sequence {
        let candidates: Vec<&DeltaV1> = deltas
            .iter()
            .filter(|delta| {
                delta.sequence == expected_sequence && delta_has_root_identity(delta, root)
            })
            .collect();
        let delta = match candidates.as_slice() {
            [] => return Err(ContractError::MissingDelta(expected_sequence)),
            [delta] => *delta,
            _ => return Err(ContractError::DuplicateDelta(expected_sequence)),
        };
        validate_delta_after_state(
            delta,
            root,
            expected_sequence,
            expected_version,
            expected_page_count,
            expected_page_size,
        )?;
        expected_version = delta.end_database_version;
        expected_page_count = delta.end_database_page_count;
        planned.push(delta.clone());
    }

    if expected_version != root.latest_database_version {
        return Err(ContractError::VersionMismatch {
            expected: root.latest_database_version,
            actual: expected_version,
        });
    }

    Ok(RecoveryPlanV1 {
        checkpoint: checkpoint.clone(),
        deltas: planned,
    })
}

impl PageObjectRefV1 {
    pub fn validate(&self, database_page_count: u64, object_prefix: &str) -> ContractResult<()> {
        require_nonempty("page_object.key", &self.key)?;
        require_nonzero("page_object.page_count", self.page_count)?;
        require_nonzero("page_object.checksum", self.checksum)?;
        if !key_has_object_prefix(&self.key, object_prefix) {
            return Err(ContractError::WrongObjectPrefix);
        }
        let end = self.first_page.checked_add(self.page_count).ok_or(
            ContractError::PageObjectOutOfRange {
                first_page: self.first_page,
                page_count: self.page_count,
                database_page_count,
            },
        )?;
        if end > database_page_count {
            return Err(ContractError::PageObjectOutOfRange {
                first_page: self.first_page,
                page_count: self.page_count,
                database_page_count,
            });
        }
        Ok(())
    }
}

fn validate_checkpoint_for_root(
    checkpoint: &CheckpointV1,
    root: &RootPointerV1,
) -> ContractResult<()> {
    checkpoint.validate()?;
    if checkpoint.tenant_scope != root.tenant_scope
        || checkpoint.database_id != root.database_id
        || checkpoint.database_incarnation_id != root.database_incarnation_id
        || checkpoint.object_prefix != root.object_prefix
        || checkpoint.writer_id != root.writer_id
        || checkpoint.lease_epoch != root.lease_epoch
        || checkpoint.checkpoint_id != root.checkpoint_id
    {
        return Err(ContractError::RootIdentityMismatch);
    }
    if checkpoint.database_version > root.latest_database_version {
        return Err(ContractError::VersionMismatch {
            expected: root.latest_database_version,
            actual: checkpoint.database_version,
        });
    }
    Ok(())
}

fn validate_delta_after_state(
    delta: &DeltaV1,
    root: &RootPointerV1,
    expected_sequence: u64,
    expected_version: u64,
    expected_page_count: u64,
    expected_page_size: u32,
) -> ContractResult<()> {
    delta.validate()?;
    if delta.tenant_scope != root.tenant_scope
        || delta.database_id != root.database_id
        || delta.database_incarnation_id != root.database_incarnation_id
        || delta.object_prefix != root.object_prefix
        || delta.writer_id != root.writer_id
        || delta.lease_epoch != root.lease_epoch
    {
        return Err(ContractError::RootIdentityMismatch);
    }
    if delta.sequence != expected_sequence {
        return Err(ContractError::NonContiguousDelta {
            expected: expected_sequence,
            actual: delta.sequence,
        });
    }
    if delta.base_database_version != expected_version {
        return Err(ContractError::VersionMismatch {
            expected: expected_version,
            actual: delta.base_database_version,
        });
    }
    if delta.base_database_page_count != expected_page_count {
        return Err(ContractError::DatabaseSizeMismatch {
            expected: expected_page_count,
            actual: delta.base_database_page_count,
        });
    }
    if delta.page_size != expected_page_size {
        return Err(ContractError::PageSizeMismatch {
            expected: expected_page_size,
            actual: delta.page_size,
        });
    }
    Ok(())
}

fn delta_has_root_identity(delta: &DeltaV1, root: &RootPointerV1) -> bool {
    delta.tenant_scope == root.tenant_scope
        && delta.database_id == root.database_id
        && delta.database_incarnation_id == root.database_incarnation_id
        && delta.object_prefix == root.object_prefix
        && delta.writer_id == root.writer_id
        && delta.lease_epoch == root.lease_epoch
}

impl DeltaV1 {
    pub fn validate(&self) -> ContractResult<()> {
        require_kind(self.kind, ObjectKind::Delta)?;
        validate_common(
            self.format_version,
            &self.tenant_scope,
            &self.database_id,
            &self.database_incarnation_id,
            &self.object_prefix,
            &self.writer_id,
            self.lease_epoch,
        )?;
        require_nonzero("base_database_version", self.base_database_version)?;
        require_nonzero("end_database_version", self.end_database_version)?;
        require_nonzero("sequence", self.sequence)?;
        require_nonzero("page_size", self.page_size as u64)?;
        require_nonempty("replay_idempotency_key", &self.replay_idempotency_key)?;
        if self.end_database_version <= self.base_database_version {
            return Err(ContractError::VersionMismatch {
                expected: self.base_database_version + 1,
                actual: self.end_database_version,
            });
        }
        for page in &self.changed_pages {
            page.validate(self.page_size, self.end_database_page_count)?;
        }
        let expected = self.fixture_checksum();
        if self.checksum != expected {
            return Err(ContractError::DeltaChecksumMismatch {
                expected,
                actual: self.checksum,
            });
        }
        Ok(())
    }

    pub fn validate_after_root(&self, root: &RootPointerV1) -> ContractResult<()> {
        self.validate()?;
        root.validate()?;
        if self.tenant_scope != root.tenant_scope
            || self.database_id != root.database_id
            || self.database_incarnation_id != root.database_incarnation_id
            || self.object_prefix != root.object_prefix
            || self.writer_id != root.writer_id
            || self.lease_epoch != root.lease_epoch
        {
            return Err(ContractError::RootIdentityMismatch);
        }
        if self.sequence != root.latest_delta_sequence + 1 {
            return Err(ContractError::NonContiguousDelta {
                expected: root.latest_delta_sequence + 1,
                actual: self.sequence,
            });
        }
        if self.base_database_version != root.latest_database_version {
            return Err(ContractError::VersionMismatch {
                expected: root.latest_database_version,
                actual: self.base_database_version,
            });
        }
        Ok(())
    }

    /// Fixture-only checksum used by this compact contract vocabulary.
    ///
    /// Live Sashimono objects must use the production delta/checksum format,
    /// not this additive helper. Keeping the method name explicit prevents the
    /// fixture validator from being mistaken for a storage integrity primitive.
    pub fn fixture_checksum(&self) -> u64 {
        let mut sum = 0u64;
        sum = add_str(sum, &self.tenant_scope);
        sum = add_str(sum, &self.database_id);
        sum = add_str(sum, &self.database_incarnation_id);
        sum = add_str(sum, &self.object_prefix);
        sum = add_str(sum, &self.writer_id);
        sum = sum.wrapping_add(self.lease_epoch);
        sum = sum.wrapping_add(self.base_database_version);
        sum = sum.wrapping_add(self.end_database_version);
        sum = sum.wrapping_add(self.sequence);
        sum = sum.wrapping_add(self.page_size as u64);
        sum = sum.wrapping_add(self.base_database_page_count);
        sum = sum.wrapping_add(self.end_database_page_count);
        sum = sum.wrapping_add(committed_boundary_discriminant(self.committed_boundary));
        sum = add_str(sum, &self.replay_idempotency_key);
        for page in &self.changed_pages {
            sum = sum.wrapping_add(page.page_number);
            for byte in &page.bytes {
                sum = sum.wrapping_add(*byte as u64);
            }
        }
        sum
    }
}

fn committed_boundary_discriminant(boundary: CommittedBoundary) -> u64 {
    match boundary {
        CommittedBoundary::AfterSqliteCommit => 1,
        CommittedBoundary::AfterWalFlush => 2,
        CommittedBoundary::AfterCheckpoint => 3,
        CommittedBoundary::ExplicitBarrier => 4,
    }
}

fn add_str(mut sum: u64, value: &str) -> u64 {
    for byte in value.as_bytes() {
        sum = sum.wrapping_add(*byte as u64);
    }
    sum
}

fn key_has_object_prefix(key: &str, object_prefix: &str) -> bool {
    key.strip_prefix(object_prefix)
        .is_some_and(|rest| rest.starts_with('/'))
}

impl DeltaPageV1 {
    pub fn validate(&self, page_size: u32, end_page_count: u64) -> ContractResult<()> {
        if self.page_number >= end_page_count {
            return Err(ContractError::DeltaPageOutOfRange {
                page_number: self.page_number,
                end_page_count,
            });
        }
        let expected = page_size as usize;
        let actual = self.bytes.len();
        if actual != expected {
            return Err(ContractError::DeltaPageWrongSize {
                page_number: self.page_number,
                expected,
                actual,
            });
        }
        Ok(())
    }
}

fn validate_common(
    format_version: u16,
    tenant_scope: &str,
    database_id: &str,
    database_incarnation_id: &str,
    object_prefix: &str,
    writer_id: &str,
    lease_epoch: u64,
) -> ContractResult<()> {
    if format_version != SASHIMONO_FORMAT_VERSION {
        return Err(ContractError::UnsupportedFormatVersion(format_version));
    }
    require_nonempty("tenant_scope", tenant_scope)?;
    require_nonempty("database_id", database_id)?;
    require_nonempty("database_incarnation_id", database_incarnation_id)?;
    require_nonempty("object_prefix", object_prefix)?;
    require_nonempty("writer_id", writer_id)?;
    require_nonzero("lease_epoch", lease_epoch)?;
    Ok(())
}

fn require_kind(actual: ObjectKind, expected: ObjectKind) -> ContractResult<()> {
    if actual != expected {
        return Err(ContractError::WrongKind { expected, actual });
    }
    Ok(())
}

fn require_nonempty(field: &'static str, value: &str) -> ContractResult<()> {
    if value.is_empty() {
        Err(ContractError::EmptyField(field))
    } else {
        Ok(())
    }
}

fn require_nonzero(field: &'static str, value: u64) -> ContractResult<()> {
    if value == 0 {
        Err(ContractError::ZeroField(field))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROOT_FIXTURE: &str = include_str!("../../tests/fixtures/sashimono_root_v1.json");
    const CHECKPOINT_FIXTURE: &str =
        include_str!("../../tests/fixtures/sashimono_checkpoint_v1.json");
    const DELTA_FIXTURE: &str = include_str!("../../tests/fixtures/sashimono_delta_v1.json");

    fn root_fixture() -> RootPointerV1 {
        serde_json::from_str(ROOT_FIXTURE).expect("root fixture decodes")
    }

    fn checkpoint_fixture() -> CheckpointV1 {
        serde_json::from_str(CHECKPOINT_FIXTURE).expect("checkpoint fixture decodes")
    }

    fn delta_fixture() -> DeltaV1 {
        serde_json::from_str(DELTA_FIXTURE).expect("delta fixture decodes")
    }

    #[test]
    fn golden_root_fixture_validates() {
        root_fixture().validate().expect("root fixture validates");
    }

    #[test]
    fn golden_checkpoint_fixture_validates_and_has_no_delta_fields() {
        checkpoint_fixture()
            .validate()
            .expect("checkpoint fixture validates");

        let with_delta_field = CHECKPOINT_FIXTURE.replace(
            "\"page_objects\"",
            "\"delta_objects\": [],\n  \"page_objects\"",
        );
        let err = serde_json::from_str::<CheckpointV1>(&with_delta_field)
            .expect_err("checkpoint fixture must reject replay fields");
        assert!(
            err.to_string().contains("delta_objects"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn golden_delta_fixture_validates() {
        let delta = delta_fixture();
        assert_eq!(delta.checksum, delta.fixture_checksum());
        delta.validate().expect("delta fixture validates");
    }

    #[test]
    fn recovery_plan_uses_root_reachable_checkpoint_and_ordered_deltas() {
        let mut root = root_fixture();
        root.latest_delta_sequence = 1;
        root.latest_database_version = 2;

        let checkpoint = checkpoint_fixture();
        let delta = delta_fixture();
        let plan = select_recovery_plan(&root, &[checkpoint.clone()], &[delta.clone()], 128)
            .expect("root reachable checkpoint plus contiguous delta plans");

        assert_eq!(plan.checkpoint.checkpoint_id, checkpoint.checkpoint_id);
        assert_eq!(plan.deltas, vec![delta]);
    }

    #[test]
    fn recovery_plan_ignores_complete_but_root_unreachable_checkpoint() {
        let mut root = root_fixture();
        root.checkpoint_id = "chk-0002".to_string();
        let checkpoint = checkpoint_fixture();

        let err = select_recovery_plan(&root, &[checkpoint], &[], 128)
            .expect_err("complete but root-unreachable checkpoint must not be current");

        assert_eq!(
            err,
            ContractError::MissingCheckpoint("chk-0002".to_string())
        );
    }

    #[test]
    fn recovery_plan_does_not_let_listing_order_select_newer_checkpoint() {
        let root = root_fixture();
        let checkpoint = checkpoint_fixture();
        let mut unreachable = checkpoint.clone();
        unreachable.checkpoint_id = "chk-9999".to_string();
        unreachable.checkpoint_delta_sequence = 99;
        unreachable.database_version = 100;

        let plan = select_recovery_plan(&root, &[unreachable, checkpoint.clone()], &[], 128)
            .expect("root pointer, not listing order, chooses current checkpoint");

        assert_eq!(plan.checkpoint.checkpoint_id, checkpoint.checkpoint_id);
        assert!(plan.deltas.is_empty());
    }

    #[test]
    fn recovery_plan_rejects_missing_delta_gap() {
        let mut root = root_fixture();
        root.latest_delta_sequence = 2;
        root.latest_database_version = 3;

        let checkpoint = checkpoint_fixture();
        let delta = delta_fixture();
        let err = select_recovery_plan(&root, &[checkpoint], &[delta], 128)
            .expect_err("missing sequence 2 must fail closed");

        assert_eq!(err, ContractError::MissingDelta(2));
    }

    #[test]
    fn recovery_plan_rejects_replay_chain_over_budget() {
        let mut root = root_fixture();
        root.latest_delta_sequence = 129;
        root.latest_database_version = 130;

        let checkpoint = checkpoint_fixture();
        let err = select_recovery_plan(&root, &[checkpoint], &[], 128)
            .expect_err("too many deltas since checkpoint must force checkpoint/compaction");

        assert_eq!(
            err,
            ContractError::ReplayChainTooLong {
                max: 128,
                actual: 129
            }
        );
    }

    #[test]
    fn recovery_plan_rejects_wrong_base_page_count() {
        let mut root = root_fixture();
        root.latest_delta_sequence = 1;
        root.latest_database_version = 2;

        let checkpoint = checkpoint_fixture();
        let mut delta = delta_fixture();
        delta.base_database_page_count = checkpoint.database_page_count + 1;
        delta.checksum = delta.fixture_checksum();

        let err = select_recovery_plan(&root, &[checkpoint.clone()], &[delta], 128)
            .expect_err("delta must start from checkpoint page count");

        assert_eq!(
            err,
            ContractError::DatabaseSizeMismatch {
                expected: checkpoint.database_page_count,
                actual: checkpoint.database_page_count + 1
            }
        );
    }

    #[test]
    fn recovery_plan_ignores_wrong_identity_delta_when_valid_candidate_exists() {
        let mut root = root_fixture();
        root.latest_delta_sequence = 1;
        root.latest_database_version = 2;

        let checkpoint = checkpoint_fixture();
        let mut wrong_identity = delta_fixture();
        wrong_identity.tenant_scope = "tenant-b/scope-a".to_string();
        wrong_identity.checksum = wrong_identity.fixture_checksum();
        let valid = delta_fixture();

        let plan =
            select_recovery_plan(&root, &[checkpoint], &[wrong_identity, valid.clone()], 128)
                .expect("wrong-identity listing entry must not hide valid root-reachable delta");

        assert_eq!(plan.deltas, vec![valid]);
    }

    #[test]
    fn recovery_plan_rejects_duplicate_root_reachable_delta_sequence() {
        let mut root = root_fixture();
        root.latest_delta_sequence = 1;
        root.latest_database_version = 2;

        let checkpoint = checkpoint_fixture();
        let delta_a = delta_fixture();
        let mut delta_b = delta_fixture();
        delta_b.replay_idempotency_key = "replay-different".to_string();
        delta_b.checksum = delta_b.fixture_checksum();

        let err = select_recovery_plan(&root, &[checkpoint], &[delta_a, delta_b], 128)
            .expect_err("ambiguous same-sequence deltas must fail closed");

        assert_eq!(err, ContractError::DuplicateDelta(1));
    }

    #[test]
    fn recovery_plan_rejects_delta_page_size_mismatch() {
        let mut root = root_fixture();
        root.latest_delta_sequence = 1;
        root.latest_database_version = 2;

        let checkpoint = checkpoint_fixture();
        let mut delta = delta_fixture();
        delta.page_size = checkpoint.page_size * 2;
        for page in &mut delta.changed_pages {
            page.bytes.resize(delta.page_size as usize, 0xDD);
        }
        delta.checksum = delta.fixture_checksum();

        let err = select_recovery_plan(&root, &[checkpoint.clone()], &[delta], 128)
            .expect_err("delta page size must match checkpoint page size");

        assert_eq!(
            err,
            ContractError::PageSizeMismatch {
                expected: checkpoint.page_size,
                actual: checkpoint.page_size * 2
            }
        );
    }

    #[test]
    fn root_update_rejects_rollback() {
        let previous = root_fixture();
        let mut next = previous.clone();
        next.state_checksum += 1;
        let err = next
            .validate_update_from(&previous)
            .expect_err("same generation is rollback");
        assert_eq!(err, ContractError::RootRollback);
    }

    #[test]
    fn root_update_rejects_latest_delta_sequence_rollback() {
        let mut previous = root_fixture();
        previous.latest_delta_sequence = 5;
        previous.latest_database_version = 6;

        let mut next = previous.clone();
        next.root_generation += 1;
        next.latest_delta_sequence = 4;

        let err = next
            .validate_update_from(&previous)
            .expect_err("root generation cannot hide delta sequence rollback");

        assert_eq!(err, ContractError::RootRollback);
    }

    #[test]
    fn root_update_rejects_latest_database_version_rollback() {
        let mut previous = root_fixture();
        previous.latest_delta_sequence = 5;
        previous.latest_database_version = 6;

        let mut next = previous.clone();
        next.root_generation += 1;
        next.latest_database_version = 5;

        let err = next
            .validate_update_from(&previous)
            .expect_err("root generation cannot hide database version rollback");

        assert_eq!(err, ContractError::RootRollback);
    }

    #[test]
    fn root_update_rejects_stale_lease_epoch() {
        let previous = root_fixture();
        let mut next = previous.clone();
        next.root_generation += 1;
        next.lease_epoch -= 1;
        let err = next
            .validate_update_from(&previous)
            .expect_err("stale lease must fail");
        assert_eq!(err, ContractError::StaleLeaseEpoch);
    }

    #[test]
    fn delta_rejects_wrong_identity_before_replay() {
        let root = root_fixture();
        let mut delta = delta_fixture();
        delta.tenant_scope = "tenant-b/scope-a".to_string();
        delta.checksum = delta.fixture_checksum();
        let err = delta
            .validate_after_root(&root)
            .expect_err("wrong tenant must fail");
        assert_eq!(err, ContractError::RootIdentityMismatch);
    }

    #[test]
    fn delta_rejects_wrong_writer_before_replay() {
        let root = root_fixture();
        let mut delta = delta_fixture();
        delta.writer_id = "writer-b".to_string();
        delta.checksum = delta.fixture_checksum();
        let err = delta
            .validate_after_root(&root)
            .expect_err("wrong writer must fail");
        assert_eq!(err, ContractError::RootIdentityMismatch);
    }

    #[test]
    fn recovery_plan_rejects_copied_database_incarnation_mismatch() {
        let root = root_fixture();
        let mut checkpoint = checkpoint_fixture();
        checkpoint.database_incarnation_id = "incarnation-copy".to_string();

        let err = select_recovery_plan(&root, &[checkpoint], &[], 128)
            .expect_err("copied/restored database identity mismatch must fail before replay");

        assert_eq!(err, ContractError::RootIdentityMismatch);
    }

    #[test]
    fn recovery_plan_rejects_wrong_object_prefix() {
        let root = root_fixture();
        let mut checkpoint = checkpoint_fixture();
        checkpoint.object_prefix = "tenant-a/scope-a/db-beta".to_string();
        checkpoint.page_objects[0].key =
            "tenant-a/scope-a/db-beta/checkpoints/chk-0001/pages-0000-0001".to_string();

        let err = select_recovery_plan(&root, &[checkpoint], &[], 128)
            .expect_err("wrong object prefix must fail before replay");

        assert_eq!(err, ContractError::RootIdentityMismatch);
    }

    #[test]
    fn checkpoint_rejects_page_object_outside_prefix() {
        let mut checkpoint = checkpoint_fixture();
        checkpoint.page_objects[0].key =
            "tenant-b/scope-a/db-alpha/checkpoints/chk-0001/pages-0000-0001".to_string();

        let err = checkpoint
            .validate()
            .expect_err("page object key outside checkpoint prefix must fail closed");

        assert_eq!(err, ContractError::WrongObjectPrefix);
    }

    #[test]
    fn checkpoint_rejects_page_object_with_prefix_sibling() {
        let mut checkpoint = checkpoint_fixture();
        checkpoint.page_objects[0].key =
            "tenant-a/scope-a/db-alpha-copy/checkpoints/chk-0001/pages-0000-0001".to_string();

        let err = checkpoint
            .validate()
            .expect_err("sibling prefix must not satisfy object prefix validation");

        assert_eq!(err, ContractError::WrongObjectPrefix);
    }

    #[test]
    fn delta_rejects_sequence_gap() {
        let root = root_fixture();
        let mut delta = delta_fixture();
        delta.sequence += 1;
        delta.checksum = delta.fixture_checksum();
        let err = delta
            .validate_after_root(&root)
            .expect_err("sequence gap must fail");
        assert_eq!(
            err,
            ContractError::NonContiguousDelta {
                expected: 1,
                actual: 2
            }
        );
    }

    #[test]
    fn delta_rejects_wrong_end_database_size() {
        let mut delta = delta_fixture();
        delta.end_database_page_count = 1;
        delta.checksum = delta.fixture_checksum();
        let err = delta
            .validate()
            .expect_err("page beyond end page count must fail");
        assert_eq!(
            err,
            ContractError::DeltaPageOutOfRange {
                page_number: 2,
                end_page_count: 1
            }
        );
    }

    #[test]
    fn delta_rejects_corrupt_checksum() {
        let mut delta = delta_fixture();
        delta.checksum += 1;
        let err = delta.validate().expect_err("corrupt checksum must fail");
        assert!(matches!(err, ContractError::DeltaChecksumMismatch { .. }));
    }

    #[test]
    fn delta_rejects_torn_page_bytes() {
        let mut delta = delta_fixture();
        delta.changed_pages[0].bytes.pop();
        delta.checksum = delta.fixture_checksum();
        let err = delta.validate().expect_err("short page must fail");
        assert!(matches!(err, ContractError::DeltaPageWrongSize { .. }));
    }
}
