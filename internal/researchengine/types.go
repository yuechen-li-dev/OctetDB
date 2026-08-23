package engine

import (
	"errors"
	"fmt"
	"time"

	generated "github.com/yuechen-li-dev/octetdb/internal/model"
)

type AccountID uint64

type Account struct {
	ID      AccountID `json:"id"`
	Balance int       `json:"balance"`
	Status  int       `json:"status"`
	Version uint64    `json:"version"`
}

const (
	StatusOpen   = 1
	StatusFrozen = 2
)

type CommandKind = generated.Main_CommandKind

var (
	Create        = generated.NewCommandKindCreate()
	Deposit       = generated.NewCommandKindDeposit()
	Withdraw      = generated.NewCommandKindWithdraw()
	Transfer      = generated.NewCommandKindTransfer()
	Freeze        = generated.NewCommandKindFreeze()
	Unfreeze      = generated.NewCommandKindUnfreeze()
	BeginTransfer = generated.NewCommandKindBeginTransfer()
	Confirm       = generated.NewCommandKindConfirm()
	Cancel        = generated.NewCommandKindCancel()
)

type Command struct {
	ID      string
	Kind    CommandKind
	Account AccountID
	Other   AccountID
	Amount  int
}

type Envelope struct {
	Sequence  uint64
	Command   Command
	Submitted time.Time
}

type Result struct {
	Sequence        uint64 `json:"sequence"`
	CommandID       string `json:"command_id"`
	Accepted        bool   `json:"accepted"`
	ReasonTag       int    `json:"reason_tag"`
	EffectTag       int    `json:"effect_tag"`
	TransitionCount int    `json:"transition_count"`
	Duplicate       bool   `json:"duplicate"`
}

type DurabilityMode uint8

const (
	MemoryOnly DurabilityMode = iota
	BatchSync
	SyncEach
)

type Config struct {
	MailboxCapacity int
	Durability      DurabilityMode
	BatchSize       int
	LogPath         string
	Trace           func(TraceEvent)
	// StorageDir selects the M1 segmented-WAL and snapshot path. LogPath keeps
	// the M0 single-file format available as an explicit control.
	StorageDir          string
	SegmentRecords      int
	GroupMax            int
	GroupWait           time.Duration
	SnapshotEvery       uint64
	DedupeWindow        int
	FailureInjector     func(FailurePoint) error
	GoBehavioralControl bool
	// FullCheckpointWAL and JSONDedupeSnapshot retain the independent M1
	// ablations. Normal M2 operation leaves both false.
	FullCheckpointWAL  bool
	JSONDedupeSnapshot bool
}

type FailurePoint string

const (
	BeforeWALAppend                   FailurePoint = "before_wal_append"
	DuringWALAppend                   FailurePoint = "during_wal_append"
	AfterWALAppendBeforeSync          FailurePoint = "after_wal_append_before_sync"
	AfterWALSyncBeforeApply           FailurePoint = "after_wal_sync_before_apply"
	AfterStepBeforeDeltaExport        FailurePoint = "after_step_before_delta_export"
	AfterDeltaExportBeforeWALAppend   FailurePoint = "after_delta_export_before_wal_append"
	AfterSyncBeforeDirtyClear         FailurePoint = "after_sync_before_dirty_clear"
	AfterStateApplyBeforeDirtyClear   FailurePoint = "after_state_apply_before_dirty_clear"
	AfterStateApplyBeforeAck          FailurePoint = "after_state_apply_before_ack"
	DuringSegmentRotation             FailurePoint = "during_segment_rotation"
	DuringSnapshotWrite               FailurePoint = "during_snapshot_temp_write"
	DuringSnapshotFlowCheckpoint      FailurePoint = "during_snapshot_flow_checkpoint"
	DuringCompactDedupeEncoding       FailurePoint = "during_compact_dedupe_encoding"
	AfterSnapshotSyncBeforeInstall    FailurePoint = "after_snapshot_sync_before_install"
	AfterSnapshotInstallBeforeCleanup FailurePoint = "after_snapshot_install_before_wal_cleanup"
)

type RecoveryStats struct {
	SnapshotSequence uint64        `json:"snapshot_sequence"`
	SnapshotBytes    int64         `json:"snapshot_bytes"`
	SnapshotDecode   time.Duration `json:"snapshot_decode"`
	WALScan          time.Duration `json:"wal_scan"`
	WALBytesScanned  int64         `json:"wal_bytes_scanned"`
	RecordsReplayed  int           `json:"records_replayed"`
	AgentsRestored   int           `json:"agents_restored"`
	DedupeDecode     time.Duration `json:"dedupe_decode"`
	AgentRestore     time.Duration `json:"agent_restore"`
	FlowDeltaApply   time.Duration `json:"flow_delta_apply"`
	TotalReady       time.Duration `json:"total_ready"`
}

type StorageStats struct {
	WALBytesWritten      uint64 `json:"wal_bytes_written"`
	SnapshotBytesWritten uint64 `json:"snapshot_bytes_written"`
	Syncs                uint64 `json:"syncs"`
	Committed            uint64 `json:"committed"`
	LogicalStateBytes    uint64 `json:"logical_state_bytes"`
	FlowCheckpointBytes  uint64 `json:"flow_checkpoint_bytes"`
	DedupeBytes          uint64 `json:"dedupe_bytes"`
	FlowDeltaBytes       uint64 `json:"flow_delta_bytes"`
	DedupeDecodeNanos    uint64 `json:"dedupe_decode_nanos"`
	PublicationBytes     uint64 `json:"publication_bytes"`
}

type TraceEvent struct {
	Sequence       uint64    `json:"sequence"`
	CommandID      string    `json:"command_id"`
	Agent          AccountID `json:"agent"`
	Phase          string    `json:"phase"`
	ConflictTokens []Token   `json:"conflict_tokens,omitempty"`
	VersionA       uint64    `json:"version_a,omitempty"`
	VersionB       uint64    `json:"version_b,omitempty"`
	Accepted       bool      `json:"accepted,omitempty"`
	ReasonTag      int       `json:"reason_tag,omitempty"`
	EffectTag      int       `json:"effect_tag,omitempty"`
	FlowState      string    `json:"flow_state,omitempty"`
	NewVersionA    uint64    `json:"new_version_a,omitempty"`
	NewVersionB    uint64    `json:"new_version_b,omitempty"`
	CheckpointHash string    `json:"checkpoint_hash,omitempty"`
	Detail         string    `json:"detail,omitempty"`
}

type ErrorKind string

const (
	MailboxFull            ErrorKind = "mailbox_full"
	EngineClosed           ErrorKind = "engine_closed"
	InvalidCommand         ErrorKind = "invalid_command"
	FlowStepFailed         ErrorKind = "flow_step_failed"
	InvalidDecision        ErrorKind = "invalid_decision"
	DurabilityWriteFailed  ErrorKind = "durability_write_failed"
	CheckpointExportFailed ErrorKind = "checkpoint_export_failed"
	RecoveryCorrupt        ErrorKind = "recovery_corrupt"
	RecoveryIncompatible   ErrorKind = "recovery_incompatible"
)

type RuntimeError struct {
	Kind ErrorKind
	Err  error
}

func (e *RuntimeError) Error() string { return fmt.Sprintf("%s: %v", e.Kind, e.Err) }
func (e *RuntimeError) Unwrap() error { return e.Err }

var errClosed = errors.New("engine closed")

func isKind(kind CommandKind, expected CommandKind) bool { return kind.Tag == expected.Tag }

func ValidateCommand(command Command) error {
	if command.ID == "" || command.Account == 0 {
		return &RuntimeError{Kind: InvalidCommand, Err: errors.New("command ID and account are required")}
	}
	if (isKind(command.Kind, Transfer) || isKind(command.Kind, BeginTransfer) || isKind(command.Kind, Confirm)) && (command.Other == 0 || command.Other == command.Account) {
		return &RuntimeError{Kind: InvalidCommand, Err: errors.New("multi-account command requires a distinct destination")}
	}
	return nil
}

func AdmitPositiveAmount(value int) (int, error) { return generated.AdmitPositiveAmount(value) }
