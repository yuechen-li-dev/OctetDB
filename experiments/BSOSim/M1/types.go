// Package bsosim implements BSO-SIM-M1, a deterministic agentic transaction
// scheduler simulation. It is an architecture experiment, not a payment product.
package bsosim

import "time"

const ProtocolVersion = 1

type Money int64

type TransferState string

const (
	StateReserved     TransferState = "reserved"
	StateAccepted     TransferState = "accepted"
	StateCommitted    TransferState = "committed"
	StateAcknowledged TransferState = "acknowledged"
	StateRejected     TransferState = "rejected"
	StateExpired      TransferState = "expired"
)

type AgentPhase string

const (
	PhaseCreated          AgentPhase = "created"
	PhaseOfferReceiver    AgentPhase = "offer_receiver"
	PhaseAwaitAccept      AgentPhase = "await_accept"
	PhaseCommitSender     AgentPhase = "commit_sender"
	PhaseCommitReceiver   AgentPhase = "commit_receiver"
	PhaseAwaitAcknowledge AgentPhase = "await_acknowledge"
	PhaseReconcile        AgentPhase = "reconcile"
	PhaseDone             AgentPhase = "done"
	PhaseRejected         AgentPhase = "rejected"
	PhaseExpired          AgentPhase = "expired"
)

func (p AgentPhase) terminal() bool { return p == PhaseDone || p == PhaseRejected || p == PhaseExpired }

type MessageKind string

const (
	MessageOffer       MessageKind = "offer"
	MessageAccept      MessageKind = "accept"
	MessageReject      MessageKind = "reject"
	MessageCommit      MessageKind = "commit"
	MessageAcknowledge MessageKind = "acknowledge"
	MessageReconcile   MessageKind = "reconcile"
)

type Attempt struct {
	ID, From, To string
	Amount       Money
}

type Transfer struct {
	ID            string        `json:"id"`
	From          string        `json:"from"`
	To            string        `json:"to"`
	Amount        Money         `json:"amount"`
	State         TransferState `json:"state"`
	DebitApplied  bool          `json:"debit_applied,omitempty"`
	CreditApplied bool          `json:"credit_applied,omitempty"`
	CreatedRound  int           `json:"created_round"`
}

type AuditEntry struct {
	TransferID string        `json:"transfer_id"`
	State      TransferState `json:"state"`
	Delta      Money         `json:"delta"`
}

type BSOState struct {
	ID              string              `json:"id"`
	Balance         Money               `json:"balance"`
	Reserved        Money               `json:"reserved"`
	Outgoing        map[string]Transfer `json:"outgoing"`
	Incoming        map[string]Transfer `json:"incoming"`
	Audit           []AuditEntry        `json:"audit"`
	ProtocolVersion int                 `json:"protocol_version"`
}

// TransactionAgent is compact migratable protocol authority. It contains no BSO
// snapshot, channel, closure, goroutine, or worker-local pointer.
type TransactionAgent struct {
	ProtocolVersion                                                                                              int
	TransferID, SenderBSO, ReceiverBSO                                                                           string
	Amount                                                                                                       Money
	Phase                                                                                                        AgentPhase
	RetryCount, NextLogicalDeadline, LastObservedSenderVersion, LastObservedReceiverVersion, PlacementGeneration int
	LastMessageKind                                                                                              MessageKind
}

type ProtocolEnvelopeV1 struct {
	ProtocolVersion                         int
	Schema, MessageID, TransferID, From, To string
	Kind                                    MessageKind
	Amount                                  Money
	State                                   TransferState
	Auth                                    string
}

type AgentPlacementV1 struct {
	ProtocolVersion               int
	TransferID                    string
	WorkerID, PlacementGeneration int
}

type FaultProfile struct {
	Name                    string `json:"name"`
	DropRate, DuplicateRate float64
	MaxDelay, ReorderWindow int
}

var FaultProfiles = map[string]FaultProfile{
	"none":        {Name: "none"},
	"fun":         {Name: "fun", DropRate: .01, DuplicateRate: .02, MaxDelay: 3, ReorderWindow: 5},
	"mean":        {Name: "mean", DropRate: .05, DuplicateRate: .10, MaxDelay: 5, ReorderWindow: 9},
	"worker-loss": {Name: "worker-loss"},
}

type Config struct {
	Seed                                     int64        `json:"seed"`
	BSOs                                     int          `json:"bsos"`
	Transfers                                int          `json:"transfers"`
	Workers                                  int          `json:"workers"`
	InitialBalance                           Money        `json:"initial_balance"`
	Workload                                 string       `json:"workload"`
	Faults                                   FaultProfile `json:"faults"`
	MaxRounds, RetryDelay, ReservationExpiry int
	KillWorker                               int
	KillRound                                int
	RestartBSO                               string
	RestartRound                             int
	DataDir                                  string `json:"-"`
}

type Metrics struct {
	Attempted, Successful, Rejected, Unresolved                                                                                                         int
	MessagesSent, MessagesDelivered, MessagesDropped, DuplicatesInjected, DuplicatesSuppressed, DelayedOrReordered                                      int
	LocalDurableMutations, DoubleDebits, DoubleCredits, AuthenticationFailures                                                                          int
	AgentSteps, ReconcileEntriesExamined, ParticipantsTouched                                                                                           int
	CoordinatorOps, NewAgentPlacements, PlacementLookups, Rebalances, WorkerFailures, AgentsMigrated, HotPathCoordinatorMessages                        int
	AgentsAffected, UnrelatedAgentsPaused, RecoveryBSOsTouched, RecoveryAgentsTouched                                                                   int
	OpenBSODatabases, PeakActiveAgents, PeakQueuedAgents                                                                                                int
	MessagesPerSuccess, DurableMutationsPerSuccess, CoordinatorOpsPerSuccess, WorkerStepsPerSuccess, ParticipantsPerSuccess, ReconcileEntriesPerSuccess float64
	TransfersPerSecond                                                                                                                                  float64
	WorkerSteps                                                                                                                                         []int
	WorkerPeakActive                                                                                                                                    []int
	OctagonBytes, JSONBaselineBytes                                                                                                                     int
	OctagonEncodeNanoseconds, OctagonDecodeNanoseconds                                                                                                  int64
}

type Result struct {
	Config                   Config  `json:"config"`
	Metrics                  Metrics `json:"metrics"`
	InitialTotal, FinalTotal Money
	Conservation, Correct    bool
	CorrectnessDigest        string
	Elapsed                  time.Duration `json:"-"`
	ElapsedMilliseconds      int64
}

func DefaultConfig() Config {
	return Config{Seed: 20260823, BSOs: 100, Transfers: 500, Workers: 4, InitialBalance: 100_000, Workload: "random", Faults: FaultProfiles["fun"], MaxRounds: 64, RetryDelay: 2, ReservationExpiry: 24, KillWorker: -1}
}
