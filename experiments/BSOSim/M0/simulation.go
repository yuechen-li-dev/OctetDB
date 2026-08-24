package bsosim

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/yuechen-li-dev/octetdb"
)

type simulation struct {
	ctx        context.Context
	config     Config
	bsos       map[string]*durableBSO
	orderedIDs []string
	transport  *transport
	metrics    Metrics
	crashed    map[CrashPoint]bool
	completed  map[string]int
	root       string
}

func Run(ctx context.Context, config Config) (result Result, err error) {
	config, err = normalizeConfig(config)
	if err != nil {
		return result, err
	}
	root := config.DataDir
	cleanup := func() {}
	if root == "" {
		root, err = os.MkdirTemp("", "octetdb-bso-sim-")
		if err != nil {
			return result, err
		}
		cleanup = func() { _ = os.RemoveAll(root) }
	}
	defer cleanup()

	s := &simulation{ctx: ctx, config: config, bsos: map[string]*durableBSO{}, crashed: map[CrashPoint]bool{}, completed: map[string]int{}, root: filepath.Join(root, "bso")}
	s.transport = newTransport(config.Seed+17, config.Faults, &s.metrics)
	defer func() {
		for _, id := range s.orderedIDs {
			if closeErr := s.bsos[id].close(); err == nil && closeErr != nil {
				err = closeErr
			}
		}
	}()
	for i := 0; i < config.BSOs; i++ {
		id := bsoID(i)
		bso, openErr := openBSO(ctx, s.root, id, config.InitialBalance, &s.metrics)
		if openErr != nil {
			return result, openErr
		}
		s.bsos[id] = bso
		s.orderedIDs = append(s.orderedIDs, id)
	}

	attempts := GenerateWorkload(config)
	s.metrics.Attempted = len(attempts)
	start := time.Now()
	for _, attempt := range attempts {
		envelope, send, reserveErr := s.bsos[attempt.From].reserve(ctx, attempt, 0)
		if reserveErr != nil {
			return result, reserveErr
		}
		if send {
			if crashErr := s.maybeCrash(CrashAfterReserve, attempt.From); crashErr != nil {
				return result, crashErr
			}
			s.transport.send(envelope)
		}
	}
	if err := s.checkAccountingInvariant(); err != nil {
		return result, err
	}

	for round := 0; round < config.MaxRounds; round++ {
		if drainErr := s.transport.drain(s.deliver); drainErr != nil {
			return result, drainErr
		}
		if invariantErr := s.checkAccountingInvariant(); invariantErr != nil {
			return result, invariantErr
		}
		if inspectErr := s.noteCompletions(round); inspectErr != nil {
			return result, inspectErr
		}
		resolved, inspectErr := s.allResolved()
		if inspectErr != nil {
			return result, inspectErr
		}
		if resolved {
			break
		}
		if reconcileErr := s.reconcile(round + 1); reconcileErr != nil {
			return result, reconcileErr
		}
	}
	if drainErr := s.transport.drain(s.deliver); drainErr != nil {
		return result, drainErr
	}
	elapsed := time.Since(start)
	return s.result(attempts, elapsed)
}

func normalizeConfig(config Config) (Config, error) {
	defaults := DefaultConfig()
	if config.Seed == 0 {
		config.Seed = defaults.Seed
	}
	if config.BSOs == 0 {
		config.BSOs = defaults.BSOs
	}
	if config.Transfers == 0 {
		config.Transfers = defaults.Transfers
	}
	if config.InitialBalance == 0 {
		config.InitialBalance = defaults.InitialBalance
	}
	if config.Workload == "" {
		config.Workload = defaults.Workload
	}
	if config.Faults.Name == "" {
		config.Faults = defaults.Faults
	}
	if config.MaxRounds == 0 {
		config.MaxRounds = defaults.MaxRounds
	}
	if config.ReservationExpiry == 0 {
		config.ReservationExpiry = defaults.ReservationExpiry
	}
	if config.BSOs < 2 || config.Transfers < 1 || config.InitialBalance < 1 || config.MaxRounds < 1 {
		return config, errors.New("BSOs >= 2, transfers >= 1, initial balance >= 1, and max rounds >= 1 are required")
	}
	switch config.Workload {
	case "random", "hot-merchant", "hot-payer", "institution":
	default:
		return config, fmt.Errorf("unknown workload %q", config.Workload)
	}
	return config, nil
}

func GenerateWorkload(config Config) []Attempt {
	rng := rand.New(rand.NewSource(config.Seed))
	attempts := make([]Attempt, 0, config.Transfers)
	for i := 0; i < config.Transfers; i++ {
		from, to := 0, 1
		amount := Money(1 + rng.Intn(1000))
		switch config.Workload {
		case "hot-merchant":
			to = config.BSOs - 1
			from = i % (config.BSOs - 1)
		case "hot-payer":
			from = 0
			to = 1 + (i % (config.BSOs - 1))
			amount = Money(1 + rng.Intn(200))
		case "institution":
			from = rng.Intn(config.BSOs)
			to = rng.Intn(config.BSOs - 1)
			if to >= from {
				to++
			}
			amount = Money(100 + rng.Intn(5000))
		default:
			from = rng.Intn(config.BSOs)
			to = rng.Intn(config.BSOs - 1)
			if to >= from {
				to++
			}
		}
		attempts = append(attempts, Attempt{ID: fmt.Sprintf("transfer:%08d", i), From: bsoID(from), To: bsoID(to), Amount: amount})
	}
	return attempts
}

func bsoID(index int) string { return fmt.Sprintf("bso:%06d", index) }

func (s *simulation) deliver(envelope Envelope) error {
	receiver := s.bsos[envelope.To]
	if receiver == nil {
		s.metrics.AuthenticationFailures++
		return nil
	}
	response, err := receiver.handle(s.ctx, envelope)
	if err != nil {
		return err
	}
	switch envelope.Kind {
	case MessageOffer:
		if err := s.maybeCrash(CrashAfterAccept, envelope.To); err != nil {
			return err
		}
	case MessageAccept:
		if err := s.maybeCrash(CrashAfterSenderCommit, envelope.To); err != nil {
			return err
		}
	case MessageCommit:
		if err := s.maybeCrash(CrashAfterReceiverCommit, envelope.To); err != nil {
			return err
		}
		if err := s.maybeCrash(CrashBeforeAck, envelope.To); err != nil {
			return err
		}
	}
	if response != nil {
		s.transport.send(*response)
	}
	return nil
}

func (s *simulation) maybeCrash(point CrashPoint, id string) error {
	if s.crashed[point] {
		return nil
	}
	wanted := false
	for _, configured := range s.config.CrashSchedule {
		if configured == point {
			wanted = true
			break
		}
	}
	if !wanted {
		return nil
	}
	s.crashed[point] = true
	s.metrics.Crashes++
	return s.bsos[id].restart(s.ctx)
}

func (s *simulation) reconcile(round int) error {
	for _, id := range s.orderedIDs {
		state, err := s.bsos[id].load(s.ctx)
		if err != nil {
			return err
		}
		outgoingIDs := sortedTransferIDs(state.Outgoing)
		for _, transferID := range outgoingIDs {
			transfer := state.Outgoing[transferID]
			switch transfer.State {
			case StateReserved, StateAccepted:
				if round-transfer.CreatedRound >= s.config.ReservationExpiry {
					outcome, _, err := s.bsos[id].mutate(s.ctx, "expire/"+transfer.ID, func(current *BSOState) (messageOutcome, error) {
						t := current.Outgoing[transfer.ID]
						if t.State == StateReserved || t.State == StateAccepted {
							current.Balance += t.Amount
							current.Reserved -= t.Amount
							t.State = StateExpired
							current.Outgoing[t.ID] = t
							return messageOutcome{Send: true, Kind: MessageReconcile, State: StateExpired}, nil
						}
						return messageOutcome{}, nil
					})
					if err != nil {
						return err
					}
					if outcome.Send {
						s.sendReconciliation(transfer, outcome.Kind, outcome.State)
					}
				} else {
					s.sendReconciliation(transfer, MessageOffer, transfer.State)
				}
			case StateCommitted:
				s.sendReconciliation(transfer, MessageCommit, transfer.State)
			case StateExpired:
				// Resend only the sender's terminal fact. The receiver decides
				// locally whether an uncommitted acceptance should expire.
				s.sendReconciliation(transfer, MessageReconcile, transfer.State)
			}
		}
		incomingIDs := sortedTransferIDs(state.Incoming)
		for _, transferID := range incomingIDs {
			transfer := state.Incoming[transferID]
			switch transfer.State {
			case StateAccepted:
				s.sendFromReceiver(transfer, MessageAccept, transfer.State)
			case StateCommitted:
				s.sendFromReceiver(transfer, MessageAck, transfer.State)
			}
		}
	}
	return nil
}

func (s *simulation) sendReconciliation(t Transfer, kind MessageKind, state TransferState) {
	s.metrics.Retries++
	s.metrics.ReconciliationActions++
	s.transport.send(newEnvelope(t.ID, t.From, t.To, t.Amount, kind, state))
}

func (s *simulation) sendFromReceiver(t Transfer, kind MessageKind, state TransferState) {
	s.metrics.Retries++
	s.metrics.ReconciliationActions++
	s.transport.send(newEnvelope(t.ID, t.To, t.From, t.Amount, kind, state))
}

func sortedTransferIDs(transfers map[string]Transfer) []string {
	ids := make([]string, 0, len(transfers))
	for id := range transfers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (s *simulation) noteCompletions(round int) error {
	for _, id := range s.orderedIDs {
		state, err := s.bsos[id].load(s.ctx)
		if err != nil {
			return err
		}
		for transferID, transfer := range state.Outgoing {
			if _, exists := s.completed[transferID]; !exists && (transfer.State == StateAcknowledged || transfer.State == StateRejected || transfer.State == StateExpired) {
				s.completed[transferID] = round - transfer.CreatedRound + 1
			}
		}
	}
	return nil
}

func (s *simulation) allResolved() (bool, error) {
	for _, id := range s.orderedIDs {
		state, err := s.bsos[id].load(s.ctx)
		if err != nil {
			return false, err
		}
		for _, transfer := range state.Outgoing {
			if transfer.State != StateAcknowledged && transfer.State != StateRejected && transfer.State != StateExpired {
				return false, nil
			}
		}
		for _, transfer := range state.Incoming {
			if transfer.State == StateAccepted {
				return false, nil
			}
		}
	}
	return true, nil
}

func (s *simulation) checkAccountingInvariant() error {
	states := make(map[string]BSOState, len(s.orderedIDs))
	total := Money(0)
	for _, id := range s.orderedIDs {
		state, err := s.bsos[id].load(s.ctx)
		if err != nil {
			return err
		}
		if state.Balance < 0 || state.Reserved < 0 {
			return fmt.Errorf("negative local value at %s", id)
		}
		states[id] = state
		total += state.Balance + state.Reserved
	}
	for _, sender := range states {
		for _, transfer := range sender.Outgoing {
			if !transfer.DebitApplied {
				continue
			}
			receiver, exists := states[transfer.To].Incoming[transfer.ID]
			if !exists || !receiver.CreditApplied {
				total += transfer.Amount
			}
		}
	}
	want := Money(s.config.BSOs) * s.config.InitialBalance
	if total != want {
		return fmt.Errorf("money conservation during protocol: accounted=%d initial=%d", total, want)
	}
	return nil
}

func (s *simulation) result(attempts []Attempt, elapsed time.Duration) (Result, error) {
	balances := make([]string, 0, len(s.orderedIDs))
	states := make([]string, 0, len(attempts))
	unresolved := make([]string, 0)
	latencies := make([]int, 0, len(attempts))
	finalTotal := Money(0)
	incoming := map[string]Transfer{}
	outgoing := map[string]Transfer{}
	debitApplications := map[string]int{}
	creditApplications := map[string]int{}
	for _, id := range s.orderedIDs {
		state, err := s.bsos[id].load(s.ctx)
		if err != nil {
			return Result{}, err
		}
		if state.Balance < 0 || state.Reserved < 0 {
			return Result{}, fmt.Errorf("negative local value at %s", id)
		}
		finalTotal += state.Balance
		balances = append(balances, fmt.Sprintf("%s=%d", id, state.Balance))
		for transferID, transfer := range state.Outgoing {
			outgoing[transferID] = transfer
		}
		for transferID, transfer := range state.Incoming {
			incoming[transferID] = transfer
		}
		for _, entry := range state.Audit {
			if entry.Delta < 0 {
				debitApplications[entry.TransferID]++
			} else if entry.Delta > 0 {
				creditApplications[entry.TransferID]++
			}
		}
	}
	for _, attempt := range attempts {
		t := outgoing[attempt.ID]
		receiver := incoming[attempt.ID]
		receiverState := TransferState("none")
		if receiver.ID != "" {
			receiverState = receiver.State
		}
		states = append(states, fmt.Sprintf("%s=%s/%s", t.ID, t.State, receiverState))
		orphanAccepted := receiver.ID != "" && receiver.State == StateAccepted
		switch t.State {
		case StateAcknowledged:
			if receiver.State == StateCommitted {
				s.metrics.Successful++
			} else {
				s.metrics.Unresolved++
				unresolved = append(unresolved, t.ID)
			}
		case StateRejected, StateExpired:
			if orphanAccepted {
				s.metrics.Unresolved++
				unresolved = append(unresolved, t.ID)
			} else {
				s.metrics.Rejected++
			}
		default:
			s.metrics.Unresolved++
			unresolved = append(unresolved, t.ID)
		}
		if debitApplications[attempt.ID] > 1 {
			s.metrics.DoubleDebits += debitApplications[attempt.ID] - 1
		}
		if creditApplications[attempt.ID] > 1 {
			s.metrics.DoubleCredits += creditApplications[attempt.ID] - 1
		}
		if latency, ok := s.completed[attempt.ID]; ok {
			latencies = append(latencies, latency)
		}
	}
	sort.Strings(balances)
	sort.Strings(states)
	sort.Strings(unresolved)
	sort.Ints(latencies)
	digestInput, _ := json.Marshal(struct{ Balances, States, Unresolved []string }{balances, states, unresolved})
	digestSum := sha256.Sum256(digestInput)
	if elapsed > 0 {
		s.metrics.TransfersPerSecond = float64(len(attempts)) / elapsed.Seconds()
	}
	if s.metrics.Successful > 0 {
		s.metrics.MessagesPerSuccess = float64(s.metrics.MessagesSent) / float64(s.metrics.Successful)
		s.metrics.DurableCommitsPerSuccess = float64(s.metrics.LocalDurableMutations) / float64(s.metrics.Successful)
	}
	s.metrics.P50LogicalLatency = percentile(latencies, 0.50)
	s.metrics.P95LogicalLatency = percentile(latencies, 0.95)
	s.metrics.P99LogicalLatency = percentile(latencies, 0.99)
	initialTotal := Money(s.config.BSOs) * s.config.InitialBalance
	return Result{Config: s.config, Metrics: s.metrics, InitialTotal: initialTotal, FinalTotal: finalTotal,
		Conservation: finalTotal == initialTotal && s.metrics.Unresolved == 0, CorrectnessDigest: hex.EncodeToString(digestSum[:]), Elapsed: elapsed, ElapsedMilliseconds: elapsed.Milliseconds()}, nil
}

func percentile(sorted []int, p float64) int {
	if len(sorted) == 0 {
		return 0
	}
	index := int(float64(len(sorted)-1)*p + 0.5)
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

type globalState struct {
	Balances  map[string]Money    `json:"balances"`
	Transfers map[string]Transfer `json:"transfers"`
}

func RunGlobalControl(ctx context.Context, config Config) (Result, error) {
	config, err := normalizeConfig(config)
	if err != nil {
		return Result{}, err
	}
	root := config.DataDir
	cleanup := func() {}
	if root == "" {
		root, err = os.MkdirTemp("", "octetdb-global-control-")
		if err != nil {
			return Result{}, err
		}
		cleanup = func() { _ = os.RemoveAll(root) }
	}
	defer cleanup()
	db, err := octetdb.OpenCatalog(ctx, filepath.Join(root, "global"), octetdb.DefaultKeyedOptions())
	if err != nil {
		return Result{}, err
	}
	defer db.Close()
	bucket, err := db.Bucket(ctx, "control")
	if err != nil {
		return Result{}, err
	}
	dataset, err := bucket.Dataset(ctx, "ledger", octetdb.DatasetOptions{TypeIdentity: "bso-sim.GlobalLedgerControl/v1"})
	if err != nil {
		return Result{}, err
	}
	state := globalState{Balances: map[string]Money{}, Transfers: map[string]Transfer{}}
	for i := 0; i < config.BSOs; i++ {
		state.Balances[bsoID(i)] = config.InitialBalance
	}
	_, err = db.Mutate(ctx, octetdb.KeyedCommand{ID: "initialize/global"}, func(tx *octetdb.Tx) (any, error) { return state, tx.Put(dataset, stateKey, state) })
	if err != nil {
		return Result{}, err
	}
	attempts := GenerateWorkload(config)
	metrics := Metrics{Attempted: len(attempts), LocalDurableMutations: 1}
	start := time.Now()
	for _, attempt := range attempts {
		decision, err := db.Mutate(ctx, octetdb.KeyedCommand{ID: "global/" + attempt.ID}, func(tx *octetdb.Tx) (any, error) {
			var current globalState
			found, err := tx.Get(dataset, stateKey, &current)
			if err != nil || !found {
				return nil, err
			}
			transfer := Transfer{ID: attempt.ID, From: attempt.From, To: attempt.To, Amount: attempt.Amount}
			if attempt.Amount <= 0 || current.Balances[attempt.From] < attempt.Amount {
				transfer.State = StateRejected
			} else {
				current.Balances[attempt.From] -= attempt.Amount
				current.Balances[attempt.To] += attempt.Amount
				transfer.State, transfer.DebitApplied, transfer.CreditApplied = StateAcknowledged, true, true
			}
			current.Transfers[attempt.ID] = transfer
			return transfer, tx.Put(dataset, stateKey, current)
		})
		if err != nil {
			return Result{}, err
		}
		metrics.GlobalSerializationOps++
		if !decision.Duplicate {
			metrics.LocalDurableMutations++
		}
	}
	elapsed := time.Since(start)
	var final globalState
	found, err := dataset.Get(ctx, stateKey, &final)
	if err != nil || !found {
		return Result{}, err
	}
	balances, states := make([]string, 0, len(final.Balances)), make([]string, 0, len(final.Transfers))
	finalTotal := Money(0)
	for id, balance := range final.Balances {
		finalTotal += balance
		balances = append(balances, fmt.Sprintf("%s=%d", id, balance))
	}
	for id, transfer := range final.Transfers {
		states = append(states, fmt.Sprintf("%s=%s", id, transfer.State))
		if transfer.State == StateAcknowledged {
			metrics.Successful++
		} else {
			metrics.Rejected++
		}
	}
	sort.Strings(balances)
	sort.Strings(states)
	encoded, _ := json.Marshal(struct{ Balances, States []string }{balances, states})
	sum := sha256.Sum256(encoded)
	metrics.TransfersPerSecond = float64(len(attempts)) / elapsed.Seconds()
	initialTotal := Money(config.BSOs) * config.InitialBalance
	return Result{Config: config, Metrics: metrics, InitialTotal: initialTotal, FinalTotal: finalTotal, Conservation: finalTotal == initialTotal,
		CorrectnessDigest: hex.EncodeToString(sum[:]), Elapsed: elapsed, ElapsedMilliseconds: elapsed.Milliseconds()}, nil
}

func RunComparison(ctx context.Context, config Config) (Comparison, error) {
	bso, err := Run(ctx, config)
	if err != nil {
		return Comparison{}, err
	}
	global, err := RunGlobalControl(ctx, config)
	if err != nil {
		return Comparison{}, err
	}
	return Comparison{BSO: bso, Global: global}, nil
}
