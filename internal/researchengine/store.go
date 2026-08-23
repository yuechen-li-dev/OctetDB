package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
)

type Store struct {
	mu       sync.RWMutex
	accounts map[AccountID]Account
	ledger   []LedgerEntry
}

type LedgerEntry struct {
	Sequence  uint64    `json:"sequence"`
	CommandID string    `json:"command_id"`
	From      AccountID `json:"from"`
	To        AccountID `json:"to,omitempty"`
	Amount    int       `json:"amount"`
	EffectTag int       `json:"effect_tag"`
}

func NewStore() *Store { return &Store{accounts: make(map[AccountID]Account)} }

func (s *Store) view(a, b AccountID) (Account, bool, Account, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	av, aok := s.accounts[a]
	bv, bok := s.accounts[b]
	return av, aok, bv, bok
}

func (s *Store) Account(id AccountID) (Account, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.accounts[id]
	return a, ok
}

func (s *Store) apply(record logRecord) {
	if !record.Result.Accepted {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch record.EffectTag {
	case 1:
		s.accounts[record.AccountA] = Account{ID: record.AccountA, Balance: record.NewBalanceA, Status: StatusOpen, Version: 1}
	case 2:
		a := s.accounts[record.AccountA]
		a.Balance = record.NewBalanceA
		a.Version++
		s.accounts[record.AccountA] = a
	case 3:
		a := s.accounts[record.AccountA]
		b := s.accounts[record.AccountB]
		a.Balance = record.NewBalanceA
		a.Version++
		b.Balance = record.NewBalanceB
		b.Version++
		s.accounts[record.AccountA] = a
		s.accounts[record.AccountB] = b
	case 4:
		a := s.accounts[record.AccountA]
		a.Status = record.NewStatusTag
		a.Version++
		s.accounts[record.AccountA] = a
	default:
		return
	}
	s.ledger = append(s.ledger, LedgerEntry{Sequence: record.Sequence, CommandID: record.CommandID, From: record.AccountA, To: record.AccountB, Amount: record.Amount, EffectTag: record.EffectTag})
}

func (s *Store) CanonicalOctagon() ([]byte, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]int, 0, len(s.accounts))
	for id := range s.accounts {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	var b bytes.Buffer
	b.WriteString("AccountSnapshot {\n  Accounts: [\n")
	for _, raw := range ids {
		a := s.accounts[AccountID(raw)]
		fmt.Fprintf(&b, "    { ID: %d Balance: %d Status: %d Version: %d }\n", a.ID, a.Balance, a.Status, a.Version)
	}
	b.WriteString("  ]\n}\n")
	digest := sha256.Sum256(b.Bytes())
	return b.Bytes(), hex.EncodeToString(digest[:])
}

func (s *Store) LedgerLen() int { s.mu.RLock(); defer s.mu.RUnlock(); return len(s.ledger) }

func (s *Store) snapshotState() ([]Account, []LedgerEntry) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	accounts := make([]Account, 0, len(s.accounts))
	for _, account := range s.accounts {
		accounts = append(accounts, account)
	}
	sort.Slice(accounts, func(i, j int) bool { return accounts[i].ID < accounts[j].ID })
	ledger := append([]LedgerEntry(nil), s.ledger...)
	return accounts, ledger
}

func (s *Store) restoreState(accounts []Account, ledger []LedgerEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accounts = make(map[AccountID]Account, len(accounts))
	for _, account := range accounts {
		s.accounts[account.ID] = account
	}
	s.ledger = append([]LedgerEntry(nil), ledger...)
}
