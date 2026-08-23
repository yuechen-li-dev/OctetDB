package engine

import (
	"sort"
	"sync"
)

type TokenKind uint8

const AccountToken TokenKind = 1

type Token struct {
	Kind TokenKind `json:"kind"`
	ID   uint64    `json:"id"`
}

type lockTable struct {
	mu    sync.Mutex
	locks map[Token]*sync.Mutex
}

func newLockTable() *lockTable { return &lockTable{locks: make(map[Token]*sync.Mutex)} }

func tokensFor(command Command) []Token {
	tokens := []Token{{Kind: AccountToken, ID: uint64(command.Account)}}
	if isKind(command.Kind, Transfer) || isKind(command.Kind, Confirm) {
		tokens = append(tokens, Token{Kind: AccountToken, ID: uint64(command.Other)})
	}
	sort.Slice(tokens, func(i, j int) bool {
		if tokens[i].Kind != tokens[j].Kind {
			return tokens[i].Kind < tokens[j].Kind
		}
		return tokens[i].ID < tokens[j].ID
	})
	return tokens
}

func (t *lockTable) acquire(tokens []Token) func() {
	locks := make([]*sync.Mutex, len(tokens))
	t.mu.Lock()
	for i, token := range tokens {
		lock := t.locks[token]
		if lock == nil {
			lock = &sync.Mutex{}
			t.locks[token] = lock
		}
		locks[i] = lock
	}
	t.mu.Unlock()
	for _, lock := range locks {
		lock.Lock()
	}
	return func() {
		for i := len(locks) - 1; i >= 0; i-- {
			locks[i].Unlock()
		}
	}
}
