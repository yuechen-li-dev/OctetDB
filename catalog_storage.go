package octetdb

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const catalogMagic = "OCTCAT01"

type catalogEnvelope struct {
	Magic   string          `json:"magic"`
	Version int             `json:"version"`
	Payload json.RawMessage `json:"payload"`
	SHA256  string          `json:"sha256"`
}

func loadOrCreateCatalog(path string, keyed *KeyedDB) (catalogState, error) {
	catalogPath := filepath.Join(path, "catalog")
	payload, err := os.ReadFile(catalogPath)
	if errors.Is(err, os.ErrNotExist) {
		if keyed.sequence != 0 || len(keyed.records) != 0 {
			return catalogState{}, &Error{Kind: ErrorIncompatible, Op: "open_catalog", err: errors.New("keyed data exists without catalog identity")}
		}
		_ = os.Remove(catalogPath + ".tmp")
		idBytes := make([]byte, 16)
		if _, err := rand.Read(idBytes); err != nil {
			return catalogState{}, &Error{Kind: ErrorStorage, Op: "open_catalog", err: err}
		}
		state := catalogState{DatabaseID: hex.EncodeToString(idBytes), NextDatasetID: 1, Buckets: make(map[string]catalogBucket)}
		if err := persistCatalog(path, state); err != nil {
			return catalogState{}, err
		}
		return state, nil
	}
	if err != nil {
		return catalogState{}, &Error{Kind: ErrorStorage, Op: "open_catalog", err: err}
	}
	var envelope catalogEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return catalogState{}, &Error{Kind: ErrorCorruption, Op: "open_catalog", err: fmt.Errorf("decode catalog: %w", err)}
	}
	if envelope.Magic != catalogMagic || envelope.Version != 1 {
		return catalogState{}, &Error{Kind: ErrorIncompatible, Op: "open_catalog", err: errors.New("unsupported catalog format")}
	}
	digest := sha256.Sum256(envelope.Payload)
	if envelope.SHA256 != hex.EncodeToString(digest[:]) {
		return catalogState{}, &Error{Kind: ErrorCorruption, Op: "open_catalog", err: errors.New("catalog checksum mismatch")}
	}
	var state catalogState
	if err := json.Unmarshal(envelope.Payload, &state); err != nil {
		return catalogState{}, &Error{Kind: ErrorCorruption, Op: "open_catalog", err: fmt.Errorf("decode catalog payload: %w", err)}
	}
	if err := validateCatalogState(state, keyed); err != nil {
		return catalogState{}, err
	}
	_ = os.Remove(catalogPath + ".tmp")
	return state, nil
}

func persistCatalog(path string, state catalogState) error {
	payload, err := json.Marshal(state)
	if err != nil {
		return &Error{Kind: ErrorStorage, Op: "catalog_write", err: err}
	}
	digest := sha256.Sum256(payload)
	envelope, err := json.Marshal(catalogEnvelope{Magic: catalogMagic, Version: 1, Payload: payload, SHA256: hex.EncodeToString(digest[:])})
	if err != nil {
		return &Error{Kind: ErrorStorage, Op: "catalog_write", err: err}
	}
	finalPath := filepath.Join(path, "catalog")
	tmpPath := finalPath + ".tmp"
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return &Error{Kind: ErrorStorage, Op: "catalog_write", err: err}
	}
	written, writeErr := file.Write(envelope)
	if writeErr == nil && written != len(envelope) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = file.Sync()
	}
	if closeErr := file.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr == nil {
		writeErr = os.Rename(tmpPath, finalPath)
	}
	if writeErr == nil {
		writeErr = syncDirectory(path)
	}
	if writeErr != nil {
		_ = os.Remove(tmpPath)
		return &Error{Kind: ErrorStorage, Op: "catalog_write", err: writeErr}
	}
	return nil
}

func validateCatalogState(state catalogState, keyed *KeyedDB) error {
	if state.DatabaseID == "" || state.NextDatasetID == 0 || state.Buckets == nil {
		return &Error{Kind: ErrorCorruption, Op: "open_catalog", err: errors.New("catalog identity or topology is invalid")}
	}
	ids := make(map[uint64]DatasetInfo)
	for bucketName, bucket := range state.Buckets {
		if err := validCatalogName("bucket", bucketName); err != nil || bucket.Datasets == nil {
			return &Error{Kind: ErrorCorruption, Op: "open_catalog", err: errors.New("catalog bucket is invalid")}
		}
		for datasetName, dataset := range bucket.Datasets {
			if err := validCatalogName("dataset", datasetName); err != nil || dataset.ID == 0 || dataset.ID >= state.NextDatasetID || dataset.BucketName != bucketName || dataset.Name != datasetName || dataset.Kind != KeyedJSON || dataset.Origin != GoCatalog || dataset.Bounds.MaxRecords <= 0 || dataset.Bounds.MaxValueBytes <= 0 {
				return &Error{Kind: ErrorCorruption, Op: "open_catalog", err: errors.New("catalog dataset is invalid")}
			}
			if _, duplicate := ids[dataset.ID]; duplicate {
				return &Error{Kind: ErrorCorruption, Op: "open_catalog", err: errors.New("catalog contains duplicate dataset identity")}
			}
			ids[dataset.ID] = dataset
		}
	}
	counts := make(map[uint64]int)
	for key, value := range keyed.records {
		id, ok := backendDatasetID(key)
		if !ok {
			return &Error{Kind: ErrorCorruption, Op: "open_catalog", err: errors.New("record has no dataset identity")}
		}
		dataset, exists := ids[id]
		if !exists {
			return &Error{Kind: ErrorCorruption, Op: "open_catalog", err: errors.New("record references an unknown dataset")}
		}
		if len(value) > dataset.Bounds.MaxValueBytes {
			return &Error{Kind: ErrorCapacity, Op: "open_catalog", err: errors.New("record exceeds its dataset value bound")}
		}
		counts[id]++
		if counts[id] > dataset.Bounds.MaxRecords {
			return &Error{Kind: ErrorCapacity, Op: "open_catalog", err: errors.New("dataset exceeds its record bound")}
		}
	}
	return nil
}
