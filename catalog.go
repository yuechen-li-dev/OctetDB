package octetdb

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"sort"
	"strings"
)

// DatasetKind identifies a product-owned logical dataset representation.
// M2B intentionally supports only the conventional JSON record path.
type DatasetKind string

const (
	// KeyedJSON stores application-defined Go values encoded with encoding/json.
	KeyedJSON DatasetKind = "keyed_json"
)

const catalogKeyedFormatContents = "OCTETDB\nformat=1\nmodel=catalog-keyed-json-v1\nengine=safe-go\n"

// DatasetOrigin identifies how a catalog entry was declared.
type DatasetOrigin string

const (
	// GoCatalog means the sane-default Go API created the entry.
	GoCatalog DatasetOrigin = "go"
)

// DatasetBounds are semantic hard limits, not allocation hints.
type DatasetBounds struct {
	MaxRecords    int `json:"max_records"`
	MaxValueBytes int `json:"max_value_bytes"`
}

// DatasetOptions declare the stable compatibility identity and bounds of a
// dataset. Zero bounds inherit the database's configured keyed bounds.
type DatasetOptions struct {
	Kind          DatasetKind
	TypeIdentity  string
	MaxRecords    int
	MaxValueBytes int
}

// DefaultDatasetOptions selects opaque keyed JSON and inherited bounds.
func DefaultDatasetOptions() DatasetOptions { return DatasetOptions{} }

// DatabaseInfo describes the logical database identity.
type DatabaseInfo struct {
	ID string `json:"id"`
}

// BucketInfo describes one first-level catalog namespace.
type BucketInfo struct {
	Name string `json:"name"`
}

// DatasetInfo describes one leaf storage object.
type DatasetInfo struct {
	ID           uint64        `json:"id"`
	BucketName   string        `json:"bucket_name"`
	Name         string        `json:"name"`
	Kind         DatasetKind   `json:"kind"`
	Origin       DatasetOrigin `json:"origin"`
	TypeIdentity string        `json:"type_identity,omitempty"`
	Bounds       DatasetBounds `json:"bounds"`
}

// Catalog is a detached, stable view of logical database topology.
type Catalog struct {
	Database DatabaseInfo  `json:"database"`
	Buckets  []BucketInfo  `json:"buckets"`
	Datasets []DatasetInfo `json:"datasets"`
}

type catalogState struct {
	DatabaseID    string                   `json:"database_id"`
	NextDatasetID uint64                   `json:"next_dataset_id"`
	Buckets       map[string]catalogBucket `json:"buckets"`
}

type catalogBucket struct {
	Datasets map[string]DatasetInfo `json:"datasets"`
}

// Database is a durable database with a shallow Database/Bucket/Dataset
// catalog. Commands and their deduplication identity are database-wide.
type Database struct {
	keyed     *KeyedDB
	catalog   catalogState
	queryKeys map[uint64][]string
}

// Bucket is a stable handle to a first-level catalog namespace.
type Bucket struct {
	db   *Database
	name string
}

// Dataset is a stable handle to a leaf keyed-record collection.
type Dataset struct {
	db   *Database
	info DatasetInfo
}

// Tx is valid only inside a Mutation callback. It can access several datasets
// in one database-wide atomic command.
type Tx struct {
	tx *KeyedTx
	db *Database
}

// Mutation atomically reads and writes records across datasets.
type Mutation func(*Tx) (any, error)

// OpenCatalog creates or recovers a conventional catalog-aware database.
// OctetDB owns the product files beneath path.
func OpenCatalog(ctx context.Context, path string, options KeyedOptions) (*Database, error) {
	keyed, err := openKeyed(ctx, path, options, catalogKeyedFormatContents)
	if err != nil {
		return nil, err
	}
	state, err := loadOrCreateCatalog(path, keyed)
	if err != nil {
		_ = keyed.wal.close()
		keyed.closed.Store(true)
		return nil, err
	}
	db := &Database{keyed: keyed, catalog: state, queryKeys: make(map[uint64][]string)}
	db.initializeQueryKeys()
	keyed.afterApply = db.applyQueryKeyMutations
	return db, nil
}

// initializeQueryKeys reconstructs the non-durable primary-key cursor used by
// Dataset.Scan. It is derived from committed records and is not a secondary
// predicate index or part of query meaning.
func (db *Database) initializeQueryKeys() {
	for backendKey := range db.keyed.records {
		id, ok := backendDatasetID(backendKey)
		if !ok {
			continue
		}
		db.queryKeys[id] = append(db.queryKeys[id], backendKey[9:])
	}
	for id := range db.queryKeys {
		sort.Strings(db.queryKeys[id])
	}
}

// applyQueryKeyMutations maintains primary-key order inside the same admission
// boundary that applies the durable record mutation.
func (db *Database) applyQueryKeyMutations(record keyedWALRecord) {
	if !record.Applied {
		return
	}
	for _, mutation := range record.Mutations {
		id, ok := backendDatasetID(mutation.Key)
		if !ok {
			continue
		}
		key := mutation.Key[9:]
		keys := db.queryKeys[id]
		position := sort.SearchStrings(keys, key)
		exists := position < len(keys) && keys[position] == key
		if mutation.Delete {
			if exists {
				db.queryKeys[id] = append(keys[:position], keys[position+1:]...)
			}
			continue
		}
		if !exists {
			keys = append(keys, "")
			copy(keys[position+1:], keys[position:])
			keys[position] = key
			db.queryKeys[id] = keys
		}
	}
}

// DatabaseID returns the durable generated identity of this database.
func (db *Database) DatabaseID() string {
	if db == nil {
		return ""
	}
	return db.catalog.DatabaseID
}

// Bucket opens or durably creates a bucket. Structural changes are not data
// transactions and are complete before this method returns.
func (db *Database) Bucket(ctx context.Context, name string) (*Bucket, error) {
	if err := validCatalogName("bucket", name); err != nil {
		return nil, err
	}
	if err := db.enter(ctx, "bucket"); err != nil {
		return nil, err
	}
	defer db.keyed.leaveKeyed()
	if _, ok := db.catalog.Buckets[name]; !ok {
		next := cloneCatalogState(db.catalog)
		next.Buckets[name] = catalogBucket{Datasets: make(map[string]DatasetInfo)}
		if err := persistCatalog(db.keyed.path, next); err != nil {
			db.keyed.poisoned.Store(true)
			return nil, err
		}
		db.catalog = next
	}
	return &Bucket{db: db, name: name}, nil
}

// ListBuckets returns catalog bucket names in deterministic order.
func (db *Database) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	if err := db.enter(ctx, "list_buckets"); err != nil {
		return nil, err
	}
	defer db.keyed.leaveKeyed()
	result := make([]BucketInfo, 0, len(db.catalog.Buckets))
	for name := range db.catalog.Buckets {
		result = append(result, BucketInfo{Name: name})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

// Catalog returns a deterministic detached topology snapshot.
func (db *Database) Catalog(ctx context.Context) (Catalog, error) {
	if err := db.enter(ctx, "catalog"); err != nil {
		return Catalog{}, err
	}
	defer db.keyed.leaveKeyed()
	view := Catalog{Database: DatabaseInfo{ID: db.catalog.DatabaseID}}
	for bucketName, bucket := range db.catalog.Buckets {
		view.Buckets = append(view.Buckets, BucketInfo{Name: bucketName})
		for _, dataset := range bucket.Datasets {
			view.Datasets = append(view.Datasets, dataset)
		}
	}
	sort.Slice(view.Buckets, func(i, j int) bool { return view.Buckets[i].Name < view.Buckets[j].Name })
	sort.Slice(view.Datasets, func(i, j int) bool {
		if view.Datasets[i].BucketName == view.Datasets[j].BucketName {
			return view.Datasets[i].Name < view.Datasets[j].Name
		}
		return view.Datasets[i].BucketName < view.Datasets[j].BucketName
	})
	return view, nil
}

// Dataset opens or durably creates a leaf dataset. Reopening with incompatible
// kind, type identity, or semantic bounds fails closed.
func (bucket *Bucket) Dataset(ctx context.Context, name string, options DatasetOptions) (*Dataset, error) {
	if bucket == nil || bucket.db == nil {
		return nil, &Error{Kind: ErrorClosed, Op: "dataset", err: errors.New("database is closed")}
	}
	if err := validCatalogName("dataset", name); err != nil {
		return nil, err
	}
	normalized, err := normalizeDatasetOptions(options, bucket.db.keyed.options)
	if err != nil {
		return nil, err
	}
	if err := bucket.db.enter(ctx, "dataset"); err != nil {
		return nil, err
	}
	defer bucket.db.keyed.leaveKeyed()
	entry := bucket.db.catalog.Buckets[bucket.name]
	if existing, ok := entry.Datasets[name]; ok {
		if existing.Kind != normalized.Kind || existing.TypeIdentity != normalized.TypeIdentity || existing.Bounds.MaxRecords != normalized.MaxRecords || existing.Bounds.MaxValueBytes != normalized.MaxValueBytes {
			return nil, &Error{Kind: ErrorIncompatible, Op: "dataset", err: errors.New("dataset kind, type identity, or bounds do not match its catalog entry")}
		}
		return &Dataset{db: bucket.db, info: existing}, nil
	}
	next := cloneCatalogState(bucket.db.catalog)
	info := DatasetInfo{ID: next.NextDatasetID, BucketName: bucket.name, Name: name, Kind: normalized.Kind, Origin: GoCatalog, TypeIdentity: normalized.TypeIdentity, Bounds: DatasetBounds{MaxRecords: normalized.MaxRecords, MaxValueBytes: normalized.MaxValueBytes}}
	next.NextDatasetID++
	nextBucket := next.Buckets[bucket.name]
	nextBucket.Datasets[name] = info
	next.Buckets[bucket.name] = nextBucket
	if err := persistCatalog(bucket.db.keyed.path, next); err != nil {
		bucket.db.keyed.poisoned.Store(true)
		return nil, err
	}
	bucket.db.catalog = next
	return &Dataset{db: bucket.db, info: info}, nil
}

// ListDatasets returns this bucket's dataset metadata in name order.
func (bucket *Bucket) ListDatasets(ctx context.Context) ([]DatasetInfo, error) {
	if bucket == nil || bucket.db == nil {
		return nil, &Error{Kind: ErrorClosed, Op: "list_datasets", err: errors.New("database is closed")}
	}
	if err := bucket.db.enter(ctx, "list_datasets"); err != nil {
		return nil, err
	}
	defer bucket.db.keyed.leaveKeyed()
	entry, ok := bucket.db.catalog.Buckets[bucket.name]
	if !ok {
		return nil, &Error{Kind: ErrorCorruption, Op: "list_datasets", err: errors.New("bucket handle is absent from catalog")}
	}
	result := make([]DatasetInfo, 0, len(entry.Datasets))
	for _, info := range entry.Datasets {
		result = append(result, info)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

// Info returns detached dataset metadata.
func (dataset *Dataset) Info() DatasetInfo {
	if dataset == nil {
		return DatasetInfo{}
	}
	return dataset.info
}

// Get decodes one dataset-scoped record.
func (dataset *Dataset) Get(ctx context.Context, key string, destination any) (bool, error) {
	if err := validRecordKey(key); err != nil {
		return false, err
	}
	if dataset == nil || dataset.db == nil {
		return false, &Error{Kind: ErrorClosed, Op: "dataset_get", err: errors.New("database is closed")}
	}
	if destination == nil {
		return false, &Error{Kind: ErrorInvalidInput, Op: "dataset_get", err: errors.New("destination is required")}
	}
	if err := dataset.db.keyed.enterKeyed(ctx, "dataset_get"); err != nil {
		return false, err
	}
	defer dataset.db.keyed.leaveKeyed()
	value, ok := dataset.db.keyed.records[backendRecordKey(dataset.info.ID, key)]
	if !ok {
		return false, nil
	}
	if err := json.Unmarshal(value, destination); err != nil {
		return false, &Error{Kind: ErrorCorruption, Op: "dataset_get", err: err}
	}
	return true, nil
}

// Mutate submits one durable atomic command that may touch several datasets.
// Command IDs and exact retry decisions are database-wide.
func (db *Database) Mutate(ctx context.Context, command KeyedCommand, mutation Mutation) (KeyedDecision, error) {
	if db == nil || db.keyed == nil {
		return KeyedDecision{}, &Error{Kind: ErrorClosed, Op: "catalog_mutate", err: errors.New("database is closed")}
	}
	if mutation == nil {
		return KeyedDecision{}, &Error{Kind: ErrorInvalidInput, Op: "catalog_mutate", err: errors.New("mutation is required")}
	}
	return db.keyed.SubmitKeyed(ctx, command, func(tx *KeyedTx) (any, error) {
		catalogTx := &Tx{tx: tx, db: db}
		result, err := mutation(catalogTx)
		if err != nil {
			return result, err
		}
		if err := catalogTx.validateDatasetBounds(); err != nil {
			return nil, err
		}
		return result, nil
	})
}

// Snapshot installs a deterministic data snapshot. Catalog state is already
// synchronized by each structural operation.
func (db *Database) Snapshot(ctx context.Context) error {
	if db == nil || db.keyed == nil {
		return &Error{Kind: ErrorClosed, Op: "catalog_snapshot", err: errors.New("database is closed")}
	}
	return db.keyed.SnapshotKeyed(ctx)
}

// Close snapshots data and closes storage. It is idempotent.
func (db *Database) Close() error {
	if db == nil || db.keyed == nil {
		return nil
	}
	return db.keyed.Close()
}

// Get reads a record in the named dataset.
func (tx *Tx) Get(dataset *Dataset, key string, destination any) (bool, error) {
	if err := tx.validDataset(dataset, key, "get"); err != nil {
		return false, err
	}
	if destination == nil {
		return false, &Error{Kind: ErrorInvalidInput, Op: "catalog_tx_get", err: errors.New("destination is required")}
	}
	backendKey := backendRecordKey(dataset.info.ID, key)
	if value, ok := tx.tx.writes[backendKey]; ok {
		if value == nil {
			return false, nil
		}
		if err := json.Unmarshal(*value, destination); err != nil {
			return false, &Error{Kind: ErrorInvalidInput, Op: "catalog_tx_get", err: err}
		}
		return true, nil
	}
	value, ok := tx.db.keyed.records[backendKey]
	if !ok {
		return false, nil
	}
	if err := json.Unmarshal(value, destination); err != nil {
		return false, &Error{Kind: ErrorCorruption, Op: "catalog_tx_get", err: err}
	}
	return true, nil
}

// Put writes a JSON record in the named dataset.
func (tx *Tx) Put(dataset *Dataset, key string, value any) error {
	if err := tx.validDataset(dataset, key, "put"); err != nil {
		return err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return &Error{Kind: ErrorInvalidInput, Op: "catalog_tx_put", err: err}
	}
	if len(encoded) > dataset.info.Bounds.MaxValueBytes {
		return &Error{Kind: ErrorCapacity, Op: "catalog_tx_put", err: errors.New("value exceeds dataset MaxValueBytes")}
	}
	copyValue := append([]byte(nil), encoded...)
	tx.tx.writes[backendRecordKey(dataset.info.ID, key)] = &copyValue
	return nil
}

// Delete removes a record in the named dataset.
func (tx *Tx) Delete(dataset *Dataset, key string) error {
	if err := tx.validDataset(dataset, key, "delete"); err != nil {
		return err
	}
	tx.tx.writes[backendRecordKey(dataset.info.ID, key)] = nil
	return nil
}

func (tx *Tx) validDataset(dataset *Dataset, key, op string) error {
	if tx == nil || tx.tx == nil || tx.db == nil {
		return &Error{Kind: ErrorInvalidInput, Op: "catalog_tx_" + op, err: errors.New("transaction is no longer active")}
	}
	if err := tx.tx.valid(op); err != nil {
		return err
	}
	if dataset == nil || dataset.db != tx.db {
		return &Error{Kind: ErrorInvalidInput, Op: "catalog_tx_" + op, err: errors.New("dataset does not belong to this database")}
	}
	return validRecordKey(key)
}

func (tx *Tx) validateDatasetBounds() error {
	counts := make(map[uint64]int)
	for key := range tx.db.keyed.records {
		if id, ok := backendDatasetID(key); ok {
			counts[id]++
		}
	}
	for key, value := range tx.tx.writes {
		id, ok := backendDatasetID(key)
		if !ok {
			continue
		}
		_, existed := tx.db.keyed.records[key]
		if value == nil && existed {
			counts[id]--
		} else if value != nil && !existed {
			counts[id]++
		}
	}
	for _, bucket := range tx.db.catalog.Buckets {
		for _, dataset := range bucket.Datasets {
			if counts[dataset.ID] > dataset.Bounds.MaxRecords {
				return &Error{Kind: ErrorCapacity, Op: "catalog_mutate", err: errors.New("dataset record capacity exceeded")}
			}
		}
	}
	return nil
}

func (db *Database) enter(ctx context.Context, op string) error {
	if db == nil || db.keyed == nil {
		return &Error{Kind: ErrorClosed, Op: op, err: errors.New("database is closed")}
	}
	if err := db.keyed.enterKeyed(ctx, op); err != nil {
		return err
	}
	if (op == "bucket" || op == "dataset") && db.keyed.poisoned.Load() {
		db.keyed.leaveKeyed()
		return &Error{Kind: ErrorPoisoned, Op: op, err: errors.New("an earlier catalog durability failure made structural writes unsafe; close and reopen the database")}
	}
	return nil
}

func normalizeDatasetOptions(options DatasetOptions, database KeyedOptions) (DatasetOptions, error) {
	if options.Kind == "" {
		options.Kind = KeyedJSON
	}
	if options.Kind != KeyedJSON {
		return DatasetOptions{}, &Error{Kind: ErrorIncompatible, Op: "dataset", err: errors.New("unsupported dataset kind")}
	}
	if options.MaxRecords < 0 || options.MaxValueBytes < 0 {
		return DatasetOptions{}, &Error{Kind: ErrorInvalidInput, Op: "dataset", err: errors.New("dataset bounds cannot be negative")}
	}
	if options.MaxRecords == 0 {
		options.MaxRecords = database.MaxRecords
	}
	if options.MaxValueBytes == 0 {
		options.MaxValueBytes = database.MaxValueBytes
	}
	if options.MaxValueBytes > database.MaxValueBytes {
		return DatasetOptions{}, &Error{Kind: ErrorInvalidInput, Op: "dataset", err: errors.New("dataset MaxValueBytes exceeds database MaxValueBytes")}
	}
	if len(options.TypeIdentity) > keyedMaxIdentityBytes {
		return DatasetOptions{}, &Error{Kind: ErrorCapacity, Op: "dataset", err: errors.New("type identity exceeds 4 KiB")}
	}
	return options, nil
}

func validCatalogName(kind, name string) error {
	if strings.TrimSpace(name) == "" || name != strings.TrimSpace(name) {
		return &Error{Kind: ErrorInvalidInput, Op: kind, err: errors.New(kind + " name is required and cannot have surrounding whitespace")}
	}
	if strings.ContainsAny(name, "/\\\x00") {
		return &Error{Kind: ErrorInvalidInput, Op: kind, err: errors.New(kind + " name cannot contain path separators or NUL")}
	}
	if len(name) > keyedMaxIdentityBytes {
		return &Error{Kind: ErrorCapacity, Op: kind, err: errors.New(kind + " name exceeds 4 KiB")}
	}
	return nil
}

func validRecordKey(key string) error {
	if key == "" {
		return &Error{Kind: ErrorInvalidInput, Op: "record_key", err: errors.New("record key is required")}
	}
	if len(key) > keyedMaxKeyBytes {
		return &Error{Kind: ErrorCapacity, Op: "record_key", err: errors.New("record key exceeds 4 KiB")}
	}
	return nil
}

func backendRecordKey(datasetID uint64, recordKey string) string {
	encoded := make([]byte, 9+len(recordKey))
	encoded[0] = 0
	binary.BigEndian.PutUint64(encoded[1:9], datasetID)
	copy(encoded[9:], recordKey)
	return string(encoded)
}

func backendDatasetID(key string) (uint64, bool) {
	if len(key) < 9 || key[0] != 0 {
		return 0, false
	}
	return binary.BigEndian.Uint64([]byte(key[1:9])), true
}

func cloneCatalogState(source catalogState) catalogState {
	clone := catalogState{DatabaseID: source.DatabaseID, NextDatasetID: source.NextDatasetID, Buckets: make(map[string]catalogBucket, len(source.Buckets))}
	for name, bucket := range source.Buckets {
		datasets := make(map[string]DatasetInfo, len(bucket.Datasets))
		for datasetName, info := range bucket.Datasets {
			datasets[datasetName] = info
		}
		clone.Buckets[name] = catalogBucket{Datasets: datasets}
	}
	return clone
}
