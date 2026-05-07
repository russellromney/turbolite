// Package turbolite provides a Go interface to turbolite-backed SQLite databases.
//
// turbolite is a storage engine, not a query API. Open() returns a standard
// *sql.DB backed by turbolite's compressed page-group VFS, giving you the full
// database/sql interface: prepared statements, param binding, transactions,
// connection pooling, etc.
//
// Local mode (default), file-first:
//
//	db, err := turbolite.Open("/data/app.db", nil)
//	defer db.Close()
//	db.Exec("INSERT INTO t VALUES (?)", 42)
//
// `/data/app.db` is the local page image (turbolite-owned).
// `/data/app.db-turbolite/` holds hidden implementation state
// (manifest, cache, staging logs).
//
// S3 cloud mode:
//
//	db, err := turbolite.Open("/data/app.db", &turbolite.Options{
//	    Mode:     "s3",
//	    Bucket:   "my-bucket",
//	    Endpoint: "https://t3.storage.dev",
//	})
//
// Note: app.db is turbolite's compressed page image. It is not promised
// to be opened directly by stock sqlite3. To get a stock SQLite file,
// run `db.Exec("VACUUM INTO 'export.sqlite'")` while connected.
package turbolite

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	sqlite3 "github.com/mattn/go-sqlite3"
)

// Options for opening a turbolite database.
type Options struct {
	// Storage mode: "local" (default) or "s3".
	Mode string
	// S3 bucket name (required for mode "s3").
	Bucket string
	// S3 endpoint URL (for Tigris, MinIO).
	Endpoint string
	// S3 key prefix (default "turbolite").
	Prefix string
	// AWS region.
	Region string
	// Local cache directory (S3 mode default: /tmp/turbolite).
	CacheDir string
	// In-memory page cache size (default "64MB"). Set to "0" to disable.
	PageCache string
	// Zstd compression level 1-22 (default 3).
	CompressionLevel int
	// Prefetch worker threads (default: num_cpus + 1).
	PrefetchThreads int
	// Open in read-only mode.
	ReadOnly bool
}

var (
	vfsCounter    uint64
	driverCounter uint64

	// Bootstrap: load extension once to register SQL functions + global VFS.
	bootstrapOnce sync.Once
	bootstrapErr  error
	bootstrapDB   *sql.DB

	extPathOnce sync.Once
	extPath     string
	extPathErr  error
)

// findExt locates the turbolite loadable extension binary.
func findExt() (string, error) {
	extPathOnce.Do(func() {
		if p := os.Getenv("TURBOLITE_EXT_PATH"); p != "" {
			if info, err := os.Stat(p); err == nil && !info.IsDir() {
				extPath = p
				return
			}
			extPathErr = fmt.Errorf("TURBOLITE_EXT_PATH set but not found: %s", p)
			return
		}

		candidates := []string{
			"turbolite.dylib",
			"turbolite.so",
			"turbolite.dll",
			"libturbolite_ffi.dylib",
			"libturbolite_ffi.so",
			"libturbolite.dylib",
			"libturbolite.so",
		}
		for _, c := range candidates {
			if _, err := os.Stat(c); err == nil {
				extPath = c
				return
			}
		}

		extPathErr = fmt.Errorf(
			"turbolite extension not found. Set TURBOLITE_EXT_PATH or place " +
				"turbolite.dylib/turbolite.so in the working directory",
		)
	})
	return extPath, extPathErr
}

// ensureBootstrap loads the extension into a :memory: connection.
// This registers the turbolite SQL functions (turbolite_register_vfs, etc.)
// and the global "turbolite" VFS. The bootstrap connection is kept alive
// so that subsequent turbolite_register_vfs() calls can be made.
func ensureBootstrap(ext string) error {
	bootstrapOnce.Do(func() {
		driverName := fmt.Sprintf("turbolite_boot_%d", atomic.AddUint64(&driverCounter, 1))
		sql.Register(driverName, &sqlite3.SQLiteDriver{
			Extensions: []string{ext},
		})
		var err error
		bootstrapDB, err = sql.Open(driverName, ":memory:")
		if err != nil {
			bootstrapErr = fmt.Errorf("turbolite: open bootstrap: %w", err)
			return
		}
		// Force connection creation so ConnectHook runs.
		if err := bootstrapDB.Ping(); err != nil {
			bootstrapErr = fmt.Errorf("turbolite: bootstrap ping: %w", err)
			return
		}
	})
	return bootstrapErr
}

// StateDirForDatabasePath returns the hidden sidecar directory path for a
// file-first database path. For "/data/app.db" this is
// "/data/app.db-turbolite". Useful for tests or tooling that asserts on
// layout without hardcoding the suffix rule.
func StateDirForDatabasePath(dbPath string) string {
	return dbPath + "-turbolite"
}

// Open opens a turbolite database and returns a standard *sql.DB.
//
// Each call creates an isolated VFS instance so multiple databases in the
// same process don't share manifest or cache state.
//
// Pass nil for default options (local mode, 64MB page cache).
func Open(path string, opts *Options) (*sql.DB, error) {
	if opts == nil {
		opts = &Options{}
	}
	if opts.Mode == "" {
		opts.Mode = "local"
	}
	if opts.Mode != "local" && opts.Mode != "s3" {
		return nil, fmt.Errorf("turbolite: mode must be 'local' or 's3', got %q", opts.Mode)
	}
	if opts.PageCache == "" {
		opts.PageCache = "64MB"
	}

	ext, err := findExt()
	if err != nil {
		return nil, err
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("turbolite: resolve path: %w", err)
	}
	dbDir := filepath.Dir(absPath)

	// Set env vars before the extension loads.
	if opts.Mode == "s3" {
		bucket := opts.Bucket
		if bucket == "" {
			bucket = os.Getenv("TURBOLITE_BUCKET")
		}
		if bucket == "" {
			return nil, fmt.Errorf("turbolite: mode='s3' requires Bucket or TURBOLITE_BUCKET")
		}
		os.Setenv("TURBOLITE_BUCKET", bucket)
		if opts.Prefix != "" {
			os.Setenv("TURBOLITE_PREFIX", opts.Prefix)
		}
		if opts.Endpoint != "" {
			os.Setenv("TURBOLITE_ENDPOINT_URL", opts.Endpoint)
		}
		if opts.Region != "" {
			os.Setenv("TURBOLITE_REGION", opts.Region)
		}
		if opts.CacheDir != "" {
			os.Setenv("TURBOLITE_CACHE_DIR", opts.CacheDir)
		}
		if opts.ReadOnly {
			os.Setenv("TURBOLITE_READ_ONLY", "true")
		}
	}
	if opts.CompressionLevel > 0 {
		os.Setenv("TURBOLITE_COMPRESSION_LEVEL", fmt.Sprintf("%d", opts.CompressionLevel))
	}
	if opts.PrefetchThreads > 0 {
		os.Setenv("TURBOLITE_PREFETCH_THREADS", fmt.Sprintf("%d", opts.PrefetchThreads))
	}
	os.Setenv("TURBOLITE_MEM_CACHE_BUDGET", opts.PageCache)

	// Load extension (registers SQL functions + global VFS).
	if err := ensureBootstrap(ext); err != nil {
		return nil, err
	}

	// Determine VFS name.
	var vfsName string
	if opts.Mode == "local" {
		// Check directory exists before VFS registration.
		if _, err := os.Stat(dbDir); os.IsNotExist(err) {
			return nil, fmt.Errorf("turbolite: directory does not exist: %s", dbDir)
		}
		// File-first: register a per-database VFS keyed to the user-supplied
		// file path. The path is the local page image; sidecar metadata
		// lives at <absPath>-turbolite/.
		vfsName = fmt.Sprintf("turbolite-go-%d", atomic.AddUint64(&vfsCounter, 1))
		_, err := bootstrapDB.Exec(
			"SELECT turbolite_register_file_first_vfs(?, ?)", vfsName, absPath)
		if err != nil {
			return nil, fmt.Errorf("turbolite: register file-first VFS %q: %w", vfsName, err)
		}
	} else {
		vfsName = "turbolite-s3"
	}

	// Register a driver that sets per-connection pragmas.
	// The extension is already loaded globally via bootstrap (VFS + SQL
	// functions are process-wide), so we don't need Extensions here.
	driverName := fmt.Sprintf("turbolite_%d", atomic.AddUint64(&driverCounter, 1))
	sql.Register(driverName, &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			conn.Exec("PRAGMA cache_size=0", nil)
			return nil
		},
	})

	// Build DSN with VFS selection.
	dsn := fmt.Sprintf("file:%s?vfs=%s", absPath, vfsName)
	if opts.ReadOnly {
		dsn += "&mode=ro"
	}

	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("turbolite: open: %w", err)
	}

	// Force connection so we catch errors early.
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("turbolite: ping: %w", err)
	}

	// Local mode is single-writer; limit pool to 1 connection.
	if opts.Mode == "local" {
		db.SetMaxOpenConns(1)
	}

	if opts.Mode == "s3" {
		if _, err := db.Exec("PRAGMA page_size=65536"); err != nil {
			db.Close()
			return nil, fmt.Errorf("turbolite: set page_size: %w", err)
		}
		if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
			db.Close()
			return nil, fmt.Errorf("turbolite: set journal_mode: %w", err)
		}
	}

	return db, nil
}
