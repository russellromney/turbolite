package turbolite

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// Go counterpart to tests/test_auto_extension_python.py +
// packages/node/test-auto-extension.mjs. Verifies that Open() gives
// a *sql.DB with `turbolite_config_set` already bound by the
// sqlite3_auto_extension hook — no explicit install call from the Go
// binding — and that the multi-connection-per-thread pApp routing
// holds, and that unrelated sqlite3 connections coexist cleanly.

func TestAutoExtension_SingleConnectionBinding(t *testing.T) {
	db := openLocal(t)

	var rc int
	err := db.QueryRow(
		`SELECT turbolite_config_set('prefetch_search', '0.5,0.5')`,
	).Scan(&rc)
	if err != nil {
		t.Fatalf("turbolite_config_set via auto-extension: %v", err)
	}
	if rc != 0 {
		t.Fatalf("expected 0 from turbolite_config_set, got %d", rc)
	}
}

func TestAutoExtension_MultiConnectionRoutesPerConnection(t *testing.T) {
	a := openLocal(t)
	b := openLocal(t)

	var rcA, rcB int
	if err := a.QueryRow(
		`SELECT turbolite_config_set('prefetch_search', '0.11,0.22')`,
	).Scan(&rcA); err != nil {
		t.Fatalf("A turbolite_config_set: %v", err)
	}
	if err := b.QueryRow(
		`SELECT turbolite_config_set('prefetch_search', '0.99,0.88')`,
	).Scan(&rcB); err != nil {
		t.Fatalf("B turbolite_config_set: %v", err)
	}
	if rcA != 0 || rcB != 0 {
		t.Fatalf("expected 0 from both, got A=%d B=%d", rcA, rcB)
	}

	// Both connections stay functional with independent state.
	if _, err := a.Exec("CREATE TABLE IF NOT EXISTS t (x)"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Exec("CREATE TABLE IF NOT EXISTS t (x)"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Exec("INSERT INTO t VALUES (1), (2)"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Exec("INSERT INTO t VALUES (10)"); err != nil {
		t.Fatal(err)
	}
	var na, nb int
	if err := a.QueryRow("SELECT count(*) FROM t").Scan(&na); err != nil {
		t.Fatal(err)
	}
	if err := b.QueryRow("SELECT count(*) FROM t").Scan(&nb); err != nil {
		t.Fatal(err)
	}
	if na != 2 || nb != 1 {
		t.Fatalf("expected A=2 B=1, got A=%d B=%d", na, nb)
	}
}

func TestAutoExtension_PlainConnectionCoexists(t *testing.T) {
	// Ensure turbolite is loaded + auto-extension is armed.
	_ = openLocal(t)

	// Plain go-sqlite3 :memory: connection — no turbolite VFS.
	plain, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open plain: %v", err)
	}
	t.Cleanup(func() { plain.Close() })

	if _, err := plain.Exec("CREATE TABLE t (x INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if _, err := plain.Exec("INSERT INTO t VALUES (42)"); err != nil {
		t.Fatal(err)
	}
	var v int
	if err := plain.QueryRow("SELECT x FROM t").Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != 42 {
		t.Fatalf("expected 42, got %d", v)
	}

	// turbolite_config_set must NOT be bound on the plain connection.
	var rc int
	err = plain.QueryRow(
		`SELECT turbolite_config_set('prefetch_search', '0.5,0.5')`,
	).Scan(&rc)
	if err == nil {
		t.Fatalf("expected turbolite_config_set to fail on plain conn, got rc=%d", rc)
	}
	if !strings.Contains(err.Error(), "no such function") &&
		!strings.Contains(err.Error(), "turbolite_config_set") {
		t.Fatalf("expected 'no such function' error, got: %v", err)
	}
}
