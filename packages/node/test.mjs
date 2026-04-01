// Node package tests: local mode + S3 mode
import { Database } from './index.js';
import { strict as assert } from 'node:assert';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'turbolite-test-'));

// ===== Local mode =====

console.log('test: local mode basic CRUD...');
{
    const dbPath = path.join(tmpDir, 'local.db');
    const db = new Database(dbPath);
    db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    db.exec("INSERT INTO t VALUES (1, 'hello')");
    db.exec("INSERT INTO t VALUES (2, 'world')");
    const rows = db.query('SELECT * FROM t ORDER BY id');
    assert.equal(rows.length, 2);
    assert.equal(rows[0].val, 'hello');
    assert.equal(rows[1].val, 'world');
    db.close();
    console.log('  PASS');
}

console.log('test: local mode with compression level...');
{
    const dbPath = path.join(tmpDir, 'local_level.db');
    const db = new Database(dbPath, { compression: 9 });
    db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    db.exec("INSERT INTO t VALUES (1, 'compressed')");
    const rows = db.query('SELECT val FROM t WHERE id = 1');
    assert.equal(rows[0].val, 'compressed');
    db.close();
    console.log('  PASS');
}

// ===== S3 mode =====

const bucket = process.env.TIERED_TEST_BUCKET || process.env.TURBOLITE_BUCKET;
const endpoint = process.env.AWS_ENDPOINT_URL;

if (bucket) {
    console.log('test: S3 mode write + read...');
    {
        const dbPath = path.join(tmpDir, 's3_test.db');
        const prefix = `test/node-${Date.now()}`;
        const db = new Database(dbPath, {
            mode: 's3',
            bucket,
            prefix,
            endpoint: endpoint || undefined,
            region: 'auto',
        });
        db.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)');
        for (let i = 0; i < 50; i++) {
            db.exec(`INSERT INTO items VALUES (${i}, 'item_${i}')`);
        }

        // Verify page size is 64KB
        const pageSize = db.query('PRAGMA page_size')[0].page_size;
        assert.equal(pageSize, 65536, `expected 64KB pages, got ${pageSize}`);

        // Verify WAL mode
        const journalMode = db.query('PRAGMA journal_mode')[0].journal_mode;
        assert.equal(journalMode, 'wal', `expected WAL mode, got ${journalMode}`);

        // Checkpoint to S3
        db.exec('PRAGMA wal_checkpoint(TRUNCATE)');

        const rows = db.query('SELECT COUNT(*) as cnt FROM items');
        assert.equal(rows[0].cnt, 50);

        const row = db.query('SELECT name FROM items WHERE id = 25');
        assert.equal(row[0].name, 'item_25');

        db.close();
        console.log('  PASS');
    }

    console.log('test: S3 mode requires bucket...');
    {
        const dbPath = path.join(tmpDir, 's3_nobucket.db');
        try {
            new Database(dbPath, { mode: 's3' });
            assert.fail('should have thrown without bucket');
        } catch (e) {
            assert.ok(e.message.includes('bucket'), `expected bucket error, got: ${e.message}`);
            console.log('  PASS');
        }
    }
} else {
    console.log('SKIP: S3 tests (no TIERED_TEST_BUCKET or TURBOLITE_BUCKET set)');
}

// Cleanup
fs.rmSync(tmpDir, { recursive: true, force: true });
console.log('\nAll tests passed!');
