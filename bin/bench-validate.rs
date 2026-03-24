//! Quick local validation: tests schema + queries with plain SQLite (no VFS, no S3).
//! Just verifies the social network benchmark queries work and are fast.

use rusqlite::Connection;
use std::time::Instant;

const FIRST_NAMES: &[&str] = &[
    "Mark", "Eduardo", "Dustin", "Chris", "Sean", "Priscilla", "Sheryl",
    "Andrew", "Adam", "Mike", "Sarah", "Jessica", "Emily", "David", "Alex",
];
const LAST_NAMES: &[&str] = &[
    "Zuckerberg", "Saverin", "Moskovitz", "Hughes", "Parker", "Chan",
    "Sandberg", "McCollum", "D'Angelo", "Schroepfer", "Smith", "Johnson",
    "Williams", "Brown", "Jones",
];
const SCHOOLS: &[&str] = &[
    "Harvard", "Stanford", "MIT", "Yale", "Princeton", "Columbia",
    "Penn", "Brown", "Cornell", "Dartmouth",
];
const CITIES: &[&str] = &[
    "Palo Alto, CA", "San Francisco, CA", "New York, NY", "Boston, MA",
    "Cambridge, MA", "Seattle, WA", "Austin, TX", "Chicago, IL",
];

fn phash(seed: u64) -> u64 {
    let mut x = seed;
    x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x
}

fn main() {
    let n_posts: usize = 100_000;
    let n_users = n_posts / 10;

    let conn = Connection::open(":memory:").unwrap();
    conn.execute_batch("PRAGMA page_size=65536; PRAGMA journal_mode=WAL;").unwrap();

    conn.execute_batch("
        CREATE TABLE users (id INTEGER PRIMARY KEY, first_name TEXT NOT NULL, last_name TEXT NOT NULL,
            email TEXT NOT NULL, school TEXT NOT NULL, city TEXT NOT NULL, bio TEXT NOT NULL, joined_at INTEGER NOT NULL);
        CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, content TEXT NOT NULL,
            created_at INTEGER NOT NULL, like_count INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE friendships (user_a INTEGER NOT NULL, user_b INTEGER NOT NULL, created_at INTEGER NOT NULL,
            PRIMARY KEY (user_a, user_b));
        CREATE TABLE likes (user_id INTEGER NOT NULL, post_id INTEGER NOT NULL, created_at INTEGER NOT NULL,
            PRIMARY KEY (user_id, post_id));
        CREATE INDEX idx_posts_user ON posts(user_id);
        CREATE INDEX idx_posts_created ON posts(created_at);
        CREATE INDEX idx_friendships_b ON friendships(user_b, user_a);
        CREATE INDEX idx_likes_post ON likes(post_id);
        CREATE INDEX idx_likes_user ON likes(user_id, created_at);
        CREATE INDEX idx_users_school ON users(school);
    ").unwrap();

    // Generate data
    let t = Instant::now();
    let tx = conn.unchecked_transaction().unwrap();
    {
        let mut s = tx.prepare("INSERT INTO users VALUES (?1,?2,?3,?4,?5,?6,?7,?8)").unwrap();
        for i in 0..n_users as i64 {
            let h = phash(i as u64);
            s.execute(rusqlite::params![
                i,
                FIRST_NAMES[(h as usize) % FIRST_NAMES.len()],
                LAST_NAMES[((h >> 16) as usize) % LAST_NAMES.len()],
                format!("user{}@school.edu", i),
                SCHOOLS[((h >> 24) as usize) % SCHOOLS.len()],
                CITIES[((h >> 32) as usize) % CITIES.len()],
                format!("Bio for user {}", i),
                1075000000 + (h % 100_000_000) as i64,
            ]).unwrap();
        }
    }
    {
        let mut s = tx.prepare("INSERT INTO posts VALUES (?1,?2,?3,?4,?5)").unwrap();
        for i in 0..n_posts as i64 {
            let h = phash(i as u64 + 1_000_000);
            let content = format!("Post {} content - just moved into the dorm, excited for the semester! {}", i,
                "x".repeat(((h >> 32) % 1500) as usize));
            s.execute(rusqlite::params![
                i, (h % n_users as u64) as i64, content,
                1075000000 + (h >> 16) as i64 % 94_000_000,
                (phash(i as u64 + 2_000_000) % 200) as i64,
            ]).unwrap();
        }
    }
    {
        let mut s = tx.prepare("INSERT OR IGNORE INTO friendships VALUES (?1,?2,?3)").unwrap();
        for i in 0..n_users as u64 {
            for j in 0..50u64 {
                let h = phash(i * 100 + j + 3_000_000);
                let friend = (h % n_users as u64) as i64;
                if friend != i as i64 {
                    let (a, b) = if (i as i64) < friend { (i as i64, friend) } else { (friend, i as i64) };
                    let _ = s.execute(rusqlite::params![a, b, 1075000000 + (h >> 16) as i64 % 94_000_000]);
                }
            }
        }
    }
    {
        let mut s = tx.prepare("INSERT OR IGNORE INTO likes VALUES (?1,?2,?3)").unwrap();
        for i in 0..(n_posts * 3) as u64 {
            let h = phash(i + 4_000_000);
            let _ = s.execute(rusqlite::params![
                (h % n_users as u64) as i64,
                ((h >> 16) % n_posts as u64) as i64,
                1075000000 + (h >> 32) as i64 % 94_000_000,
            ]);
        }
    }
    tx.commit().unwrap();
    println!("Data gen: {:.2}s ({} users, {} posts)", t.elapsed().as_secs_f64(), n_users, n_posts);

    // Verify row counts
    let users: i64 = conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0)).unwrap();
    let posts: i64 = conn.query_row("SELECT COUNT(*) FROM posts", [], |r| r.get(0)).unwrap();
    let friends: i64 = conn.query_row("SELECT COUNT(*) FROM friendships", [], |r| r.get(0)).unwrap();
    let likes: i64 = conn.query_row("SELECT COUNT(*) FROM likes", [], |r| r.get(0)).unwrap();
    println!("Rows: {} users, {} posts, {} friendships, {} likes\n", users, posts, friends, likes);

    // Test each query
    let queries: Vec<(&str, &str, Vec<rusqlite::types::Value>)> = vec![
        ("News feed (3-join)",
         "SELECT p.id, p.content, p.created_at, p.like_count, u.first_name, u.last_name \
          FROM friendships f JOIN posts p ON p.user_id = f.user_b JOIN users u ON u.id = p.user_id \
          WHERE f.user_a = ?1 ORDER BY p.created_at DESC LIMIT 20",
         vec![rusqlite::types::Value::Integer(42)]),

        ("Who liked (2-join)",
         "SELECT u.first_name, u.last_name, u.school, l.created_at \
          FROM likes l JOIN users u ON u.id = l.user_id WHERE l.post_id = ?1 \
          ORDER BY l.created_at DESC LIMIT 50",
         vec![rusqlite::types::Value::Integer(1000)]),

        ("Mutual friends (self-join)",
         "SELECT u.id, u.first_name, u.last_name, u.school \
          FROM friendships f1 JOIN friendships f2 ON f1.user_b = f2.user_b \
          JOIN users u ON u.id = f1.user_b WHERE f1.user_a = ?1 AND f2.user_a = ?2 LIMIT 20",
         vec![rusqlite::types::Value::Integer(42), rusqlite::types::Value::Integer(99)]),

        ("School leaderboard (scan+join)",
         "SELECT u.school, COUNT(DISTINCT u.id) as users, COUNT(p.id) as posts, SUM(p.like_count) as total_likes \
          FROM users u JOIN posts p ON p.user_id = u.id GROUP BY u.school ORDER BY total_likes DESC",
         vec![]),

        ("Post detail (2-join)",
         "SELECT p.id, p.content, p.created_at, p.like_count, u.first_name, u.last_name, u.school, u.city \
          FROM posts p JOIN users u ON u.id = p.user_id WHERE p.id = ?1",
         vec![rusqlite::types::Value::Integer(5000)]),

        ("User profile (join+sort)",
         "SELECT u.first_name, u.last_name, u.school, u.city, u.bio, p.id, p.content, p.created_at, p.like_count \
          FROM users u JOIN posts p ON p.user_id = u.id WHERE u.id = ?1 ORDER BY p.created_at DESC LIMIT 10",
         vec![rusqlite::types::Value::Integer(42)]),

        ("Engagement scan",
         "SELECT (p.created_at / 3600) % 24 as hour, COUNT(*) as posts, SUM(p.like_count) as likes \
          FROM posts p GROUP BY hour ORDER BY hour",
         vec![]),
    ];

    println!("{:<30} {:>8} {:>8}", "Query", "Rows", "Time");
    println!("{:-<30} {:->8} {:->8}", "", "", "");

    for (label, sql, params) in &queries {
        let start = Instant::now();
        let mut stmt = conn.prepare(sql).unwrap();
        let row_count: usize = if params.is_empty() {
            stmt.query_map([], |_| Ok(())).unwrap().count()
        } else {
            stmt.query_map(rusqlite::params_from_iter(params), |_| Ok(())).unwrap().count()
        };
        let elapsed = start.elapsed();

        // Run 100 iterations for timing
        let iter_start = Instant::now();
        for _ in 0..100 {
            let mut s = conn.prepare_cached(sql).unwrap();
            if params.is_empty() {
                let _: Vec<_> = s.query_map([], |_| Ok(())).unwrap().collect();
            } else {
                let _: Vec<_> = s.query_map(rusqlite::params_from_iter(params), |_| Ok(())).unwrap().collect();
            }
        }
        let avg_us = iter_start.elapsed().as_micros() as f64 / 100.0;

        let time_str = if avg_us >= 1000.0 {
            format!("{:.1}ms", avg_us / 1000.0)
        } else {
            format!("{:.0}us", avg_us)
        };
        println!("{:<30} {:>8} {:>8}", label, row_count, time_str);
        let _ = elapsed;
    }

    println!("\nAll queries OK!");
}
