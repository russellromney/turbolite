#!/usr/bin/env python3
"""Generate the same deterministic social_* SQLite DB used by tiered-bench.

This avoids invoking tiered-bench just to create a local file, which lets the
sqlite-s3vfs benchmark wrapper sidestep manifest/CAS conflicts on the
Turbolite S3 prefix.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time

FIRST_NAMES = [
    "Mark", "Eduardo", "Dustin", "Chris", "Sean", "Priscilla", "Sheryl",
    "Andrew", "Adam", "Mike", "Sarah", "Jessica", "Emily", "David", "Alex",
    "Randi", "Naomi", "Kevin", "Amy", "Dan", "Lisa", "Tom", "Rachel",
    "Brian", "Caitlin", "Nicole", "Matt", "Laura", "Jake", "Megan",
]

LAST_NAMES = [
    "Zuckerberg", "Saverin", "Moskovitz", "Hughes", "Parker", "Chan",
    "Sandberg", "McCollum", "D'Angelo", "Schroepfer", "Smith", "Johnson",
    "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez",
    "Martinez", "Anderson", "Taylor", "Thomas", "Hernandez", "Moore",
    "Martin", "Jackson", "Thompson", "White", "Lopez",
]

SCHOOLS = [
    "Harvard", "Stanford", "MIT", "Yale", "Princeton", "Columbia", "Penn",
    "Brown", "Cornell", "Dartmouth", "Duke", "Georgetown", "UCLA",
    "Berkeley", "Michigan", "NYU", "Boston University", "Northeastern",
    "USC", "Emory",
]

CITIES = [
    "Palo Alto, CA", "San Francisco, CA", "New York, NY", "Boston, MA",
    "Cambridge, MA", "Seattle, WA", "Austin, TX", "Chicago, IL",
    "Los Angeles, CA", "Miami, FL", "Denver, CO", "Portland, OR",
    "Philadelphia, PA", "Washington, DC", "Atlanta, GA",
]

POST_TEMPLATES = [
    "Just moved into my new dorm room! {} is going to be amazing this year.",
    "Can't believe we won the game last night. Go {}!",
    "Anyone else studying for the {} midterm? This is brutal.",
    "Looking for people to join our {} intramural team. DM me!",
    "Had the best {} at that new place downtown. Highly recommend!",
    "Working on a new project with {}. Can't say much yet but stay tuned...",
    "Missing home but {} makes it worth it. Great people here.",
    "Just finished reading {}. Changed my perspective on everything.",
    "Road trip to {} this weekend! Who's in?",
    "Three exams in one week. {} life is no joke.",
    "Happy birthday to my roommate {}! Best {} ever.",
    "Throwback to that {} concert last summer. Need to see them again.",
    "Anyone want to grab {} at the dining hall? Meeting at 6pm.",
    "Finally submitted my {} paper. Time to celebrate!",
    "The weather in {} is unreal today. Perfect for frisbee on the quad.",
]

FILL_WORDS = [
    "college", "freshman year", "organic chemistry", "basketball", "pizza",
    "the team", "campus", "Malcolm Gladwell", "New York", "Harvard", "Alex",
    "friend", "Radiohead", "dinner", "thesis", "San Francisco",
]

PADDING = [
    " Can't wait to see what happens next.",
    " This semester is flying by.",
    " Anyone else feel the same way?",
    " Comment below if you're interested!",
    " Life is good right now.",
    " Really grateful for this experience.",
    " Shoutout to everyone who made this happen.",
    " More updates coming soon!",
]

SCHEMA = """
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL,
    school TEXT NOT NULL,
    city TEXT NOT NULL,
    bio TEXT NOT NULL,
    joined_at INTEGER NOT NULL
);

CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    content TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    like_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE friendships (
    user_a INTEGER NOT NULL,
    user_b INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (user_a, user_b)
);

CREATE TABLE likes (
    user_id INTEGER NOT NULL,
    post_id INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (user_id, post_id)
);

CREATE INDEX idx_posts_user ON posts(user_id);
CREATE INDEX idx_posts_created ON posts(created_at);
CREATE INDEX idx_friendships_b ON friendships(user_b, user_a);
CREATE INDEX idx_likes_post ON likes(post_id);
CREATE INDEX idx_likes_user ON likes(user_id, created_at);
CREATE INDEX idx_users_school ON users(school);
"""


def phash(seed: int) -> int:
    x = seed & 0xFFFFFFFFFFFFFFFF
    x = (x * 6364136223846793005 + 1442695040888963407) & 0xFFFFFFFFFFFFFFFF
    x ^= x >> 33
    x = (x * 0xFF51AFD7ED558CCD) & 0xFFFFFFFFFFFFFFFF
    x ^= x >> 33
    return x


def generate_post_content(i: int) -> str:
    h = phash(i + 9_000_000)
    template = POST_TEMPLATES[h % len(POST_TEMPLATES)]
    fill1 = FILL_WORDS[(h >> 16) % len(FILL_WORDS)]
    fill2 = FILL_WORDS[(h >> 24) % len(FILL_WORDS)]
    content = template.replace("{}", fill1, 1)
    content = content.replace("{}", fill2, 1)
    target_len = 200 + ((h >> 32) % 1800)
    while len(content) < target_len:
        pidx = phash(len(content) + i) % len(PADDING)
        content += PADDING[pidx]
    return content


def generate_bio(i: int) -> str:
    h = phash(i + 7_000_000)
    interests = ["music", "startups", "hiking", "photography", "cooking",
                 "travel", "reading", "sports", "gaming", "art"]
    i1 = interests[(h >> 8) % len(interests)]
    i2 = interests[(h >> 16) % len(interests)]
    i3 = interests[(h >> 24) % len(interests)]
    year = 2004 + (h % 4)
    return f"Class of {year}. Into {i1}, {i2}, and {i3}. Looking to connect!"


def generate_data(conn: sqlite3.Connection, n_posts: int, batch_size: int) -> None:
    n_users = max(n_posts // 10, 100)
    friends_per_user = 50
    n_friendships = n_users * friends_per_user // 2
    n_likes = n_posts * 3

    print(f"  Generating: {n_users:,} users, {n_posts:,} posts, "
          f"{n_friendships:,} friendships, {n_likes:,} likes")

    # Users
    cur = conn.cursor()
    cur.execute("BEGIN")
    for i in range(n_users):
        h = phash(i)
        email = (
            f"{FIRST_NAMES[h % len(FIRST_NAMES)].lower()}."
            f"{LAST_NAMES[(h >> 16) % len(LAST_NAMES)].lower()}"
            f"{i}@"
            f"{SCHOOLS[(h >> 24) % len(SCHOOLS)].lower().replace(' ', '')}"
        )
        cur.execute(
            "INSERT INTO users VALUES (?,?,?,?,?,?,?,?)",
            (
                i,
                FIRST_NAMES[h % len(FIRST_NAMES)],
                LAST_NAMES[(h >> 16) % len(LAST_NAMES)],
                email,
                SCHOOLS[(h >> 24) % len(SCHOOLS)],
                CITIES[(h >> 32) % len(CITIES)],
                generate_bio(i),
                1_075_000_000 + (h % 100_000_000),
            ),
        )
        if (i + 1) % batch_size == 0:
            conn.commit()
            cur.execute("BEGIN")
    conn.commit()
    print(f"    users inserted: {n_users:,}")

    # Posts
    cur.execute("BEGIN")
    for i in range(n_posts):
        h = phash(i + 1_000_000)
        cur.execute(
            "INSERT INTO posts (id, user_id, content, created_at, like_count) VALUES (?,?,?,?,?)",
            (
                i,
                h % n_users,
                generate_post_content(i),
                1_075_000_000 + ((h >> 16) % 94_000_000),
                phash(i + 2_000_000) % 200,
            ),
        )
        if (i + 1) % batch_size == 0:
            conn.commit()
            if (i + 1) % (batch_size * 10) == 0:
                print(f"    posts: {i + 1:,}/{n_posts:,}")
            cur.execute("BEGIN")
    conn.commit()
    print(f"    posts inserted: {n_posts:,}")

    # Friendships
    cur.execute("BEGIN")
    count = 0
    batch = 0
    for i in range(n_users):
        n_friends = min(friends_per_user, n_users - 1)
        for j in range(n_friends):
            h = phash(i * 100 + j + 3_000_000)
            friend = h % n_users
            if friend != i:
                a, b = (i, friend) if i < friend else (friend, i)
                cur.execute(
                    "INSERT OR IGNORE INTO friendships VALUES (?,?,?)",
                    (a, b, 1_075_000_000 + ((h >> 16) % 94_000_000)),
                )
                count += 1
                batch += 1
                if batch >= batch_size:
                    conn.commit()
                    cur.execute("BEGIN")
                    batch = 0
            if count >= n_friendships:
                break
        if count >= n_friendships:
            break
    conn.commit()
    print(f"    friendships inserted: {count:,}")

    # Likes
    cur.execute("BEGIN")
    for i in range(n_likes):
        h = phash(i + 4_000_000)
        cur.execute(
            "INSERT OR IGNORE INTO likes VALUES (?,?,?)",
            (
                h % n_users,
                (h >> 16) % n_posts,
                1_075_000_000 + ((h >> 32) % 94_000_000),
            ),
        )
        if (i + 1) % batch_size == 0:
            conn.commit()
            if (i + 1) % (batch_size * 10) == 0:
                print(f"    likes: {i + 1:,}/{n_likes:,}")
            cur.execute("BEGIN")
    conn.commit()
    print(f"    likes inserted: {n_likes:,}")


def generate_local_db(path: str, n_posts: int, batch_size: int, page_size: int) -> None:
    if os.path.exists(path):
        os.remove(path)
    conn = sqlite3.connect(path)
    conn.execute(f"PRAGMA page_size={page_size}")
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA cache_size=-262144")
    conn.executescript(SCHEMA)
    generate_data(conn, n_posts, batch_size)

    count = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
    page_count = conn.execute("PRAGMA page_count").fetchone()[0]
    ps = conn.execute("PRAGMA page_size").fetchone()[0]
    size_mb = (page_count * ps) / (1024.0 * 1024.0)
    print(f"[local-gen] {count:,} posts, {page_count:,} pages x {ps:,} bytes = {size_mb:.1f} MB")
    conn.execute("PRAGMA integrity_check")
    conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate deterministic social SQLite DB")
    parser.add_argument("--size", type=int, default=100000, help="number of posts")
    parser.add_argument("--page-size", type=int, default=65536, help="SQLite page size")
    parser.add_argument("--batch-size", type=int, default=10000, help="rows per commit")
    parser.add_argument("--output", type=str, required=True, help="output DB path")
    args = parser.parse_args()

    start = time.time()
    generate_local_db(args.output, args.size, args.batch_size, args.page_size)
    elapsed = time.time() - start
    print(f"Generated {args.output} in {elapsed:.2f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
