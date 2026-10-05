from __future__ import annotations

import json
import secrets
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .common import Permanent, dumps

SCHEMA = "1"


class Store:
    """All cursor changes and associated inbox records share one FULL transaction.

    This store is used by one asyncio process. No transaction may span an await.
    External-effect intent is committed before making the network request.
    """
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.path = path
        self.db = sqlite3.connect(path, isolation_level=None, timeout=1)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS routes(
          peer INTEGER PRIMARY KEY, thread INTEGER UNIQUE, title TEXT NOT NULL,
          muted INTEGER NOT NULL DEFAULT 0, topic_state TEXT NOT NULL DEFAULT 'new');
        CREATE TABLE IF NOT EXISTS inbox(
          source TEXT NOT NULL, key TEXT NOT NULL, peer INTEGER NOT NULL DEFAULT 0,
          payload TEXT NOT NULL, processed INTEGER NOT NULL DEFAULT 0,
          created REAL NOT NULL, PRIMARY KEY(source,key));
        CREATE TABLE IF NOT EXISTS jobs(
          id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT UNIQUE NOT NULL,
          kind TEXT NOT NULL, peer INTEGER NOT NULL DEFAULT 0,
          lane TEXT NOT NULL, priority INTEGER NOT NULL DEFAULT 10,
          payload TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
          tries INTEGER NOT NULL DEFAULT 0, next_at REAL NOT NULL DEFAULT 0,
          created REAL NOT NULL, updated REAL NOT NULL, error TEXT NOT NULL DEFAULT '',
          effect INTEGER NOT NULL DEFAULT 0, random_id INTEGER UNIQUE);
        CREATE INDEX IF NOT EXISTS jobs_queue ON jobs(state,next_at,priority,id);
        CREATE INDEX IF NOT EXISTS jobs_lane ON jobs(peer,lane,id,state);
        CREATE TABLE IF NOT EXISTS links(
          tg INTEGER PRIMARY KEY, peer INTEGER NOT NULL, mid INTEGER NOT NULL DEFAULT 0,
          cmid INTEGER NOT NULL DEFAULT 0, incoming INTEGER NOT NULL DEFAULT 0,
          created REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS links_vk ON links(peer,cmid,mid);
        CREATE TABLE IF NOT EXISTS echoes(
          peer INTEGER NOT NULL, random_id INTEGER NOT NULL, mid INTEGER NOT NULL DEFAULT 0,
          cmid INTEGER NOT NULL DEFAULT 0, created REAL NOT NULL, PRIMARY KEY(peer,random_id));
        CREATE TABLE IF NOT EXISTS names(id INTEGER PRIMARY KEY, name TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS imports(
          peer INTEGER PRIMARY KEY, state TEXT NOT NULL, anchor INTEGER NOT NULL DEFAULT 0,
          upper_mid INTEGER NOT NULL DEFAULT 0, scanned INTEGER NOT NULL DEFAULT 0,
          since REAL NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '', updated REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS archive(
          peer INTEGER NOT NULL, mid INTEGER NOT NULL, cmid INTEGER NOT NULL,
          stamp INTEGER NOT NULL, payload TEXT NOT NULL, queued INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY(peer,mid));
        """)
        version = self.get("schema")
        if version is not None and version != SCHEMA:
            raise Permanent(f"Unsupported database schema {version}; do not overwrite the database")
        self.set("schema", SCHEMA)

    def close(self) -> None:
        self.db.close()

    @contextmanager
    def tx(self) -> Iterator[None]:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def get(self, key: str, default=None):
        row = self.db.execute("SELECT v FROM meta WHERE k=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set(self, key: str, value) -> None:
        self.db.execute("INSERT INTO meta VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (key, dumps(value)))

    def bind_identity(self, vk: int, tg: int, group: int, owner: int) -> None:
        identity = [vk, tg, group, owner]
        previous = self.get("identity")
        if previous is not None and previous != identity:
            raise Permanent("Account/bot/group/owner differs from this database. Use a separate STATE_DIR.")
        self.set("identity", identity)

    def names(self, response: dict) -> None:
        for p in response.get("profiles", []):
            self.db.execute("INSERT OR REPLACE INTO names VALUES(?,?)", (p["id"], (p.get("first_name", "") + " " + p.get("last_name", "")).strip()))
        for g in response.get("groups", []):
            self.db.execute("INSERT OR REPLACE INTO names VALUES(?,?)", (-abs(g["id"]), g.get("name", str(g["id"]))))

    def name(self, uid: int) -> str:
        r = self.db.execute("SELECT name FROM names WHERE id=?", (uid,)).fetchone()
        return r[0] if r else f"VK {uid}"

    def route(self, peer: int = 0, thread: int = 0):
        if thread:
            return self.db.execute("SELECT * FROM routes WHERE thread=?", (thread,)).fetchone()
        return self.db.execute("SELECT * FROM routes WHERE peer=?", (peer,)).fetchone()

    def ensure_route(self, peer: int, title: str = "") -> None:
        self.db.execute("INSERT OR IGNORE INTO routes(peer,title) VALUES(?,?)", (peer, title or self.name(peer)))

    def bind(self, peer: int, thread: int, title: str = "") -> None:
        if not peer or thread <= 1:
            raise Permanent("Use a regular topic, not General, and a nonzero peer_id")
        other = self.route(thread=thread)
        current = self.route(peer=peer)
        if other and other["peer"] != peer:
            raise Permanent("This topic already has another VK conversation")
        if current and current["thread"] not in (None, thread):
            raise Permanent("VK conversation is already bound to another topic")
        self.ensure_route(peer, title)
        self.db.execute("UPDATE routes SET thread=?,topic_state='ready' WHERE peer=?", (thread, peer))

    def ingest(self, source: str, key: str, payload: dict, peer: int = 0) -> bool:
        cur = self.db.execute("INSERT OR IGNORE INTO inbox VALUES(?,?,?,?,0,?)", (source, key, peer, dumps(payload), time.time()))
        return cur.rowcount == 1

    @staticmethod
    def vk_key(message: dict) -> str:
        peer = int(message["peer_id"])
        cmid, mid = int(message.get("conversation_message_id", 0)), int(message.get("id", 0))
        if not cmid and not mid:
            raise Permanent("VK message has neither id nor conversation_message_id")
        return f"{peer}:c{cmid}" if cmid else f"{peer}:m{mid}"

    def ingest_vk_batch(self, messages: list[dict], cursor: dict, extra: dict) -> None:
        with self.tx():
            self.names(extra)
            for m in messages:
                self.ingest("vk", self.vk_key(m), m, int(m["peer_id"]))
            self.set("vk_cursor", cursor)
            self.set("vk_last_ok", time.time())

    def add_job(self, key: str, kind: str, payload: dict, *, peer: int = 0,
                lane: str = "control", priority: int = 0) -> int:
        row = self.db.execute("SELECT id FROM jobs WHERE key=?", (key,)).fetchone()
        if row:
            return row[0]
        rid = None
        if kind == "vk_send":
            for _ in range(100):
                rid = secrets.randbelow(2**31 - 1) + 1
                if not self.db.execute("SELECT 1 FROM echoes WHERE random_id=?", (rid,)).fetchone() and not self.db.execute("SELECT 1 FROM jobs WHERE random_id=?", (rid,)).fetchone():
                    break
            else:
                raise Permanent("Unable to allocate a unique random_id")
        now = time.time()
        cur = self.db.execute("INSERT INTO jobs(key,kind,peer,lane,priority,payload,created,updated,random_id) VALUES(?,?,?,?,?,?,?,?,?)", (key, kind, peer, lane, priority, dumps(payload), now, now, rid))
        if rid:
            self.db.execute("INSERT INTO echoes(peer,random_id,created) VALUES(?,?,?)", (peer, rid, now))
        return cur.lastrowid

    def recover(self) -> None:
        with self.tx():
            self.db.execute("UPDATE jobs SET state=CASE WHEN effect=1 THEN 'uncertain' ELSE 'pending' END, error='Process interrupted', updated=? WHERE state='running'", (time.time(),))
            self.db.execute("UPDATE routes SET topic_state='uncertain' WHERE topic_state='creating'")

    def claim(self):
        with self.tx():
            row = self.db.execute("""SELECT j.* FROM jobs j WHERE j.state='pending' AND j.next_at<=?
              AND NOT (j.lane LIKE 'archive_%' AND EXISTS(SELECT 1 FROM routes r WHERE r.peer=j.peer AND r.muted=1))
              AND NOT EXISTS(SELECT 1 FROM jobs older WHERE older.peer=j.peer AND older.lane=j.lane
                AND older.id<j.id AND older.state IN ('pending','running','uncertain'))
              ORDER BY j.priority,j.id LIMIT 1""", (time.time(),)).fetchone()
            if row:
                self.db.execute("UPDATE jobs SET state='running',tries=tries+1,updated=? WHERE id=?", (time.time(), row["id"]))
            return dict(row) if row else None

    def effect(self, jid: int, value: bool = True) -> None:
        self.db.execute("UPDATE jobs SET effect=?,updated=? WHERE id=?", (int(value), time.time(), jid))

    def payload(self, jid: int, data: dict) -> None:
        self.db.execute("UPDATE jobs SET payload=?,updated=? WHERE id=?", (dumps(data), time.time(), jid))

    def finish(self, jid: int, state: str = "done", error: str = "", delay: float = 0) -> None:
        self.db.execute("UPDATE jobs SET state=?,effect=0,error=?,next_at=?,updated=? WHERE id=?", (state, error[:500], time.time() + delay, time.time(), jid))

    def link(self, tg: int, peer: int, mid: int, cmid: int, incoming: bool = False) -> None:
        self.db.execute("INSERT INTO links VALUES(?,?,?,?,?,?) ON CONFLICT(tg) DO UPDATE SET mid=excluded.mid,cmid=excluded.cmid", (tg, peer, mid, cmid, int(incoming), time.time()))

    def vk_link(self, peer: int, *, mid: int = 0, cmid: int = 0):
        return self.db.execute("SELECT * FROM links WHERE peer=? AND ((? > 0 AND cmid=?) OR (? > 0 AND mid=?)) ORDER BY tg LIMIT 1", (peer, cmid, cmid, mid, mid)).fetchone()

    def tg_link(self, tg: int, peer: int):
        return self.db.execute("SELECT * FROM links WHERE tg=? AND peer=?", (tg, peer)).fetchone()

    def echo(self, message: dict) -> bool:
        peer, rid, mid = message["peer_id"], message.get("random_id", 0), message.get("id", 0)
        return bool(self.db.execute("SELECT 1 FROM echoes WHERE peer=? AND ((?>0 AND random_id=?) OR (?>0 AND mid=?))", (peer, rid, rid, mid, mid)).fetchone())

    def retry(self, jid: int, force: bool = False) -> str:
        row = self.db.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()
        if not row or row["state"] not in {"dead", "uncertain"}:
            raise Permanent("No failed job with this ID")
        if row["state"] == "uncertain" and not force:
            raise Permanent("Result unknown: check delivery first, then /retry_dlq ID force")
        with self.tx():
            self.db.execute("UPDATE jobs SET state='pending',effect=0,tries=0,error='',next_at=0,created=?,updated=? WHERE id=?", (time.time(), time.time(), jid))
            # Keep random_id and already-completed parts unchanged.
        return "Retry scheduled; already-delivered parts are not replayed."

    def clear_dlq(self) -> int:
        # Dismiss DLQ jobs but retain audit rows until normal cleanup.
        with self.tx():
            count = self.db.execute(
                "SELECT COUNT(*) FROM jobs WHERE state IN ('dead','uncertain')"
            ).fetchone()[0]
            self.db.execute(
                "UPDATE jobs SET state='suppressed',effect=0,updated=? "
                "WHERE state IN ('dead','uncertain')",
                (time.time(),),
            )
        return int(count)

    def backlog(self) -> int:
        return self.db.execute("SELECT COUNT(*) FROM jobs WHERE state IN ('pending','running')").fetchone()[0] + self.db.execute("SELECT COUNT(*) FROM inbox WHERE processed=0").fetchone()[0]

    def status(self) -> str:
        counts = ", ".join(f"{r[0]}={r[1]}" for r in self.db.execute("SELECT state,COUNT(*) FROM jobs GROUP BY state"))
        oldest = self.db.execute("SELECT MIN(created) FROM jobs WHERE state IN ('pending','running')").fetchone()[0]
        age = int(time.time() - oldest) if oldest else 0
        return f"Jobs: {counts or 'none'}\nBacklog: {self.backlog()}\nOldest queued: {age}s"

    def cleanup(self, days: int) -> None:
        cutoff = time.time() - days * 86400
        with self.tx():
            self.db.execute("DELETE FROM jobs WHERE state IN ('done','suppressed') AND updated<?", (cutoff,))
            # Keep compact inbox dedup tombstones; raw payloads are not retained forever.
            self.db.execute("UPDATE inbox SET payload='{}' WHERE processed=1 AND created<? AND payload!='{}'", (cutoff,))
            self.db.execute("DELETE FROM archive WHERE queued=1 AND peer IN (SELECT peer FROM imports WHERE state='done' AND updated<?)", (cutoff,))
        self.db.execute("PRAGMA wal_checkpoint(PASSIVE)")
