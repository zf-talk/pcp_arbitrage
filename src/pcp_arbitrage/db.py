"""SQLite persistence for triplets and opportunity snapshots."""

from __future__ import annotations

import datetime
import math
import sqlite3
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pcp_arbitrage.models import Triplet
    from pcp_arbitrage.opportunity_dashboard import _Row


def _ensure_column(con: sqlite3.Connection, table: str, name: str, decl: str) -> None:
    cur = con.execute(f"PRAGMA table_info({table})")
    if name in {row[1] for row in cur.fetchall()}:
        return
    con.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")


def coalesce_per_leg_fees(
    fee_usdt: float | None,
    call_fee: float | None,
    put_fee: float | None,
    fut_fee: float | None,
) -> tuple[float | None, float | None, float | None]:
    """
    总手续费满足 2×(C+P+F)。仅当「恰好缺一条腿」时用代数补全；不做三腿均分（避免 C=P=F 误导）。
    缺两条及以上或无法从 fee 推算时返回 (None,…)，前端只显示总手续费。
    """

    def _fin(x: float | None) -> bool:
        return x is not None and math.isfinite(float(x))

    if _fin(call_fee) and _fin(put_fee) and _fin(fut_fee):
        return (float(call_fee), float(put_fee), float(fut_fee))

    if fee_usdt is None or not math.isfinite(float(fee_usdt)):
        return (None, None, None)
    half = float(fee_usdt) / 2.0

    c = float(call_fee) if _fin(call_fee) else None
    p = float(put_fee) if _fin(put_fee) else None
    f = float(fut_fee) if _fin(fut_fee) else None
    n_miss = sum(1 for x in (c, p, f) if x is None)
    if n_miss != 1:
        return (None, None, None)
    s = (c or 0.0) + (p or 0.0) + (f or 0.0)
    rem = max(0.0, half - s)
    if c is None:
        c = rem
    elif p is None:
        p = rem
    else:
        f = rem
    return (c, p, f)


def init_db(path: str) -> None:
    """Create tables if not exist (idempotent)."""
    con = sqlite3.connect(path)
    try:
        con.executescript("""
            CREATE TABLE IF NOT EXISTS triplets (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                updated_at  TEXT NOT NULL,
                exchange    TEXT NOT NULL,
                settle_type TEXT NOT NULL DEFAULT '',
                symbol      TEXT NOT NULL,
                expiry      TEXT NOT NULL,
                strike      REAL NOT NULL,
                call_id     TEXT NOT NULL,
                put_id      TEXT NOT NULL,
                future_id   TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS opportunity_snapshots (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_utc TEXT NOT NULL,
                exchange     TEXT NOT NULL,
                contract     TEXT NOT NULL,
                direction    TEXT NOT NULL,
                gross_usdt   REAL NOT NULL,
                fee_usdt     REAL NOT NULL,
                net_usdt     REAL NOT NULL,
                ann_pct      REAL NOT NULL,
                ann_pct_max  REAL NOT NULL,
                active       INTEGER NOT NULL,
                duration_sec REAL
            );
            CREATE TABLE IF NOT EXISTS opportunity_current (
                exchange     TEXT NOT NULL,
                contract     TEXT NOT NULL,
                direction    TEXT NOT NULL,
                updated_at   TEXT NOT NULL,
                gross_usdt   REAL NOT NULL,
                fee_usdt     REAL NOT NULL,
                net_usdt     REAL NOT NULL,
                tradeable    REAL NOT NULL DEFAULT 0,
                ann_pct      REAL NOT NULL,
                ann_pct_max  REAL NOT NULL,
                days_to_exp  REAL NOT NULL DEFAULT 0,
                active       INTEGER NOT NULL,
                duration_sec REAL,
                PRIMARY KEY (exchange, contract, direction)
            );
            CREATE TABLE IF NOT EXISTS opportunity_sessions (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                exchange      TEXT NOT NULL,
                contract      TEXT NOT NULL,
                direction     TEXT NOT NULL,
                started_utc   TEXT NOT NULL,
                ended_utc     TEXT,
                duration_sec  REAL,
                gross_usdt    REAL,
                fee_usdt      REAL,
                net_usdt      REAL,
                tradeable     REAL,
                ann_pct       REAL,
                ann_pct_max   REAL,
                days_to_exp   REAL
            );
            CREATE INDEX IF NOT EXISTS idx_opportunity_sessions_ended
            ON opportunity_sessions (ended_utc DESC);
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER,
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                expiry TEXT NOT NULL,
                strike REAL NOT NULL,
                direction TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                realized_pnl_usdt REAL,
                opened_at TEXT NOT NULL,
                closed_at TEXT,
                current_mark_usdt REAL,
                last_updated TEXT
            );
            CREATE TABLE IF NOT EXISTS app_config (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS orders (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                position_id       INTEGER NOT NULL REFERENCES positions(id),
                signal_id         INTEGER,
                inst_id           TEXT,
                leg               TEXT NOT NULL,
                action            TEXT NOT NULL,
                order_type        TEXT NOT NULL DEFAULT 'limit',
                side              TEXT NOT NULL,
                qty               REAL NOT NULL,
                limit_px          REAL NOT NULL,
                status            TEXT NOT NULL DEFAULT 'pending',
                exchange_order_id TEXT,
                filled_px         REAL,
                fee_type          TEXT,
                actual_fee_usdt   REAL,
                submitted_at      TEXT NOT NULL,
                filled_at         TEXT
            );
        """)
        _ensure_column(con, "opportunity_snapshots", "duration_sec", "REAL")
        _ensure_column(con, "opportunity_current", "duration_sec", "REAL")
        _ensure_column(con, "opportunity_current", "strike", "REAL")
        _ensure_column(con, "opportunity_current", "call_px_usdt", "REAL")
        _ensure_column(con, "opportunity_current", "put_px_usdt", "REAL")
        _ensure_column(con, "opportunity_current", "fut_px_usdt", "REAL")
        _ensure_column(con, "opportunity_current", "last_active_eval", "REAL")
        _ensure_column(con, "opportunity_current", "expected_max_usdt", "REAL")
        _ensure_column(con, "opportunity_current", "call_fee_usdt", "REAL")
        _ensure_column(con, "opportunity_current", "put_fee_usdt", "REAL")
        _ensure_column(con, "opportunity_current", "fut_fee_usdt", "REAL")
        _ensure_column(con, "opportunity_current", "index_price_usdt", "REAL")
        _ensure_column(con, "opportunity_sessions", "expected_max_usdt", "REAL")
        _ensure_column(con, "positions", "current_mark_usdt", "REAL")
        _ensure_column(con, "positions", "last_updated", "TEXT")
        _ensure_column(con, "orders", "actual_fee_usdt", "REAL")
        _ensure_column(con, "orders", "actual_fee", "REAL")
        _ensure_column(con, "orders", "fee_ccy", "TEXT")
        _ensure_column(con, "orders", "filled_qty", "REAL")
        _ensure_column(con, "positions", "call_inst_id",   "TEXT")
        _ensure_column(con, "positions", "put_inst_id",    "TEXT")
        _ensure_column(con, "positions", "future_inst_id", "TEXT")
        _ensure_column(con, "positions", "last_error", "TEXT")
        con.commit()
        # Backfill expected_max_usdt for rows where it is NULL
        con.execute(
            "UPDATE opportunity_current SET expected_max_usdt = net_usdt * tradeable "
            "WHERE expected_max_usdt IS NULL AND tradeable IS NOT NULL AND tradeable != 0"
        )
        con.execute(
            "UPDATE opportunity_sessions SET expected_max_usdt = net_usdt * tradeable "
            "WHERE expected_max_usdt IS NULL AND tradeable IS NOT NULL AND tradeable != 0"
        )
        # Backfill call/put/future_inst_id for existing positions that have orders
        con.execute(
            """
            UPDATE positions SET
                call_inst_id   = (SELECT inst_id FROM orders
                                   WHERE position_id=positions.id AND leg='call'   AND action='open' LIMIT 1),
                put_inst_id    = (SELECT inst_id FROM orders
                                   WHERE position_id=positions.id AND leg='put'    AND action='open' LIMIT 1),
                future_inst_id = (SELECT inst_id FROM orders
                                   WHERE position_id=positions.id AND leg='future' AND action='open' LIMIT 1)
            WHERE call_inst_id IS NULL
            """
        )
        con.commit()
    finally:
        con.close()


def upsert_triplets(path: str, exchange: str, triplets: list[Triplet], settle_type: str) -> None:
    """Replace all triplets for this exchange (DELETE + bulk INSERT in one transaction)."""
    now = datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    rows = [
        (now, exchange, settle_type, t.symbol, t.expiry, t.strike, t.call_id, t.put_id, t.future_id)
        for t in triplets
    ]
    con = sqlite3.connect(path)
    try:
        with con:
            con.execute("DELETE FROM triplets WHERE exchange = ?", (exchange,))
            con.executemany(
                "INSERT INTO triplets "
                "(updated_at, exchange, settle_type, symbol, expiry, strike, call_id, put_id, future_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
    finally:
        con.close()


def flush_opportunities_sqlite(path: str, rows: list[_Row]) -> None:
    """Append current opportunity rows to opportunity_snapshots."""
    if not rows:
        return
    from pcp_arbitrage.opportunity_dashboard import duration_sec_for_storage

    tnow = time.time()
    now = datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    data = [
        (
            now,
            r.exchange,
            r.label,
            r.direction_cn,
            r.gross,
            r.fee,
            r.net,
            r.ann_pct,
            r.max_ann_pct,
            1 if r.active else 0,
            duration_sec_for_storage(r, tnow),
        )
        for r in rows
    ]
    con = sqlite3.connect(path)
    try:
        with con:
            con.executemany(
                "INSERT INTO opportunity_snapshots "
                "(snapshot_utc, exchange, contract, direction, gross_usdt, fee_usdt, net_usdt, "
                "ann_pct, ann_pct_max, active, duration_sec) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                data,
            )
    finally:
        con.close()


def upsert_opportunity_current(path: str, rows: "list[_Row]") -> None:
    """Upsert current opportunity state into opportunity_current (one row per key)."""
    if not rows:
        return
    from pcp_arbitrage.opportunity_dashboard import duration_sec_for_storage

    tnow = time.time()
    now = datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    data = [
        (
            r.exchange,
            r.label,
            r.direction_cn,
            now,
            r.gross,
            r.fee,
            r.net,
            r.tradeable,
            r.ann_pct,
            r.max_ann_pct,
            r.days_to_expiry,
            1 if r.active else 0,
            duration_sec_for_storage(r, tnow),
            r.strike,
            r.call_px_usdt,
            r.put_px_usdt,
            r.fut_px_usdt,
            r.last_active_eval,
            r.net * r.tradeable if r.tradeable is not None else None,
            r.call_fee,
            r.put_fee,
            r.fut_fee,
            r.index_price_usdt,
        )
        for r in rows
    ]
    con = sqlite3.connect(path)
    try:
        with con:
            con.executemany(
                "INSERT INTO opportunity_current "
                "(exchange, contract, direction, updated_at, gross_usdt, fee_usdt, net_usdt, "
                " tradeable, ann_pct, ann_pct_max, days_to_exp, active, duration_sec, "
                " strike, call_px_usdt, put_px_usdt, fut_px_usdt, last_active_eval, expected_max_usdt, "
                " call_fee_usdt, put_fee_usdt, fut_fee_usdt, index_price_usdt) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(exchange, contract, direction) DO UPDATE SET "
                "  updated_at=excluded.updated_at, gross_usdt=excluded.gross_usdt, "
                "  fee_usdt=excluded.fee_usdt, net_usdt=excluded.net_usdt, "
                "  tradeable=excluded.tradeable, ann_pct=excluded.ann_pct, "
                "  ann_pct_max=excluded.ann_pct_max, days_to_exp=excluded.days_to_exp, "
                "  active=excluded.active, duration_sec=excluded.duration_sec, "
                "  strike=excluded.strike, call_px_usdt=excluded.call_px_usdt, "
                "  put_px_usdt=excluded.put_px_usdt, fut_px_usdt=excluded.fut_px_usdt, "
                "  last_active_eval=excluded.last_active_eval, "
                "  expected_max_usdt=excluded.expected_max_usdt, "
                "  call_fee_usdt=excluded.call_fee_usdt, put_fee_usdt=excluded.put_fee_usdt, "
                "  fut_fee_usdt=excluded.fut_fee_usdt, "
                "  index_price_usdt=excluded.index_price_usdt",
                data,
            )
    finally:
        con.close()


def load_opportunity_current(path: str) -> list[dict]:
    """Load all rows from opportunity_current; returns list of dicts."""
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    try:
        cur = con.execute(
            "SELECT exchange, contract, direction, updated_at, gross_usdt, fee_usdt, "
            "net_usdt, tradeable, ann_pct, ann_pct_max, days_to_exp, active, duration_sec, "
            "strike, call_px_usdt, put_px_usdt, fut_px_usdt, last_active_eval, expected_max_usdt, "
            "call_fee_usdt, put_fee_usdt, fut_fee_usdt, index_price_usdt "
            "FROM opportunity_current"
        )
        return [dict(r) for r in cur.fetchall()]
    except sqlite3.OperationalError:
        return []
    finally:
        con.close()


def _utc_now_iso() -> str:
    return datetime.datetime.utcnow().isoformat(timespec="milliseconds") + "Z"


_POSITION_ERROR_MAX_LEN = 8000


def _truncate_position_error(message: str, max_len: int = _POSITION_ERROR_MAX_LEN) -> str:
    s = (message or "").strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 24] + "\n…(已截断)"


def insert_opportunity_session(
    path: str,
    *,
    exchange: str,
    contract: str,
    direction: str,
    started_utc: str | None = None,
) -> int:
    """Open a new signal session; returns new row id."""
    started = started_utc or _utc_now_iso()
    con = sqlite3.connect(path)
    try:
        with con:
            cur = con.execute(
                "INSERT INTO opportunity_sessions "
                "(exchange, contract, direction, started_utc) VALUES (?,?,?,?)",
                (exchange, contract, direction, started),
            )
            return int(cur.lastrowid)
    finally:
        con.close()


def close_opportunity_session(
    path: str,
    session_id: int,
    *,
    ended_utc: str | None = None,
    duration_sec: float | None = None,
    gross_usdt: float,
    fee_usdt: float,
    net_usdt: float,
    tradeable: float,
    ann_pct: float,
    ann_pct_max: float,
    days_to_exp: float,
) -> None:
    ended = ended_utc or _utc_now_iso()
    expected_max = net_usdt * tradeable if tradeable is not None else None
    con = sqlite3.connect(path)
    try:
        with con:
            con.execute(
                "UPDATE opportunity_sessions SET "
                "ended_utc=?, duration_sec=?, gross_usdt=?, fee_usdt=?, net_usdt=?, "
                "tradeable=?, ann_pct=?, ann_pct_max=?, days_to_exp=?, expected_max_usdt=? "
                "WHERE id=?",
                (
                    ended,
                    duration_sec,
                    gross_usdt,
                    fee_usdt,
                    net_usdt,
                    tradeable,
                    ann_pct,
                    ann_pct_max,
                    days_to_exp,
                    expected_max,
                    session_id,
                ),
            )
    finally:
        con.close()


def close_open_opportunity_sessions(path: str, *, ended_utc: str | None = None) -> tuple[int, str]:
    """Set ended_utc on any session still open (e.g. process restart).

    duration_sec is set to the last-known value from opportunity_current rather than NULL,
    so crash-interrupted sessions retain their pre-crash duration (downtime excluded).

    Returns (rows_updated, ended_utc_iso) so restore can reopen rows still active in opportunity_current.
    """
    ended = ended_utc or _utc_now_iso()
    con = sqlite3.connect(path)
    try:
        with con:
            rows = con.execute(
                "SELECT s.id, s.exchange, s.contract, s.direction "
                "FROM opportunity_sessions s WHERE s.ended_utc IS NULL"
            ).fetchall()
            for sid, exchange, contract, direction in rows:
                cur_row = con.execute(
                    "SELECT duration_sec FROM opportunity_current "
                    "WHERE exchange=? AND contract=? AND direction=?",
                    (exchange, contract, direction),
                ).fetchone()
                last_known_dur = float(cur_row[0]) if cur_row and cur_row[0] is not None else None
                con.execute(
                    "UPDATE opportunity_sessions SET ended_utc=?, duration_sec=? WHERE id=?",
                    (ended, last_known_dur, sid),
                )
            return len(rows), ended
    finally:
        con.close()


def write_heartbeat(path: str, *, ts: str | None = None) -> None:
    """Upsert the last-seen heartbeat timestamp into app_config (called every ~1 s)."""
    now = ts or _utc_now_iso()
    con = sqlite3.connect(path)
    try:
        with con:
            con.execute(
                "INSERT INTO app_config (key, value) VALUES ('last_seen_utc', ?)"
                " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (now,),
            )
    finally:
        con.close()


def read_last_heartbeat(path: str) -> str | None:
    """Return the last heartbeat timestamp from app_config, or None if absent."""
    con = sqlite3.connect(path)
    try:
        row = con.execute(
            "SELECT value FROM app_config WHERE key='last_seen_utc'"
        ).fetchone()
        return row[0] if row else None
    finally:
        con.close()


def reopen_last_session_if_mass_closed(
    path: str,
    *,
    exchange: str,
    contract: str,
    direction: str,
    mass_close_ended_utc: str,
) -> int | None:
    """If newest session for this key was just bulk-closed at mass_close_ended_utc, reopen it."""
    con = sqlite3.connect(path)
    try:
        cur = con.execute(
            "SELECT id, ended_utc FROM opportunity_sessions "
            "WHERE exchange=? AND contract=? AND direction=? ORDER BY id DESC LIMIT 1",
            (exchange, contract, direction),
        )
        row = cur.fetchone()
        if row is None:
            return None
        sid, eu = int(row[0]), row[1]
        if eu != mass_close_ended_utc:
            return None
        with con:
            con.execute(
                "UPDATE opportunity_sessions SET ended_utc=NULL, duration_sec=NULL WHERE id=?",
                (sid,),
            )
        return sid
    finally:
        con.close()


def find_open_opportunity_session_id(
    path: str,
    *,
    exchange: str,
    contract: str,
    direction: str,
) -> int | None:
    """Return id of an still-open session for this key, if any."""
    con = sqlite3.connect(path)
    try:
        cur = con.execute(
            "SELECT id FROM opportunity_sessions "
            "WHERE exchange=? AND contract=? AND direction=? AND ended_utc IS NULL LIMIT 1",
            (exchange, contract, direction),
        )
        row = cur.fetchone()
        return int(row[0]) if row else None
    except sqlite3.OperationalError:
        return None
    finally:
        con.close()


def aggregate_opportunity_sessions_stats(path: str) -> dict[str, dict[str, float | int]]:
    """Sum metrics from ``opportunity_sessions`` (not live snapshot).

    - ``all``: only **closed** sessions with ``gross_usdt`` filled (typical completed signals).
    - ``today``: among sessions whose **local start day** is today, each distinct opportunity
      ``(exchange, contract, direction)`` is counted **once**; its contribution is
      ``MAX(expected_max_usdt)`` that day (then summed per direction / overall).
    """
    out: dict[str, dict[str, float | int]] = {
        "all": {
            "closed_sessions": 0,
            "avg_ann_pct": 0.0,
            "max_ann_pct": 0.0,
            "avg_duration_sec": 0.0,
            "sum_expected_max": 0.0,
        },
        "today": {
            "fwd_count": 0,
            "rev_count": 0,
            "total_count": 0,
            "sum_max_fwd": 0.0,
            "sum_max_rev": 0.0,
            "sum_max_total": 0.0,
        },
    }
    con = sqlite3.connect(path)
    try:
        cur = con.execute(
            """
            SELECT COUNT(*),
                   COALESCE(AVG(ann_pct), 0),
                   COALESCE(MAX(ann_pct), 0),
                   COALESCE(AVG(duration_sec), 0),
                   COALESCE(SUM(expected_max_usdt), 0)
            FROM opportunity_sessions
            WHERE ended_utc IS NOT NULL AND gross_usdt IS NOT NULL
            """
        )
        row = cur.fetchone()
        if row:
            out["all"]["closed_sessions"] = int(row[0])
            out["all"]["avg_ann_pct"] = float(row[1])
            out["all"]["max_ann_pct"] = float(row[2])
            out["all"]["avg_duration_sec"] = float(row[3])
            out["all"]["sum_expected_max"] = float(row[4])

        cur = con.execute(
            """
            SELECT direction,
                   COUNT(*),
                   COALESCE(SUM(mx), 0)
            FROM (
                SELECT exchange, contract, direction,
                       MAX(COALESCE(expected_max_usdt, 0)) AS mx
                FROM opportunity_sessions
                WHERE date(replace(started_utc, '+00:00Z', 'Z'), 'localtime') = date('now', 'localtime')
                GROUP BY exchange, contract, direction
            ) AS u
            GROUP BY direction
            """
        )
        fwd_c, rev_c = 0, 0
        fwd_m, rev_m = 0.0, 0.0
        for r in cur.fetchall():
            d, c, m = str(r[0]), int(r[1]), float(r[2])
            if d == "正向":
                fwd_c, fwd_m = c, m
            elif d == "反向":
                rev_c, rev_m = c, m
        cur = con.execute(
            """
            SELECT COUNT(*), COALESCE(SUM(mx), 0)
            FROM (
                SELECT MAX(COALESCE(expected_max_usdt, 0)) AS mx
                FROM opportunity_sessions
                WHERE date(replace(started_utc, '+00:00Z', 'Z'), 'localtime') = date('now', 'localtime')
                GROUP BY exchange, contract, direction
            ) AS u
            """
        )
        trow = cur.fetchone()
        tot_c, tot_m = (int(trow[0]), float(trow[1])) if trow else (0, 0.0)
        out["today"].update(
            {
                "fwd_count": fwd_c,
                "rev_count": rev_c,
                "total_count": tot_c,
                "sum_max_fwd": fwd_m,
                "sum_max_rev": rev_m,
                "sum_max_total": tot_m,
            }
        )
    finally:
        con.close()
    return out


def daily_expected_max_series_local(path: str, *, days: int = 30) -> list[dict[str, float | str]]:
    """Last ``days`` local calendar days (inclusive), one point per day.

    Uses the **host OS timezone** (SQLite ``localtime``). For each day, only **distinct**
    opportunities ``(exchange, contract, direction)`` that **started** that local day are
    included; each contributes ``MAX(expected_max_usdt)`` among its rows that day, then
    days are summed. Missing days are included with sum 0.
    """
    from datetime import datetime, timedelta

    today = datetime.now().astimezone().date()
    start = today - timedelta(days=days - 1)
    start_s, end_s = start.isoformat(), today.isoformat()
    con = sqlite3.connect(path)
    try:
        cur = con.execute(
            """
            SELECT d, COALESCE(SUM(mx), 0)
            FROM (
                SELECT date(replace(started_utc, '+00:00Z', 'Z'), 'localtime') AS d,
                       exchange, contract, direction,
                       MAX(COALESCE(expected_max_usdt, 0)) AS mx
                FROM opportunity_sessions
                WHERE date(replace(started_utc, '+00:00Z', 'Z'), 'localtime') >= ? AND date(replace(started_utc, '+00:00Z', 'Z'), 'localtime') <= ?
                GROUP BY date(replace(started_utc, '+00:00Z', 'Z'), 'localtime'), exchange, contract, direction
            ) AS u
            GROUP BY d
            """,
            (start_s, end_s),
        )
        by_day = {str(row[0]): float(row[1]) for row in cur.fetchall()}
    finally:
        con.close()
    out: list[dict[str, float | str]] = []
    d = start
    while d <= today:
        ds = d.isoformat()
        out.append({"day": ds, "sum_expected_max": by_day.get(ds, 0.0)})
        d += timedelta(days=1)
    return out


def list_opportunity_sessions_history(path: str, *, limit: int = 500) -> list[dict]:
    """Open sessions first (ended_utc NULL), then closed rows by ended_utc desc.

    Within the open group, newest started_utc first; within closed, newest ended_utc first.
    """
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    try:
        cur = con.execute(
            "SELECT id, exchange, contract, direction, started_utc, ended_utc, duration_sec, "
            "gross_usdt, fee_usdt, net_usdt, tradeable, ann_pct, ann_pct_max, days_to_exp "
            "FROM opportunity_sessions "
            "ORDER BY (ended_utc IS NOT NULL) ASC, "
            "COALESCE(ended_utc, started_utc) DESC LIMIT ?",
            (max(1, min(limit, 10_000)),),
        )
        return [dict(r) for r in cur.fetchall()]
    except sqlite3.OperationalError:
        return []
    finally:
        con.close()


def history_peak_ann_session_by_key(
    path: str,
) -> dict[tuple[str, str, str], dict[str, float | None]]:
    """Per (exchange, contract, direction), session with highest ``ann_pct`` in ``opportunity_sessions``.

    Tie-break: larger ``id``. Only rows with non-NULL ``ann_pct`` are considered (typically closed
    sessions). Returns ``{ (exchange, contract, direction): {"ann_pct", "duration_sec"} }``.
    """
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    try:
        cur = con.execute(
            """
            WITH ranked AS (
              SELECT exchange, contract, direction, ann_pct, duration_sec,
                ROW_NUMBER() OVER (
                  PARTITION BY exchange, contract, direction
                  ORDER BY CASE WHEN ann_pct IS NULL THEN 1 ELSE 0 END ASC,
                           ann_pct DESC,
                           id DESC
                ) AS rn
              FROM opportunity_sessions
            )
            SELECT exchange, contract, direction, ann_pct, duration_sec
            FROM ranked WHERE rn = 1 AND ann_pct IS NOT NULL
            """
        )
        out: dict[tuple[str, str, str], dict[str, float | None]] = {}
        for r in cur.fetchall():
            key = (str(r["exchange"]), str(r["contract"]), str(r["direction"]))
            ap = r["ann_pct"]
            ds = r["duration_sec"]
            out[key] = {
                "ann_pct": float(ap),
                "duration_sec": float(ds) if ds is not None else None,
            }
        return out
    except sqlite3.OperationalError:
        return {}
    finally:
        con.close()


# ---------------------------------------------------------------------------
# positions / orders helpers
# ---------------------------------------------------------------------------


def create_position(
    conn: sqlite3.Connection,
    *,
    signal_id: int | None,
    exchange: str,
    symbol: str,
    expiry: str,
    strike: float,
    direction: str,
    call_inst_id: str | None = None,
    put_inst_id: str | None = None,
    future_inst_id: str | None = None,
    opened_at: str | None = None,
) -> int:
    """Insert a new position row and return its id."""
    opened = opened_at or _utc_now_iso()
    cur = conn.execute(
        "INSERT INTO positions "
        "(signal_id, exchange, symbol, expiry, strike, direction, status, "
        " call_inst_id, put_inst_id, future_inst_id, opened_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (signal_id, exchange, symbol, expiry, strike, direction, "opening",
         call_inst_id, put_inst_id, future_inst_id, opened),
    )
    return int(cur.lastrowid)


def create_order(
    conn: sqlite3.Connection,
    *,
    signal_id: int | None = None,
    position_id: int,
    inst_id: str | None = None,
    leg: str,
    action: str,
    order_type: str = "limit",
    side: str,
    limit_px: float,
    qty: float,
    submitted_at: str | None = None,
    exchange_order_id: str | None = None,
) -> int:
    """Insert a new order row and return its id."""
    submitted = submitted_at or _utc_now_iso()
    cur = conn.execute(
        "INSERT INTO orders "
        "(signal_id, position_id, inst_id, leg, action, order_type, side, limit_px, qty, submitted_at, exchange_order_id) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (signal_id, position_id, inst_id, leg, action, order_type, side, limit_px, qty, submitted, exchange_order_id),
    )
    return int(cur.lastrowid)


def update_order_status(
    conn: sqlite3.Connection,
    order_id: int,
    status: str,
    *,
    filled_px: float | None = None,
    filled_qty: float | None = None,
    fee_type: str | None = None,
    actual_fee: float | None = None,
    fee_ccy: str | None = None,
    filled_at: str | None = None,
    order_type: str | None = None,
) -> None:
    """Update mutable fields on an orders row."""
    if order_type is not None:
        conn.execute(
            "UPDATE orders SET status=?, filled_px=?, filled_qty=?, fee_type=?, "
            "actual_fee=?, fee_ccy=?, filled_at=?, order_type=? WHERE id=?",
            (status, filled_px, filled_qty, fee_type, actual_fee, fee_ccy, filled_at, order_type, order_id),
        )
    else:
        conn.execute(
            "UPDATE orders SET status=?, filled_px=?, filled_qty=?, fee_type=?, "
            "actual_fee=?, fee_ccy=?, filled_at=? WHERE id=?",
            (status, filled_px, filled_qty, fee_type, actual_fee, fee_ccy, filled_at, order_id),
        )


def update_position_status(
    conn: sqlite3.Connection,
    position_id: int,
    status: str,
    *,
    realized_pnl_usdt: float | None = None,
    closed_at: str | None = None,
    last_error: str | None = None,
) -> None:
    """Update mutable fields on a positions row.

    Clears last_error when status is open or closed. For partial_failed / failed,
    pass last_error to persist the reason (shown in dashboard).
    """
    if status in ("open", "closed"):
        conn.execute(
            "UPDATE positions SET status=?, realized_pnl_usdt=?, closed_at=?, last_error=NULL "
            "WHERE id=?",
            (status, realized_pnl_usdt, closed_at, position_id),
        )
    elif last_error is not None:
        conn.execute(
            "UPDATE positions SET status=?, realized_pnl_usdt=?, closed_at=?, last_error=? "
            "WHERE id=?",
            (
                status,
                realized_pnl_usdt,
                closed_at,
                _truncate_position_error(last_error),
                position_id,
            ),
        )
    else:
        conn.execute(
            "UPDATE positions SET status=?, realized_pnl_usdt=?, closed_at=? WHERE id=?",
            (status, realized_pnl_usdt, closed_at, position_id),
        )


def get_open_positions(conn: sqlite3.Connection) -> list[dict]:
    """Return all positions with status='open'."""
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT id, signal_id, exchange, symbol, expiry, strike, direction, "
        "status, realized_pnl_usdt, opened_at, closed_at, current_mark_usdt, last_updated, "
        "call_inst_id, put_inst_id, future_inst_id, last_error "
        "FROM positions WHERE status='open'"
    )
    return [dict(r) for r in cur.fetchall()]


def get_failed_positions(conn: sqlite3.Connection, limit: int = 50) -> list[dict]:
    """Return recent positions with failed/partial_failed status, newest first."""
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT id, signal_id, exchange, symbol, expiry, strike, direction, "
        "status, realized_pnl_usdt, opened_at, closed_at, current_mark_usdt, last_updated, "
        "last_error "
        "FROM positions WHERE status IN ('failed', 'partial_failed') "
        "ORDER BY id DESC LIMIT ?",
        (limit,),
    )
    return [dict(r) for r in cur.fetchall()]


def has_open_position(
    conn: sqlite3.Connection,
    exchange: str,
    symbol: str,
    expiry: str,
    strike: float,
    direction: str,
) -> bool:
    """Return True if there is a filled live position (status='open') for this key."""
    cur = conn.execute(
        "SELECT 1 FROM positions "
        "WHERE exchange=? AND symbol=? AND expiry=? AND strike=? AND direction=? AND status='open' "
        "LIMIT 1",
        (exchange, symbol, expiry, strike, direction),
    )
    return cur.fetchone() is not None


def blocking_entry_status(
    conn: sqlite3.Connection,
    exchange: str,
    symbol: str,
    expiry: str,
    strike: float,
    direction: str,
) -> str | None:
    """If new entry should be skipped, return status ('open'|'opening'|'closing'); else None.

    partial_failed / failed / closed do not block (可重新下单).
    """
    cur = conn.execute(
        "SELECT status FROM positions "
        "WHERE exchange=? AND symbol=? AND expiry=? AND strike=? AND direction=? "
        "AND status IN ('open', 'opening', 'closing') "
        "ORDER BY id DESC LIMIT 1",
        (exchange, symbol, expiry, strike, direction),
    )
    row = cur.fetchone()
    return str(row[0]) if row else None


def get_active_position_keys(path: str) -> set[tuple[str, str, str, float, str]]:
    """Return keys that still occupy the opportunity (open / in-flight / 平仓中).

    Excludes partial_failed so UI 与自动下单均可重试；不含 closed/failed。
    """
    try:
        conn = sqlite3.connect(path)
        try:
            cur = conn.execute(
                "SELECT exchange, symbol, expiry, strike, direction FROM positions "
                "WHERE status IN ('open', 'opening', 'closing')"
            )
            return {(r[0], r[1], r[2], float(r[3]), r[4]) for r in cur.fetchall()}
        finally:
            conn.close()
    except Exception:
        return set()


def update_position_mark(
    conn: sqlite3.Connection,
    position_id: int,
    current_mark_usdt: float,
    last_updated: str,
) -> None:
    """Update the current mark price and timestamp on a position row."""
    conn.execute(
        "UPDATE positions SET current_mark_usdt=?, last_updated=? WHERE id=?",
        (current_mark_usdt, last_updated, position_id),
    )


def get_position_orders(
    conn: sqlite3.Connection,
    position_id: int,
    action: str = "open",
) -> list[dict]:
    """Return orders for a position filtered by action ('open' or 'close')."""
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT id, position_id, inst_id, leg, action, order_type, side, exchange_order_id, "
        "limit_px, filled_px, qty, filled_qty, status, fee_type, actual_fee_usdt, actual_fee, fee_ccy, "
        "submitted_at, filled_at "
        "FROM orders WHERE position_id=? AND action=?",
        (position_id, action),
    )
    return [dict(r) for r in cur.fetchall()]


def get_session_ids_with_positions(path: str) -> dict[int, str]:
    """Return {signal_id: position_status} for positions linked to opportunity sessions."""
    con = sqlite3.connect(path)
    try:
        cur = con.execute(
            "SELECT signal_id, status FROM positions WHERE signal_id IS NOT NULL"
        )
        return {row[0]: row[1] for row in cur.fetchall()}
    except sqlite3.OperationalError:
        return {}
    finally:
        con.close()
