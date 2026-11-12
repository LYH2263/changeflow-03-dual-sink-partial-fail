from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any

from app.modules.dual_sink_partial_fail import (
    OUTCOME_BOTH_COMMITTED,
    OUTCOME_PARTIAL_SHADOW_ISOLATED,
    OUTCOME_ROLLED_BACK_BOTH,
)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def record_attempt(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    stream_id: int,
    seq: int,
    entity_key: str,
    op: str,
    policy: str,
    outcome: str,
    main_committed: bool,
    shadow_committed: bool,
    cursor_advanced: bool,
    reason: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO dual_write_attempts(
            run_id, stream_id, seq, entity_key, op, policy, outcome,
            main_committed, shadow_committed, cursor_advanced, reason, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            run_id,
            stream_id,
            seq,
            entity_key,
            op,
            policy,
            outcome,
            1 if main_committed else 0,
            1 if shadow_committed else 0,
            1 if cursor_advanced else 0,
            reason,
            _now(),
        ),
    )


def list_attempts(
    conn: sqlite3.Connection, run_id: int | None = None, limit: int = 500
) -> list[dict[str, Any]]:
    if run_id is None:
        rows = conn.execute(
            """
            SELECT id, run_id, stream_id, seq, entity_key, op, policy, outcome,
                   main_committed, shadow_committed, cursor_advanced, reason, created_at
            FROM dual_write_attempts ORDER BY id DESC LIMIT ?
            """,
            (limit,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT id, run_id, stream_id, seq, entity_key, op, policy, outcome,
                   main_committed, shadow_committed, cursor_advanced, reason, created_at
            FROM dual_write_attempts WHERE run_id=? ORDER BY id
            """,
            (run_id,),
        ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["main_committed"] = bool(item["main_committed"])
        item["shadow_committed"] = bool(item["shadow_committed"])
        item["cursor_advanced"] = bool(item["cursor_advanced"])
        out.append(item)
    return out


def summarize_run(conn: sqlite3.Connection, run_id: int) -> dict[str, Any]:
    """计数唯一来源：本次 run 的双写尝试表。报告与界面都从这里派生。"""
    rows = conn.execute(
        """
        SELECT outcome, COUNT(*) AS c, SUM(main_committed) AS main_c,
               SUM(shadow_committed) AS shadow_c
        FROM dual_write_attempts WHERE run_id=? GROUP BY outcome
        """,
        (run_id,),
    ).fetchall()
    by_outcome = {row["outcome"]: int(row["c"]) for row in rows}
    attempts = sum(by_outcome.values())
    failed_seqs = conn.execute(
        """
        SELECT seq FROM dual_write_attempts
        WHERE run_id=? AND outcome<>? ORDER BY seq LIMIT 1
        """,
        (run_id, OUTCOME_BOTH_COMMITTED),
    ).fetchone()
    return {
        "attempts": attempts,
        "both_success": by_outcome.get(OUTCOME_BOTH_COMMITTED, 0),
        "partial_failures": by_outcome.get(OUTCOME_PARTIAL_SHADOW_ISOLATED, 0),
        "rollbacks": by_outcome.get(OUTCOME_ROLLED_BACK_BOTH, 0),
        "main_committed": int(
            sum(int(row["main_c"] or 0) for row in rows)
        ),
        "shadow_committed": int(
            sum(int(row["shadow_c"] or 0) for row in rows)
        ),
        "failed_seqs": [
            int(r["seq"])
            for r in conn.execute(
                "SELECT seq FROM dual_write_attempts WHERE run_id=? AND outcome<>? ORDER BY seq",
                (run_id, OUTCOME_BOTH_COMMITTED),
            ).fetchall()
        ],
        "first_failed_seq": int(failed_seqs["seq"]) if failed_seqs else None,
    }
