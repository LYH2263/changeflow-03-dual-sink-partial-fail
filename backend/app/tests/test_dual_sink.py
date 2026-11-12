"""双 Sink 扇出与部分失败策略的端到端测试（本地 SQLite + 夹具）。"""

from __future__ import annotations

import importlib

import pytest


@pytest.fixture()
def app_env(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    # config/db 在 import 时读取 DATA_DIR，需重载后再初始化种子。
    import app.config
    import app.db
    import app.seed

    importlib.reload(app.config)
    importlib.reload(app.db)
    importlib.reload(app.seed)

    from app.services import apply_service

    importlib.reload(apply_service)
    app.seed.init_db()
    yield


def _conn():
    from app.db import connect

    return connect()


def _set(key: str, value: str) -> None:
    from app.repositories import settings as settings_repo

    conn = _conn()
    settings_repo.put(conn, key, value)
    conn.commit()
    conn.close()


def _run(stream_id: int) -> dict:
    from app.services import apply_service

    return apply_service.run_apply(stream_id)


def test_dual_write_both_succeed(app_env):
    assert _run(1)["report"]["dual_write"]["both_success"] == 5


def test_shadow_failure_isolates_and_advances_cursor(app_env):
    _set("dual_sink_policy", "main_commit_isolate_shadow")
    _set("shadow_fail_seqs", "2")
    run = _run(2)
    report = run["report"]
    assert report["dual_write"]["attempts"] == 3
    assert report["dual_write"]["both_success"] == 2
    assert report["dual_write"]["partial_failures"] == 1
    assert report["dual_write"]["rollbacks"] == 0
    assert run["cursor_after"] == 3
    conn = _conn()
    try:
        main = {
            r["entity_key"]: r["last_seq"]
            for r in conn.execute("SELECT entity_key, last_seq FROM sink_records")
        }
        shadow = {
            r["entity_key"]: r["last_seq"]
            for r in conn.execute("SELECT entity_key, last_seq FROM sink_shadow_records")
        }
        assert main == {"INV-2001": 2, "INV-2002": 3}
        # seq 2 的旁路写入被隔离，旁路 Sink 中 INV-2001 仍停在 seq 1。
        assert shadow == {"INV-2001": 1, "INV-2002": 3}
        quarantine = conn.execute(
            "SELECT seq, reason FROM quarantine WHERE reason='shadow_sink_failed'"
        ).fetchall()
        assert [int(r["seq"]) for r in quarantine] == [2]
    finally:
        conn.close()


def test_rollback_both_keeps_cursor_and_retry_catches_up(app_env):
    _set("dual_sink_policy", "rollback_both")
    _set("shadow_fail_seqs", "3")
    run = _run(2)
    report = run["report"]
    assert report["dual_write"]["rollbacks"] == 1
    assert report["dual_write"]["partial_failures"] == 0
    assert run["cursor_after"] == 2
    assert report["halted_at_seq"] == 3
    conn = _conn()
    try:
        assert conn.execute("SELECT COUNT(*) c FROM sink_records").fetchone()["c"] == 1
        assert conn.execute("SELECT COUNT(*) c FROM sink_shadow_records").fetchone()["c"] == 1
        assert conn.execute("SELECT COUNT(*) c FROM quarantine").fetchone()["c"] == 0
    finally:
        conn.close()

    _set("shadow_fail_seqs", "")
    retry = _run(2)
    assert retry["cursor_after"] == 3
    assert retry["applied_count"] == 1
    assert retry["skipped_count"] == 2
    assert retry["report"]["dual_write"]["both_success"] == 1


def test_policy_rejects_unknown_value():
    from app.modules import dual_sink_partial_fail as dual

    with pytest.raises(ValueError):
        dual.normalize_policy("loose")
