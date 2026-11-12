from __future__ import annotations

from typing import Any

from app.config import FIXTURES_DIR
from app.db import connect
from app.engines.apply_rules import classify_event
from app.engines.parse_change import parse_change_fixture
from app.modules import dual_sink_partial_fail as dual
from app.repositories import cursors as cursor_repo
from app.repositories import dual_write as dual_write_repo
from app.repositories import ledger as ledger_repo
from app.repositories import quarantine as quarantine_repo
from app.repositories import runs as runs_repo
from app.repositories import settings as settings_repo
from app.repositories import sink as sink_repo
from app.repositories import streams as streams_repo

POLICY_SETTING = "dual_sink_policy"
SHADOW_FAIL_SETTING = "shadow_fail_seqs"


def _write_main(conn, stream_id: int, event: dict[str, Any]) -> None:
    if event["op"] == "upsert":
        sink_repo.upsert(
            conn,
            stream_id=stream_id,
            entity_key=event["entity_key"],
            payload=event.get("payload") or {},
            seq=int(event["seq"]),
            event_ts=event["event_ts"],
        )
    else:
        sink_repo.delete(conn, stream_id=stream_id, entity_key=event["entity_key"])


def _write_shadow(conn, stream_id: int, event: dict[str, Any]) -> None:
    if event["op"] == "upsert":
        sink_repo.shadow_upsert(
            conn,
            stream_id=stream_id,
            entity_key=event["entity_key"],
            payload=event.get("payload") or {},
            seq=int(event["seq"]),
            event_ts=event["event_ts"],
        )
    else:
        sink_repo.shadow_delete(conn, stream_id=stream_id, entity_key=event["entity_key"])


def preview_stream(stream_id: int) -> dict[str, Any]:
    conn = connect()
    try:
        stream = streams_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        path = FIXTURES_DIR / stream["fixture_path"]
        events = parse_change_fixture(path)
        return {"stream": stream, "count": len(events), "events": events[:50]}
    finally:
        conn.close()


def run_apply(stream_id: int) -> dict[str, Any]:
    conn = connect()
    try:
        stream = streams_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        if not stream["enabled"]:
            raise ValueError("stream disabled")

        cursor_before = int(stream["apply_cursor"] or 0)
        gap_policy = settings_repo.get(conn, "gap_policy", "stop_quarantine") or "stop_quarantine"
        dual_policy = dual.normalize_policy(
            settings_repo.get(conn, POLICY_SETTING, dual.DEFAULT_POLICY)
        )
        shadow_fail_seqs = dual.parse_failure_seqs(
            settings_repo.get(conn, SHADOW_FAIL_SETTING, "") or ""
        )

        run_id = runs_repo.create_run(conn, stream_id, cursor_before)
        path = FIXTURES_DIR / stream["fixture_path"]

        try:
            events = parse_change_fixture(path)
        except Exception as exc:  # noqa: BLE001
            runs_repo.finish_run(
                conn,
                run_id,
                status="failed",
                loaded=0,
                applied=0,
                quarantined=0,
                skipped=0,
                cursor_after=cursor_before,
                report={"error": str(exc)},
            )
            conn.commit()
            raise

        loaded = len(events)
        applied = 0
        quarantined = 0
        skipped = 0
        shadow_isolated = 0
        cursor = cursor_before
        dispositions: list[str] = []
        stopped = False
        halted_at_seq: int | None = None
        halt_reason: str | None = None

        for event in events:
            seq = int(event["seq"])
            already = cursor_repo.is_applied(conn, stream_id, seq)
            decision = classify_event(
                event=event,
                cursor=cursor,
                already_applied=already,
                gap_policy=gap_policy,
            )

            if decision == "skipped_duplicate":
                ledger_repo.insert_ledger(
                    conn, stream_id=stream_id, run_id=run_id, event=event, disposition=decision
                )
                skipped += 1
                dispositions.append(decision)
                continue

            if decision == "quarantine_gap":
                ledger_repo.insert_ledger(
                    conn, stream_id=stream_id, run_id=run_id, event=event, disposition=decision
                )
                quarantine_repo.insert(
                    conn,
                    stream_id=stream_id,
                    run_id=run_id,
                    seq=seq,
                    entity_key=event["entity_key"],
                    reason="gap_before_seq",
                    payload=event.get("payload") or {},
                )
                quarantined += 1
                dispositions.append(decision)
                stopped = True
                halted_at_seq = seq
                halt_reason = "gap_before_seq"
                break

            # --- 双 Sink 扇出：主 + 旁路 ---
            # savepoint 由内到外依次嵌套：dual_event 圈住两条写入，dual_shadow 只圈旁路。
            # 旁路失败只回滚 dual_shadow；需要连主一起撤销时再回滚 dual_event。
            conn.execute("SAVEPOINT dual_event")
            shadow_savepoint = False
            try:
                _write_main(conn, stream_id, event)
                conn.execute("SAVEPOINT dual_shadow")
                shadow_savepoint = True
                try:
                    if dual.should_fail_shadow(seq, shadow_fail_seqs):
                        raise dual.ShadowSinkError(f"injected shadow failure at seq {seq}")
                    _write_shadow(conn, stream_id, event)
                except Exception as exc:  # noqa: BLE001 - 旁路写入异常都算旁路失败
                    raise dual.ShadowSinkError(str(exc)) from exc
            except dual.ShadowSinkError as exc:
                # 撤销旁路的半写，再按策略决定主 Sink 的去留。
                conn.execute("ROLLBACK TO SAVEPOINT dual_shadow")
                conn.execute("RELEASE SAVEPOINT dual_shadow")
                if dual_policy == dual.POLICY_ROLLBACK_BOTH:
                    conn.execute("ROLLBACK TO SAVEPOINT dual_event")
                    conn.execute("RELEASE SAVEPOINT dual_event")
                    dual_write_repo.record_attempt(
                        conn,
                        run_id=run_id,
                        stream_id=stream_id,
                        seq=seq,
                        entity_key=event["entity_key"],
                        op=event["op"],
                        policy=dual_policy,
                        outcome=dual.OUTCOME_ROLLED_BACK_BOTH,
                        main_committed=False,
                        shadow_committed=False,
                        cursor_advanced=False,
                        reason=f"{dual.SHADOW_FAILURE_REASON}: {exc}",
                    )
                    ledger_repo.insert_ledger(
                        conn,
                        stream_id=stream_id,
                        run_id=run_id,
                        event=event,
                        disposition="rolled_back_shadow_failed",
                    )
                    dispositions.append("rolled_back_shadow_failed")
                    stopped = True
                    halted_at_seq = seq
                    halt_reason = dual.SHADOW_FAILURE_REASON
                    break

                # 主提交 + 旁路隔离：保留主 Sink 写入，旁路失败进入隔离区，
                # 游标照常前进（报告与界面明确标注）。
                conn.execute("RELEASE SAVEPOINT dual_event")
                dual_write_repo.record_attempt(
                    conn,
                    run_id=run_id,
                    stream_id=stream_id,
                    seq=seq,
                    entity_key=event["entity_key"],
                    op=event["op"],
                    policy=dual_policy,
                    outcome=dual.OUTCOME_PARTIAL_SHADOW_ISOLATED,
                    main_committed=True,
                    shadow_committed=False,
                    cursor_advanced=True,
                    reason=f"{dual.SHADOW_FAILURE_REASON}: {exc}",
                )
                quarantine_repo.insert(
                    conn,
                    stream_id=stream_id,
                    run_id=run_id,
                    seq=seq,
                    entity_key=event["entity_key"],
                    reason=dual.SHADOW_FAILURE_REASON,
                    payload={
                        "side": "shadow",
                        "policy": dual_policy,
                        "op": event["op"],
                        "event_payload": event.get("payload") or {},
                    },
                )
                cursor_repo.mark_applied(conn, stream_id, seq, run_id)
                cursor = seq
                streams_repo.set_cursor(conn, stream_id, cursor)
                ledger_repo.insert_ledger(
                    conn,
                    stream_id=stream_id,
                    run_id=run_id,
                    event=event,
                    disposition="applied_main_shadow_isolated",
                )
                applied += 1
                shadow_isolated += 1
                quarantined += 1
                dispositions.append("applied_main_shadow_isolated")
                continue
            except Exception as exc:  # noqa: BLE001 - 主 Sink 自身失败：两侧都不能提交
                if shadow_savepoint:
                    conn.execute("ROLLBACK TO SAVEPOINT dual_shadow")
                    conn.execute("RELEASE SAVEPOINT dual_shadow")
                conn.execute("ROLLBACK TO SAVEPOINT dual_event")
                conn.execute("RELEASE SAVEPOINT dual_event")
                dual_write_repo.record_attempt(
                    conn,
                    run_id=run_id,
                    stream_id=stream_id,
                    seq=seq,
                    entity_key=event["entity_key"],
                    op=event["op"],
                    policy=dual_policy,
                    outcome=dual.OUTCOME_ROLLED_BACK_BOTH,
                    main_committed=False,
                    shadow_committed=False,
                    cursor_advanced=False,
                    reason=f"main_sink_failed: {exc}",
                )
                ledger_repo.insert_ledger(
                    conn,
                    stream_id=stream_id,
                    run_id=run_id,
                    event=event,
                    disposition="rolled_back_main_failed",
                )
                dispositions.append("rolled_back_main_failed")
                stopped = True
                halted_at_seq = seq
                halt_reason = "main_sink_failed"
                break
            else:
                conn.execute("RELEASE SAVEPOINT dual_shadow")
                conn.execute("RELEASE SAVEPOINT dual_event")
                dual_write_repo.record_attempt(
                    conn,
                    run_id=run_id,
                    stream_id=stream_id,
                    seq=seq,
                    entity_key=event["entity_key"],
                    op=event["op"],
                    policy=dual_policy,
                    outcome=dual.OUTCOME_BOTH_COMMITTED,
                    main_committed=True,
                    shadow_committed=True,
                    cursor_advanced=True,
                )
                cursor_repo.mark_applied(conn, stream_id, seq, run_id)
                cursor = seq
                streams_repo.set_cursor(conn, stream_id, cursor)
                ledger_repo.insert_ledger(
                    conn,
                    stream_id=stream_id,
                    run_id=run_id,
                    event=event,
                    disposition="applied",
                )
                applied += 1
                dispositions.append("applied")

        # 报告计数直接取自本次 run 的双写尝试表，保证与 Sink/隔离/游标同源。
        summary = dual_write_repo.summarize_run(conn, run_id)
        status = "partial" if (stopped or summary["partial_failures"] or summary["rollbacks"]) else "succeeded"
        report = {
            "stream_id": stream_id,
            "gap_policy": gap_policy,
            "dual_sink_policy": dual_policy,
            "dual_sink_policy_label": dual.POLICY_LABELS[dual_policy],
            "shadow_fail_seqs": shadow_fail_seqs,
            "dual_write": summary,
            "cursor_policy": dual.cursor_policy_text(dual_policy, summary["first_failed_seq"]),
            "cursor_advanced": cursor > cursor_before,
            "cursor_after": cursor,
            "halted_at_seq": halted_at_seq,
            "halt_reason": halt_reason,
            "dispositions": dispositions,
            "stopped_on_gap": stopped and halt_reason == "gap_before_seq",
            "shadow_isolation_count": shadow_isolated,
            "sink_count_after": sink_repo.count_sink(conn),
            "shadow_sink_count_after": sink_repo.count_shadow_sink(conn),
            "quarantine_count_after": quarantine_repo.count_quarantine(conn),
        }
        runs_repo.finish_run(
            conn,
            run_id,
            status=status,
            loaded=loaded,
            applied=applied,
            quarantined=quarantined,
            skipped=skipped,
            cursor_after=cursor,
            report=report,
        )
        conn.commit()
        return runs_repo.get_run(conn, run_id) or {}
    finally:
        conn.close()
