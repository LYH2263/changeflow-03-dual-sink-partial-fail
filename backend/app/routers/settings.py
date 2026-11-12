from fastapi import APIRouter, HTTPException

from app.db import connect
from app.modules import dual_sink_partial_fail as dual
from app.repositories import settings as settings_repo
from app.schemas.entities import SettingPut

router = APIRouter(tags=["settings"])

DUAL_POLICY_KEY = "dual_sink_policy"
SHADOW_FAIL_KEY = "shadow_fail_seqs"


def _normalize_setting(key: str, value: str) -> str:
    if key == DUAL_POLICY_KEY:
        return dual.normalize_policy(value)
    if key == SHADOW_FAIL_KEY:
        return dual.normalize_failure_seqs(value)
    return value


@router.get("/settings")
def list_settings():
    conn = connect()
    try:
        return settings_repo.as_list(conn)
    finally:
        conn.close()


@router.get("/settings/dual-sink")
def get_dual_sink_settings():
    conn = connect()
    try:
        policy = dual.normalize_policy(
            settings_repo.get(conn, DUAL_POLICY_KEY, dual.DEFAULT_POLICY)
        )
        fail_seqs = dual.parse_failure_seqs(
            settings_repo.get(conn, SHADOW_FAIL_KEY, "") or ""
        )
        return {
            "policy": policy,
            "policy_label": dual.POLICY_LABELS[policy],
            "policies": [
                {"value": p, "label": dual.POLICY_LABELS[p]} for p in dual.POLICIES
            ],
            "shadow_fail_seqs": fail_seqs,
        }
    finally:
        conn.close()


@router.put("/settings/{key}")
def put_setting(key: str, body: SettingPut):
    conn = connect()
    try:
        try:
            value = _normalize_setting(key, body.value)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        settings_repo.put(conn, key, value)
        conn.commit()
        return {"key": key, "value": value}
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, str(exc)) from exc
    finally:
        conn.close()
