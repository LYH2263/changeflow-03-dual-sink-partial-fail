from fastapi import APIRouter

from app.db import connect
from app.repositories import dual_write as dual_write_repo

router = APIRouter(tags=["dual-write"])


@router.get("/runs/{run_id}/dual-write")
def list_run_dual_write(run_id: int):
    conn = connect()
    try:
        return {
            "run_id": run_id,
            "summary": dual_write_repo.summarize_run(conn, run_id),
            "attempts": dual_write_repo.list_attempts(conn, run_id=run_id),
        }
    finally:
        conn.close()


@router.get("/dual-write")
def list_dual_write(limit: int = 200):
    conn = connect()
    try:
        return dual_write_repo.list_attempts(conn, limit=limit)
    finally:
        conn.close()
