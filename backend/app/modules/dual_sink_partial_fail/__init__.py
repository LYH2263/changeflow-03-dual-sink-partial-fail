"""双 Sink 扇出与部分失败取舍。

一次 Apply 中的每条 apply 事件都要同时写主 Sink 与旁路 Sink。旁路失败时，
只有两种互斥、可持久化配置的取舍：

- ``rollback_both``：本条两侧回滚到写入前，游标停在该条之前不前进，整次 Apply 停止。
- ``main_commit_isolate_shadow``：主 Sink 提交，旁路失败进入隔离区（失败清单），
  游标照常前进并继续后续事件；报告显式写明游标已前进。

没有“一侧失败、两侧都算成功”的第三种状态。
"""

from __future__ import annotations

from typing import Iterable

POLICY_ROLLBACK_BOTH = "rollback_both"
POLICY_MAIN_COMMIT_ISOLATE_SHADOW = "main_commit_isolate_shadow"

DEFAULT_POLICY = POLICY_MAIN_COMMIT_ISOLATE_SHADOW

POLICIES: tuple[str, ...] = (
    POLICY_ROLLBACK_BOTH,
    POLICY_MAIN_COMMIT_ISOLATE_SHADOW,
)

POLICY_LABELS: dict[str, str] = {
    POLICY_ROLLBACK_BOTH: "两侧回滚 · 游标不前进",
    POLICY_MAIN_COMMIT_ISOLATE_SHADOW: "主 Sink 提交 · 旁路隔离 · 游标前进",
}

OUTCOME_BOTH_COMMITTED = "both_committed"
OUTCOME_PARTIAL_SHADOW_ISOLATED = "main_committed_shadow_isolated"
OUTCOME_ROLLED_BACK_BOTH = "rolled_back_both"

SHADOW_FAILURE_REASON = "shadow_sink_failed"


class ShadowSinkError(RuntimeError):
    """旁路 Sink 写入失败（由注入器或真实写入异常触发）。"""


class MainSinkError(RuntimeError):
    """主 Sink 写入失败；此时任何策略都必须两侧回滚。"""


def normalize_policy(value: str | None) -> str:
    value = (value or "").strip()
    if value not in POLICIES:
        raise ValueError(
            "dual_sink_policy must be one of: " + ", ".join(POLICIES)
        )
    return value


def parse_failure_seqs(value: str | None) -> list[int]:
    """解析旁路故障注入配置：逗号/空格分隔的 seq 列表；空值表示不注入。"""
    if not value:
        return []
    parts = value.replace(",", " ").split()
    seqs: list[int] = []
    for part in parts:
        try:
            seq = int(part)
        except ValueError as exc:
            raise ValueError(f"invalid shadow failure seq: {part!r}") from exc
        if seq < 1:
            raise ValueError(f"invalid shadow failure seq: {seq}")
        if seq not in seqs:
            seqs.append(seq)
    return seqs


def normalize_failure_seqs(value: str | None) -> str:
    """规范化后写回 settings 的字符串形式。"""
    return ",".join(str(seq) for seq in parse_failure_seqs(value))


def should_fail_shadow(seq: int, failure_seqs: Iterable[int]) -> bool:
    return int(seq) in set(int(s) for s in failure_seqs)


def rollback_mode(policy: str) -> bool:
    return normalize_policy(policy) == POLICY_ROLLBACK_BOTH


def cursor_policy_text(policy: str, halted_at_seq: int | None) -> str:
    policy = normalize_policy(policy)
    if policy == POLICY_ROLLBACK_BOTH:
        if halted_at_seq is None:
            return "两侧回滚策略：本次无失败，游标按正常序前进。"
        return (
            f"两侧回滚策略：seq {halted_at_seq} 旁路失败，两侧回滚，"
            f"游标未前进到该条并停止。"
        )
    if halted_at_seq is None:
        return "主提交+旁路隔离策略：本次无失败，游标按正常序前进。"
    return (
        f"主提交+旁路隔离策略：seq {halted_at_seq} 主 Sink 已提交、旁路进入隔离区，"
        f"游标已前进并继续后续事件。"
    )
