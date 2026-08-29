"""
群成员身份校验模块 — GroupMemberGuard
====================================

独立可复用的 TTL 缓存群成员检测器。
**不依赖 AstrBot / OneBot / 任何框架**，只依赖 Python 标准库，
可直接复制到任意 asyncio 项目中使用。

功能：
  - 受管群白名单：直接填群号（支持 int / str 混合）
  - 按群缓存成员集合，TTL 过期自动刷新
  - 同一群并发拉取只允许一个协程执行（防抖）
  - 拉取失败时保留旧缓存并续命，避免查询全挂
  - 验证语义 = 存在性：发送者在**任意一个**受管群里，即视为群成员
  - 提供强制刷新与统计接口

用法示例：
    guard = GroupMemberGuard(allowed_groups=[123456, 789012], cache_ttl=600)
    guard.set_member_fetcher(my_async_fetch)   # async (group_id) -> list[dict]
    ok = await guard.is_member("2951537603")   # True = 放行 / False = 拦截

宿主只需要提供一个「按群号拉取成员列表」的异步函数：
    async def my_async_fetch(group_id: str) -> list[dict]:
        # 返回成员 dict 列表，每个 dict 至少含 "user_id" 字段
        # 例（OneBot 11 / NapCat）：
        #   payload = await bot.api.call_action("get_group_member_list", group_id=group_id)
        #   return payload if isinstance(payload, list) else []
        ...

注意：
  - is_member() 返回 True 表示「放行」（是成员，或未启用/未配置受管群）
  - 返回 False 表示「拦截」（非成员，应静默丢弃）
  - 拉取失败且无旧缓存时返回空集合 → is_member() 返回 False，
    宿主可自行决定失败策略（保守拦截或放行）。
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Awaitable, Callable, Iterable

# 成员拉取器类型：async (group_id: str) -> list[dict]，dict 需含 "user_id"
MemberFetcher = Callable[[str], Awaitable[list[dict[str, Any]]]]


class GroupMemberGuard:
    """TTL 缓存的群成员身份校验器（asyncio 环境）。"""

    def __init__(
        self,
        allowed_groups: Iterable[int | str] | None = None,
        cache_ttl: int = 600,
        enabled: bool = True,
    ) -> None:
        """
        :param allowed_groups: 受管群白名单，直接写群号（int 或 str 均可）
        :param cache_ttl:      成员缓存有效期（秒），最小 30 秒
        :param enabled:        总开关；关闭时 is_member() 恒返回 True（不拦截）
        """
        self.enabled: bool = enabled
        self.cache_ttl: int = max(30, int(cache_ttl))
        self.allowed_groups: set[str] = set()
        self.set_allowed_groups(allowed_groups)

        self._member_fetcher: MemberFetcher | None = None
        # group_id -> (成员集合, 过期时间戳)
        self._cache: dict[str, tuple[frozenset[str], float]] = {}
        self._lock = asyncio.Lock()

        # 统计信息（调试用）
        self.stats: dict[str, int] = {
            "hits": 0,            # 命中成员
            "misses": 0,          # 未命中成员
            "fetches": 0,         # 成功拉取次数
            "fetch_failures": 0,  # 拉取失败次数
        }

    # ── 配置 ────────────────────────────────────────────────

    def set_allowed_groups(self, groups: Iterable[int | str] | None) -> None:
        """设置受管群白名单（直接写群号）。"""
        self.allowed_groups = {str(g) for g in (groups or [])}

    def set_member_fetcher(self, fetcher: MemberFetcher | None) -> None:
        """注入成员拉取器：async (group_id: str) -> list[dict]。"""
        self._member_fetcher = fetcher

    def is_managed(self, group_id: int | str) -> bool:
        """该群是否在受管白名单内。"""
        return str(group_id) in self.allowed_groups

    # ── 核心验证 ────────────────────────────────────────────

    async def is_member(self, user_id: int | str) -> bool:
        """
        校验发送者是否为群成员（存在性语义）。

        遍历所有受管群，**任一命中即通过**。
        未启用或未配置受管群时恒返回 True（不拦截）。
        """
        user_id = str(user_id)
        if not self.enabled or not self.allowed_groups:
            return True

        for group_id in self.allowed_groups:
            members = await self._get_members(group_id)
            if user_id in members:
                self.stats["hits"] += 1
                return True
        self.stats["misses"] += 1
        return False

    # ── 缓存与拉取 ──────────────────────────────────────────

    async def _get_members(self, group_id: str) -> frozenset[str]:
        """读取某群成员集合，带 TTL 缓存；过期时刷新。"""
        now = time.monotonic()
        cached = self._cache.get(group_id)
        if cached and cached[1] > now:
            return cached[0]

        # 过期：加锁刷新（同一群并发只允许一个协程拉取）
        async with self._lock:
            now = time.monotonic()
            cached = self._cache.get(group_id)
            if cached and cached[1] > now:
                return cached[0]
            return await self._fetch(group_id)

    async def _fetch(self, group_id: str) -> frozenset[str]:
        """拉取并缓存成员集合；失败时保留旧缓存并续命。"""
        if self._member_fetcher is None:
            return frozenset()
        try:
            raw = await self._member_fetcher(group_id)
            ids = frozenset(
                str(m["user_id"])
                for m in raw
                if isinstance(m, dict) and m.get("user_id") is not None
            )
            self._cache[group_id] = (ids, time.monotonic() + self.cache_ttl)
            self.stats["fetches"] += 1
            return ids
        except Exception:  # noqa: BLE001 拉取失败不中断主流程
            self.stats["fetch_failures"] += 1
            cached = self._cache.get(group_id)
            if cached:
                # 保留旧缓存并续命一半 TTL，避免查询全挂
                self._cache[group_id] = (
                    cached[0],
                    time.monotonic() + self.cache_ttl / 2,
                )
                return cached[0]
            return frozenset()

    # ── 维护 ────────────────────────────────────────────────

    async def refresh(self, group_id: int | str | None = None) -> None:
        """
        强制刷新指定群（或全部受管群）的成员缓存。
        拉取失败时保留旧缓存（与自动刷新行为一致）。
        """
        targets = [str(group_id)] if group_id is not None else list(self.allowed_groups)
        for gid in targets:
            async with self._lock:
                self._cache.pop(gid, None)
                await self._fetch(gid)

    def clear(self) -> None:
        """清空全部缓存。"""
        self._cache.clear()

    def dump_stats(self) -> str:
        """返回统计摘要（调试用）。"""
        return (
            f"enabled={self.enabled} groups={sorted(self.allowed_groups)} "
            f"cached={len(self._cache)} hits={self.stats['hits']} "
            f"misses={self.stats['misses']} fetches={self.stats['fetches']} "
            f"failures={self.stats['fetch_failures']}"
        )
