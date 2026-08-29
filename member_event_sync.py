"""
群成员事件批量同步模块 — MemberEventSyncManager
=================================================

监听进群/退群通知事件（OneBot V11 notice），按批量窗口累积后交给宿主处理。

与 GroupSyncManager（轮询差异）互补：本模块是事件驱动的即时增量，
用于把「离群名单」「入群名单」尽快上报，减少轮询间隔带来的延迟。

批量策略（防抖 + 上限）：
  - 收到第一个事件后开始计时
  - 每来一个新事件就重置 quiet_window 秒静默窗口（默认 1 秒）
  - 若持续有事件到达，最多累积 max_window 秒后强制推送（默认 5 秒，避免无限推迟）
  - 静默满 quiet_window 秒即推送本批

上报协议：复用现成 EWhitelistPush(Type=3) 群白名单增量推送版本链，
由宿主把事件桥接进 GroupSyncManager.submit_member_change()，
服务端无需新增命令码（不需要 BotCommand.ts 改动）。

零依赖（仅标准库），不绑定 AstrBot。宿主只需注入事件处理器：
  async (kind: str, events: list[dict]) -> None
  kind: "decrease"（离群）或 "increase"（入群）
  events: [ { "GroupId": "...", "UserId": "...", "SubType": "...", "Time": 1234567890 } ]
  SubType 对齐 OneBot V11：
    group_decrease: leave(主动退群) / kick(被移出) / kick_me(机器人被移出)
    group_increase: approve(管理员同意) / invite(邀请) / other
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Awaitable, Callable, Iterable

# ELoginChannel：1=QQ, 2=Discord（与 group_sync 对齐）
PLATFORM_QQ = 1
PLATFORM_DISCORD = 2

# 事件类型（语义标记，不对应网络命令码；网络命令码由宿主桥接层决定）
KIND_DECREASE = "decrease"  # 离群
KIND_INCREASE = "increase"  # 入群
_KINDS = (KIND_DECREASE, KIND_INCREASE)

# 事件处理器类型：async (kind: str, events: list[dict]) -> None
Handler = Callable[[str, list[dict]], Awaitable[None]]


class MemberEventSyncManager:
    """群成员事件批量同步管理器（asyncio 环境，事件驱动 + 防抖累积）。"""

    def __init__(
        self,
        groups: Iterable[int | str] | None = None,
        quiet_window: float = 1.0,
        max_window: float = 5.0,
        enabled: bool = True,
        logger: Any = None,
    ) -> None:
        """
        :param groups: 受管群白名单（直接填群号，int/str 均可）
        :param quiet_window: 静默窗口（秒），期间无新事件即推送本批
        :param max_window: 最大累积窗口（秒），持续有事件时强制推送
        :param enabled: 总开关；关闭时不累积、不推送
        :param logger: 可选 logger（有 info/warning/error 即可）
        """
        self.enabled: bool = enabled
        self.quiet_window: float = max(0.1, float(quiet_window))
        self.max_window: float = max(self.quiet_window, float(max_window))
        # 受管名单按平台拆分：platform -> set[gid]
        self._groups: dict[int, set[str]] = {}
        self.set_groups(groups)
        self._logger = logger

        self._handler: Handler | None = None
        # 待推送事件缓冲：(kind, event_dict) 列表
        self._pending: list[tuple[str, dict]] = []
        self._trigger = asyncio.Event()
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None
        self._closed = False

        self.stats: dict[str, int] = {
            "submitted": 0,  # 累计提交事件数
            "events": 0,     # 累计成功上报事件数
            "pushes": 0,     # 批量上报次数
            "dropped": 0,    # 丢弃事件数（无处理器/处理失败）
        }

    # ── 配置 ────────────────────────────────────────────────

    def set_groups(self, groups: Iterable[int | str] | None) -> None:
        """设置 QQ 平台受管群白名单（直接写群号）。"""
        self.set_platform_groups(PLATFORM_QQ, groups)

    def set_platform_groups(
        self, platform: int, groups: Iterable[int | str] | None
    ) -> None:
        """设置指定平台的受管名单（群号/服务器ID），保留其他平台配置。"""
        self._groups[platform] = {str(g) for g in (groups or [])}

    @property
    def groups(self) -> set[str]:
        """全部平台的受管 ID 合并集合（兼容旧调用/日志）。"""
        merged: set[str] = set()
        for g in self._groups.values():
            merged |= g
        return merged

    def set_handler(self, handler: Handler | None) -> None:
        """注入事件处理器：async (kind: str, events: list[dict]) -> None。

        kind 为 "decrease"/"increase"；由宿主负责桥接到底层上报通道
        （如 GroupSyncManager.submit_member_change 复用 EWhitelistPush=3）。
        events 内每条含 GroupId/UserId/SubType/Time/Platform。
        """
        self._handler = handler

    def is_managed(self, group_id: int | str, platform: int = PLATFORM_QQ) -> bool:
        """该群/服务器是否在指定平台的受管白名单内。

        Discord 受管条目支持「服务器ID」或「服务器ID:频道ID」：
        服务器级成员事件用服务器ID 也能命中频道级条目（前缀匹配）。
        """
        gid = str(group_id)
        groups = self._groups.get(platform, set())
        if gid in groups:
            return True
        if platform == PLATFORM_DISCORD:
            return any(e.startswith(gid + ":") for e in groups)
        return False

    # ── 生命周期 ────────────────────────────────────────────

    async def start(self) -> None:
        """启动后台批量循环（幂等）。"""
        if not self.enabled:
            self._log("成员事件同步未启用，跳过启动", "warning")
            return
        if self._task is None or self._task.done():
            self._closed = False
            self._task = asyncio.create_task(self._run())
            self._log(
                f"成员事件同步已启动：groups={sorted(self.groups)} "
                f"quiet={self.quiet_window}s max={self.max_window}s"
            )

    async def stop(self) -> None:
        """停止批量循环，并在退出前冲刷残留事件（尽力而为）。"""
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        # 停前冲刷残留（插件重载/禁用时不丢事件）
        try:
            async with self._lock:
                if not self._pending:
                    return
            await self._flush()
        except Exception as e:  # noqa: BLE001
            self._log(f"停止时冲刷残留失败: {type(e).__name__}: {e}", "warning")

    # ── 事件入口 ────────────────────────────────────────────

    def submit(
        self,
        kind: str,
        group_id: int | str,
        user_id: int | str,
        sub_type: str = "",
        ts: int | float | None = None,
        platform: int = PLATFORM_QQ,
    ) -> bool:
        """入队一个成员事件并唤醒批量循环。

        :param kind: "decrease"（离群）或 "increase"（入群）
        :param group_id: 群号/服务器ID
        :param user_id: 成员 ID（QQ 号 / Discord 用户ID）
        :param sub_type: OneBot sub_type（leave/kick/approve/invite 等）
        :param ts: 事件时间戳（秒），缺省用当前时间
        :param platform: 平台（PLATFORM_QQ=1 / PLATFORM_DISCORD=2）
        :return: 是否入队成功（未启用/非受管群/未知 kind 返回 False）
        """
        if not self.enabled:
            return False
        if kind not in _KINDS:
            return False
        if not self.is_managed(group_id, platform):
            return False
        ev: dict = {
            "GroupId": str(group_id),
            "UserId": str(user_id),
            "SubType": str(sub_type or ""),
            "Time": int(ts or time.time()),
            "Platform": platform,
        }
        # asyncio 单线程协作式：此处无 await 中断点，追加与唤醒是原子的
        self._pending.append((kind, ev))
        self._trigger.set()
        self.stats["submitted"] += 1
        return True

    # ── 后台批量循环 ────────────────────────────────────────

    async def _run(self) -> None:
        """批量循环：等待首个事件 → 静默窗口计时 → 到期推送。"""
        while not self._closed:
            await self._trigger.wait()
            self._trigger.clear()

            async with self._lock:
                if not self._pending:
                    continue

            start = time.monotonic()
            while not self._closed:
                try:
                    await asyncio.wait_for(
                        self._trigger.wait(), timeout=self.quiet_window
                    )
                    self._trigger.clear()
                except asyncio.TimeoutError:
                    # 静默满 quiet_window → 推送本批
                    break
                except asyncio.CancelledError:
                    raise
                # 又有新事件：若累积已超过 max_window → 强制推送
                if time.monotonic() - start >= self.max_window:
                    break
            await self._flush()

    async def _flush(self) -> None:
        """取走缓冲并按类型分组交给宿主（decrease/increase）。"""
        async with self._lock:
            batch = self._pending
            self._pending = []
        if not batch:
            return
        if self._handler is None:
            self._log("未注入事件处理器，本次批量丢弃", "warning")
            self.stats["dropped"] += len(batch)
            return

        for kind in _KINDS:
            events = [ev for k, ev in batch if k == kind]
            if not events:
                continue
            try:
                await self._handler(kind, events)
                self.stats["pushes"] += 1
                self.stats["events"] += len(events)
                self._log(f"{kind} 事件批量上报：{len(events)} 条")
            except Exception as e:  # noqa: BLE001
                self.stats["dropped"] += len(events)
                self._log(
                    f"{kind} 事件批量上报失败: {type(e).__name__}: {e}", "warning"
                )

    # ── 工具 ────────────────────────────────────────────────

    def dump_stats(self) -> str:
        return (
            f"enabled={self.enabled} groups={sorted(self.groups)} "
            f"quiet={self.quiet_window}s max={self.max_window}s "
            f"submitted={self.stats['submitted']} pushes={self.stats['pushes']} "
            f"events={self.stats['events']} dropped={self.stats['dropped']}"
        )

    def _log(self, text: str, level: str = "info") -> None:
        if self._logger is None:
            return
        try:
            if level == "warning":
                self._logger.warning(f"[MemberEventSync] {text}")
            else:
                self._logger.info(f"[MemberEventSync] {text}")
        except Exception:
            pass
