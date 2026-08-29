"""
群名单同步模块 — GroupSyncManager
=================================

对接 MasterServer GroupSyncManager 的群白名单同步协议（GroupWhitelist）。

职责：
  - 维护受管群的成员名单快照（group_id -> set[成员ID]）
  - 定期轮询成员列表，对比差异生成增量变更（add/remove）
  - 推送增量（EWhitelistPush=3），携带自增版本号（BaseVersion/Version）
  - 响应 Master 的版本查询（Type=14 EGetLatestVersion -> { RequestId, Version }）
  - 响应 Master 的补包请求（Type=15 -> 返回 Version=FromVersion 的精确块；找不到则回链头覆盖块兜底）
  - 保留最近 N 个版本变更历史，用于补包；历史被 GC 时直接回退快照
  - 硬盘持久化（可选）：版本号/成员快照/差量历史落盘到 JSON；
    重启后读取上次保存的版本续接，基于最后版本计算差量推送，不从 v0 重建

协议（对齐服务端 src/Types/GroupSync.ts + src/Utils/RpcServer.ts）：
  FWhitelistChange    = { Action: number(1=add,2=remove), Platform: number(1=QQ,2=Discord), Members: string[] }
  FWhitelistPush      = { Type: 3, RequestId, BaseVersion, Version, Changes: FWhitelistChange[] }
  Master 主动调用：
    EGetLatestVersion       = { Type: 14, RequestId: N }          → 回 { RequestId: N, Version }
    EWhitelistDeltaRequest  = { Type: 15, RequestId: N, FromVersion: X }
        → 若 X 在历史链中，回 { RequestId: N, BaseVersion: 链上前一项, WhitelistPushAction: 该项动作, Version: X, Changes: 该项差量 }
        → 否则回链头覆盖块 { RequestId: N, BaseVersion: 链头版本, WhitelistPushAction: 1(Override), Version: 链头版本, Changes: 链头全量快照 }
      （配合 master PendingChain 追链，逐版本拼完整链路）

语义：
  - 版本号从 0 开始，每次推送自增 1（同一 Bot 命名空间内连续）
  - Master 发现版本跳跃（BaseVersion != 本地 LatestVersion）时请求补包；
    补包仍不连续则请求完整快照（版本链被 GC）。
  - 本模块补包失败（FromVersion 太旧已被 GC）时直接回退快照兜底。
  - Discord 平台（Platform=2）为「成员级」白名单：受管条目为服务器ID 或
    服务器ID:频道ID（discord_sync_guilds），成员拉取器按服务器拉取全部成员ID
    （Discord 用户ID，即玩家唯一平台ID）后推送，与 QQ 平台一致；
    QQ 平台（Platform=1）同样为「成员级」白名单：推送群成员QQ号。

零依赖（仅标准库），不绑定 AstrBot。宿主只需提供：
  - 成员拉取器：async (group_id: str) -> list[dict]，dict 需含 "user_id"
  - 单向发送函数：async (msg: dict) -> None（发完不等响应，用于增量推送）
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any, Awaitable, Callable, Iterable

# EBotCommand 群同步命令码（对齐服务端 BotCommand.ts）
CMD_WHITELIST_PUSH = 3              # 白名单增量推送（客户端→服务器）
CMD_SNAPSHOT_REQUEST = 14           # 服务器→客户端：请求完整快照
CMD_DELTA_REQUEST = 15              # 服务器→客户端：请求补包

# EWhitelistAction（对齐服务端 GroupSync.ts）
ACTION_ADD = 1
ACTION_REMOVE = 2
_ACTION_NAMES = {ACTION_ADD: "add", ACTION_REMOVE: "remove"}

# ELoginChannel：1=QQ, 2=Discord
PLATFORM_QQ = 1
PLATFORM_DISCORD = 2
_PLATFORM_NAMES = {PLATFORM_QQ: "QQ", PLATFORM_DISCORD: "Discord"}
# 支持的平台列表（轮询/快照遍历顺序）
_PLATFORMS = (PLATFORM_QQ, PLATFORM_DISCORD)

# 成员拉取器类型：async (group_id: str) -> list[dict]
MemberFetcher = Callable[[str], Awaitable[list[dict[str, Any]]]]
# 单向发送器类型：async (msg: dict) -> None
Sender = Callable[[dict], Awaitable[None]]


class GroupSyncManager:
    """群名单同步管理器（asyncio 环境，轮询驱动）。"""

    def __init__(
        self,
        groups: Iterable[int | str] | None = None,
        poll_interval: int = 300,
        retention_versions: int = 50,
        enabled: bool = True,
        logger: Any = None,
        state_path: str | None = None,
        persist: bool = True,
    ) -> None:
        """
        :param groups: 受管群白名单（直接填群号，int/str 均可）
        :param poll_interval: 轮询间隔（秒），最小 30
        :param retention_versions: 保留的变更历史版本数（用于补包），最小 2
        :param enabled: 总开关；关闭时不启动轮询、不推送
        :param logger: 可选 logger（有 info/warning/error 即可）
        :param state_path: 状态持久化文件路径（JSON）；为 None 或 persist=False 时不落盘
        :param persist: 是否把版本号/成员快照/差量历史保存到硬盘，重启后从上次版本续接
        """
        self.enabled: bool = enabled
        self.poll_interval: int = max(30, int(poll_interval))
        self.retention_versions: int = max(2, int(retention_versions))
        # 受管名单按平台拆分：platform -> set[gid]（gid 为群号/服务器ID）
        self._groups: dict[int, set[str]] = {}
        self.set_groups(groups)
        self._logger = logger

        # 硬盘持久化：state_path 为空或 persist=False 时完全禁用，行为与旧版一致
        self._state_path: str | None = state_path
        self._persist_enabled: bool = bool(persist) and bool(state_path)
        self._state_loaded: bool = False

        # 成员拉取器按平台注册：platform -> fetcher
        self._member_fetchers: dict[int, MemberFetcher] = {}
        self._sender: Sender | None = None

        # 版本与快照：version 自增；snapshot["platform:group_id"] = frozenset[member_id]
        self._version: int = 0
        self._snapshot: dict[str, frozenset[str]] = {}
        # 版本基线是否已就绪：连接建立后需先发完整快照重建基线，
        # 就绪前不推增量，避免重连后版本重置导致服务端"版本跳跃"误判
        self._baseline_ready: bool = False
        # 变更历史：version -> list[FGroupChange]（用于补包）
        self._history: dict[int, list[dict]] = {}
        # 每个版本对应的推送动作：0=增量(EDelta), 1=覆盖(EOverride)；链头恒为覆盖块
        self._history_action: dict[int, int] = {}
        self._history_order: list[int] = []  # 版本顺序（升序）
        # 链头（最早版本）时刻的并集快照：GC 剪头后用它重建链头覆盖块
        self._chain_head_snapshot: dict[int, set[str]] = {}

        self._task: asyncio.Task | None = None
        # 数据锁：保护 _snapshot / _version / _history 的读写
        self._lock = asyncio.Lock()
        # 发送锁：串行化所有外发消息（增量/快照/补包），
        # 保证发送顺序与版本序列一致，避免并发乱套
        self._send_lock = asyncio.Lock()
        self._closed = False

        self.stats: dict[str, int] = {
            "polls": 0,          # 轮询次数
            "poll_failures": 0,  # 轮询失败次数
            "pushes": 0,         # 增量推送次数
            "snapshots": 0,      # 快照发送次数
            "deltas": 0,         # 补包发送次数
            "changes": 0,        # 累计变更条目数
            "persists": 0,       # 持久化写盘次数
        }

    # ── 配置 ────────────────────────────────────────────────

    def set_groups(self, groups: Iterable[int | str] | None) -> None:
        """设置 QQ 平台受管群白名单（直接写群号）。"""
        self.set_platform_groups(PLATFORM_QQ, groups)

    def set_platform_groups(
        self, platform: int, groups: Iterable[int | str] | None
    ) -> None:
        """设置指定平台的受管名单（群号/服务器ID），保留其他平台配置。"""
        if platform not in _PLATFORMS:
            raise ValueError(f"未知平台: {platform}")
        self._groups[platform] = {str(g) for g in (groups or [])}

    def get_platform_groups(self) -> dict[int, set[str]]:
        """返回按平台拆分的受管名单副本。"""
        return {p: set(g) for p, g in self._groups.items()}

    @property
    def groups(self) -> set[str]:
        """全部平台的受管 ID 合并集合（兼容旧调用/日志）。"""
        merged: set[str] = set()
        for g in self._groups.values():
            merged |= g
        return merged

    def set_member_fetcher(
        self, fetcher: MemberFetcher | None, platform: int = PLATFORM_QQ
    ) -> None:
        """注入成员拉取器：async (group_id: str) -> list[dict]。

        :param fetcher: 拉取器；None 表示移除该平台的拉取器
        :param platform: 目标平台（PLATFORM_QQ / PLATFORM_DISCORD）
        """
        if fetcher is None:
            self._member_fetchers.pop(platform, None)
        else:
            self._member_fetchers[platform] = fetcher

    def set_sender(self, sender: Sender | None) -> None:
        """注入单向发送器：async (msg: dict) -> None（发完不等响应）。"""
        self._sender = sender

    def is_managed(self, group_id: int | str, platform: int = PLATFORM_QQ) -> bool:
        """该群/服务器是否在指定平台的受管白名单内。

        Discord 受管条目支持「服务器ID」或「服务器ID:频道ID」：
        服务器级事件（成员加入/离开）用服务器ID 也能命中频道级条目（前缀匹配）。
        """
        gid = str(group_id)
        groups = self._groups.get(platform, set())
        if gid in groups:
            return True
        if platform == PLATFORM_DISCORD:
            return any(e.startswith(gid + ":") for e in groups)
        return False

    def _managed_keys(self, group_id: int | str, platform: int) -> list[str]:
        """返回匹配该群/服务器的所有受管条目快照键（精确 + Discord 前缀）。"""
        gid = str(group_id)
        keys: list[str] = []
        for entry in sorted(self._groups.get(platform, set())):
            if entry == gid or entry.startswith(gid + ":"):
                keys.append(self._snapshot_key(platform, entry))
        return keys

    def _snapshot_key(self, platform: int, group_id: int | str) -> str:
        """快照键：平台前缀 + ID，避免 QQ 群号与 Discord 服务器ID 撞键。"""
        return f"{platform}:{group_id}"

    # ── 生命周期 ────────────────────────────────────────────

    async def start(self) -> None:
        """启动后台轮询任务（幂等）。"""
        if not self.enabled:
            self._log("群名单同步未启用，跳过启动", "warning")
            return
        if self._task is None or self._task.done():
            self._closed = False
            # 首次启动：从硬盘恢复上次版本基线（版本号/成员快照/差量历史）
            if not self._state_loaded:
                self.load_state()
                self._state_loaded = True
            # 有持久化版本基线（version>0）→ 直接就绪：重启后第一次轮询
            # 即可基于上次保存的快照计算差量并推送新版本（版本号连续续接），
            # 不再从 v0 开始构建；无持久化基线则等服务端快照请求重建（原行为）。
            self._baseline_ready = self._state_loaded and self._version > 0
            self._task = asyncio.create_task(self._run())
            groups_desc = ", ".join(
                f"{_PLATFORM_NAMES.get(p, p)}:{sorted(g)}"
                for p, g in sorted(self._groups.items())
                if g
            ) or "无"
            self._log(
                f"群名单同步已启动：groups=[{groups_desc}] "
                f"interval={self.poll_interval}s retention={self.retention_versions} "
                f"persist={'on' if self._persist_enabled else 'off'}"
            )

    async def stop(self) -> None:
        """停止轮询任务。"""
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        # 退出前保存当前状态（版本号/快照/历史），供下次启动差量续接
        self.save_state()

    async def _run(self) -> None:
        """后台主循环：启动后立即同步一次，然后按间隔轮询。"""
        try:
            await self.poll_once()
        except asyncio.CancelledError:
            return
        except Exception as e:  # noqa: BLE001
            self._log(f"首次同步失败: {type(e).__name__}: {e}", "warning")
        while not self._closed:
            try:
                await asyncio.sleep(self.poll_interval)
            except asyncio.CancelledError:
                return
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                return
            except Exception as e:  # noqa: BLE001
                self.stats["poll_failures"] += 1
                self._log(f"轮询失败: {type(e).__name__}: {e}", "warning")

    # ── 轮询与差异计算 ──────────────────────────────────────

    async def poll_once(self) -> None:
        """拉取所有受管群/服务器成员，按「平台级并集」对比上次快照推送差量。

        白名单是「所有受管群成员的并集」：同一成员在多个群只算一次。减群、
        成员退出统一通过「旧并集 - 新并集」得到 remove，正确去重，不会误删
        仍在其它受管群中的成员。
        """
        if not self.enabled or not self._groups:
            return
        if not self._member_fetchers:
            self._log("未注入任何平台成员拉取器，跳过轮询", "warning")
            return

        self.stats["polls"] += 1

        async with self._lock:
            # 旧并集：当前快照按平台合并（去重）
            old_union: dict[int, set[str]] = {}
            for key, members in self._snapshot.items():
                try:
                    platform = int(key.split(":", 1)[0])
                except (ValueError, IndexError):
                    continue
                old_union.setdefault(platform, set()).update(members)

            # 拉取当前受管群，构建新快照；拉取失败的群保留旧快照，避免误删
            new_snapshot: dict[str, frozenset[str]] = {}
            for platform in _PLATFORMS:
                fetcher = self._member_fetchers.get(platform)
                if fetcher is None:
                    continue
                for group_id in sorted(self._groups.get(platform, set())):
                    key = self._snapshot_key(platform, group_id)
                    try:
                        raw = await fetcher(group_id)
                    except Exception as e:  # noqa: BLE001
                        self._log(
                            f"拉取 {_PLATFORM_NAMES.get(platform, platform)} "
                            f"{group_id} 成员失败: {e}",
                            "warning",
                        )
                        new_snapshot[key] = self._snapshot.get(key, frozenset())
                        continue
                    new_snapshot[key] = frozenset(
                        str(m["user_id"])
                        for m in raw
                        if isinstance(m, dict) and m.get("user_id") is not None
                    )

            # 新并集
            new_union: dict[int, set[str]] = {}
            for key, members in new_snapshot.items():
                try:
                    platform = int(key.split(":", 1)[0])
                except (ValueError, IndexError):
                    continue
                new_union.setdefault(platform, set()).update(members)

            # 并集差量：正确去重，减群/退群都不会误删多群共享成员
            changes: list[dict] = []
            for platform in _PLATFORMS:
                old = old_union.get(platform, set())
                new = new_union.get(platform, set())
                added = new - old
                removed = old - new
                if added:
                    changes.append(
                        {
                            "Action": ACTION_ADD,
                            "Platform": platform,
                            "Members": sorted(added),
                        }
                    )
                if removed:
                    changes.append(
                        {
                            "Action": ACTION_REMOVE,
                            "Platform": platform,
                            "Members": sorted(removed),
                        }
                    )

            # 更新快照
            self._snapshot = new_snapshot

        if not changes:
            return  # 无变化，不推送

        self.stats["changes"] += len(changes)
        await self._push(changes)

    async def submit_member_change(
        self,
        group_id: int | str,
        action: int,
        members: Iterable[int | str],
        platform: int = PLATFORM_QQ,
    ) -> None:
        """事件驱动的即时成员变更（进群/退群通知）。

        复用现成 EWhitelistPush(Type=3) 版本链上报，与轮询增量共用
        同一版本号序列，服务端无需新增命令码；同时更新本地快照，
        避免后续轮询重复上报。

        :param group_id: 群号/服务器ID（需在对应平台受管白名单内）
        :param action: ACTION_ADD(1) 或 ACTION_REMOVE(2)
        :param members: 成员 ID 列表（QQ 号 / Discord 用户ID）
        :param platform: 平台（PLATFORM_QQ=1 / PLATFORM_DISCORD=2）
        """
        if not self.enabled or self._sender is None:
            return
        if not self.is_managed(group_id, platform):
            return
        members_set = {str(m) for m in members if m is not None}
        if not members_set:
            return

        # 更新快照（与轮询共用 _lock，保证差异计算不冲突）。
        # Discord 受管条目可能是「服务器ID:频道ID」，事件按服务器ID 到达，
        # 因此把所有匹配条目（精确 + 前缀）的快照一并更新，避免后续轮询重复上报。
        keys = self._managed_keys(group_id, platform) or [
            self._snapshot_key(platform, group_id)
        ]
        changed = False
        async with self._lock:
            for key in keys:
                old = self._snapshot.get(key, frozenset())
                if action == ACTION_ADD:
                    new = old | members_set
                elif action == ACTION_REMOVE:
                    new = old - members_set
                else:
                    continue
                if new == old:
                    continue
                self._snapshot[key] = new
                changed = True
            if not changed:
                return  # 快照无变化，不上报

        change = {
            "Action": action,
            "Platform": platform,
            "Members": sorted(members_set),
        }
        await self._push([change])

    # ── 推送 / 快照 / 补包 ──────────────────────────────────

    async def _push(self, changes: list[dict], push_action: int = 0) -> None:
        """推送白名单变更（EWhitelistPush），并记录版本历史。

        push_action: 0=增量(EDelta), 1=覆盖(EOverride)，对齐服务端
        EWhitelistPushAction。消息不带 RequestId 字段：由 ws_client.send_no_wait
        自动附加。
        """
        if self._sender is None:
            self._log("未注入发送器，增量丢弃", "warning")
            return
        async with self._lock:
            base = self._version
            self._version += 1
            # 链头（首个历史版本）恒为覆盖块：首次推送用 Override 全量，
            # 保证 GC 剪头后仍可从链头全量对齐；其余版本为增量。
            if not self._history_order:
                push_action = 1
                self._chain_head_snapshot = self._snapshot_to_union()
            self._record_history(self._version, changes, push_action)
            msg = {
                "Type": CMD_WHITELIST_PUSH,
                "BaseVersion": base,
                "WhitelistPushAction": push_action,
                "Version": self._version,
                "Changes": changes,
            }
        async with self._send_lock:
            await self._sender(msg)
        self.stats["pushes"] += 1
        # 版本/快照/历史已更新：落盘，重启后从本版本续接
        await self._persist_state()
        self._log(
            f"增量推送 v{base} -> v{self._version}："
            f"{self._changes_summary(changes)}"
        )

    def _snapshot_to_union(self) -> dict[int, set[str]]:
        """当前快照 -> 平台并集（platform -> set[members]，去重）。"""
        union: dict[int, set[str]] = {}
        for key, members in self._snapshot.items():
            try:
                platform = int(key.split(":", 1)[0])
            except (ValueError, IndexError):
                continue
            union.setdefault(platform, set()).update(members)
        return union

    def _union_from_changes(self, changes: list[dict]) -> dict[int, set[str]]:
        """从全量 add changes 构建平台并集。"""
        union: dict[int, set[str]] = {}
        for ch in changes:
            platform = ch.get("Platform")
            if ch.get("Action") != ACTION_ADD or not isinstance(platform, int):
                continue
            union.setdefault(platform, set()).update(ch.get("Members", []))
        return union

    def _union_to_changes(self, union: dict[int, set[str]]) -> list[dict]:
        """平台并集 -> 全量 add changes。"""
        return [
            {"Action": ACTION_ADD, "Platform": p, "Members": sorted(m)}
            for p, m in sorted(union.items())
            if m
        ]

    def _apply_delta_to_union(self, union: dict[int, set[str]], changes: list[dict]) -> None:
        """把 changes（add/remove）正向应用到并集（GC 剪头推进链头快照用）。"""
        for ch in changes:
            platform = ch.get("Platform")
            if not isinstance(platform, int):
                continue
            members = ch.get("Members", [])
            if ch.get("Action") == ACTION_ADD:
                union.setdefault(platform, set()).update(members)
            elif ch.get("Action") == ACTION_REMOVE:
                if platform in union:
                    union[platform].difference_update(members)

    def reset_state(self) -> None:
        """重置本地群名单同步状态：版本归零、快照/历史清空、删除持久化文件。

        重置后插件从零开始：下一次轮询/快照请求将全量重建并推送。
        用于测试环境 master 重启/状态不一致时手动恢复。
        """
        self._version = 0
        self._snapshot = {}
        self._history = {}
        self._history_action = {}
        self._history_order = []
        self._chain_head_snapshot = {}
        self._baseline_ready = False
        for k in self.stats:
            self.stats[k] = 0
        path = self._state_path
        if path and os.path.exists(path):
            try:
                os.remove(path)
                self._log(f"已删除持久化文件: {path}", "info")
            except Exception as e:  # noqa: BLE001
                self._log(f"删除持久化文件失败: {type(e).__name__}: {e}", "warning")
        self._log("本地群名单同步状态已重置（v0）", "info")

    async def reset_and_rebuild(self) -> dict:
        """重置本地状态并立即全量重建推送（BaseVersion=0 → v1 全量 add）。

        步骤：清空持久化 → 重新拉取全部受管群成员 → 构建全量变更推送。
        返回 {"Version", "Total", "Pushed"} 供命令展示。
        """
        self.reset_state()
        # 1. 重新拉取全部受管群成员（填充快照）
        async with self._lock:
            for platform in _PLATFORMS:
                fetcher = self._member_fetchers.get(platform)
                for group_id in sorted(self._groups.get(platform, set())):
                    key = self._snapshot_key(platform, group_id)
                    raw: list = []
                    if fetcher is not None:
                        try:
                            raw = await fetcher(group_id)
                        except Exception as e:  # noqa: BLE001
                            self._log(
                                f"重建拉取 {_PLATFORM_NAMES.get(platform, platform)} "
                                f"{group_id} 失败: {type(e).__name__}: {e}",
                                "warning",
                            )
                    self._snapshot[key] = frozenset(
                        str(m["user_id"])
                        for m in raw
                        if isinstance(m, dict) and m.get("user_id") is not None
                    )
        # 2. 按平台合并生成全量 add 变更并推送（v0 -> v1）
        by_platform: dict[int, set[str]] = {p: set() for p in _PLATFORMS}
        async with self._lock:
            for platform in _PLATFORMS:
                for group_id in sorted(self._groups.get(platform, set())):
                    key = self._snapshot_key(platform, group_id)
                    by_platform[platform] |= self._snapshot.get(key, frozenset())
        changes = [
            {"Action": 1, "Platform": p, "Members": sorted(by_platform[p])}
            for p in _PLATFORMS
            if by_platform[p]
        ]
        if not changes:
            await self._persist_state()
            return {"Version": self._version, "Total": 0, "Pushed": False}
        # 全量重建用 Override（WhitelistPushAction=1），服务端一次性覆盖对齐，
        # 避免残留旧成员。
        await self._push(changes, push_action=1)
        total = sum(len(m) for m in by_platform.values())
        return {"Version": self._version, "Total": total, "Pushed": True}

    async def handle_latest_version_request(self) -> dict:
        """响应 master 的 EGetLatestVersion（Type=14）：只回版本号。

        新版 master 上线后会主动调用 EGetLatestVersion 获取客户端最新版本，
        若落后再发 EWhitelistDeltaRequest 补包。因此这里只需返回 { Version }，
        不构建完整快照（避免序列化大对象、避免服务端 5 秒 RPC 超时）。
        """
        async with self._lock:
            version = self._version
        # master 已建立版本关系，后续增量可推送
        self._baseline_ready = True
        self.stats["snapshots"] += 1
        self._log(f"响应版本查询 v{version}", "info")
        return {"Version": version}

    async def handle_snapshot_request(self) -> dict:
        """构建完整白名单快照数据（调试/手动用途）。

        新版 master 不再主动请求完整快照（Type=14 已改为 EGetLatestVersion，
        由 handle_latest_version_request 响应），本方法保留给 ds groupsync
        snapshot/full 等调试命令，以及需要显式查看快照的场景。

        返回 { "Whitelist": {"1": [...], "2": [...]}, "Version": v }，
        多群/多服务器成员按平台合并。只使用当前缓存，不触发网络拉取。
        """
        by_platform: dict[int, set[str]] = {p: set() for p in _PLATFORMS}
        async with self._lock:
            for platform in _PLATFORMS:
                for group_id in sorted(self._groups.get(platform, set())):
                    key = self._snapshot_key(platform, group_id)
                    by_platform[platform] |= self._snapshot.get(key, frozenset())
            whitelist = {
                str(p): sorted(by_platform[p]) for p in _PLATFORMS if by_platform[p]
            }
            data = {
                "Whitelist": whitelist,
                "Version": self._version,
            }
        # 快照回包后服务端会据此重置 LatestVersion，版本基线就绪
        self._baseline_ready = True
        self.stats["snapshots"] += 1
        total = sum(len(v) for v in by_platform.values())
        self._log(f"完整快照已构建 (v{self._version}，{total} 人，平台 {sorted(whitelist)})")
        return data

    async def handle_delta_request(self, from_version: int) -> dict:
        """响应 master 的补包请求（EWhitelistDeltaRequest，Type=15）。

        对齐 master PendingChain 追链语义（近→远逐段取回）：
        - FromVersion 在历史链中 → 返回该版本精确块（BaseVersion=链上前一项，
          链头则 BaseVersion=自身版本 + Override）；
        - FromVersion 不在（已被 GC）→ 返回链头覆盖块兜底（链头时刻全量快照），
          让 master ApplyOverride 对齐链头后再追后继增量。
        """
        try:
            from_version = int(from_version)
        except (TypeError, ValueError):
            from_version = 0

        async with self._lock:
            # 精确块：FromVersion 仍在历史链中
            if from_version in self._history:
                try:
                    idx = self._history_order.index(from_version)
                except ValueError:
                    idx = -1
                if idx >= 0:
                    # 链头（最早版本）必须返回 Override 覆盖块，否则 master 本地对不齐会死循环追链
                    if idx == 0:
                        head_changes = self._union_to_changes(self._chain_head_snapshot)
                        if not head_changes:
                            head_changes = self._union_to_changes(self._snapshot_to_union())
                        self._baseline_ready = True
                        self.stats["deltas"] += 1
                        self._log(
                            f"补包链头覆盖块 v{from_version}（Override）",
                            "info",
                        )
                        return {
                            "BaseVersion": from_version,
                            "WhitelistPushAction": 1,  # EOverride
                            "Version": from_version,
                            "Changes": head_changes,
                        }
                    base = self._history_order[idx - 1]
                    action = self._history_action.get(from_version, 0)
                    changes = self._history.get(from_version, [])
                    self._baseline_ready = True
                    self.stats["deltas"] += 1
                    self._log(
                        f"补包精确块 v{from_version}（Base={base}, Action={action}, "
                        f"{len(changes)} 项）",
                        "info",
                    )
                    return {
                        "BaseVersion": base,
                        "WhitelistPushAction": action,
                        "Version": from_version,
                        "Changes": changes,
                    }

            # 链头覆盖块兜底：链头（最早版本）是 Override，其 changes 即全量快照
            if self._history_order:
                head = self._history_order[0]
                head_changes = self._history.get(head, [])
                head_action = self._history_action.get(head, 0)
                if head_action == 1:
                    self._baseline_ready = True
                    self.stats["deltas"] += 1
                    self._log(
                        f"补包 FromVersion={from_version} 不在历史，回链头覆盖块 v{head}",
                        "info",
                    )
                    return {
                        "BaseVersion": head,
                        "WhitelistPushAction": 1,  # EOverride
                        "Version": head,
                        "Changes": head_changes,
                    }

            # 无历史或链头非覆盖块：以当前快照构建全量覆盖兜底
            by_platform: dict[int, set[str]] = {p: set() for p in _PLATFORMS}
            for platform in _PLATFORMS:
                for group_id in sorted(self._groups.get(platform, set())):
                    key = self._snapshot_key(platform, group_id)
                    by_platform[platform] |= self._snapshot.get(key, frozenset())
            version = self._version

        changes = [
            {"Action": ACTION_ADD, "Platform": p, "Members": sorted(by_platform[p])}
            for p in _PLATFORMS
            if by_platform[p]
        ]
        total = sum(len(m) for m in by_platform.values())
        self._baseline_ready = True
        self.stats["deltas"] += 1
        self._log(
            f"补包 FromVersion={from_version} 回全量覆盖 v{version}（{total} 人）",
            "info",
        )
        return {
            "BaseVersion": 0,
            "WhitelistPushAction": 1,  # EOverride
            "Version": version,
            "Changes": changes,
        }

    async def on_connected(self) -> None:
        """WebSocket（重）连接建立后调用：重置版本基线状态。

        服务端在 Bot 上线时会主动请求最新版本（Type=14 EGetLatestVersion），
        插件回包版本号后 _baseline_ready 置 True，增量才被允许推送。
        """
        if not self.enabled:
            return
        self._baseline_ready = False
        self._log("连接已建立，等待服务端版本查询以重建版本基线")

    # ── 内部工具 ────────────────────────────────────────────

    # ── 硬盘持久化：版本号 / 成员快照 / 差量历史 ─────────────

    def load_state(self) -> bool:
        """从硬盘加载上次保存的群名单状态（版本号/快照/差量历史）。

        成功返回 True；无文件、格式非法或校验失败返回 False（从零开始）。
        幂等：start() 只调用一次。
        """
        if not self._persist_enabled:
            return False
        path = self._state_path
        if not path or not os.path.exists(path):
            return False
        try:
            with open(path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if not isinstance(raw, dict) or raw.get("schema") != 1:
                self._log("持久化状态 schema 不匹配，忽略并从零开始", "warning")
                return False
            version = raw.get("version", 0)
            snapshot_raw = raw.get("snapshot", {})
            history_raw = raw.get("history", {})
            history_action_raw = raw.get("history_action", {})
            order_raw = raw.get("history_order", [])
            if (
                not isinstance(version, int)
                or version < 0
                or not isinstance(snapshot_raw, dict)
                or not isinstance(history_raw, dict)
            ):
                self._log("持久化状态字段非法，忽略并从零开始", "warning")
                return False

            snapshot: dict[str, frozenset[str]] = {}
            for gid, members in snapshot_raw.items():
                if isinstance(members, (list, tuple)):
                    key = str(gid)
                    # 旧版持久化键为裸群号（无平台前缀），归到 QQ 平台
                    if ":" not in key:
                        key = f"{PLATFORM_QQ}:{key}"
                    snapshot[key] = frozenset(str(m) for m in members)

            history: dict[int, list[dict]] = {}
            for v, changes in history_raw.items():
                try:
                    iv = int(v)
                except (TypeError, ValueError):
                    continue
                if isinstance(changes, list):
                    history[iv] = changes

            order: list[int] = []
            for v in order_raw:
                try:
                    iv = int(v)
                except (TypeError, ValueError):
                    continue
                if iv in history and iv not in order:
                    order.append(iv)
            order.sort()
            if not order and history:
                order = sorted(history.keys())

            history_action: dict[int, int] = {}
            for v, action in (history_action_raw or {}).items():
                try:
                    iv = int(v)
                except (TypeError, ValueError):
                    continue
                if isinstance(action, int) and action in (0, 1):
                    history_action[iv] = action
            # 兼容旧数据：无 action 记录时，链头（最小版本）标为覆盖块；
            # 旧版首次推送的差量即全量 add，语义等价于 Override。
            if order and history_action.get(order[0]) is None:
                history_action[order[0]] = 1

            self._version = version
            self._snapshot = snapshot
            self._history = history
            self._history_action = history_action
            self._history_order = order

            # 恢复链头快照，并保证链头恒为覆盖块（Override）。
            # 旧版数据链头可能是 Delta（导致 master 追链死循环），这里强制修复：
            # 若链头 action != 1，改写为 Override 并用当前快照重写其 changes。
            if order:
                head = order[0]
                head_action = history_action.get(head, 0)
                if head_action == 1 and history.get(head):
                    self._chain_head_snapshot = self._union_from_changes(history[head])
                else:
                    # 用当前快照兜底并修复链头为非覆盖块
                    self._chain_head_snapshot = self._snapshot_to_union()
                    history[head] = self._union_to_changes(self._chain_head_snapshot)
                    history_action[head] = 1
            else:
                self._chain_head_snapshot = {}

            self._log(
                f"已从硬盘加载群名单状态：version={version} "
                f"groups={len(snapshot)} history={len(order)}",
                "info",
            )
            return True
        except Exception as e:  # noqa: BLE001
            self._log(f"加载持久化状态失败（{type(e).__name__}: {e}），从零开始", "warning")
            return False

    def save_state(self) -> None:
        """同步保存当前状态到硬盘（stop/terminate 退出前调用）。"""
        if not self._persist_enabled:
            return
        try:
            self._write_state(self._state_payload())
        except Exception as e:  # noqa: BLE001
            self._log(f"保存持久化状态失败: {type(e).__name__}: {e}", "warning")

    async def _persist_state(self) -> None:
        """异步保存：锁内收集数据，锁外原子写盘。"""
        if not self._persist_enabled:
            return
        payload = None
        async with self._lock:
            payload = self._state_payload()
        if payload is not None:
            try:
                self._write_state(payload)
            except Exception as e:  # noqa: BLE001
                self._log(f"保存持久化状态失败: {type(e).__name__}: {e}", "warning")

    def _state_payload(self) -> dict:
        """当前状态序列化（JSON 可写；每个历史版本仅保存差量与推送动作）。"""
        return {
            "schema": 1,
            "version": self._version,
            "snapshot": {g: sorted(m) for g, m in self._snapshot.items()},
            "history": {str(v): ch for v, ch in self._history.items()},
            "history_action": {str(v): a for v, a in self._history_action.items()},
            "history_order": list(self._history_order),
            "saved_at": time.time(),
        }

    def _write_state(self, payload: dict) -> None:
        """原子写盘：先写临时文件再 os.replace，避免写一半损坏。"""
        path = self._state_path
        if not path:
            return
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        self.stats["persists"] += 1

    def _record_history(self, version: int, changes: list[dict], action: int = 0) -> None:
        """记录版本变更历史（含推送动作），并按 retention_versions 清理最旧版本。

        关键不变量：链头（_history_order[0]）恒为覆盖块（WhitelistPushAction=1）。
        GC 剪头后把新链头改写为 Override，且用「推进后的链头快照」重写其 changes，
        保证 master 追链追到链头时能拿到全量覆盖、不会因 Delta 对不齐而死循环。
        """
        self._history[version] = changes
        self._history_action[version] = action
        self._history_order.append(version)
        while len(self._history_order) > self.retention_versions:
            oldest = self._history_order.pop(0)
            old_changes = self._history.pop(oldest, None)
            self._history_action.pop(oldest, None)
            # 被剪块的差量叠加到链头快照，使新链头代表「剪头后的全量」
            if self._chain_head_snapshot and old_changes:
                self._apply_delta_to_union(self._chain_head_snapshot, old_changes)
            # 新链头改写为 Override，changes = 推进后的链头快照
            if self._history_order:
                new_head = self._history_order[0]
                self._history[new_head] = self._union_to_changes(self._chain_head_snapshot)
                self._history_action[new_head] = 1

    def _changes_summary(self, changes: list[dict]) -> str:
        parts = []
        for ch in changes:
            action = ch.get("Action")
            if isinstance(action, int):
                action = _ACTION_NAMES.get(action, str(action))
            platform = ch.get("Platform", "?")
            if isinstance(platform, int):
                platform = _PLATFORM_NAMES.get(platform, platform)
            parts.append(f"{action}({platform})x{len(ch.get('Members', []))}")
        return ", ".join(parts) if parts else "无"

    def get_snapshot(self) -> dict[str, list[str]]:
        """返回当前快照（调试/统计用）。"""
        return {g: sorted(m) for g, m in self._snapshot.items()}

    def dump_stats(self) -> str:
        return (
            f"enabled={self.enabled} groups={sorted(self.groups)} "
            f"version={self._version} history={len(self._history_order)} "
            f"persisted={'on' if self._persist_enabled else 'off'} "
            f"loaded={self._state_loaded} "
            f"polls={self.stats['polls']} pushes={self.stats['pushes']} "
            f"snapshots={self.stats['snapshots']} deltas={self.stats['deltas']} "
            f"changes={self.stats['changes']} persists={self.stats['persists']} "
            f"failures={self.stats['poll_failures']}"
        )

    def _log(self, text: str, level: str = "info") -> None:
        if self.logger is None:
            return
        try:
            if level == "warning":
                self.logger.warning(f"[GroupSync] {text}")
            else:
                self.logger.info(f"[GroupSync] {text}")
        except Exception:
            pass

    @property
    def logger(self):
        return getattr(self, "_logger", None)

    @logger.setter
    def logger(self, value):
        self._logger = value
