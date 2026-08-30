"""
astrbot_plugin_notice_server - NoticeServer / BotNotice WebSocket 封装插件

适配 dscontrol 指令体系（ds 前缀、功能与参数完全对齐）
对接 BotNotice /ws/bot WebSocket 命令通道（UUID 封禁、控制台命令、验证回调等）
群聊白名单机制保持不变
"""
import asyncio
import datetime
import os
import re
import time

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star
from astrbot.core.config.astrbot_config import AstrBotConfig
from astrbot.core.message.components import Node, Nodes, Plain
from astrbot.core.message.message_event_result import MessageChain

from .group_sync import GroupSyncManager, PLATFORM_DISCORD, PLATFORM_QQ
from .member_event_sync import MemberEventSyncManager
from .ws_client import NoticeServerWS, EBotCommand


class NoticeServerPlugin(Star):
    """NoticeServer HTTP API 封装插件"""

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.server_host = config.get("server_host", "127.0.0.1")
        self.server_port = config.get("server_port", 11470)
        self.api_key = config.get("api_key", "")
        self.bot_id = config.get("bot_id", "botA")
        self.ws_token = config.get("ws_token", "") or config.get("api_key", "")
        self.timeout_sec = config.get("timeout", 10)
        self.group_whitelist = config.get("group_whitelist", [])
        self.group_user_whitelist = config.get("group_user_whitelist", [])
        self.friend_whitelist = config.get("friend_whitelist", [])
        # 全量白名单（基础功能：验证码 / ds version / ds servers 的群级门槛）
        self.full_group_whitelist = config.get("full_group_whitelist", [])
        self.discord_full_group_whitelist = config.get("discord_full_group_whitelist", [])
        # Discord 名单与 QQ 名单相互独立（旧版混在 QQ 名单里的 discord: 前缀条目自动剥离迁移）
        qq_wl, dc_wl = self._extract_discord_entries(self.group_whitelist)
        self.group_whitelist = qq_wl
        self.discord_group_whitelist = config.get("discord_group_whitelist", []) or dc_wl
        qq_uwl, dc_uwl = self._extract_discord_entries(self.group_user_whitelist)
        self.group_user_whitelist = qq_uwl
        self.discord_group_user_whitelist = config.get("discord_group_user_whitelist", []) or dc_uwl
        qq_fwl, dc_fwl = self._extract_discord_entries(self.friend_whitelist)
        self.friend_whitelist = qq_fwl
        self.discord_friend_whitelist = config.get("discord_friend_whitelist", []) or dc_fwl
        qq_bwl, dc_bwl = self._extract_discord_entries(config.get("group_blacklist", []))
        self.group_blacklist = qq_bwl
        self.discord_group_blacklist = config.get("discord_group_blacklist", []) or dc_bwl
        self.discord_sync_guilds = config.get("discord_sync_guilds", [])
        self.max_inline_items = config.get("max_inline_items", 7)
        self.auth_control = config.get("auth_control", True)
        self._pending_confirms = {}  # {f"{gid}:{uid}": {"type": "ban"/"unban", "data": {...}, "ts": ...}}
        self._rate_limits = {}        # {cmd_name: {user_key: timestamp}} 限频记录
        self.rate_limit_window = 300  # 5分钟冷却窗口（秒）
        self._verify_fail_tip_cooldown = {}  # 验证码失败「手机端提示」冷却：{user_key: timestamp}（30分钟）
        self._verify_fail_tip_window = 1800  # 验证码失败提示冷却窗口（秒）
        self.group_blacklist = config.get("group_blacklist", [])
        self._confirm_timeout = 40   # 确认超时秒数
        self._select_timeout = 60    # 序号选择超时秒数
        self._timeout_tasks = {}     # {cache_key: asyncio.Task} 后台超时计时器
        self._tag_cache = {}         # {ServerId: Tag} 服务器名缓存（10秒）
        self._tag_cache_ts = 0.0     # 服务器名缓存刷新时间戳

        # WebSocket 长连接客户端（替代原 HTTP 请求，自动重连 + 心跳保活）
        self.ws_client = NoticeServerWS(
            host=self.server_host,
            port=self.server_port,
            bot_id=self.bot_id,
            token=self.ws_token,
            timeout=self.timeout_sec,
            logger=logger,
        )

        # 【群名单同步】受管群名单定期上报 MasterServer（GroupWhitelist）
        # 语义：轮询受管群成员列表，对比差异推送增量；响应快照/补包请求。
        # 持久化：版本号/成员快照/差量历史落盘到 JSON，重启后从上次版本续接。
        self.group_sync = GroupSyncManager(
            groups=config.get("group_sync_groups", []),
            poll_interval=config.get("group_sync_interval", 300),
            retention_versions=config.get("group_sync_retention", 50),
            enabled=bool(config.get("group_sync_enabled", False)),
            state_path=(
                config.get("group_sync_state_file")
                or os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    "data",
                    "group_sync_state.json",
                )
            ),
            persist=bool(config.get("group_sync_persist", True)),
            logger=logger,
        )
        self.group_sync.set_member_fetcher(
            self._make_group_sync_fetcher(PLATFORM_QQ), PLATFORM_QQ
        )
        self.group_sync.set_member_fetcher(
            self._make_group_sync_fetcher(PLATFORM_DISCORD), PLATFORM_DISCORD
        )
        self.group_sync.set_sender(self._make_group_sync_sender())
        # Discord 服务器名单同步（可选）：与 QQ 群共用 EWhitelistPush 版本链
        self.group_sync.set_platform_groups(PLATFORM_DISCORD, self.discord_sync_guilds)
        # 服务端主动推送回调（版本查询 / 补包请求 / 连接建立）
        self.ws_client.on_snapshot_request = self.group_sync.handle_latest_version_request
        self.ws_client.on_delta_request = self.group_sync.handle_delta_request
        self.ws_client.on_connected = self._on_ws_connected
        self._cached_call_action = None  # 后台任务用的 bot API（无 event 上下文）

        # 【群成员事件同步】进群/退群通知批量上报 MasterServer
        # 语义：监听 OneBot notice 事件（group_decrease/group_increase），
        # 防抖累积（静默 1 秒 / 最长 5 秒）后批量上报；受管群沿用 group_sync_groups。
        # 上报通道：复用现成 EWhitelistPush(Type=3) 群白名单增量推送版本链，
        # 无需服务端新增命令码。
        self.member_event_sync = MemberEventSyncManager(
            groups=config.get("group_sync_groups", []),
            quiet_window=config.get("member_event_quiet_window", 1),
            max_window=config.get("member_event_max_window", 5),
            enabled=bool(config.get("member_event_sync_enabled", False)),
            logger=logger,
        )
        self.member_event_sync.set_handler(self._make_member_event_handler())
        # Discord 服务器成员事件同步（可选）：受管范围沿用 discord_sync_guilds
        # （服务器ID 或 服务器ID:频道ID），与 group_sync 轮询互补，事件驱动即时增量。
        self.member_event_sync.set_platform_groups(
            PLATFORM_DISCORD, self.discord_sync_guilds
        )
        # Discord 成员事件监听（add_listener 挂载，等待客户端就绪后执行）
        self._discord_attach_task = None
        self._discard_guard = False

    async def initialize(self) -> None:
        """插件加载完成：启动 WebSocket 长连接、群名单同步与成员事件同步后台任务。"""
        try:
            # 主动拉起 WebSocket 长连接（ws_client 为惰性启动：
            # 只有首次 request/send_no_wait 才会创建后台连接任务。
            # 若不在此处主动拉起，重载插件后必须调用任意 ds 指令才会连接服务器）
            await self.ws_client.ensure_started()
        except Exception as e:
            logger.warning(f"[NoticeServerWS] 启动连接失败: {e}")
        try:
            await self.group_sync.start()
        except Exception as e:
            logger.warning(f"[GroupSync] 启动失败: {e}")
        try:
            await self.member_event_sync.start()
        except Exception as e:
            logger.warning(f"[MemberEventSync] 启动失败: {e}")
        # Discord 成员事件监听：等待 Discord 客户端就绪后挂载 add_listener
        # （on_member_join/on_member_remove → MemberEventSync → GroupSync 版本链上报）
        try:
            self._discord_attach_task = asyncio.create_task(
                self._attach_discord_member_listeners()
            )
        except Exception as e:
            logger.warning(f"[DiscordSync] 挂载监听任务启动失败: {e}")

    async def terminate(self) -> None:
        """插件禁用/重载时关闭 WebSocket 连接与后台任务"""
        self._discard_guard = True
        if self._discord_attach_task is not None:
            self._discord_attach_task.cancel()
            try:
                await self._discord_attach_task
            except (asyncio.CancelledError, Exception):
                pass
            self._discord_attach_task = None
        try:
            await self.group_sync.stop()
        except Exception as e:
            logger.warning(f"[GroupSync] 停止失败: {e}")
        try:
            await self.member_event_sync.stop()
        except Exception as e:
            logger.warning(f"[MemberEventSync] 停止失败: {e}")
        try:
            await self.ws_client.close()
        except Exception as e:
            logger.warning(f"[NoticeServerWS] 关闭连接失败: {e}")

    # ─── 权限检查 ─────────────────────────────────────────────

    def _match_list(self, entries, platform: str, *ids) -> bool:
        """白名单匹配：支持「平台:ID」前缀格式，无前缀则任意平台匹配。

        entries: 白名单列表
        platform: 事件平台名（如 aiocqhttp / discord）
        ids: 依次拼接的标识（私聊传 sender_id，群聊传 gid, sender_id）
        """
        if not entries:
            return False
        plain = ":".join(str(i) for i in ids)
        prefixed = f"{platform}:{plain}"
        return plain in entries or prefixed in entries

    @staticmethod
    def _extract_discord_entries(entries) -> tuple[list, list]:
        """把旧版混在 QQ 名单里的「discord:xxx」前缀条目剥离迁移。

        返回 (QQ条目列表, Discord条目列表)；Discord 条目去掉 discord: 前缀。
        """
        qq, dc = [], []
        for e in entries or []:
            s = str(e).strip()
            if s.lower().startswith("discord:"):
                dc.append(s.split(":", 1)[1])
            elif s:
                qq.append(s)
        return qq, dc

    def _get_discord_scope(self, event) -> tuple[str, str]:
        """获取 Discord 事件的 (服务器ID, 频道ID)。

        AstrBot Discord 事件的 group_id 是频道ID（channel.id），
        服务器ID 需从底层 py-cord 消息对象取 raw_message.guild.id。
        私聊/取不到服务器时返回 ("", 频道ID)。
        """
        channel_id = str(event.get_group_id() or "")
        raw = getattr(getattr(event, "message_obj", None), "raw_message", None)
        guild = getattr(raw, "guild", None)
        guild_id = str(getattr(guild, "id", "")) if guild is not None else ""
        return guild_id, channel_id

    def _match_discord_scope(
        self, entries, guild_id: str, channel_id: str, member_roles=None
    ) -> bool:
        """Discord 范围名单匹配：条目 = 服务器ID | 服务器ID:频道ID | 服务器ID:身份组ID。

        - 条目为服务器ID → 该服务器内所有频道命中
        - 条目为 服务器ID:频道ID → 仅该频道命中
        - 条目为 服务器ID:身份组ID → 成员持有该身份组即命中（member_roles 为身份组ID集合）
        """
        if not entries or not guild_id:
            return False
        if guild_id in entries:
            return True
        if channel_id and f"{guild_id}:{channel_id}" in entries:
            return True
        if member_roles:
            prefix = f"{guild_id}:"
            for e in entries:
                if e.startswith(prefix) and e[len(prefix):] in member_roles:
                    return True
        return False

    def _match_discord_user(self, entries, guild_id: str, channel_id: str, user_id: str) -> bool:
        """Discord 群聊-个人名单匹配：条目 = 服务器ID:用户ID 或 服务器ID:频道ID:用户ID。"""
        if not entries or not guild_id or not user_id:
            return False
        if f"{guild_id}:{user_id}" in entries:
            return True
        if channel_id and f"{guild_id}:{channel_id}:{user_id}" in entries:
            return True
        return False

    def _discord_sync_role_ids(self, guild_id: str) -> set[str] | None:
        """discord_sync_guilds 中该服务器的身份组条目集合（服务器ID:身份组ID）。

        - 该服务器存在「服务器ID」级条目 → None（服务器级受管，全部成员不过滤）
        - 否则收集「服务器ID:身份组ID」条目为身份组集合（用于成员加入事件过滤）
        - 无任何条目 → None（不过滤）
        """
        if not self.discord_sync_guilds:
            return None
        guild_id = str(guild_id or "")
        roles: set[str] = set()
        has_role_entry = False
        for e in self.discord_sync_guilds:
            s = str(e).strip()
            if s == guild_id:
                return None  # 服务器级受管条目：全部成员
            if s.startswith(guild_id + ":"):
                rid = s[len(guild_id) + 1:].strip()
                if rid:
                    has_role_entry = True
                    roles.add(rid)
        return roles if has_role_entry else None

    def _get_discord_member_roles(self, event) -> set[str]:
        """获取 Discord 发送者在当前服务器的身份组ID集合（群聊）；私聊/取不到返回空集。"""
        raw = getattr(getattr(event, "message_obj", None), "raw_message", None)
        author = getattr(raw, "author", None)
        return {str(r.id) for r in getattr(author, "roles", []) or []}

    def _check_discord_access(self, event, guild_id: str, channel_id: str, sender_id: str) -> bool:
        """Discord 平台权限检查（与 QQ 名单相互独立）。

        群聊白名单支持身份组条目（服务器ID:身份组ID）：
        持有该服务器白名单身份组的成员直接放行。
        """
        member_roles = self._get_discord_member_roles(event)
        # Discord 群聊白名单：服务器ID | 服务器ID:频道ID | 服务器ID:身份组ID
        if guild_id:
            if self._match_discord_scope(
                self.discord_group_whitelist, guild_id, channel_id, member_roles
            ):
                logger.info(
                    f"[access] discord guild={guild_id} channel={channel_id} "
                    f"在 discord_group_whitelist 中，允许"
                )
                return True
            if self._match_discord_user(self.discord_group_user_whitelist, guild_id, channel_id, sender_id):
                logger.info(
                    f"[access] discord guild={guild_id} channel={channel_id} user={sender_id} "
                    f"在 discord_group_user_whitelist 中，允许"
                )
                return True
            logger.info(f"[access] discord 群聊 guild={guild_id} channel={channel_id} user={sender_id} 无匹配白名单，拒绝")
            return False
        # Discord 私聊（无服务器上下文）：discord_friend_whitelist 按用户ID
        if sender_id in self.discord_friend_whitelist:
            logger.info(f"[access] discord 私聊 user={sender_id} 在 discord_friend_whitelist 中，允许")
            return True
        logger.info(f"[access] discord 私聊 user={sender_id} 不在 discord_friend_whitelist，拒绝")
        return False

    def _check_access(self, event: AstrMessageEvent) -> bool:
        """检查权限。

        QQ 平台：
          1. group_whitelist（群聊白名单）：填入群号，群内所有人都可用
          2. group_user_whitelist（群聊-个人白名单）：格式 群号:QQ号，指定群内只有指定用户可用
          3. friend_whitelist（私聊白名单）：填入QQ号，私聊可用
        Discord 平台：使用独立的 discord_group_whitelist / discord_group_user_whitelist /
          discord_friend_whitelist，条目为服务器ID / 服务器ID:频道ID 格式。

        管理员不受任何白名单限制。
        """
        # 管理员（主人）无条件放行
        if event.is_admin():
            logger.info(f"[access] sender={event.get_sender_id()} 是管理员，允许")
            return True

        gid = event.get_group_id()
        sender_id = str(event.get_sender_id())
        platform = event.get_platform_name()

        # Discord 平台走独立名单
        if platform == "discord":
            guild_id, channel_id = self._get_discord_scope(event)
            return self._check_discord_access(event, guild_id, channel_id, sender_id)

        if not gid:
            # 私聊（gid 为 None 或 0）：检查 friend_whitelist
            in_friend = self._match_list(self.friend_whitelist, platform, sender_id)
            logger.info(f"[access] 私聊 platform={platform} sender_id={sender_id} friend_whitelist={self.friend_whitelist} => {in_friend}")
            return in_friend

        # 群聊
        gid_str = str(gid)
        key = f"{gid_str}:{sender_id}"

        # 所有白名单都为空 → 不限制（向后兼容）
        all_empty = (
            not self.group_whitelist
            and not self.group_user_whitelist
            and not self.friend_whitelist
        )
        if all_empty:
            logger.info("[access] 所有白名单为空，允许")
            return True

        # 1) 群聊白名单：群号在白名单 → 群内所有人可用
        if self._match_list(self.group_whitelist, platform, gid_str):
            logger.info(f"[access] 群聊 platform={platform} gid={gid_str} 在 group_whitelist 中，允许")
            return True

        # 2) 群聊-个人白名单：群号:QQ号 精确匹配
        if self._match_list(self.group_user_whitelist, platform, gid_str, sender_id):
            logger.info(f"[access] 群聊 platform={platform} {key} 在 group_user_whitelist 中，允许")
            return True

        logger.info(f"[access] 群聊 gid={gid_str} sender={sender_id} 无匹配白名单，拒绝")
        return False

    def _is_blacklisted_group(self, event: AstrMessageEvent) -> bool:
        """检查来源群是否在群黑名单中。

        QQ 平台：group_blacklist（QQ群号）；Discord 平台：discord_group_blacklist
        （服务器ID 或 服务器ID:频道ID）。命中黑名单的群聊，所有指令完全静默忽略；
        私聊不受黑名单影响。
        """
        gid = event.get_group_id()
        if not gid:
            return False  # 私聊不受黑名单影响
        platform = event.get_platform_name()
        if platform == "discord":
            guild_id, channel_id = self._get_discord_scope(event)
            if not guild_id:
                return False
            return self._match_discord_scope(self.discord_group_blacklist, guild_id, channel_id)
        return self._match_list(self.group_blacklist, platform, str(gid))

    def _check_basic_access(self, event: AstrMessageEvent) -> bool:
        """基础功能权限（验证码 / ds version / ds servers）与插件可用范围。

        - 管理员：放行
        - 私聊 / 临时会话：放行（默认开放无需鉴权指令）
        - QQ 群聊：命中全量白名单（full_group_whitelist）或管理组（group_whitelist）才放行
        - Discord 群聊：命中 discord_full_group_whitelist / discord_group_whitelist
          （支持 服务器ID / 服务器ID:频道ID / 服务器ID:身份组ID）才放行
        """
        if event.is_admin():
            return True
        gid = event.get_group_id()
        sender_id = str(event.get_sender_id())
        platform = event.get_platform_name()

        # Discord 平台
        if platform == "discord":
            if not gid:
                return True  # Discord 私聊 DM
            guild_id, channel_id = self._get_discord_scope(event)
            member_roles = self._get_discord_member_roles(event)
            if self._match_discord_scope(
                self.discord_full_group_whitelist, guild_id, channel_id, member_roles
            ):
                return True
            return self._match_discord_scope(
                self.discord_group_whitelist, guild_id, channel_id, member_roles
            )

        # QQ 及其他平台：私聊 / 临时会话放行
        if not gid:
            return True
        gid_str = str(gid)
        if self._match_list(self.full_group_whitelist, platform, gid_str):
            return True
        return self._match_list(self.group_whitelist, platform, gid_str)

    def _check_verify_access(self, event: AstrMessageEvent) -> bool:
        """验证码使用场景校验：私聊/临时会话放行；群聊须命中全量白名单或管理组。"""
        return self._check_basic_access(event)

    # ─── 频率限制 ────────────────────────────────────────────

    def _get_user_key(self, event: AstrMessageEvent) -> str:
        """获取用户标识 key（用于限频和白名单匹配）"""
        gid = event.get_group_id()
        if gid:
            return f"{gid}:{event.get_sender_id()}"
        return f"private:{event.get_sender_id()}"

    def _is_in_whitelist(self, event: AstrMessageEvent) -> bool:
        """仅检查白名单（不含管理员判断），兼容所有白名单为空时放行"""
        gid = event.get_group_id()
        sender_id = str(event.get_sender_id())
        platform = event.get_platform_name()

        if platform == "discord":
            guild_id, channel_id = self._get_discord_scope(event)
            return self._check_discord_access(event, guild_id, channel_id, sender_id)

        if not gid:
            return self._match_list(self.friend_whitelist, platform, sender_id)

        gid_str = str(gid)

        all_empty = (
            not self.group_whitelist
            and not self.group_user_whitelist
            and not self.friend_whitelist
        )
        if all_empty:
            return True
        if self._match_list(self.group_whitelist, platform, gid_str):
            return True
        if self._match_list(self.group_user_whitelist, platform, gid_str, sender_id):
            return True
        return False

    def _check_rate_limit(self, event: AstrMessageEvent, cmd_name: str) -> tuple[bool, str]:
        """频率限制检查。

        auth_control=True 时跳过限频（由外部鉴权兜底）。
        auth_control=False 时：管理员/白名单不限频，其余每5分钟1次。

        Returns:
            (passed, message): passed=True 表示放行
        """
        if self.auth_control:
            return True, ""

        if event.is_admin():
            return True, ""

        if self._is_in_whitelist(event):
            return True, ""

        user_key = self._get_user_key(event)
        now = time.time()

        if cmd_name not in self._rate_limits:
            self._rate_limits[cmd_name] = {}

        # 清理过期记录（超过窗口的数据扔掉，防止内存膨胀）
        cutoff = now - self.rate_limit_window
        self._rate_limits[cmd_name] = {
            k: v for k, v in self._rate_limits[cmd_name].items() if v >= cutoff
        }

        last_ts = self._rate_limits[cmd_name].get(user_key)
        if last_ts is not None and (now - last_ts) < self.rate_limit_window:
            remaining = int(self.rate_limit_window - (now - last_ts))
            return False, ""

        self._rate_limits[cmd_name][user_key] = now
        return True, ""

    # ─── 群成员身份校验（迁移自消息缓冲 v1.9.0） ─────────────

    def _get_call_action(self, event: AstrMessageEvent):
        """从事件中取出 OneBot call_action 可调用对象（兼容 bot.api / bot 两种形态）"""
        bot = getattr(event, "bot", None)
        api = getattr(bot, "api", None)
        call_action = getattr(api, "call_action", None)
        if callable(call_action):
            self._cached_call_action = call_action
            return call_action
        call_action = getattr(bot, "call_action", None)
        if callable(call_action):
            self._cached_call_action = call_action
            return call_action
        return None

    # ── 群名单同步：后台成员拉取与发送 ───────────────────────

    def _make_group_sync_fetcher(self, platform: int = PLATFORM_QQ):
        """为 GroupSyncManager 构造成员拉取器（后台任务无 event，用缓存 API）。

        :param platform: PLATFORM_QQ 走 OneBot get_group_member_list；
                         PLATFORM_DISCORD 走 py-cord guild.fetch_members()
        """

        if platform == PLATFORM_DISCORD:
            return self._make_discord_member_fetcher()

        async def _fetch(group_id: str) -> list[dict]:
            call_action = self._cached_call_action
            if call_action is None:
                # 兜底：尝试从平台适配器拿 API（aiocqhttp / qq 等）
                call_action = self._find_platform_call_action()
            if call_action is None:
                logger.warning("[GroupSync] 无法获取 bot API，跳过群成员拉取")
                raise RuntimeError("无法获取 bot API，暂不更新该群快照")
            try:
                payload = await call_action(
                    "get_group_member_list", group_id=group_id, no_cache=False
                )
            except TypeError:
                payload = await call_action("get_group_member_list", group_id=group_id)
            except Exception as e:
                logger.warning(f"[GroupSync] 拉取群 {group_id} 成员失败: {e}")
                raise
            if not isinstance(payload, list):
                raise RuntimeError("get_group_member_list 返回非 list")
            return [m for m in payload if isinstance(m, dict)]

        return _fetch

    def _make_discord_member_fetcher(self):
        """为 GroupSyncManager 构造 Discord 平台成员拉取器。

        受管条目格式（discord_sync_guilds）：
        - 服务器ID → 拉取该服务器全部成员
        - 服务器ID:身份组ID → 只拉取持有该身份组的成员（按身份组过滤）
        Members 只含玩家唯一平台ID（Discord 用户ID），对齐 MasterServer 的
        FWhitelistChange.Members「平台账号唯一ID」语义（服务端
        IsInWhitelistedGroups 按 PlatformUniqueId 查表判定）。
        """

        async def _fetch(entry: str) -> list[dict]:
            entry = str(entry).strip()
            if not entry:
                raise RuntimeError("Discord 受管条目为空")
            # 条目可能是「服务器ID」或「服务器ID:身份组ID」，取服务器ID 与可选身份组ID
            parts = entry.split(":", 1)
            guild_id = parts[0].strip()
            role_id = parts[1].strip() if len(parts) > 1 and parts[1].strip() else None
            if not guild_id.isdigit():
                logger.warning(f"[GroupSync] Discord 受管条目格式非法: {entry}")
                raise RuntimeError(f"Discord 受管条目格式非法: {entry}")
            client = self._find_discord_client()
            if client is None:
                logger.warning("[GroupSync] Discord 适配器不可用，跳过服务器成员拉取")
                raise RuntimeError("Discord 适配器不可用，暂不更新该服务器快照")
            try:
                gid = int(guild_id)
                guild = client.get_guild(gid)
                if guild is None:
                    guild = await client.fetch_guild(gid)
                if guild is None:
                    logger.warning(f"[GroupSync] Discord 服务器 {guild_id} 不存在或 Bot 未加入")
                    raise RuntimeError(f"Discord 服务器 {guild_id} 不存在或 Bot 未加入")
                members: list[dict] = []
                async for m in guild.fetch_members(limit=None):
                    if m.bot:
                        continue
                    if role_id:
                        m_roles = {str(r.id) for r in getattr(m, "roles", []) or []}
                        if role_id not in m_roles:
                            continue  # 未持有该身份组，不纳入白名单
                    members.append({"user_id": str(m.id)})
                logger.info(
                    f"[GroupSync] Discord 服务器 {guild_id} 拉取到 {len(members)} 名成员"
                    + (f"（身份组 {role_id}）" if role_id else "（全部成员）")
                )
                return members
            except Exception as e:
                logger.warning(
                    f"[GroupSync] 拉取 Discord 服务器 {guild_id} 成员失败: "
                    f"{type(e).__name__}: {e}"
                )
                raise

        return _fetch

    def _find_discord_adapter(self):
        """从已加载的平台适配器中查找 Discord 适配器实例（尽力而为）。"""
        try:
            insts = getattr(self.context, "platform_manager", None)
            if insts is None:
                return None
            for platform in getattr(insts, "platform_insts", []) or []:
                try:
                    name = platform.meta().name
                except Exception:
                    name = None
                if name == "discord":
                    return platform
            return None
        except Exception as e:
            logger.debug(f"[DiscordSync] 查找 Discord 适配器失败: {e}")
            return None

    def _find_discord_client(self):
        """获取 Discord 适配器的 py-cord 客户端（未就绪返回 None）。"""
        adapter = self._find_discord_adapter()
        if adapter is None:
            return None
        try:
            return getattr(adapter, "client", None)
        except Exception:
            return None

    def _find_platform_call_action(self):
        """从已加载的平台适配器实例中寻找可用的 call_action（尽力而为）。"""
        try:
            insts = getattr(self.context, "platform_manager", None)
            if insts is None:
                return None
            for platform in getattr(insts, "platform_insts", []) or []:
                try:
                    client = platform.get_client()
                except Exception:
                    client = None
                if client is None:
                    continue
                ca = getattr(client, "call_action", None)
                if callable(ca):
                    self._cached_call_action = ca
                    return ca
        except Exception as e:
            logger.debug(f"[GroupSync] 查找平台 API 失败: {e}")
        return None

    def _make_group_sync_sender(self):
        """为 GroupSyncManager 构造单向发送器（发完不等响应）。"""

        async def _send(msg: dict) -> None:
            command = msg.get("Type", 0)
            payload = {k: v for k, v in msg.items() if k != "Type"}
            ok = await self.ws_client.send_no_wait(command, payload)
            if not ok:
                logger.warning(f"[GroupSync] 发送失败 Type={command}")

        return _send

    def _make_member_event_handler(self):
        """为 MemberEventSyncManager 构造事件处理器：桥接进 GroupSync 版本链。

        复用现成 EWhitelistPush(Type=3) 群白名单增量推送接口：
        按「平台+群」聚合事件，作为 add/remove 增量注入 GroupSyncManager，
        与服务端现有协议完全兼容，无需服务端新增命令码。
        """
        from .group_sync import ACTION_ADD, ACTION_REMOVE

        async def _handle(kind: str, events: list[dict]) -> None:
            action = ACTION_REMOVE if kind == "decrease" else ACTION_ADD
            # (platform, gid) -> [uid, ...]
            by_group: dict[tuple[int, str], list[str]] = {}
            for ev in events:
                gid = str(ev.get("GroupId", "") or "")
                uid = str(ev.get("UserId", "") or "")
                platform = ev.get("Platform", PLATFORM_QQ)
                if not gid or not uid:
                    continue
                by_group.setdefault((platform, gid), []).append(uid)
            for (platform, gid), uids in by_group.items():
                await self.group_sync.submit_member_change(
                    gid, action, uids, platform=platform
                )

        return _handle

    async def _on_ws_connected(self) -> None:
        """WS 连接建立/重连后：重建群同步版本基线。

        服务端在 Bot 上线时会主动请求最新版本（Type=14 EGetLatestVersion），
        插件用缓存回包版本号（handle_latest_version_request 不实时拉取，避免超时）；
        服务端落后时再发补包（Type=15），插件返回 Override 全量块一次对齐。
        因此无需主动全量推送，避免重复占用带宽。
        """
        try:
            await self.group_sync.on_connected()
        except Exception as e:
            logger.warning(f"[GroupSync] 连接后重建基线失败: {e}")

    # ── Discord 成员事件监听（add_listener 挂载） ────────────

    async def _attach_discord_member_listeners(self) -> None:
        """等待 Discord 客户端就绪后挂载 on_member_join/on_member_remove 监听。

        Discord 适配器启动时机不定（adapter.run 中才创建 client），
        这里轮询等待最多 60 秒；未配置受管服务器时静默退出。
        """
        if not self.discord_sync_guilds:
            return
        for _ in range(30):
            if self._closed_flag():
                return
            client = self._find_discord_client()
            if client is not None:
                try:
                    client.add_listener(
                        self._on_discord_member_join, "on_member_join"
                    )
                    client.add_listener(
                        self._on_discord_member_remove, "on_member_remove"
                    )
                    logger.info(
                        "[DiscordSync] 已挂载 Discord 成员事件监听 "
                        f"(guilds={sorted(self.discord_sync_guilds)})"
                    )
                except Exception as e:
                    logger.warning(f"[DiscordSync] 挂载 Discord 监听失败: {e}")
                return
            try:
                await asyncio.sleep(2)
            except asyncio.CancelledError:
                return
        logger.warning("[DiscordSync] 等待 Discord 客户端就绪超时，跳过成员事件监听")

    def _closed_flag(self) -> bool:
        """插件是否已进入终止流程（尽力判断）。"""
        try:
            return self._discard_guard
        except Exception:
            return False

    async def _on_discord_member_join(self, member) -> None:
        await self._handle_discord_member_event("increase", member)

    async def _on_discord_member_remove(self, member) -> None:
        await self._handle_discord_member_event("decrease", member)

    async def _handle_discord_member_event(self, kind: str, member) -> None:
        """处理 Discord 成员加入/离开事件，桥接进 MemberEventSync 批量窗口。

        受管范围沿用 discord_sync_guilds（服务器ID 或 服务器ID:频道ID），
        事件按服务器ID 匹配（前缀匹配命中频道级条目）；与 group_sync 轮询互补，
        复用 EWhitelistPush(Type=3) 版本链上报，服务端无需新增命令码。

        :param kind: "increase"（加入） / "decrease"（离开）
        :param member: py-cord Member 对象（含 guild/id/bot 属性）
        """
        try:
            guild = getattr(member, "guild", None)
            if guild is None:
                return
            guild_id = str(guild.id)
            user_id = str(member.id)
            # 与轮询拉取一致：机器人成员不进白名单，跳过
            if getattr(member, "bot", False):
                return
            client = self._find_discord_client()
            self_id = getattr(client, "user", None)
            if self_id is not None and user_id == str(self_id.id):
                return  # 机器人自身（被移出/自己加入）不上报
            if not self.member_event_sync.is_managed(guild_id, PLATFORM_DISCORD):
                return
            # 与轮询拉取一致：该服务器受管条目为身份组时，未持有这些身份组的
            # 成员加入不上报（避免白名单成员表混入非目标用户）；离开事件照常上报
            if kind == "increase":
                sync_roles = self._discord_sync_role_ids(guild_id)
                if sync_roles is not None:
                    m_roles = {str(r.id) for r in getattr(member, "roles", []) or []}
                    if not (m_roles & sync_roles):
                        return
            sub_type = "join" if kind == "increase" else "remove"
            self.member_event_sync.submit(
                kind, guild_id, user_id, sub_type, time.time(), platform=PLATFORM_DISCORD
            )
        except Exception as e:
            logger.warning(f"[DiscordSync] 处理成员事件失败: {e}")

    # ─── 合并转发（聊天记录）发送 ─────────────────────────────

    def _get_bot_uin(self, event: AstrMessageEvent) -> str:
        """获取当前机器人的 QQ 号（优先取事件 self_id，避免跨 bot 用错号导致转发失败）"""
        try:
            sid = event.get_self_id()
            if sid:
                return str(sid)
        except Exception:
            pass
        try:
            raw = getattr(event.message_obj, "raw_message", None)
            if raw and isinstance(raw, dict):
                self_id = raw.get("self_id")
                if self_id:
                    return str(self_id)
        except Exception:
            pass
        return ""

    async def _send_as_forward(
        self,
        event: AstrMessageEvent,
        segments: list[str],
    ) -> None:
        """将多条文本以合并转发（聊天记录）形式发送。

        Args:
            event: 消息事件
            segments: 每条消息的文本列表，每条作为一个聊天记录节点
        """
        bot_uin = self._get_bot_uin(event)
        nodes = []
        for i, seg in enumerate(segments):
            nodes.append(
                Node(
                    name="爱丽丝" if i > 0 else "系统",
                    uin=bot_uin,
                    content=[Plain(text=seg)],
                )
            )
        await event.send(MessageChain([Nodes(nodes=nodes)]))

    def _merge_items_to_segments(
        self,
        header: str,
        items: list[str],
        footer: str = "",
        max_len: int = 2800,
    ) -> list[str]:
        """将 header + items + footer 合并为聊天记录段，超长时自动切分。

        Returns:
            segments: 每条不超过 max_len 的文本段列表
        """
        segments = []
        current = header
        for item in items:
            candidate = current + "\n" + item if current else item
            if len(candidate) > max_len and current:
                segments.append(current)
                current = item
            else:
                current = candidate
        if footer:
            candidate = current + "\n" + footer if current else footer
            if len(candidate) > max_len and current:
                segments.append(current)
                current = footer
            else:
                current = candidate
        if current:
            segments.append(current)
        return segments

    def _should_use_forward(self, item_count: int) -> bool:
        """检查条目数是否超过内联阈值，需要走合并转发"""
        return self.max_inline_items > 0 and item_count > self.max_inline_items

    async def _send_list_result(
        self,
        event: AstrMessageEvent,
        title: str,
        items: list[str],
        footer: str = "",
        max_items: int = 20,
    ) -> None:
        """按平台分流发送列表结果。

        - QQ 等非 Discord 平台：不限条数，全部以合并转发（聊天记录）形式发送
        - Discord：只转发前 max_items 条，合并为一条消息发送
        """
        total = len(items)

        if event.get_platform_name() == "discord":
            shown = items[:max_items] if max_items > 0 and total > max_items else items
            note = ""
            if len(shown) != total:
                note = f"\n（仅显示前 {max_items} 条，共 {total} 条）"
            parts = []
            if title:
                parts.append(title)
            parts.extend(shown)
            if footer:
                parts.append(footer)
            text = "\n".join(parts) + note
            # Discord 单条消息上限 2000 字符，超长时从尾部截断并提示
            if len(text) > 1900:
                cut_note = "\n…（内容过长，已截断）"
                text = text[: 1900 - len(cut_note)] + cut_note
            await event.send(MessageChain([Plain(text=text)]))
            return

        # QQ 等平台：合并转发（聊天记录），但限制条数避免海量记录导致转发超量发送失败
        shown = items[:max_items] if max_items > 0 and total > max_items else items
        note = ""
        if len(shown) != total:
            note = f"\n（仅显示前 {max_items} 条，共 {total} 条）"
        segments = self._merge_items_to_segments(title, shown, footer + note)
        await self._send_as_forward(event, segments)

    # ─── API 辅助 ────────────────────────────────────────────

    def _clean_expired_confirms(self):
        """清理超过 40 秒未确认的 pending 记录"""
        now = time.time()
        expired = [
            key for key, val in self._pending_confirms.items()
            if now - val.get("ts", 0) > (
                self._select_timeout if val.get("type") == "select" else self._confirm_timeout
            )
        ]
        for key in expired:
            self._pending_confirms.pop(key, None)

    async def _schedule_timeout_notice(
        self,
        cache_key: str,
        event: AstrMessageEvent,
        action_type: str,
    ):
        """超时后自动发送超时通知（如果 pending 仍存在）"""
        timeout = self._select_timeout if action_type == "select" else self._confirm_timeout
        try:
            await asyncio.sleep(timeout)
            if cache_key in self._pending_confirms:
                self._pending_confirms.pop(cache_key, None)
                self._timeout_tasks.pop(cache_key, None)
                if action_type == "select":
                    text = "⏰ 序号选择超时（60秒），操作已取消，请重新发起"
                else:
                    label = "解封" if action_type == "unban" else "封禁"
                    text = f"❌ {label}操作超时，请重新发起"
                try:
                    await event.send(
                        MessageChain([Plain(text=text)])
                    )
                except Exception as e:
                    logger.warning(f"[timeout] 发送超时通知失败: {e}")
        except asyncio.CancelledError:
            pass  # 被取消说明用户已确认，正常

    async def _request_ws(
        self,
        command: int,
        payload: dict | None = None,
        with_status: bool = False,
    ) -> dict | list:
        """通过 WebSocket 发送命令并等待响应。

        with_status=True 时，会在返回的 dict 里附带 "status" 字段
        （对应服务端返回的 Status，如 auth/verify 的 200/400/403/404/410/500），
        供调用方精确区分服务端响应状态。
        """
        result = await self.ws_client.request(command, payload)
        if with_status and isinstance(result, dict) and "Status" in result:
            result["status"] = result["Status"]
        return result

    # ===================================================================
    #  公开 API 辅助函数
    # ===================================================================

    async def _get_version(self) -> dict:
        return await self._request_ws(EBotCommand.EGetVersion)

    async def _get_server_list(self) -> list | dict:
        result = await self._request_ws(EBotCommand.EGetServerList)
        if isinstance(result, dict) and result.get("Success"):
            return result.get("Servers", [])
        return result

    async def _get_online_status(self) -> list | dict:
        result = await self._request_ws(EBotCommand.EGetOnlineStatus)
        if isinstance(result, dict) and result.get("Success"):
            return result.get("Status", [])
        return result

    # ===================================================================
    #  鉴权 API 辅助函数
    # ===================================================================

    async def _reload_config(self) -> dict:
        return await self._request_ws(EBotCommand.EReload)

    async def _get_ban_list(self, offset: int = 0, limit: int = 100) -> dict:
        return await self._request_ws(
            EBotCommand.EGetBanList,
            {"Offset": offset, "Limit": limit},
        )

    async def _ban_uuid(self, uuid: str, reason: str) -> dict:
        return await self._request_ws(
            EBotCommand.EBanUUID,
            {"UUID": uuid, "Reason": reason},
        )

    async def _unban_uuid(self, uuid: str) -> dict:
        return await self._request_ws(
            EBotCommand.EUnbanUUID,
            {"UUID": uuid},
        )

    async def _run_command(self, server_id: str, command: str) -> dict:
        return await self._request_ws(
            EBotCommand.ERunCommand,
            {"ServerId": server_id, "Command": command},
        )

    async def _get_player_records(self, mode: str, params: dict) -> dict:
        """按模式查询玩家记录（WS 命令 EGetPlayerRecords）。

        mode 与 Params 字段（对齐服务端 /api/player-records 及文档4.txt）：
          - search: {"keyword": str}                      按玩家名模糊匹配
          - full:   {"offset": int, "limit": int}         分页
          - full:   {"startTime": str, "endTime": str}    按时间段过滤
          - filter: {"playerName": str, "playerIP": str}  按玩家名 + IP 筛选
        """
        return await self._request_ws(
            EBotCommand.EGetPlayerRecords,
            {"Mode": mode, "Params": params},
        )

    async def _search_player_records(self, keyword: str) -> dict:
        """按玩家名模糊搜索记录（向后兼容，供 ds ban / ds unban 复用）"""
        return await self._get_player_records("search", {"keyword": keyword})

    async def _get_account_bindings(self, uuid: str) -> dict:
        """查询账号绑定的渠道信息（WS 命令 EGetAccountBindings）"""
        return await self._request_ws(
            EBotCommand.EGetAccountBindings,
            {"UUID": uuid},
        )

    async def _get_uuid_by_binding(self, platform: int, platform_unique_id: str) -> dict:
        """按绑定渠道反查 UUID（WS 命令 EGetUUIDByBinding）

        请求：Platform（1=QQ, 2=Discord）+ PlatformUniqueId（QQ号/Discord ID）
        响应：Success + UUID（无绑定时为 null）
        """
        return await self._request_ws(
            EBotCommand.EGetUUIDByBinding,
            {
                "Platform": platform,
                "PlatformUniqueId": platform_unique_id,
            },
        )

    @staticmethod
    def _map_platform_name(platform) -> str:
        """ELoginChannel -> 平台名：1=QQ, 2=Discord"""
        return {1: "QQ", 2: "Discord"}.get(platform, f"未知({platform})")

    @staticmethod
    def _safe_msg(msg) -> str:
        """脱敏：去掉消息中的 URL（含服务器地址与查询参数）及敏感字段，防止泄露。

        例如 "无法连接服务器 ws://1.2.3.4:11470/ws/bot?BotId=xxx&Token=yyy"
        → "无法连接服务器"（服务器 IP、BotId、Token 均不暴露）。
        """
        s = str(msg)
        # 整段去掉 ws/http URL（含 host:port 与 query）
        s = re.sub(r"(?i)(?:ws|wss|http|https)://[^\s\"'<>]+", "", s)
        # IPv4 地址（可带端口）打码，如 26.182.49.113:11470
        s = re.sub(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(?::\d+)?\b", "[IP]", s)
        # 兜底：独立出现的 BotId/Token 等敏感字段打码
        s = re.sub(r"(?i)(BotId|Token|api_key|ws_token|apikey|secret)=[^\s\"'&<>]+", r"\1=***", s)
        # 清理 URL/IP 移除后留下的连续/首尾空白
        s = re.sub(r"\s+", " ", s).strip()
        return s

    async def _verify_auth(self, code: str, channel: int, channel_unique_id: str, scope: str | None = None) -> dict:
        """玩家登录验证（Device Code Flow 第二步）

        消息体：VerificationCode / Channel / ChannelUniqueId。
        Channel：0=Unknown, 1=QQ, 2=Discord；ChannelUniqueId：QQ号/Discord ID。
        注：master 的 NotifyVerificationResult(code, channel, channelUniqueId) 不接收
        Scope；登录资格由 master 按账号绑定群（IsInWhitelistedGroups）判定，而
        "会话所在群/服务器是否可用"由插件本地 _check_basic_access 校验，两者互补。

        服务端 EVerify 响应 Status（NotifyVerificationResult 返回）：
        - 200  验证成功（含 UUID）
        - 400  登录渠道不在枚举定义中
        - 403  账号封禁 / 不在白名单群
        - 404  验证码不存在
        - 410  验证码过期
        - 500  服务器内部错误
        - 鉴权失败 → ws_client 返回 Success=False + 鉴权失败提示
        """
        payload = {
            "VerificationCode": code,
            "Channel": channel,
            "ChannelUniqueId": channel_unique_id,
        }
        if scope:
            payload["Scope"] = scope
        return await self._request_ws(
            EBotCommand.EVerify,
            payload,
            with_status=True,
        )

    @staticmethod
    def _map_verify_failure(status, message) -> str:
        """把 auth/verify 的失败响应映射成可读的登录失败原因。

        优先按服务端返回的 Message 精确匹配（与 MasterServer 的
        TranslateMessage 约定一致，2026-08-16 补充），再按 HTTP 状态码兜底：
        - 200 验证成功 / 400 登录渠道无效 / 403 账号封禁
        - 404 验证码不存在 / 410 验证码过期 / 500 服务器内部错误
        - 401 客户端拦截的鉴权失败

        注意：服务端对"不在白名单群"同样回 403，必须靠 Message 区分
        "Not in whitelisted group"（不在可用群聊）与 "Account banned"（封禁）。
        """
        msg = str(message or "").strip()
        # 与 MasterServer TranslateMessage 一致的服务端 Message 精确映射
        message_map = {
            "Invalid channel": "登录渠道无效（仅支持 QQ/Discord）",
            "Invalid or expired verification code": "验证码无效或已过期",
            "Verification code expired": "验证码已过期，请重新获取",
            "Account banned": "账号已被封禁",
            "Account disabled": "账号已被禁用",
            "Not in whitelisted group": "账号不在可用群聊内，请先加入受管群",
            "Missing required fields": "缺少必要参数，请重新发起验证",
            "Player service unavailable": "玩家服务不可用，请稍后再试",
            "Internal server error": "服务器内部错误，请稍后再试",
        }
        if msg in message_map:
            return message_map[msg]
        # 模糊匹配兜底（服务端可能带大小写/前后缀差异）
        lower = msg.lower()
        if "whitelist" in lower or "不在可用群聊" in msg:
            return "账号不在可用群聊内，请先加入受管群"
        if "banned" in lower or "封禁" in msg:
            return "账号已被封禁"
        if "disabled" in lower or "禁用" in msg:
            return "账号已被禁用"
        if "expired" in lower or "过期" in msg:
            return "验证码已过期，请重新获取"
        if "invalid" in lower or "无效" in msg:
            return "验证码无效或已过期"
        # HTTP 状态码兜底
        if status == 400:
            return "登录渠道无效（仅支持 QQ/Discord）"
        if status == 403:
            return "账号已被封禁"
        if status == 404:
            return "验证码不存在，请确认后重试"
        if status == 410:
            return "验证码已过期，请重新获取"
        if status == 500:
            return "服务器内部错误，请稍后再试"
        if status == 401 or "鉴权失败" in msg:
            return "鉴权失败（Token 无效或未配置）"
        if status == 200:
            return "验证成功"
        return msg if msg else "未知错误"

    @staticmethod
    def _map_verify_failure_en(status, message) -> str:
        """把 auth/verify 的失败响应映射成英文登录失败原因（Discord 双语展示用）。"""
        msg = str(message or "").strip()
        message_map = {
            "Invalid channel": "Invalid login channel (QQ/Discord only)",
            "Invalid or expired verification code": "Invalid or expired verification code",
            "Verification code expired": "Verification code expired, please get a new one",
            "Account banned": "Account banned",
            "Account disabled": "Account disabled",
            "Not in whitelisted group": "Account not in a whitelisted group, please join one",
            "Missing required fields": "Missing required parameters, please retry",
            "Player service unavailable": "Player service unavailable, try again later",
            "Internal server error": "Internal server error, try again later",
        }
        if msg in message_map:
            return message_map[msg]
        lower = msg.lower()
        if "whitelist" in lower:
            return "Account not in a whitelisted group, please join one"
        if "banned" in lower:
            return "Account banned"
        if "disabled" in lower:
            return "Account disabled"
        if "expired" in lower:
            return "Verification code expired, please get a new one"
        if "invalid" in lower:
            return "Invalid or expired verification code"
        if status == 400:
            return "Invalid login channel (QQ/Discord only)"
        if status == 403:
            return "Account banned"
        if status == 404:
            return "Verification code not found, please retry"
        if status == 410:
            return "Verification code expired, please get a new one"
        if status == 500:
            return "Internal server error, try again later"
        if status == 401 or "鉴权失败" in msg:
            return "Auth failed (invalid or missing token)"
        return msg if msg else "Unknown error"

    # ─── 辅助：合并服务器列表与在线状态后的格式化 ────────────

    # 服务器中文 Tag → 英文名（Discord 双语展示用，子串匹配）
    _SERVER_EN_NAME_KEYWORDS = {
        "乱斗": "Brawl",
        "通用": "General",
        "手机": "Mobile",
        "新手": "Newbie",
    }

    @classmethod
    def _server_en_name(cls, tag: str) -> str:
        """根据中文 Tag 返回英文服务器名；未命中返回空串"""
        for zh, en in cls._SERVER_EN_NAME_KEYWORDS.items():
            if zh in tag:
                return en
        return ""

    async def _format_server_list_with_status(self, bilingual: bool = False) -> str:
        """获取服务器列表+在线状态，返回格式化文本（空行分隔）"""
        servers, status_list = await asyncio.gather(
            self._get_server_list(),
            self._get_online_status(),
        )

        if not isinstance(servers, list) or not servers:
            if bilingual:
                return "当前无可用服务器 (No servers available)"
            return "当前无可用服务器"

        status_map = {}
        if isinstance(status_list, list):
            for s in status_list:
                status_map[self._normalize_server_id(s.get("ServerId"))] = s

        lines = []
        for s in servers:
            status = status_map.get(self._normalize_server_id(s.get("ServerId")))
            if status:
                players = f"{status.get('CurrentPlayers', 0)}/{status.get('MaxPlayers', 0)}"
            else:
                players = "-/-"
            tag = s.get("Tag", "?")
            if bilingual:
                en = self._server_en_name(tag)
                tag = f"{tag} ({en})" if en else tag
            lines.append(f"{tag} | {players}")
        return "\n".join(lines)

    async def _get_server_tag_map(self) -> dict:
        """获取 ServerId -> Tag 映射（10 秒缓存 + 3 秒刷新冷却）。

        刷新失败时返回旧缓存（可能为空 dict），调用方需自行兜底显示原始 ServerId。
        """
        now = time.time()
        # 10 秒内缓存有效
        if self._tag_cache and now - self._tag_cache_ts < 10:
            return self._tag_cache
        # 3 秒刷新冷却：距上次刷新不足 3 秒，直接用旧缓存
        if now - self._tag_cache_ts < 3:
            return self._tag_cache
        try:
            servers = await self._get_server_list()
            if isinstance(servers, list):
                new_map = {}
                for s in servers:
                    sid = s.get("ServerId")
                    if sid:
                        new_map[sid] = s.get("Tag") or str(sid)
                if new_map:
                    self._tag_cache = new_map
            self._tag_cache_ts = now
        except Exception as e:
            logger.warning(f"[tag] 获取服务器列表失败: {e}")
            self._tag_cache_ts = now
        return self._tag_cache

    @staticmethod
    def _normalize_server_id(server_id) -> str:
        """归一化 ServerId：server01 -> server1，兼容 str/int 类型"""
        s = str(server_id).strip()
        m = re.match(r'^(\D*)(\d+)$', s)
        if m:
            return m.group(1) + str(int(m.group(2)))
        return s

    def _resolve_server_tag(self, server_id) -> str:
        """把 ServerId 解析为服务器 Tag（兼容 str/int 类型不一致与补零差异），失败时回退显示原值"""
        if server_id is None:
            return "?"
        if server_id in self._tag_cache:
            return self._tag_cache[server_id]
        for k, v in self._tag_cache.items():
            if str(k) == str(server_id):
                return v
        # 归一化匹配：server01 <-> server1 互相兼容
        norm = self._normalize_server_id(server_id)
        for k, v in self._tag_cache.items():
            if self._normalize_server_id(k) == norm:
                return v
        return str(server_id)

    @staticmethod
    def _format_display_time(iso_str: str) -> str:
        """把 ISO 8601 时间（UTC）格式化为北京时间显示，失败时原样返回"""
        if not iso_str:
            return "?"
        try:
            dt = datetime.datetime.fromisoformat(str(iso_str).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            return dt.astimezone(
                datetime.timezone(datetime.timedelta(hours=8))
            ).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return str(iso_str)

    @staticmethod
    def _format_short_time(iso_str: str) -> str:
        """把 ISO 8601 时间格式化为简短显示（MM-DD HH:MM），失败时返回 ?"""
        if not iso_str:
            return "?"
        try:
            dt = datetime.datetime.fromisoformat(str(iso_str).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            return dt.astimezone(
                datetime.timezone(datetime.timedelta(hours=8))
            ).strftime("%m-%d %H:%M")
        except Exception:
            return "?"

    @staticmethod
    def _sort_records_by_time(players: list, time_key: str) -> list:
        """按 ISO 时间降序（最新在前），解析失败的最后，同时刻保持原顺序（稳定排序）。

        排序键：解析成功 -> (1, -ts)，失败 -> (0, 0)。
        ts 越大 -ts 越小，升序排列时最新记录排最前。
        """
        def _parse(iso_str):
            if not iso_str:
                return None
            try:
                dt = datetime.datetime.fromisoformat(str(iso_str).replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=datetime.timezone.utc)
                return dt.timestamp()
            except Exception:
                return None

        def _key(p):
            ts = _parse(p.get(time_key, ""))
            if ts is None:
                return (1, 0)   # 解析失败组排最后
            return (0, -ts)    # 正常组排前，ts 越大 -ts 越小，最新在前

        return sorted(players, key=_key)

    # ===================================================================
    #  玩家记录查询辅助（ds records 多模式）
    # ===================================================================

    @staticmethod
    def _flatten_record_players(data: dict) -> list:
        """把按 ServerId 分组的记录响应展平成玩家列表"""
        players = []
        for _sid, sdata in (data or {}).items():
            if not isinstance(sdata, dict):
                continue
            for p in sdata.get("Players", []) or []:
                if isinstance(p, dict):
                    players.append(p)
        return players

    def _render_record_items(self, players: list) -> list:
        """把玩家记录渲染成统一展示条目（含 UUID）"""
        return [
            f"• {p.get('PlayerName', '?')} | {p.get('PlayerIP', '?')} | "
            f"{self._resolve_server_tag(p.get('ServerId'))} | "
            f"{self._format_display_time(p.get('JoinTime', ''))} | "
            f"UUID: {p.get('UUID', '?')}"
            for p in players
        ]

    async def _send_records_result(self, event: AstrMessageEvent, result, empty_msg: str) -> bool:
        """统一处理记录查询响应：校验 → 展平 → 排序 → 渲染 → 发送。

        返回 True 表示已成功发送结果，False 表示已发送错误/空提示。
        """
        if not isinstance(result, dict) or not result.get("Success"):
            msg = result.get("Message", "查询失败") if isinstance(result, dict) else result
            await event.send(MessageChain([Plain(text=f"查询失败：{self._safe_msg(msg)}")]))
            return False

        all_players = self._flatten_record_players(result.get("Data"))
        if not all_players:
            await event.send(MessageChain([Plain(text=empty_msg)]))
            return False

        if self.config.get("sort_by_time", True):
            all_players = self._sort_records_by_time(all_players, "JoinTime")

        total = result.get("TotalCount", len(all_players))
        title = f"共 {total} 条记录"
        await self._get_server_tag_map()
        await self._send_list_result(event, title, self._render_record_items(all_players))
        return True

    async def _records_search(self, event: AstrMessageEvent, keyword: str):
        """ds records <关键词> —— 按玩家名模糊搜索"""
        await self._send_records_result(
            event,
            await self._get_player_records("search", {"keyword": keyword}),
            f'未找到匹配 "{keyword}" 的玩家记录',
        )

    async def _records_full_paged(self, event: AstrMessageEvent, args: list):
        """ds records list [offset] [limit] —— 分页查看全部记录"""
        offset = 0
        limit = 50
        if args:
            try:
                offset = max(0, int(args[0]))
            except ValueError:
                await event.send(MessageChain([Plain(text="❌ offset 需为整数")]))
                return
        if len(args) > 1:
            try:
                limit = max(1, int(args[1]))
            except ValueError:
                await event.send(MessageChain([Plain(text="❌ limit 需为整数")]))
                return
        await self._send_records_result(
            event,
            await self._get_player_records("full", {"offset": str(offset), "limit": str(limit)}),
            "当前无玩家记录",
        )

    async def _records_full_range(self, event: AstrMessageEvent, args: list):
        """ds records range <开始时间> <结束时间> —— 按时间段查询"""
        if len(args) < 2:
            await event.send(MessageChain([Plain(text="❌ 格式: ds records range <开始时间> <结束时间>")]))
            return
        start_time, end_time = args[0], args[1]
        await self._send_records_result(
            event,
            await self._get_player_records("full", {"startTime": start_time, "endTime": end_time}),
            "该时间段内无玩家记录",
        )

    async def _records_filter(self, event: AstrMessageEvent, args: list):
        """ds records filter <玩家名> [IP] —— 按玩家名 + IP 筛选"""
        if not args:
            await event.send(MessageChain([Plain(text="❌ 格式: ds records filter <玩家名> [IP]")]))
            return
        player_name = args[0]
        player_ip = args[1] if len(args) > 1 else ""
        await self._send_records_result(
            event,
            await self._get_player_records("filter", {"playerName": player_name, "playerIP": player_ip}),
            f'未找到玩家 "{player_name}" 的记录',
        )

    async def _get_player_records_all(self, limit: int = 200) -> list:
        """分页拉取全部玩家记录（展平为玩家列表，用于本地按 UUID 聚合曾用名）。"""
        all_players: list = []
        offset = 0
        while True:
            result = await self._get_player_records(
                "full", {"offset": str(offset), "limit": str(limit)}
            )
            if not isinstance(result, dict) or not result.get("Success"):
                break
            players = self._flatten_record_players(result.get("Data"))
            all_players.extend(players)
            total = int(result.get("TotalCount") or len(all_players))
            offset += len(players)
            if len(players) == 0 or len(all_players) >= total:
                break
        return all_players

    async def _records_uuid_names(self, event: AstrMessageEvent, uuid: str):
        """按 UUID 汇总玩家曾用名（聚合 PlayerRecords 里该 UUID 的所有不同名字）。"""
        all_players = await self._get_player_records_all()
        uuid_norm = str(uuid).strip().lower()
        matched = [
            p for p in all_players
            if str(p.get("UUID", "")).strip().lower() == uuid_norm
        ]
        if not matched:
            await self._send_list_result(
                event, f"未找到 UUID {uuid} 的玩家记录", [], ""
            )
            return
        matched.sort(key=lambda p: str(p.get("JoinTime", "") or ""))
        seen = set()
        name_history: list[str] = []
        for p in matched:
            name = str(p.get("PlayerName", "")).strip()
            if name and name not in seen:
                seen.add(name)
                name_history.append(name)
        current = str(matched[-1].get("PlayerName", "")).strip() if matched else "?"
        title = f"UUID: {uuid} · 曾用名共 {len(name_history)} 个"
        items = [f"{i}. {name}" for i, name in enumerate(name_history, 1)]
        footer = f"当前：{current}"
        await self._send_list_result(event, title, items, footer)

    # ===================================================================
    #  指令定义 — 全部对齐 dscontrol
    # ===================================================================

    # ── ds version ─────────────────────────────────────────────

    @filter.command("ds version")
    async def cmd_ds_version(self, event: AstrMessageEvent):
        """获取服务器版本号"""
        if self._is_blacklisted_group(event):
            event.stop_event()
            return
        # 基础功能：全量白名单 / 管理组群内所有人可用；无关群静默
        if not self._check_basic_access(event):
            event.stop_event()
            return
        passed, _ = self._check_rate_limit(event, "version")
        if not passed:
            event.stop_event()
            return
        try:
            result = await self._get_version()
            if isinstance(result, dict) and result.get("Success"):
                ver = result.get("Version", "未知")
                if event.get_platform_name() == "discord":
                    yield event.plain_result(f"NoticeServer 版本 (Version): {ver}")
                else:
                    yield event.plain_result(f"NoticeServer 版本: {ver}")
            else:
                msg = result.get("Message", "获取版本失败") if isinstance(result, dict) else result
                yield event.plain_result(f"获取版本失败：{self._safe_msg(msg)}")
        except Exception as e:
            logger.error(f"[dsversion] 请求失败: {e}")
            yield event.plain_result(f"获取版本失败：{self._safe_msg(e)}")

    # ── ds servers ────────────────────────────────────────────

    @filter.command("ds servers")
    async def cmd_ds_servers(self, event: AstrMessageEvent):
        """显示服务器列表（含在线状态）"""
        if self._is_blacklisted_group(event):
            event.stop_event()
            return
        # 基础功能：全量白名单 / 管理组群内所有人可用；无关群静默
        if not self._check_basic_access(event):
            event.stop_event()
            return
        passed, _ = self._check_rate_limit(event, "servers")
        if not passed:
            event.stop_event()
            return
        try:
            bilingual = event.get_platform_name() == "discord"
            text = await self._format_server_list_with_status(bilingual=bilingual)
            yield event.plain_result(text)
        except Exception as e:
            logger.error(f"[dsservers] 请求失败: {e}")
            yield event.plain_result(f"获取服务器信息失败：{self._safe_msg(e)}")

    # ── ds reload ─────────────────────────────────────────────

    @filter.command("ds reload")
    async def cmd_ds_reload(self, event: AstrMessageEvent):
        """重新加载服务器配置（ServerList.json）"""
        if self._is_blacklisted_group(event):
            event.stop_event()
            return
        # 插件可用范围：无关群完全静默
        if not self._check_basic_access(event):
            event.stop_event()
            return
        if not self._check_access(event):
            if event.get_group_id():
                event.stop_event()  # 群聊无权限（普通玩家群）静默，避免刷屏
                return
            yield event.plain_result("❌ 未获得使用权限，请联系管理员")
            return
        try:
            result = await self._reload_config()
            if isinstance(result, dict) and result.get("Success"):
                yield event.plain_result(f"配置重载成功：{result.get('Message', '')}")
            else:
                msg = result.get("Message", "配置重载失败") if isinstance(result, dict) else result
                yield event.plain_result(f"配置重载失败：{self._safe_msg(msg)}")
        except Exception as e:
            logger.error(f"[dsreload] 请求失败: {e}")
            yield event.plain_result(f"重载配置失败：{self._safe_msg(e)}")

    # ── ds groupsync ─────────────────────────────────────────

    @filter.command("ds groupsync")
    async def cmd_ds_groupsync(self, event: AstrMessageEvent):
        """查看/手动触发群名单同步（GroupWhitelist）"""
        if self._is_blacklisted_group(event):
            event.stop_event()
            return
        # 插件可用范围：无关群完全静默
        if not self._check_basic_access(event):
            event.stop_event()
            return
        if not self._check_access(event):
            if event.get_group_id():
                event.stop_event()  # 群聊无权限（普通玩家群）静默，避免刷屏
                return
            yield event.plain_result("❌ 未获得使用权限，请联系管理员")
            return
        try:
            parts = str(event.message_str).strip().split()
            action = parts[2].lower() if len(parts) > 2 else "status"

            gs = self.group_sync
            if action in ("now", "push", "sync"):
                # 手动触发一次轮询（对比差异并推送增量）
                await gs.poll_once()
                yield event.plain_result(
                    "✅ 群名单轮询完成\n" + gs.dump_stats()
                )
            elif action in ("snapshot", "full"):
                # 构建完整快照（实际回包由服务端请求时自动进行）
                data = await gs.handle_snapshot_request()
                whitelist = data.get("Whitelist", {})
                total = sum(len(v) for v in whitelist.values())
                yield event.plain_result(
                    f"✅ 完整快照已构建（v{data.get('Version')}，{total} 人）\n"
                    "（快照将在服务端请求时自动回包）\n" + gs.dump_stats()
                )
            elif action in ("delta", "repair"):
                # 构建补包（实际回包由服务端请求时自动进行）
                fv = int(parts[3]) if len(parts) > 3 else 0
                data = await gs.handle_delta_request(fv)
                changes = data.get("Changes", [])
                yield event.plain_result(
                    f"✅ 补包已构建（FromVersion={fv}，{len(changes)} 条变更）\n"
                    "（补包将在服务端请求时自动回包）\n" + gs.dump_stats()
                )
            elif action in ("reset", "重置"):
                # 重置本地持久化并立即全量重建推送（BaseVersion=0 全量 add）
                result = await gs.reset_and_rebuild()
                yield event.plain_result(
                    f"✅ 本地群名单已重置并全量重建推送\n"
                    f"（v{result['Version']}，{result['Total']} 人，"
                    f"推送成功：{'是' if result['Pushed'] else '否'}）\n"
                    + gs.dump_stats()
                )
            else:
                # 默认：查看状态
                plat_groups = gs.get_platform_groups()
                groups_parts = []
                for p, gset in sorted(plat_groups.items()):
                    if gset:
                        name = "QQ" if p == PLATFORM_QQ else ("Discord" if p == PLATFORM_DISCORD else f"P{p}")
                        groups_parts.append(f"{name}:{','.join(sorted(gset))}")
                groups = "、".join(groups_parts) if groups_parts else "（未配置）"
                snap = gs.get_snapshot()
                lines = [
                    "📋 群名单同步状态",
                    f"开关: {'✅ 开启' if gs.enabled else '❌ 关闭'}",
                    f"受管范围: {groups}",
                    f"当前版本: v{gs._version}",
                    f"变更历史: {len(gs._history_order)} 个版本",
                ]
                for gid, members in sorted(snap.items()):
                    lines.append(f"  · {gid}: {len(members)} 条")
                lines.append(gs.dump_stats())
                yield event.plain_result("\n".join(lines))
        except Exception as e:
            logger.error(f"[dsgroupsync] 执行失败: {e}")
            yield event.plain_result(f"❌ 群名单同步操作失败：{self._safe_msg(e)}")

    # ── ds banlist ────────────────────────────────────────────

    @filter.command("ds banlist")
    async def cmd_ds_banlist(self, event: AstrMessageEvent):
        """获取封禁列表（UUID 封禁）"""
        if self._is_blacklisted_group(event):
            event.stop_event()
            return
        # 插件可用范围：无关群完全静默
        if not self._check_basic_access(event):
            event.stop_event()
            return
        if not self._check_access(event):
            if event.get_group_id():
                event.stop_event()  # 群聊无权限（普通玩家群）静默，避免刷屏
                return
            yield event.plain_result("❌ 未获得使用权限，请联系管理员")
            return
        try:
            result = await self._get_ban_list()
            if not isinstance(result, dict):
                yield event.plain_result("❌ 获取封禁列表失败：响应格式错误")
                return
            if not result.get("Success"):
                yield event.plain_result(f"获取封禁列表失败：{result.get('Message', '未知错误')}")
                return

            accounts = result.get("Accounts", [])
            total = result.get("TotalCount", len(accounts))
            if not accounts:
                yield event.plain_result("当前无封禁记录")
                return

            if self.config.get("sort_by_time", True):
                accounts = self._sort_records_by_time(accounts, "CreatedAt")

            title = f"共 {total} 条封禁记录"
            items = [
                f"• {a.get('UUID', '?')} | "
                f"{a.get('BanReason', '?')} | "
                f"{self._format_display_time(a.get('CreatedAt', ''))}"
                for a in accounts
            ]

            await self._send_list_result(event, title, items)
        except Exception as e:
            logger.error(f"[dsbanlist] 请求失败: {e}")
            yield event.plain_result(f"获取封禁列表失败：{self._safe_msg(e)}")

    # ── ds unban <UUID|玩家名|IP> ─────────────────────────────

    @filter.command("ds unban")
    async def cmd_ds_unban(self, event: AstrMessageEvent):
        """解封账号 UUID（40秒等待，直接回复 y 确认）"""
        if self._is_blacklisted_group(event):
            event.stop_event()
            return
        # 插件可用范围：无关群完全静默
        if not self._check_basic_access(event):
            event.stop_event()
            return
        if not self._check_access(event):
            if event.get_group_id():
                event.stop_event()  # 群聊无权限（普通玩家群）静默，避免刷屏
                return
            yield event.plain_result("❌ 未获得使用权限，请联系管理员")
            return

        gid = event.get_group_id()
        cache_key = f"private:{event.get_sender_id()}" if gid is None else f"{gid}:{event.get_sender_id()}"
        text = str(event.message_str).strip()
        words = text.split()
        target = None
        for w in words[2:]:
            if w.lower() in ('y', 'yes'):
                continue  # 忽略 y，统一走 on_message 确认
            if target is None:
                target = w

        if target is None:
            yield event.plain_result("❌ 格式: ds unban <UUID|玩家名|IP>")
            return

        # 输入是 UUID（8-4-4-4-12 的 32/36 位 hex）→ 直接进入确认
        uuid_pattern = re.compile(
            r'^[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}$'
        )
        if uuid_pattern.match(target):
            self._clean_expired_confirms()
            self._pending_confirms[cache_key] = {
                "type": "unban",
                "data": {"uuid": target},
                "ts": time.time(),
            }
            old_task = self._timeout_tasks.get(cache_key)
            if old_task and not old_task.done():
                old_task.cancel()
            self._timeout_tasks[cache_key] = asyncio.create_task(
                self._schedule_timeout_notice(cache_key, event, "unban")
            )
            yield event.plain_result(
                f"即将解封 UUID: {target}\n"
                f"请直接回复 y 来确认解封（40秒内有效）"
            )
            return

        # 否则按玩家名/IP 查记录，列出候选（含 UUID）供选择
        try:
            result = await self._search_player_records(target)
            if not isinstance(result, dict) or not result.get("Success"):
                msg = result.get("Message", "查询失败") if isinstance(result, dict) else result
                yield event.plain_result(f"查询玩家记录失败：{self._safe_msg(msg)}")
                return
            data = result.get("Data", {})
            all_players = []
            for sid, sdata in data.items():
                for p in sdata.get("Players", []):
                    all_players.append(p)
            if not all_players:
                yield event.plain_result(f"未找到匹配 \"{target}\" 的玩家记录")
                return
            if self.config.get("sort_by_time", True):
                all_players = self._sort_records_by_time(all_players, "JoinTime")

            await self._get_server_tag_map()
            record_items = []
            for i, p in enumerate(all_players, 1):
                pid = p.get('PlayerId', '')
                record_items.append(
                    f"{i}. {p.get('PlayerName', '?')} | "
                    f"UUID: {pid} | "
                    f"IP: {p.get('PlayerIP', '?')} | "
                    f"服务器: {self._resolve_server_tag(p.get('ServerId'))} | "
                    f"{self._format_short_time(p.get('JoinTime', ''))}"
                )

            self._clean_expired_confirms()
            self._pending_confirms[cache_key] = {
                "type": "unban_select",
                "data": {"players": all_players},
                "ts": time.time(),
            }
            old_task = self._timeout_tasks.get(cache_key)
            if old_task and not old_task.done():
                old_task.cancel()
            self._timeout_tasks[cache_key] = asyncio.create_task(
                self._schedule_timeout_notice(cache_key, event, "select")
            )

            hint_text = "请回复对应序号选择要解封的玩家（60秒内有效）"
            header = f"找到 {len(all_players)} 个匹配的玩家记录："
            await self._send_list_result(event, header, record_items, hint_text)
            return
        except Exception as e:
            logger.error(f"[dsunban] 查询玩家记录失败: {e}")
            yield event.plain_result(f"查询玩家记录失败：{self._safe_msg(e)}")

    # ── ds records [list|range|filter] ────────────────────────

    @filter.command("ds records")
    async def cmd_ds_records(self, event: AstrMessageEvent):
        """查询玩家登录记录。

        子命令（对齐服务端 EGetPlayerRecords 多模式）：
          ds records <关键词>                按玩家名模糊搜索
          ds records list [offset] [limit]   分页查看全部记录
          ds records range <开始> <结束>     按时间段查询（ISO 时间）
          ds records filter <玩家名> [IP]    按玩家名 + IP 筛选
        """
        if self._is_blacklisted_group(event):
            event.stop_event()
            return
        # 插件可用范围：无关群完全静默
        if not self._check_basic_access(event):
            event.stop_event()
            return
        if not self._check_access(event):
            if event.get_group_id():
                event.stop_event()  # 群聊无权限（普通玩家群）静默，避免刷屏
                return
            yield event.plain_result("❌ 未获得使用权限，请联系管理员")
            return

        text = str(event.message_str).strip()
        words = text.split()
        args = words[2:]

        if not args:
            yield event.plain_result(
                "ds records 用法：\n"
                "  ds records <关键词>                按玩家名搜索\n"
                "  ds records list [offset] [limit]   分页查看全部\n"
                "  ds records range <开始> <结束>     按时间段查询\n"
                "  ds records filter <玩家名> [IP]    按玩家名+IP筛选"
            )
            return

        sub = args[0].lower()
        try:
            if sub == "list":
                await self._records_full_paged(event, args[1:])
            elif sub == "range":
                await self._records_full_range(event, args[1:])
            elif sub == "filter":
                await self._records_filter(event, args[1:])
            else:
                # 默认按关键词搜索（保持原 ds records <keyword> 行为）
                await self._records_search(event, " ".join(args))
        except Exception as e:
            logger.error(f"[dsrecords] 请求失败: {e}")
            yield event.plain_result(f"查询玩家记录失败：{self._safe_msg(e)}")

    # ── ds names <UUID> ────────────────────────────────────────

    @filter.command("ds names")
    async def cmd_ds_names(self, event: AstrMessageEvent):
        """按 UUID 查询玩家曾用名（聚合 PlayerRecords 里该 UUID 的所有名字）"""
        if self._is_blacklisted_group(event):
            event.stop_event()
            return
        # 插件可用范围：无关群完全静默
        if not self._check_basic_access(event):
            event.stop_event()
            return
        if not self._check_access(event):
            if event.get_group_id():
                event.stop_event()  # 群聊无权限（普通玩家群）静默，避免刷屏
                return
            yield event.plain_result("❌ 未获得使用权限，请联系管理员")
            return
        parts = str(event.message_str).strip().split()
        if len(parts) < 3:
            yield event.plain_result(
                "❌ 格式: ds names <UUID>\n"
                "例如: ds names 550e8400-e29b-41d4-a716-446655440000"
            )
            return
        uuid = parts[2].strip()
        try:
            await self._records_uuid_names(event, uuid)
        except Exception as e:
            logger.error(f"[dsnames] 查询曾用名失败: {e}")
            yield event.plain_result(f"查询曾用名失败：{self._safe_msg(e)}")

    # ── ds run <服务器> <命令> ─────────────────────────────────

    @filter.command("ds run")
    async def cmd_ds_run(self, event: AstrMessageEvent):
        """向指定 DS 服务器发送控制台命令（run-command）"""
        if self._is_blacklisted_group(event):
            event.stop_event()
            return
        # 插件可用范围：无关群完全静默
        if not self._check_basic_access(event):
            event.stop_event()
            return
        if not self._check_access(event):
            if event.get_group_id():
                event.stop_event()  # 群聊无权限（普通玩家群）静默，避免刷屏
                return
            yield event.plain_result("❌ 未获得使用权限，请联系管理员")
            return

        text = str(event.message_str).strip()
        parts = text.split(maxsplit=3)
        if len(parts) < 4:
            yield event.plain_result("❌ 格式: ds run <服务器> <命令>\n例如: ds run 通用服 kill 玩家名")
            return
        server_arg = parts[2].strip()
        command = parts[3].strip()
        if not command:
            yield event.plain_result("❌ 格式: ds run <服务器> <命令>")
            return

        try:
            # 服务器名 → ServerId（支持 Tag 或 ServerId 匹配）
            servers = await self._get_server_list()
            if not isinstance(servers, list) or not servers:
                yield event.plain_result("当前无可用服务器，无法执行命令")
                return

            target_sid = None
            matches = []
            norm_arg = self._normalize_server_id(server_arg)
            for s in servers:
                sid = s.get("ServerId")
                tag = s.get("Tag", "")
                if sid is None:
                    continue
                if server_arg == tag or server_arg == sid or norm_arg == self._normalize_server_id(sid):
                    matches.append(s)
            if not matches:
                tags = "、".join(s.get("Tag", "?") for s in servers)
                yield event.plain_result(f"未找到服务器 \"{server_arg}\"，可用服务器: {tags}")
                return

            if len(matches) == 1:
                target_sid = matches[0].get("ServerId")
            else:
                # 多匹配：优先精确 Tag 匹配
                for s in matches:
                    if s.get("Tag") == server_arg:
                        target_sid = s.get("ServerId")
                        break
                if target_sid is None:
                    target_sid = matches[0].get("ServerId")

            result = await self._run_command(target_sid, command)
            if isinstance(result, dict) and result.get("Success"):
                yield event.plain_result(
                    f"命令已发送到 {server_arg}\n"
                    f"命令: {command}\n"
                    f"{result.get('Message', '')}"
                )
            else:
                msg = result.get("Message", "命令发送失败") if isinstance(result, dict) else result
                yield event.plain_result(f"命令发送失败：{self._safe_msg(msg)}")
        except Exception as e:
            logger.error(f"[dsrun] 发送命令失败: {e}")
            yield event.plain_result(f"命令发送失败：{self._safe_msg(e)}")

    # ── ds lookup <QQ号|discord:ID> ───────────────────────────

    @filter.command("ds lookup")
    async def cmd_ds_lookup(self, event: AstrMessageEvent):
        """按绑定渠道反查 UUID（走服务端 Type 16，实时查询）"""
        if self._is_blacklisted_group(event):
            event.stop_event()
            return
        # 插件可用范围：无关群完全静默
        if not self._check_basic_access(event):
            event.stop_event()
            return
        if not self._check_access(event):
            if event.get_group_id():
                event.stop_event()  # 群聊无权限（普通玩家群）静默，避免刷屏
                return
            yield event.plain_result("❌ 未获得使用权限，请联系管理员")
            return

        text = str(event.message_str).strip()
        parts = text.split(maxsplit=2)
        if len(parts) < 3:
            yield event.plain_result("❌ 格式: ds lookup <QQ号|discord:ID>；例如 ds lookup 123456789 或 ds lookup discord:987654321098765432")
            return
        raw = parts[2].strip()
        if not raw:
            yield event.plain_result("❌ 格式: ds lookup <QQ号|discord:ID>")
            return

        # 平台判定：纯数字默认 QQ(1)；discord:<ID> 显式指定 Discord(2)
        m = re.match(r"^discord:(\S+)$", raw, re.IGNORECASE)
        if m:
            platform, pid = 2, m.group(1)
        elif raw.isdigit():
            platform, pid = 1, raw
        else:
            yield event.plain_result("❌ 无法识别的账号格式：纯数字=QQ号，或 discord:<ID>")
            return

        try:
            result = await self._get_uuid_by_binding(platform, pid)
            if not isinstance(result, dict):
                yield event.plain_result(f"查询失败：{self._safe_msg(result)}")
                return
            if not result.get("Success"):
                msg = result.get("Message", "查询失败")
                yield event.plain_result(f"查询失败：{self._safe_msg(msg)}")
                return
            uuid = result.get("UUID")
            platform_name = self._map_platform_name(platform)
            if not uuid:
                yield event.plain_result(f"❌ {platform_name} 账号 {pid} 未绑定任何游戏账号")
                return
            yield event.plain_result(f"✅ {platform_name} {pid} → UUID: {uuid}")
        except Exception as e:
            logger.error(f"[dslookup] 请求失败: {e}")
            yield event.plain_result(f"查询失败：{self._safe_msg(e)}")

    # ── ds bindings <UUID> ────────────────────────────────────

    @filter.command("ds bindings")
    async def cmd_ds_bindings(self, event: AstrMessageEvent):
        """查询账号绑定的渠道信息（QQ号、Discord ID 等）"""
        if self._is_blacklisted_group(event):
            event.stop_event()
            return
        # 插件可用范围：无关群完全静默
        if not self._check_basic_access(event):
            event.stop_event()
            return
        if not self._check_access(event):
            if event.get_group_id():
                event.stop_event()  # 群聊无权限（普通玩家群）静默，避免刷屏
                return
            yield event.plain_result("❌ 未获得使用权限，请联系管理员")
            return

        text = str(event.message_str).strip()
        parts = text.split(maxsplit=2)
        if len(parts) < 3:
            yield event.plain_result("❌ 格式: ds bindings <UUID>\n例如: ds bindings 550e8400-e29b-41d4-a716-446655440000")
            return
        uuid = parts[2].strip()
        if not uuid:
            yield event.plain_result("❌ 格式: ds bindings <UUID>")
            return

        try:
            result = await self._get_account_bindings(uuid)
            if not isinstance(result, dict) or not result.get("Success"):
                msg = result.get("Message", "查询失败") if isinstance(result, dict) else result
                yield event.plain_result(f"查询失败：{self._safe_msg(msg)}")
                return

            bindings = result.get("Bindings", []) or []
            total = result.get("TotalCount", len(bindings))
            if total == 0 or not bindings:
                yield event.plain_result(f"该账号暂无绑定渠道（UUID: {uuid}）")
                return

            items = []
            for b in bindings:
                platform = b.get("Platform")
                platform_name = self._map_platform_name(platform)
                pid = b.get("PlatformUniqueId", "?")
                created = self._format_display_time(b.get("CreatedAt", ""))
                items.append(
                    f"• {platform_name} | {pid} | 绑定于 {created}"
                )

            title = f"UUID {uuid} 共 {total} 个绑定渠道"
            await self._send_list_result(event, title, items)
        except Exception as e:
            logger.error(f"[dsbindings] 请求失败: {e}")
            yield event.plain_result(f"查询绑定渠道失败：{self._safe_msg(e)}")

    # ── ds ban <PlayerId> [Reason] ────────────────────────────

    @filter.command("ds ban")
    async def cmd_ds_ban(self, event: AstrMessageEvent):
        """封禁玩家（支持 --select N，40秒等待，直接回复 y 确认）"""
        if self._is_blacklisted_group(event):
            event.stop_event()
            return
        # 插件可用范围：无关群完全静默
        if not self._check_basic_access(event):
            event.stop_event()
            return
        if not self._check_access(event):
            if event.get_group_id():
                event.stop_event()  # 群聊无权限（普通玩家群）静默，避免刷屏
                return
            yield event.plain_result("❌ 未获得使用权限，请联系管理员")
            return

        gid = event.get_group_id()
        cache_key = f"private:{event.get_sender_id()}" if gid is None else f"{gid}:{event.get_sender_id()}"

        # 解析参数
        text = str(event.message_str).strip()
        words = text.split()
        player_id = None
        ban_reason = "管理员封禁"
        select_idx = None
        reason_parts = []
        i = 2
        select_seen = False
        while i < len(words):
            w = words[i]
            if w.lower() in ('y', 'yes'):
                pass  # 忽略 y，统一走 on_message 确认
            elif w == '--select':
                select_seen = True
                if i + 1 < len(words):
                    try:
                        select_idx = int(words[i + 1])
                        i += 1  # 成功解析为数字才消费，否则该词归入原因
                    except ValueError:
                        pass
            elif w.startswith('--select='):
                select_seen = True
                try:
                    select_idx = int(w.split('=', 1)[1])
                except ValueError:
                    pass
            elif player_id is None:
                player_id = w
            else:
                # 原因收集：--select 之后的词同样计入原因（修复原逻辑原因丢失）
                reason_parts.append(w)
            i += 1

        if reason_parts:
            ban_reason = ' '.join(reason_parts)

        if player_id is None:
            yield event.plain_result("格式错误，使用: ds ban <PlayerId> [Reason] [--select N]")
            return

        # Step 1: 查询玩家记录
        try:
            response = await self._search_player_records(player_id)
        except Exception as e:
            logger.error(f"[dsban] 查询玩家记录失败: {e}")
            yield event.plain_result(f"查询玩家记录失败：{self._safe_msg(e)}")
            return

        if not isinstance(response, dict):
            yield event.plain_result("查询玩家记录失败：响应格式错误")
            return

        # Step 2: 提取并排序（不再按 IP 去重，同一 IP 的所有记录全部列出，防止玩家更换名称躲避封禁）
        data = response.get("Data", {})
        all_players = []
        for sid, sdata in data.items():
            for p in sdata.get("Players", []):
                all_players.append(p)

        if not all_players:
            yield event.plain_result("未找到玩家 '" + player_id + "' 的记录")
            return

        if self.config.get("sort_by_time", True):
            all_players = self._sort_records_by_time(all_players, "JoinTime")

        players = all_players

        # Step 3: 分流处理
        if len(players) == 1:
            target = players[0]
            await self._get_server_tag_map()  # 单结果也确保服务器名可解析
        else:
            # ── 多结果 + 有 --select → 按序号选 ──
            if select_idx is not None:
                if select_idx < 1 or select_idx > len(players):
                    yield event.plain_result("序号无效，有效范围 1-" + str(len(players)))
                    return
                target = players[select_idx - 1]
            else:
                # -- 多结果 无 --select → 保存 pending select 状态 --
                await self._get_server_tag_map()
                record_items = []
                for i, p in enumerate(players, 1):
                    record_items.append(
                        f"{i}. {p.get('PlayerName', '?')} | "
                        f"UUID: {p.get('PlayerId', '?')} | "
                        f"IP: {p.get('PlayerIP', '?')} | "
                        f"服务器: {self._resolve_server_tag(p.get('ServerId'))} | "
                        f"{self._format_short_time(p.get('JoinTime', ''))}"
                    )

                self._clean_expired_confirms()
                self._pending_confirms[cache_key] = {
                    "type": "select",
                    "data": {
                        "players": players,
                        "reason": ban_reason,
                        "player_id": player_id,
                    },
                    "ts": time.time(),
                }
                # 启动后台超时计时器
                old_task = self._timeout_tasks.get(cache_key)
                if old_task and not old_task.done():
                    old_task.cancel()
                self._timeout_tasks[cache_key] = asyncio.create_task(
                    self._schedule_timeout_notice(cache_key, event, "select")
                )

                hint_text = "请回复对应序号选择要封禁的玩家（60秒内有效）"

                header = f"找到 {len(players)} 个匹配的玩家记录："
                await self._send_list_result(event, header, record_items, hint_text)
                return

        # Step 4: 缓存确认信息并启动后台超时计时器
        pname = target.get('PlayerName', '?')
        puuid = target.get('PlayerId', '?')
        await self._get_server_tag_map()
        ptag = self._resolve_server_tag(target.get('ServerId'))
        self._clean_expired_confirms()
        self._pending_confirms[cache_key] = {
            "type": "ban",
            "data": {
                "player_name": target.get('PlayerName', ''),
                "uuid": target.get('PlayerId', ''),
                "reason": ban_reason,
            },
            "ts": time.time(),
        }
        # 取消旧计时器，启动新计时器
        old_task = self._timeout_tasks.get(cache_key)
        if old_task and not old_task.done():
            old_task.cancel()
        self._timeout_tasks[cache_key] = asyncio.create_task(
            self._schedule_timeout_notice(cache_key, event, "ban")
        )
        yield event.plain_result(
            "即将封禁玩家 " + pname + "\n"
            "UUID: " + puuid + "\n"
            "服务器: " + ptag + "\n"
            "原因: " + ban_reason + "\n"
            "请直接回复 y 来确认封禁（40秒内有效）"
        )

    # ===================================================================
    #  通用消息处理器 — @bot + 6位hex 验证码（auth/verify）
    # ===================================================================

    @filter.regex(r'^(?!ds\b).*')
    async def on_auth_verify_message(self, event: AstrMessageEvent):
        """监听被 @ 且含 6 位 hex 验证码的消息，调用 auth/verify 完成玩家登录验证。

        - 只响应被 @/唤醒（或私聊）且消息中含 6 位 hex 验证码的消息
        - 不拦截 ds 指令（正则排除）
        - 存在 pending 确认流程时让位，避免抢消息
        - 群聊白名单/黑名单照旧生效
        """
        if self._is_blacklisted_group(event):
            return  # 不 yield，完全无视
        if not event.is_at_or_wake_command:
            return

        msg_text = str(event.message_str).strip()
        m = re.search(r'\b[0-9a-fA-F]{6}\b', msg_text)
        if not m:
            return
        code = m.group(0)

        # 确认流程优先：y / 序号回复让位给 on_confirm_message；
        # 其余含验证码的消息正常处理验证码，不打断确认流程（修复验证码消息误取消 pending）
        gid = event.get_group_id()
        cache_key = f"private:{event.get_sender_id()}" if gid is None else f"{gid}:{event.get_sender_id()}"
        pending = self._pending_confirms.get(cache_key)
        if pending is not None:
            ptype = pending.get("type")
            if msg_text.lower() == 'y' or (
                ptype in ("select", "unban_select") and msg_text.isdigit()
            ):
                return

        # 本地先做群级白名单校验（私聊/临时会话放行，群聊须命中群白名单，否则静默）；
        # 成员级判定仍提交服务器，服务器未放行时静默不回复（见下方失败分支）。
        # 验证码不限频：允许任何人短时间内连续触发（按需求移除限流）

        # 群聊白名单校验：私聊/临时会话放行；群聊须命中群白名单，否则静默
        if not self._check_verify_access(event):
            logger.info(
                f"[authverify] 未命中群白名单，静默: "
                f"sender={event.get_sender_id()} gid={event.get_group_id()}"
            )
            return

        # 渠道映射：0=Unknown, 1=QQ, 2=Discord（与文档2 Channel 枚举一致）
        platform_name = event.get_platform_name()
        if platform_name == "discord":
            channel = 2
        elif platform_name in ("aiocqhttp", "qq"):
            channel = 1
        else:
            channel = 0
        channel_unique_id = str(event.get_sender_id())
        # Discord 平台附带授权范围：服务器ID 或 服务器ID:频道ID（白名单按服务器/频道级判定）
        scope = None
        if platform_name == "discord":
            guild_id, channel_id = self._get_discord_scope(event)
            if guild_id:
                scope = f"{guild_id}:{channel_id}" if channel_id else guild_id

        try:
            result = await self._verify_auth(code, channel, channel_unique_id, scope)
            status = result.get("status") if isinstance(result, dict) else None
            if isinstance(result, dict) and (result.get("Success") or status == 200):
                uuid = result.get("UUID") or ""
                logger.info(f"[authverify] 验证成功: code={code} channel={channel} uid={channel_unique_id} uuid={uuid}")
                if platform_name == "discord":
                    yield event.plain_result(
                        "✅ Login successful, verification code accepted"
                    )
                else:
                    yield event.plain_result("✅ 登录成功，验证码已通过")
            else:
                msg = result.get("Message", "") if isinstance(result, dict) else str(result)
                reason = self._map_verify_failure(status, msg)
                logger.warning(
                    f"[authverify] 服务器未放行: code={code} status={status} "
                    f"msg={msg} reason={reason}"
                )
                # 回复失败原因（QQ 中文，Discord 只英文）
                if platform_name == "discord":
                    en = self._map_verify_failure_en(status, msg)
                    text = f"❌ {en}"
                else:
                    text = f"❌ {reason}"
                # 私聊/临时会话验证失败：首次追加手机端提示（30 分钟冷却）
                # 账号被封禁/禁用、不在白名单群等都不是验证环境问题，不提示手机端小窗
                skip_tip = (
                    "banned" in str(msg).lower()
                    or "disabled" in str(msg).lower()
                    or "whitelist" in str(msg).lower()
                    or "封禁" in reason
                    or "禁用" in reason
                    or "白名单" in reason
                    or "可用群聊" in reason
                    or "受管群" in reason
                )
                if not gid and not skip_tip:
                    now = time.time()
                    cutoff = now - self._verify_fail_tip_window
                    self._verify_fail_tip_cooldown = {
                        k: v for k, v in self._verify_fail_tip_cooldown.items() if v >= cutoff
                    }
                    last = self._verify_fail_tip_cooldown.get(cache_key)
                    if last is None or (now - last) >= self._verify_fail_tip_window:
                        self._verify_fail_tip_cooldown[cache_key] = now
                        text += (
                            "\n💡 若你使用手机端进行验证，请在保持游戏进程存活的情况下，"
                            "用小窗或者分屏进入QQ发起验证"
                            "注意验证码大小写匹配"
                        )
                yield event.plain_result(text)
        except Exception as e:
            logger.error(f"[authverify] 请求失败: {e}")
            # 请求异常也静默，避免打扰（与其他 bot 行为一致）

    # ===================================================================
    #  通用消息处理器 — 处理 ds ban / ds unban 的 y 确认
    # ===================================================================

    @filter.regex(r'^(?!ds\b).*')
    async def on_confirm_message(self, event: AstrMessageEvent):
        """监听所有非指令消息，处理 pending 确认（y/Y）"""
        if self._is_blacklisted_group(event):
            return  # 不 yield，完全无视

        msg_text = str(event.message_str).strip()

        gid = event.get_group_id()
        cache_key = f"private:{event.get_sender_id()}" if gid is None else f"{gid}:{event.get_sender_id()}"

        pending = self._pending_confirms.get(cache_key)
        if pending is None:
            return  # 无 pending，不处理

        # 验证码让位：被 @/唤醒且含 6 位 hex 验证码的消息应交给
        # on_auth_verify_message 处理，避免误取消确认流程；
        # 仅当处于序号选择流程且消息为纯数字时，确认流程优先（用户在选人）
        if event.is_at_or_wake_command and re.search(r'\b[0-9a-fA-F]{6}\b', msg_text):
            if not (msg_text.isdigit() and pending.get("type") in ("select", "unban_select")):
                return

        # 检查超时（select 类型 60 秒，ban/unban 类型 40 秒）
        timeout_limit = self._select_timeout if pending.get("type") in ("select", "unban_select") else self._confirm_timeout
        if time.time() - pending.get("ts", 0) > timeout_limit:
            self._pending_confirms.pop(cache_key, None)
            timeout_task = self._timeout_tasks.pop(cache_key, None)
            if timeout_task and not timeout_task.done():
                timeout_task.cancel()
            yield event.plain_result("操作超时，请重新发起操作")
            return

        action_type = pending.get("type")
        data = pending.get("data", {})

        # 取消后台超时计时器
        timeout_task = self._timeout_tasks.pop(cache_key, None)
        if timeout_task and not timeout_task.done():
            timeout_task.cancel()

        # ── select 类型：等待用户回复序号 ──
        if action_type == "select":
            if msg_text.isdigit():
                select_num = int(msg_text)
                select_data = pending.get("data", {})
                players = select_data.get("players", [])

                if select_num < 1 or select_num > len(players):
                    # 取消超时计时器
                    timeout_task = self._timeout_tasks.pop(cache_key, None)
                    if timeout_task and not timeout_task.done():
                        timeout_task.cancel()
                    self._pending_confirms.pop(cache_key, None)
                    yield event.plain_result("请输入对应序号，有效范围 1-" + str(len(players)) + "。本次操作已取消，请重新执行封禁操作")
                    return

                # 选择有效，转为 ban 确认
                target = players[select_num - 1]
                pname = target.get('PlayerName', '?')
                puuid = target.get('PlayerId', '?')

                # 更新 pending 为 ban 类型，刷新时间戳
                self._pending_confirms[cache_key] = {
                    "type": "ban",
                    "data": {
                        "player_name": target.get('PlayerName', ''),
                        "uuid": target.get('PlayerId', ''),
                        "reason": select_data.get("reason", "管理员封禁"),
                    },
                    "ts": time.time(),
                }
                # 重启超时计时器
                old_task = self._timeout_tasks.get(cache_key)
                if old_task and not old_task.done():
                    old_task.cancel()
                self._timeout_tasks[cache_key] = asyncio.create_task(
                    self._schedule_timeout_notice(cache_key, event, "ban")
                )

                yield event.plain_result(
                    "即将封禁玩家 " + pname + "\n"
                    "UUID: " + puuid + "\n"
                    "原因: " + select_data.get("reason", "管理员封禁") + "\n"
                    "请直接回复 y 来确认封禁（40秒内有效）"
                )
                return
            else:
                # 非数字 → 操作错误，结束流程
                timeout_task = self._timeout_tasks.pop(cache_key, None)
                if timeout_task and not timeout_task.done():
                    timeout_task.cancel()
                self._pending_confirms.pop(cache_key, None)
                yield event.plain_result("请输入对应序号，操作已取消，请重新执行封禁操作")
                return

        # ── unban_select 类型：等待用户回复序号（解封选人）──
        if action_type == "unban_select":
            if msg_text.isdigit():
                select_num = int(msg_text)
                select_data = pending.get("data", {})
                players = select_data.get("players", [])

                if select_num < 1 or select_num > len(players):
                    self._pending_confirms.pop(cache_key, None)
                    timeout_task = self._timeout_tasks.pop(cache_key, None)
                    if timeout_task and not timeout_task.done():
                        timeout_task.cancel()
                    yield event.plain_result("请输入对应序号，有效范围 1-" + str(len(players)) + "。本次操作已取消，请重新执行解封操作")
                    return

                target = players[select_num - 1]
                puuid = target.get('PlayerId', '?')

                # 更新 pending 为 unban 类型，刷新时间戳
                self._pending_confirms[cache_key] = {
                    "type": "unban",
                    "data": {"uuid": target.get('PlayerId', '')},
                    "ts": time.time(),
                }
                old_task = self._timeout_tasks.get(cache_key)
                if old_task and not old_task.done():
                    old_task.cancel()
                self._timeout_tasks[cache_key] = asyncio.create_task(
                    self._schedule_timeout_notice(cache_key, event, "unban")
                )
                yield event.plain_result(
                    "即将解封 UUID: " + puuid + "\n"
                    "请直接回复 y 来确认解封（40秒内有效）"
                )
                return
            else:
                self._pending_confirms.pop(cache_key, None)
                timeout_task = self._timeout_tasks.pop(cache_key, None)
                if timeout_task and not timeout_task.done():
                    timeout_task.cancel()
                yield event.plain_result("请输入对应序号，操作已取消，请重新执行解封操作")
                return

        # ── ban / unban 类型：等待 y 确认 ──
        # 检查消息是否为 y/Y（不区分大小写）
        if msg_text.lower() == 'y':
            self._pending_confirms.pop(cache_key, None)

            if action_type == "ban":
                try:
                    result = await self._ban_uuid(
                        data.get("uuid", ""),
                        data.get("reason", "管理员封禁"),
                    )
                    if isinstance(result, dict) and result.get("Success"):
                        logger.info(f"[dsban] 封禁成功: {data.get('player_name')} ({data.get('uuid')})")
                        yield event.plain_result("封禁成功：" + str(result.get('Message', '')))
                    else:
                        msg = result.get("Message", "封禁失败") if isinstance(result, dict) else result
                        yield event.plain_result("封禁请求失败：" + self._safe_msg(msg))
                except Exception as e:
                    logger.error(f"[dsban] 封禁请求失败: {e}")
                    yield event.plain_result("封禁请求失败：" + self._safe_msg(e))
            elif action_type == "unban":
                uuid = data.get("uuid", "")
                try:
                    result = await self._unban_uuid(uuid)
                    if isinstance(result, dict) and result.get("Success"):
                        logger.info(f"[dsunban] 解封成功: {uuid}")
                        yield event.plain_result("解封成功：" + str(result.get('Message', '')))
                    else:
                        msg = result.get("Message", "解封失败") if isinstance(result, dict) else result
                        yield event.plain_result("解封请求失败：" + self._safe_msg(msg))
                except Exception as e:
                    logger.error(f"[dsunban] 解封请求失败: {e}")
                    yield event.plain_result("解封请求失败：" + self._safe_msg(e))
        else:
            # 非 y 内容 → 操作错误，结束流程
            self._pending_confirms.pop(cache_key, None)
            label = "封禁" if action_type == "ban" else "解封"
            yield event.plain_result("操作错误，请重新发起" + label + "操作")
        
    # end of on_confirm_message

    # ===================================================================
    #  群成员事件监听 — 进群/退群通知批量上报 MasterServer
    # ===================================================================

    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def on_member_event(self, event: AstrMessageEvent):
        """监听进群/退群通知（OneBot notice），按批量窗口上报 MasterServer。

        与 GroupSyncManager（轮询差异）互补：本处理器是事件驱动的即时增量。
        - 只处理 post_type=notice 且 notice_type=group_decrease/group_increase
        - 只处理受管群（沿用 group_sync_groups）
        - 机器人自身（被移出/自己进群）跳过
        - 事件进入 MemberEventSyncManager 防抖累积（静默 1 秒 / 最长 5 秒），
          到期后桥接进 GroupSyncManager，复用 EWhitelistPush(Type=3) 版本链上报
        """
        raw = getattr(event.message_obj, "raw_message", None)
        if not isinstance(raw, dict):
            return
        if raw.get("post_type") != "notice":
            return
        notice_type = raw.get("notice_type")
        if notice_type not in ("group_decrease", "group_increase"):
            return

        group_id = str(raw.get("group_id", "") or "")
        user_id = str(raw.get("user_id", "") or "")
        if not group_id or not user_id:
            return
        # 机器人自身（被移出/自己进群）不上报
        try:
            if user_id == event.get_self_id():
                return
        except Exception:
            pass
        # 只处理受管群（沿用 group_sync_groups）
        if not self.member_event_sync.is_managed(group_id):
            return

        kind = "decrease" if notice_type == "group_decrease" else "increase"
        sub_type = str(raw.get("sub_type", "") or "")
        ts = raw.get("time")
        try:
            ts = int(ts) if ts is not None else None
        except (TypeError, ValueError):
            ts = None
        self.member_event_sync.submit(kind, group_id, user_id, sub_type, ts)
    # end of on_member_event
