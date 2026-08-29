"""
NoticeServer WebSocket 客户端

替代原 HTTP(aiohttp) 请求，对接 MasterServer BotNotice 的 /ws/bot 长连接。

协议参考（MasterServer-1 src/Utils/RpcServer.ts + src/Types/BotCommand.ts）：
- 连接地址: ws://host:port/ws/bot?BotId=<id>&Token=<token>
- 双向 RPC 调用: { "Type": <EBotCommand>, "RequestId": <自增ID>, ...业务字段 }
- 调用响应:    { "RequestId": <回显原请求ID>, ...结果字段 }（靠 RequestId 关联）
- 心跳保活: 定期发送 Type=2(EHeartbeat)
- 服务端主动调用（Bot 上线后获取最新版本 / 版本跳跃时请求补包）：
  { "Type": 14(EGetLatestVersion) | 15(EWhitelistDeltaRequest), "RequestId": N }
  客户端必须以 { "RequestId": N, ...结果 } 回包（无 Type，纯响应）
- 认证失败: { "RequestId": 0, "Success": false, "Reason": ... }
"""
import asyncio
import json
import time
from typing import Any, Dict, Optional
from urllib.parse import quote

import aiohttp


class EBotCommand:
    """双向 RPC 命令码（对齐服务端 BotCommand.ts）"""
    EUnknown = 0
    EVerify = 1               # 验证码回调
    EHeartbeat = 2            # 心跳保活
    EWhitelistPush = 3        # 白名单增量推送
    EBanUUID = 4              # 按 UUID 封禁
    EUnbanUUID = 5            # 按 UUID 解封
    EGetBanList = 6           # 查询封禁列表
    EGetAccountBindings = 7   # 查询账号绑定
    ERunCommand = 8           # 执行 DS 控制台命令
    EReload = 9               # 重载服务器配置
    EGetVersion = 10          # 查询版本
    EGetServerList = 11       # 查询服务器列表
    EGetOnlineStatus = 12     # 查询在线状态
    EGetPlayerRecords = 13    # 查询玩家记录
    EGetLatestVersion = 14       # 服务器→客户端：获取最新版本（只回 Version）
    EWhitelistSnapshotRequest = 14  # 兼容旧名（历史误称，实际是 EGetLatestVersion）
    EWhitelistDeltaRequest = 15     # 服务器→客户端：请求补包
    EGetUUIDByBinding = 16          # 按绑定渠道反查 UUID


class NoticeServerWS:
    """MasterServer /ws/bot WebSocket 客户端。

    特性：
    - 后台常驻连接，断线自动重连（固定间隔）
    - 协议层心跳（EHeartbeat），服务端 60 秒无心跳判定离线
    - 请求/响应按 RequestId 关联（自增 ID）；同一时间可存在多个未决请求
    - 认证失败（RequestId=0 + Success=false）记录原因，后续请求直接返回鉴权失败
    - 服务端主动调用（快照请求 Type=14 / 补包请求 Type=15）通过回调上抛宿主，
      回调返回结果 dict 后自动以 { RequestId, ...结果 } 回包
    """

    def __init__(
        self,
        host: str,
        port: int,
        bot_id: str,
        token: str,
        timeout: float = 10.0,
        logger: Any = None,
        heartbeat_interval: float = 25.0,
        reconnect_delay: float = 5.0,
    ):
        self.host = host
        self.port = port
        self.bot_id = bot_id
        self.token = token
        self.timeout = timeout
        self.logger = logger
        self.heartbeat_interval = heartbeat_interval
        self.reconnect_delay = reconnect_delay

        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._task: Optional[asyncio.Task] = None
        self._pending: Dict[int, asyncio.Future] = {}
        self._lock = asyncio.Lock()
        # 发送锁：串行化所有 send_str（请求/单向发送/心跳/调用回包），
        # 避免 aiohttp WebSocket 并发写帧交织导致数据错乱
        self._send_lock = asyncio.Lock()
        self._closed = False
        self._auth_error = ""
        # RequestId 自增分配（服务端从 1 起；0 保留给认证失败响应）
        self._next_request_id = 1

        # 服务端主动调用回调：快照请求 / 补包请求（回调返回 dict 即回包内容）
        self.on_snapshot_request: Optional[Any] = None
        self.on_delta_request: Optional[Any] = None
        # 连接建立/重连回调（用于重建群同步版本基线）
        self.on_connected: Optional[Any] = None

    # ─── 基础属性 ────────────────────────────────────────────

    @property
    def ws_url(self) -> str:
        return (
            f"ws://{self.host}:{self.port}/ws/bot"
            f"?BotId={quote(self.bot_id)}&Token={quote(self.token)}"
        )

    @property
    def ws_url_safe(self) -> str:
        """不含 BotId/Token 的连接地址（用于日志/错误提示，避免泄露敏感信息）。"""
        return f"ws://{self.host}:{self.port}/ws/bot"

    def is_connected(self) -> bool:
        return self._ws is not None and not self._ws.closed

    def _new_request_id(self) -> int:
        """分配一个自增 RequestId（服务端 RpcServer.NextCallId 同款策略）。"""
        rid = self._next_request_id
        self._next_request_id += 1
        if self._next_request_id > 0x7FFFFFFF:
            self._next_request_id = 1
        return rid

    # ─── 生命周期 ────────────────────────────────────────────

    async def ensure_started(self) -> None:
        """惰性启动后台连接/心跳/读取任务（幂等）。"""
        if self._task is None or self._task.done():
            self._closed = False
            self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        """后台主循环：连接 -> 心跳+读取 -> 断线重连。"""
        while not self._closed:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(
                        self.ws_url,
                        heartbeat=30,  # aiohttp 层 ping/pong 探测死链
                        max_msg_size=8 * 1024 * 1024,
                        compress=15,  # aiohttp>=3.14 压缩参数为 int 窗口位(9-15)，True 会抛 ValueError
                    ) as ws:
                        self._ws = ws
                        self._auth_error = ""
                        self._log(f"已连接 {self.ws_url_safe}", level="info")
                        cb = self.on_connected
                        if cb is not None:
                            try:
                                asyncio.create_task(cb())
                            except Exception as e:
                                self._log(f"连接回调执行失败: {e}", level="warning")
                        await asyncio.gather(
                            self._heartbeat_loop(ws),
                            self._read_loop(ws),
                        )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if not self._closed:
                    self._log(f"连接异常: {type(e).__name__}: {e}", level="warning")
            finally:
                self._ws = None
                self._fail_pending("连接已断开")
                if not self._closed:
                    self._log(f"{self.reconnect_delay}s 后重连...", level="warning")
                    await asyncio.sleep(self.reconnect_delay)

    async def _heartbeat_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """协议层心跳：定期发送 EHeartbeat。"""
        while not self._closed and not ws.closed:
            await asyncio.sleep(self.heartbeat_interval)
            try:
                async with self._send_lock:
                    await ws.send_str(json.dumps({"Type": EBotCommand.EHeartbeat}))
            except Exception:
                break

    async def _read_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """读取服务端消息并分发。"""
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except (json.JSONDecodeError, TypeError):
                    continue
                self._dispatch(data)
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break

    def _dispatch(self, data: dict) -> None:
        """分发服务端消息。

        服务端 Rpc 中间件规则（RpcServer.HandleMessage）：
        - 带 Type → 服务端主动调用（快照请求 14 / 补包请求 15）
        - 仅带 RequestId → 调用响应（回显请求 ID）
        - 两者皆无 → 忽略
        """
        if "Type" in data:
            cmd = data.get("Type")
            rid = data.get("RequestId")
            if cmd == EBotCommand.EGetLatestVersion:
                # 服务端主动：获取最新版本 → 上抛宿主，宿主返回 {Version} 后自动回包
                self._log("收到服务端版本查询请求", level="info")
                self._respond_call(rid, self.on_snapshot_request)
            elif cmd == EBotCommand.EWhitelistDeltaRequest:
                # 服务端主动：请求补包（带 FromVersion）→ 上抛宿主
                from_version = data.get("FromVersion", 0)
                self._log(f"收到服务端补包请求 FromVersion={from_version}", level="info")
                self._respond_call(rid, self.on_delta_request, from_version)
            return

        if "RequestId" in data:
            rid = data.get("RequestId")
            if rid == 0 and data.get("Success") is False:
                # 认证失败（服务端唯一使用 RequestId=0 的场景），随后会断开连接
                self._auth_error = str(
                    data.get("Reason") or data.get("Message") or "BotId/Token 无效"
                )
                self._log("认证失败: " + self._auth_error, level="warning")
                return
            fut = self._pending.pop(rid, None)
            if fut is not None and not fut.done():
                fut.set_result(data)
        # 其余消息忽略

    def _respond_call(self, rid, callback, *args) -> None:
        """异步执行服务端主动调用的回调，并以 { RequestId, ...结果 } 回包。"""
        if callback is None:
            return
        async def _run() -> None:
            try:
                result = await callback(*args)
                if result is None:
                    result = {}
                msg: dict = {"RequestId": rid}
                if isinstance(result, dict):
                    msg.update(result)
                else:
                    msg["Result"] = result
                async with self._send_lock:
                    await self._ws.send_str(json.dumps(msg, ensure_ascii=False))
            except Exception as e:
                self._log(
                    f"服务端调用响应失败: {type(e).__name__}: {e}", level="warning"
                )
        try:
            asyncio.create_task(_run())
        except Exception as e:
            self._log(f"服务端调用回调调度失败: {e}", level="warning")

    # ─── 请求接口 ────────────────────────────────────────────

    async def request(
        self,
        command: int,
        payload: Optional[dict] = None,
        timeout: Optional[float] = None,
    ) -> dict:
        """发送一个命令并等待响应。失败时返回 {Success: False, Message} 与旧 HTTP 层语义一致。"""
        timeout = timeout or self.timeout
        await self.ensure_started()

        if self._auth_error:
            return {"Success": False, "Message": f"鉴权失败，BotId/Token 无效：{self._auth_error}"}
        if not self.is_connected():
            await self._wait_connected(timeout)
        if not self.is_connected():
            if self._auth_error:
                return {"Success": False, "Message": f"鉴权失败，BotId/Token 无效：{self._auth_error}"}
            return {"Success": False, "Message": "无法连接服务器"}

        async with self._lock:
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            rid = self._new_request_id()
            self._pending[rid] = fut
            try:
                msg: dict = {"Type": command, "RequestId": rid}
                if payload:
                    msg.update(payload)
                async with self._send_lock:
                    await self._ws.send_str(json.dumps(msg, ensure_ascii=False))
                return await asyncio.wait_for(fut, timeout)
            except asyncio.TimeoutError:
                return {"Success": False, "Message": "请求超时"}
            except Exception as e:
                return {"Success": False, "Message": f"请求异常: {type(e).__name__}: {e}"}
            finally:
                self._pending.pop(rid, None)

    async def send_no_wait(self, command: int, payload: Optional[dict] = None) -> bool:
        """单向发送（不等待响应）。

        用于白名单增量推送（EWhitelistPush）等无需等待结果的调用。
        仍携带自增 RequestId（服务端响应会被忽略，仅用于协议一致性）。
        返回 True 表示已写入 socket。
        """
        await self.ensure_started()
        if self._auth_error:
            return False
        if not self.is_connected():
            await self._wait_connected(self.timeout)
        if not self.is_connected():
            return False
        try:
            msg: dict = {"Type": command, "RequestId": self._new_request_id()}
            if payload:
                msg.update(payload)
            async with self._send_lock:
                await self._ws.send_str(json.dumps(msg, ensure_ascii=False))
            return True
        except Exception as e:
            self._log(f"单向发送失败: {type(e).__name__}: {e}", level="warning")
            return False

    async def _wait_connected(self, timeout: float) -> None:
        """轮询等待连接就绪（最多 timeout 秒）。"""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.is_connected() or self._auth_error:
                return
            await asyncio.sleep(0.2)

    def _fail_pending(self, reason: str) -> None:
        for fut in self._pending.values():
            if not fut.done():
                fut.set_result({"Success": False, "Message": reason})
        self._pending.clear()

    # ─── 工具 ────────────────────────────────────────────────

    def _log(self, text: str, level: str = "info") -> None:
        if self.logger is None:
            return
        try:
            if level == "warning":
                self.logger.warning(f"[NoticeServerWS] {text}")
            else:
                self.logger.info(f"[NoticeServerWS] {text}")
        except Exception:
            pass

    async def close(self) -> None:
        """关闭连接与后台任务（插件卸载时调用）。"""
        self._closed = True
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self._fail_pending("客户端已关闭")
