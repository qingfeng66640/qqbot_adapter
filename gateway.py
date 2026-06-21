"""QQ Bot WebSocket OpCode 网关连接管理。

实现 QQ 官方 Bot 的 WebSocket 网关协议，包括：
- 获取网关地址（含 fallback）
- 建立 WSS 连接
- OpCode 握手 (Hello → Identify)
- 心跳维持 (Heartbeat / Heartbeat ACK)
- 事件分发 (Dispatch)
- 断线恢复 (Resume)
- intents 自动降级探测
- 指数退避重连（含 jitter）
"""
from __future__ import annotations

import asyncio
import json
import random
import sys
from typing import Any, Callable, Awaitable

import httpx
import websockets
from websockets.asyncio.client import ClientConnection

from src.app.plugin_system.api.log_api import get_logger

logger = get_logger("qqbot_adapter.gateway")

# OpCode 常量
OP_DISPATCH = 0
OP_HEARTBEAT = 1
OP_IDENTIFY = 2
OP_RESUME = 6
OP_RECONNECT = 7
OP_INVALID_SESSION = 9
OP_HELLO = 10
OP_HEARTBEAT_ACK = 11

# 网关 API 端点
GATEWAY_URL_SANDBOX = "https://sandbox.api.sgroup.qq.com/gateway"
GATEWAY_URL_PRODUCTION = "https://api.sgroup.qq.com/gateway"

# 硬编码 fallback WSS 地址（当 /gateway 接口不可用时使用）
FALLBACK_WSS_SANDBOX = "wss://sandbox.api.sgroup.qq.com/websocket/"
FALLBACK_WSS_PRODUCTION = "wss://api.sgroup.qq.com/websocket/"

# Dispatch 事件类型
DISPATCH_READY = "READY"
DISPATCH_RESUMED = "RESUMED"

# 消息事件类型
EVENT_C2C_MESSAGE_CREATE = "C2C_MESSAGE_CREATE"
EVENT_GROUP_AT_MESSAGE_CREATE = "GROUP_AT_MESSAGE_CREATE"

# 网关获取超时（秒）
GATEWAY_FETCH_TIMEOUT = 5.0

# intents 位定义与降级优先级（从高到低剥离）
# 参考 karin-plugin-adapter-qqbot 的 intents 探测机制
INTENT_BIT_MAP: dict[int, str] = {
    1 << 0: "GUILDS",
    1 << 1: "GUILD_MEMBERS",
    1 << 9: "GUILD_MESSAGES",
    1 << 12: "DIRECT_MESSAGE",
    1 << 25: "GROUP_AND_C2C_EVENT",
    1 << 26: "INTERACTION",
    1 << 27: "MESSAGE_AUDIT",
    1 << 28: "FORUMS_EVENT",
    1 << 29: "AUDIO_ACTION",
    1 << 30: "PUBLIC_GUILD_MESSAGES",
}

# 降级剥离顺序：按优先级从高到低逐位移除
INTENT_FALLBACK_ORDER: tuple[int, ...] = (
    1 << 1,   # GUILD_MEMBERS
    1 << 9,   # GUILD_MESSAGES (私域)
    1 << 30,  # PUBLIC_GUILD_MESSAGES (公域)
    1 << 12,  # DIRECT_MESSAGE
    1 << 25,  # GROUP_AND_C2C_EVENT
    1 << 0,   # GUILDS
)


def _describe_intents(intents: int) -> str:
    """将 intents 位掩码转换为可读名称列表。"""
    names = []
    for bit, name in INTENT_BIT_MAP.items():
        if intents & bit:
            names.append(name)
    return ", ".join(names) if names else "NONE"


def _compute_fallback_intents(current: int) -> int:
    """按优先级降级 intents：移除当前最高优先级的一个 intent 位。

    用于鉴权失败时自动探测机器人实际拥有的权限。

    Args:
        current: 当前的 intents 位掩码

    Returns:
        降级后的 intents 位掩码；如果无可降级的位，返回 0
    """
    for bit in INTENT_FALLBACK_ORDER:
        if current & bit:
            return current & ~bit
    return current  # 无法再降级


DispatchCallback = Callable[[str, dict[str, Any], int], Awaitable[None]]


class GatewayConnection:
    """QQ Bot WebSocket 网关连接。

    管理完整的 OpCode 协议生命周期：
    连接 → Hello → Identify → 心跳循环 → Dispatch 事件 → 断线恢复

    特性：
    - intents 自动降级探测（鉴权失败时逐级回退）
    - 指数退避重连（含随机 jitter 防惊群）
    - /gateway 接口 fallback 到硬编码 WSS 地址
    """

    def __init__(
        self,
        app_id: str,
        token_provider: Callable[[], Awaitable[str]],
        intents: int,
        shard: tuple[int, int] = (0, 1),
        env: str = "sandbox",
        reconnect_interval: float = 5.0,
        max_reconnect_attempts: int = 0,
    ) -> None:
        self._app_id = app_id
        self._token_provider = token_provider
        self._intents = intents
        self._original_intents = intents  # 保存原始 intents 用于日志
        self._shard = shard
        self._env = env
        self._reconnect_interval = reconnect_interval
        self._max_reconnect_attempts = max_reconnect_attempts

        # 运行时状态
        self._ws: ClientConnection | None = None
        self._running = False
        self._heartbeat_task: asyncio.Task[Any] | None = None
        self._listen_task: asyncio.Task[Any] | None = None

        # 会话状态（用于 Resume）
        self._session_id: str | None = None
        self._last_seq: int | None = None

        # Ready 事件
        self._ready_event = asyncio.Event()
        self._bot_user: dict[str, Any] = {}

        # dispatch 回调
        self._on_dispatch: DispatchCallback | None = None

        # intents 探测历史
        self._intent_history: list[int] = [intents]

    @property
    def is_connected(self) -> bool:
        """检查 WebSocket 是否已连接且认证通过"""
        return (
            self._ws is not None
            and self._ready_event.is_set()
        )

    @property
    def session_id(self) -> str | None:
        """获取当前会话 ID，用于 Resume"""
        return self._session_id

    @property
    def last_seq(self) -> int | None:
        """获取最后一个事件的序列号"""
        return self._last_seq

    @property
    def bot_user(self) -> dict[str, Any]:
        """获取 Bot 用户信息（READY 事件中包含的 user 字段）"""
        return self._bot_user

    def set_dispatch_callback(self, callback: DispatchCallback | None) -> None:
        """设置 Dispatch 事件回调。"""
        self._on_dispatch = callback

    async def _get_gateway_url(self) -> str:
        """获取 WebSocket 网关地址（带 fallback）。

        优先调用 /gateway 接口，失败或超时时降级到硬编码 WSS 地址。
        """
        api_url = GATEWAY_URL_PRODUCTION if self._env == "production" else GATEWAY_URL_SANDBOX
        fallback_wss = FALLBACK_WSS_PRODUCTION if self._env == "production" else FALLBACK_WSS_SANDBOX

        try:
            token = await self._token_provider()

            async with httpx.AsyncClient(timeout=GATEWAY_FETCH_TIMEOUT) as client:
                response = await client.get(
                    api_url,
                    headers={"Authorization": f"QQBot {token}"},
                )
                response.raise_for_status()
                data = response.json()

            url = data.get("url", "")
            if url:
                logger.info(f"获取网关地址成功: {url}")
                return url

            logger.warning(f"/gateway 返回为空，使用硬编码 WSS {fallback_wss}")
            return fallback_wss

        except Exception as e:
            logger.warning(f"/gateway 调用失败 ({e})，使用硬编码 WSS {fallback_wss}")
            return fallback_wss

    async def connect(self) -> None:
        """建立 WebSocket 连接并完成认证握手"""
        wss_url = await self._get_gateway_url()

        token = await self._token_provider()

        extra_headers = {
            "Authorization": f"QQBot {token}",
            "X-Union-Appid": str(self._app_id),
        }

        logger.info("正在连接 QQ Bot WebSocket 网关...")
        self._ws = await websockets.connect(
            wss_url,
            additional_headers=extra_headers,
            max_size=32 * 1024 * 1024,
            ping_interval=None,  # 自行管理心跳
        )
        logger.info("WebSocket 连接已建立")

        # 启动监听循环
        self._listen_task = asyncio.create_task(self._listen_loop())

    async def start(self) -> None:
        """启动网关连接（含自动重连逻辑）"""
        self._running = True
        attempt = 0

        while self._running:
            try:
                await self.connect()
                # connect() 内部启动了 _listen_loop，等待它结束
                if self._listen_task:
                    try:
                        await self._listen_task
                    except asyncio.CancelledError:
                        break

            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("网关连接异常")

            # 判断是否需要重连
            if not self._running:
                break

            attempt += 1
            if self._max_reconnect_attempts > 0 and attempt > self._max_reconnect_attempts:
                logger.error(f"已达到最大重连次数 {self._max_reconnect_attempts}，停止重连")
                break

            # 指数退避 + jitter: min(30s, 1.5s * 2^min(attempt,5)) + random(0, 1.5s)
            wait_time = min(30.0, 1.5 * (2 ** min(attempt - 1, 5))) + random.uniform(0, 1.5)
            logger.info(f"将在 {wait_time:.1f} 秒后重连（第 {attempt} 次）...")
            await asyncio.sleep(wait_time)

    async def stop(self) -> None:
        """停止网关连接"""
        self._running = False
        self._ready_event.clear()

        # 取消心跳
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        # 取消监听
        if self._listen_task and not self._listen_task.done():
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
            self._listen_task = None

        # 关闭 WS
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        logger.info("网关连接已停止")

    async def _listen_loop(self) -> None:
        """WebSocket 消息监听循环。"""
        try:
            async for raw in self._ws:
                if not self._running:
                    break
                try:
                    payload = json.loads(raw)
                    await self._handle_payload(payload)
                except json.JSONDecodeError:
                    logger.warning(f"收到非 JSON 消息: {raw[:200]}")
                except Exception:
                    logger.exception("处理网关消息异常")
        except asyncio.CancelledError:
            raise
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"WebSocket 连接已关闭: code={e.code} reason={e.reason}")
        except Exception:
            logger.exception("WebSocket 监听循环异常")

    async def _handle_payload(self, payload: dict[str, Any]) -> None:
        """按 OpCode 分发处理消息"""
        op = payload.get("op")
        d = payload.get("d")
        s = payload.get("s")
        t = payload.get("t")

        if s is not None:
            self._last_seq = s

        if op == OP_HELLO:
            await self._handle_hello(d)
        elif op == OP_DISPATCH:
            await self._handle_dispatch(d, s, t)
        elif op == OP_HEARTBEAT_ACK:
            logger.debug("收到心跳 ACK")
        elif op == OP_RECONNECT:
            logger.info("服务端要求重连 (op=7)")
            await self._handle_reconnect()
        elif op == OP_INVALID_SESSION:
            await self._handle_invalid_session()
        else:
            logger.debug(f"收到未知 op={op}")

    async def _handle_hello(self, d: dict[str, Any]) -> None:
        """处理 OpCode 10 Hello"""
        heartbeat_interval = d.get("heartbeat_interval", 45000)
        interval_sec = heartbeat_interval / 1000.0

        logger.info(f"收到 Hello，心跳间隔 {interval_sec:.1f} 秒")

        # 取消旧的心跳任务（避免重连时多个心跳并存）
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        # 判断使用 Identify 还是 Resume
        if self._session_id and self._last_seq is not None:
            logger.info(f"尝试 Resume（session_id={self._session_id}, seq={self._last_seq}）")
            await self._send_resume()
        else:
            await self._send_identify()

        # 启动心跳循环（首心跳带 jitter 防止惊群和超时）
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(interval_sec))

    async def _handle_dispatch(self, d: dict[str, Any], s: int | None, t: str | None) -> None:
        """处理 OpCode 0 Dispatch 事件"""
        if t is None:
            logger.warning("收到 Dispatch 但没有事件类型 t")
            return

        if t == DISPATCH_READY:
            self._session_id = d.get("session_id", "")
            self._bot_user = d.get("user", {})
            bot_name = self._bot_user.get("username", "unknown")
            bot_id = self._bot_user.get("id", "unknown")
            self._ready_event.set()
            logger.info(
                f"Ready! Bot: {bot_name} (id={bot_id}), "
                f"session_id={self._session_id}, version={d.get('version', 'unknown')}"
            )
            return

        if t == DISPATCH_RESUMED:
            self._ready_event.set()
            logger.info("Resumed 成功！")
            return

        # 调用业务回调
        if self._on_dispatch:
            try:
                await self._on_dispatch(t, d, s or 0)
            except Exception:
                logger.exception(f"Dispatch 回调处理异常 (t={t})")

    async def _handle_reconnect(self) -> None:
        """处理 OpCode 7 Reconnect（服务端要求重连）。

        保留 session_id 和 seq 用于 Resume。
        """
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        if self._listen_task:
            self._listen_task.cancel()

    async def _handle_invalid_session(self) -> None:
        """处理 OpCode 9 Invalid Session。

        区分两种情况：
        - 有 prior session_id：曾经鉴权成功过，可能是 session 过期，
          清除 session 后重新 Identify（session_lost）
        - 无 prior session_id：从未成功鉴权，可能是 intents 无权限，
          尝试降级 intents 后重试（auth_fail）
        """
        if self._session_id:
            # 曾成功鉴权 → session 过期，清除状态重新 Identify
            session_preview = (
                self._session_id[:16] + "..."
                if len(self._session_id) > 16
                else self._session_id
            )
            logger.warning(
                f"会话无效（曾有 session_id={session_preview}），清除会话状态后重新 Identify"
            )
            self._session_id = None
            self._last_seq = None
            self._ready_event.clear()
            await self._send_identify()
        else:
            # 从未成功鉴权 → 尝试 intents 降级
            logger.error(
                f"鉴权失败！当前 intents={self._intents}"
                f" ({_describe_intents(self._intents)})"
            )
            fallback = _compute_fallback_intents(self._intents)
            if fallback and fallback != self._intents:
                logger.warning(
                    f"intents 降级: {self._intents} → {fallback}"
                    f" (移除: {_describe_intents(self._intents ^ fallback)})"
                )
                self._intents = fallback
                self._intent_history.append(fallback)
                # 断开当前连接，让重连逻辑用新的 intents 重新连接
                await self._handle_reconnect()
            else:
                logger.error(
                    "所有 intents 均不可用，机器人可能未上线或无权限。"
                    " 请检查 QQ 开放平台的应用状态和 intents 订阅配置。"
                )
                # 停止重连
                self._running = False
                if self._ws:
                    try:
                        await self._ws.close()
                    except Exception:
                        pass
                    self._ws = None

    async def _send_identify(self) -> None:
        """发送 OpCode 2 Identify"""
        token = await self._token_provider()

        payload = {
            "op": OP_IDENTIFY,
            "d": {
                "token": f"QQBot {token}",
                "intents": self._intents,
                "shard": list(self._shard),
                "properties": {
                    "$os": sys.platform,
                    "$browser": "neo-mofox-qqbot-adapter",
                    "$device": "neo-mofox-qqbot-adapter",
                },
            },
        }

        logger.info(
            f"发送 Identify (intents={self._intents} "
            f"[{_describe_intents(self._intents)}], shard={list(self._shard)})"
        )
        await self._ws.send(json.dumps(payload))

    async def _send_resume(self) -> None:
        """发送 OpCode 6 Resume"""
        token = await self._token_provider()

        payload = {
            "op": OP_RESUME,
            "d": {
                "token": f"QQBot {token}",
                "session_id": self._session_id,
                "seq": self._last_seq or 0,
            },
        }

        logger.info(f"发送 Resume (session_id={self._session_id}, seq={self._last_seq})")
        await self._ws.send(json.dumps(payload))

    async def _heartbeat_loop(self, interval: float) -> None:
        """心跳循环。

        首心跳使用随机 jitter（0 ~ interval）避免惊群，
        之后按固定间隔发送 OpCode 1 Heartbeat。
        d 字段填最近一次的 s 序列号，首次连接传 null。
        """
        logger.info(f"心跳循环启动，间隔 {interval:.1f} 秒")

        try:
            # 首心跳：随机延迟 0 ~ interval 防惊群
            first_jitter = interval * random.random()
            logger.debug(f"首心跳将在 {first_jitter:.1f} 秒后发送")
            await asyncio.sleep(first_jitter)

            while self._running and self._ws is not None:
                if not self._running:
                    break

                seq = self._last_seq
                payload = {
                    "op": OP_HEARTBEAT,
                    "d": seq,
                }

                try:
                    await self._ws.send(json.dumps(payload))
                    logger.debug(f"发送心跳 (seq={seq})")
                except websockets.exceptions.ConnectionClosed:
                    logger.warning("心跳发送失败，连接已关闭")
                    break

                # 等待下一个心跳周期
                await asyncio.sleep(interval)

        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("心跳循环异常")

    async def wait_ready(self, timeout: float = 30.0) -> bool:
        """等待 READY 事件（认证成功）"""
        try:
            await asyncio.wait_for(self._ready_event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            logger.error(f"等待 READY 超时（{timeout:.0f} 秒）")
            return False


