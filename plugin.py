"""QQ 小龙虾 Bot 适配器（基于 QQ 官方 WebSocket OpCode 网关协议）

核心流程：
1. AppID + AppSecret → access_token（定时自动刷新）
2. WebSocket 连接 QQ 网关 → Hello → Identify → Ready
3. Dispatch 事件 → from_platform_message → MessageEnvelope
4. CoreSink → 推送到 Neo-MoFox 核心
5. 核心回复 → _send_platform_message → QQ REST API

与 OneBotAdapter 不同，QQ 官方 Bot 使用自定义 OpCode 网关协议，
无法使用 mofox_wire 的 WebSocketAdapterOptions 自动传输，
因此采用完全自定义的传输层（类似 MqttAdapter 的模式）。
"""
from __future__ import annotations

import logging
from typing import Any, cast

from mofox_wire import CoreSink, MessageEnvelope

from src.app.plugin_system.api.log_api import get_logger
from src.core.components.base import BaseAdapter, BasePlugin
from src.core.components.loader import register_plugin

from .config import QQBotAdapterConfig
from .gateway import GatewayConnection
from .token_manager import TokenManager
from .message_handler import MessageHandler
from .send_handler import SendHandler

# 抑制 httpx/httpcore 的 SSL INFO/DEBUG 日志（避免刷屏 "SSL connection is closed"）
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

logger = get_logger("qqbot_adapter")


def _validate_bot_identity(config: QQBotAdapterConfig) -> None:
    """校验 Bot 身份配置。"""
    app_id = str(config.bot.app_id).strip()
    app_secret = str(config.bot.app_secret).strip()

    invalid_values = {"", "none", "null", "undefined", "pydanticundefined"}
    if app_id.lower() in invalid_values:
        raise ValueError("配置项 bot.app_id 无效：必须为非空 AppID")

    if app_secret.lower() in invalid_values:
        raise ValueError("配置项 bot.app_secret 无效：必须为非空 AppSecret")


class QQBotAdapter(BaseAdapter):
    """QQ 小龙虾 Bot 适配器。

    使用 QQ 官方 WebSocket OpCode 网关协议接收消息，
    通过 REST API 发送消息。
    完全自定义传输层，不依赖 mofox_wire 的自动传输。
    """

    adapter_name = "qqbot_adapter"
    adapter_version = "1.0.0"
    adapter_description = "QQ 官方机器人适配器（小龙虾 Bot），基于 WebSocket OpCode 网关协议"
    platform = "qqbot"

    def __init__(self, core_sink: CoreSink, plugin: QQBotAdapterPlugin | None = None, **kwargs: Any) -> None:
        """初始化 QQ Bot 适配器。

        不传 transport 参数，采用完全自定义的传输层。

        Args:
            core_sink: 核心消息接收器
            plugin: 所属插件实例
            **kwargs: 其他参数
        """
        super().__init__(core_sink, plugin=plugin, **kwargs)

        self._gateway: GatewayConnection | None = None
        self._token_mgr: TokenManager | None = None
        self._message_handler: MessageHandler | None = None
        self._send_handler: SendHandler | None = None
        self._gateway_task: Any = None

    async def on_adapter_loaded(self) -> None:
        """适配器加载时的初始化。

        1. 校验配置
        2. 创建 TokenManager 并获取首个 token
        3. 创建 GatewayConnection 并开始监听
        4. 设置 Dispatch 回调 → 父类 AdapterBase.on_platform_message() 处理
        """
        logger.info("QQ Bot 适配器正在启动...")

        if not self.plugin or not self.plugin.config:
            raise RuntimeError("QQ Bot 适配器启动失败：缺少插件配置")

        config = cast(QQBotAdapterConfig, self.plugin.config)
        _validate_bot_identity(config)

        # 创建 Token 管理器
        token_mgr = TokenManager(config.bot.app_id, config.bot.app_secret)
        self._token_mgr = token_mgr
        await token_mgr.start()

        # 创建发送处理器
        self._send_handler = SendHandler(
            token_provider=token_mgr,
            env=config.connection.env,
            bot_name=config.bot.bot_name,
        )

        # 创建消息处理器
        self._message_handler = MessageHandler()

        # 创建网关连接
        gateway = GatewayConnection(
            app_id=config.bot.app_id,
            token_provider=token_mgr.ensure_token,
            intents=config.connection.intents,
            shard=(0, config.connection.shard_count),
            env=config.connection.env,
            reconnect_interval=config.connection.reconnect_interval,
            max_reconnect_attempts=config.connection.max_reconnect_attempts,
        )
        self._gateway = gateway

        # Dispatch 回调：QQ 事件 → 父类 AdapterBase.on_platform_message()
        # 父类的 async on_platform_message 流程：
        #   from_platform_message(raw) → core_sink.send(envelope)
        # 不需要重写 on_platform_message，父类已正确实现。
        async def on_dispatch(t: str, d: dict[str, Any], s: int) -> None:
            raw = {"t": t, "d": d, "s": s}
            await self.on_platform_message(raw)

        gateway.set_dispatch_callback(on_dispatch)

        # 启动网关连接（在后台异步运行）
        import asyncio
        self._gateway_task = asyncio.create_task(gateway.start())

        # 等待 READY
        if await gateway.wait_ready(timeout=30.0):
            logger.info("QQ Bot 适配器已就绪")
        else:
            logger.error("QQ Bot 适配器启动超时：未在 30 秒内收到 READY")

    async def on_adapter_unloaded(self) -> None:
        """适配器卸载时的清理"""
        logger.info("QQ Bot 适配器正在关闭...")

        # 停止网关
        if self._gateway:
            await self._gateway.stop()
            self._gateway = None

        # 取消后台网关任务
        if self._gateway_task and not self._gateway_task.done():
            self._gateway_task.cancel()
            try:
                await self._gateway_task
            except Exception:
                pass

        # 停止 token 管理器
        if self._token_mgr:
            await self._token_mgr.stop()
            self._token_mgr = None

        logger.info("QQ Bot 适配器已关闭")

    async def from_platform_message(self, raw: dict[str, Any]) -> MessageEnvelope | None:  # type: ignore[override]
        """将 QQ Bot 事件转换为 MessageEnvelope。

        由父类 AdapterBase.on_platform_message 调用。

        Args:
            raw: 包含 t (事件类型), d (事件数据), s (序列号) 的字典

        Returns:
            MessageEnvelope | None: 转换后的消息信封，不需要处理时返回 None
        """
        if not self.plugin or not self.plugin.config:
            return None

        if not self._message_handler:
            return None

        config = cast(QQBotAdapterConfig, self.plugin.config)
        t = raw.get("t", "")
        d = raw.get("d", {})

        return await self._message_handler.convert(
            t=t,
            d=d,
            platform=self.platform,
            features_config=config.features,
        )

    async def _send_platform_message(self, envelope: MessageEnvelope) -> None:  # type: ignore[override]
        """将 MessageEnvelope 发送到 QQ 平台。

        通过 QQ REST API 发送消息。
        由于未使用自动传输，必须重写此方法。

        Args:
            envelope: 要发送的消息信封
        """
        if not self._send_handler:
            logger.error("发送处理器未初始化，无法发送消息")
            return

        try:
            await self._send_handler.send(envelope)
        except Exception:
            logger.error("发送 QQ 消息失败", exc_info=True)

    async def health_check(self) -> bool:
        """自定义健康检查：检查网关连接状态。

        Returns:
            bool: 网关是否已连接且就绪
        """
        if self._gateway is None:
            return False

        return self._gateway.is_connected

    async def reconnect(self) -> None:
        """自定义重连逻辑。

        停止网关后重新启动。
        注意：resume 逻辑由 GatewayConnection 内部处理。
        """
        logger.info("手动触发重连...")

        if self._gateway:
            await self._gateway.stop()

        if not self.plugin or not self.plugin.config:
            return

        config = cast(QQBotAdapterConfig, self.plugin.config)

        # 确保 token 有效
        if self._token_mgr:
            await self._token_mgr.ensure_token()

        # 重新创建网关连接
        gateway = GatewayConnection(
            app_id=config.bot.app_id,
            token_provider=self._token_mgr.ensure_token if self._token_mgr else _noop_token,
            intents=config.connection.intents,
            shard=(0, config.connection.shard_count),
            env=config.connection.env,
            reconnect_interval=config.connection.reconnect_interval,
            max_reconnect_attempts=config.connection.max_reconnect_attempts,
        )
        self._gateway = gateway

        async def on_dispatch(t: str, d: dict[str, Any], s: int) -> None:
            raw = {"t": t, "d": d, "s": s}
            await self.on_platform_message(raw)

        gateway.set_dispatch_callback(on_dispatch)

        import asyncio
        self._gateway_task = asyncio.create_task(gateway.start())

        if await gateway.wait_ready(timeout=30.0):
            logger.info("QQ Bot 适配器重连成功")
        else:
            logger.error("QQ Bot 适配器重连超时")

    async def get_bot_info(self) -> dict[str, Any]:  # type: ignore[override]
        """获取 Bot 信息。

        bot_id 必须是 QQ 内部用户 ID（来自 READY 事件的 user.id），
        而非 AppID（AppID 是开发者后台标识，仅用于认证，不用于消息路由）。

        优先级：实时网关数据 > 网关缓存的旧数据 > unknown_bot

        Returns:
            dict: 包含 bot_id、bot_name、platform 的信息
        """
        if not self.plugin or not self.plugin.config:
            return {
                "bot_id": "unknown_bot",
                "bot_name": "Unknown Bot",
                "platform": self.platform,
            }

        config = cast(QQBotAdapterConfig, self.plugin.config)

        # 确定有效的 bot_user 来源
        bot_user: dict[str, Any] = {}
        if self._gateway:
            # gateway._bot_user 在重连期间不会被清空（stop() 才清），
            # 优先用实时数据，其次用缓存的旧 Ready 数据
            if self._gateway.is_connected:
                bot_user = self._gateway.bot_user
            else:
                bot_user = self._gateway.bot_user  # 重连期间保留旧值

        bot_id = bot_user.get("id", "")
        if bot_id:
            return {
                "bot_id": bot_id,
                "bot_name": bot_user.get("username", "") or config.bot.bot_name,
                "bot_is_bot": bot_user.get("bot", True),
                "platform": self.platform,
            }

        # 从未获取到 Ready 信息（罕见：首次连接且 READY 尚未到达）
        logger.warning("Bot 信息不可用，网关尚未收到 READY 事件")
        return {
            "bot_id": "unknown_bot",
            "bot_name": config.bot.bot_name,
            "platform": self.platform,
        }


async def _noop_token() -> str:
    """无操作 token 提供者（fallback）"""
    return ""


@register_plugin
class QQBotAdapterPlugin(BasePlugin):
    """QQ 小龙虾 Bot 适配器插件"""

    plugin_name = "qqbot_adapter"
    plugin_version = "1.0.0"
    plugin_description = "对接腾讯 QQ 官方机器人（小龙虾 Bot），支持 AppID + AppSecret 认证"
    configs = [QQBotAdapterConfig]

    def get_components(self) -> list[type]:
        """获取插件内所有组件类。

        Returns:
            list[type]: 组件类列表
        """
        return [QQBotAdapter]