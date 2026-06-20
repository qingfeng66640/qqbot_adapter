"""QQ Bot Adapter 配置定义"""
from __future__ import annotations

from typing import ClassVar

from src.core.components.base.config import BaseConfig, Field, SectionBase, config_section


class QQBotAdapterConfig(BaseConfig):
    """QQ 小龙虾 Bot 适配器配置"""

    config_name: ClassVar[str] = "config"
    config_description: ClassVar[str] = "QQ 官方机器人适配器配置"

    @config_section("plugin", title="插件设置", tag="plugin")
    class PluginSection(SectionBase):
        """插件基本配置"""

        enabled: bool = Field(
            default=True,
            description="是否启用 QQ Bot 适配器",
            label="启用适配器",
            tag="plugin",
        )
        config_version: str = Field(
            default="1.0.0",
            description="配置文件版本",
            label="配置版本",
            disabled=True,
            tag="general",
        )

    @config_section("bot", title="Bot 配置", tag="user")
    class BotSection(SectionBase):
        """Bot 身份与认证配置"""

        app_id: str = Field(
            description="QQ 开放平台的 AppID",
            label="AppID",
            placeholder="输入你的 AppID",
            tag="user",
        )
        app_secret: str = Field(
            description="QQ 开放平台的 AppSecret",
            label="AppSecret",
            input_type="password",
            placeholder="输入你的 AppSecret",
            tag="security",
        )
        bot_name: str = Field(
            default="QQBot",
            description="Bot 的显示名称",
            label="Bot 名称",
            placeholder="Bot 显示名称",
            tag="user",
        )

    @config_section("connection", title="连接配置", tag="network")
    class ConnectionSection(SectionBase):
        """WebSocket 网关连接配置"""

        env: str = Field(
            default="sandbox",
            description="运行环境：sandbox(沙箱) / production(正式)",
            label="运行环境",
            input_type="select",
            choices=["sandbox", "production"],
            tag="network",
            hint="沙箱环境不需要 IP 白名单，正式环境需在 QQ 开放平台配置 IP 白名单",
        )
        intents: int = Field(
            default=33554432,
            description="事件订阅位掩码。默认 33554432=GROUP_AND_C2C_EVENT（群聊@+单聊）",
            label="Intents",
            tag="network",
            hint="传入未授权的 intents 会导致 WebSocket 连接被拒",
        )
        shard_count: int = Field(
            default=1,
            description="分片总数，不涉及分片时固定为 1",
            label="分片数",
            ge=1,
            le=100,
            tag="network",
        )
        reconnect_interval: float = Field(
            default=5.0,
            description="WebSocket 重连间隔（秒）",
            label="重连间隔",
            ge=1.0,
            le=60.0,
            step=1.0,
            tag="network",
        )
        max_reconnect_attempts: int = Field(
            default=0,
            description="最大重连次数，0 表示无限重连",
            label="最大重连次数",
            ge=0,
            le=100,
            tag="network",
        )

    @config_section("features", title="功能特性", tag="general")
    class FeaturesSection(SectionBase):
        """功能特性配置"""

        group_list_type: str = Field(
            default="blacklist",
            description="群聊名单模式: blacklist/whitelist",
            label="群聊名单模式",
            input_type="select",
            choices=["blacklist", "whitelist"],
            tag="list",
        )
        group_list: list[str | int] = Field(
            default_factory=list,
            description="群聊名单（填入 group_openid）；根据名单模式过滤",
            label="群聊名单",
            input_type="list",
            item_type="str",
            tag="list",
            hint="填入 group_openid，根据上面的模式进行过滤",
        )
        private_list_type: str = Field(
            default="blacklist",
            description="私聊名单模式: blacklist/whitelist",
            label="私聊名单模式",
            input_type="select",
            choices=["blacklist", "whitelist"],
            tag="list",
        )
        private_list: list[str | int] = Field(
            default_factory=list,
            description="私聊名单（填入 user_openid）；根据名单模式过滤",
            label="私聊名单",
            input_type="list",
            item_type="str",
            tag="list",
            hint="填入 user_openid，根据上面的模式进行过滤",
        )
        ban_user_id: list[str | int] = Field(
            default_factory=list,
            description="全局封禁的用户 openid 列表",
            label="封禁用户列表",
            input_type="list",
            item_type="str",
            tag="list",
            hint="这些用户的消息将被完全忽略",
        )
        enable_group_message: bool = Field(
            default=True,
            description="是否启用群聊消息处理（GROUP_AT_MESSAGE_CREATE 事件）",
            label="启用群聊消息",
            tag="general",
        )
        enable_c2c_message: bool = Field(
            default=True,
            description="是否启用单聊消息处理（C2C_MESSAGE_CREATE 事件）",
            label="启用单聊消息",
            tag="general",
        )

    plugin: PluginSection = Field(default_factory=PluginSection)
    bot: BotSection = Field(default_factory=BotSection)
    connection: ConnectionSection = Field(default_factory=ConnectionSection)
    features: FeaturesSection = Field(default_factory=FeaturesSection)