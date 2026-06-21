"""QQ Bot 入站消息处理器：将 QQ Dispatch 事件转换为 MessageEnvelope"""
from __future__ import annotations

import base64
from typing import Any

import httpx
from mofox_wire import MessageEnvelope
from mofox_wire.builder import MessageBuilder
from mofox_wire.types import UserRole

from src.app.plugin_system.api.log_api import get_logger

logger = get_logger("qqbot_adapter.message_handler")

# QQ Bot Dispatch 事件类型
EVENT_C2C_MESSAGE_CREATE = "C2C_MESSAGE_CREATE"
EVENT_GROUP_AT_MESSAGE_CREATE = "GROUP_AT_MESSAGE_CREATE"
EVENT_FRIEND_ADD = "FRIEND_ADD"
EVENT_FRIEND_DEL = "FRIEND_DEL"
EVENT_GROUP_ADD_ROBOT = "GROUP_ADD_ROBOT"
EVENT_GROUP_DEL_ROBOT = "GROUP_DEL_ROBOT"


class MessageHandler:
    """QQ Bot 入站消息处理器。

    将 QQ 官方 Bot 的 Dispatch 事件 JSON 转换为 Neo-MoFox 的 MessageEnvelope。
    支持单聊 (C2C_MESSAGE_CREATE) 和群聊 @ 消息 (GROUP_AT_MESSAGE_CREATE)。
    """

    def __init__(self) -> None:
        pass

    async def convert(
        self,
        t: str,
        d: dict[str, Any],
        platform: str,
        features_config: Any,
    ) -> MessageEnvelope | None:
        """将 QQ Bot 事件转换为 MessageEnvelope。

        Args:
            t: 事件类型（如 C2C_MESSAGE_CREATE）
            d: 事件数据
            platform: 平台标识
            features_config: 功能特性配置

        Returns:
            MessageEnvelope | None: 转换后的消息信封，不需要处理时返回 None
        """
        if t == EVENT_C2C_MESSAGE_CREATE:
            return await self._handle_c2c_message(d, platform, features_config)
        elif t == EVENT_GROUP_AT_MESSAGE_CREATE:
            return await self._handle_group_at_message(d, platform, features_config)
        elif t in (EVENT_FRIEND_ADD, EVENT_GROUP_ADD_ROBOT):
            logger.info(f"收到系统事件: {t}")
            return None
        else:
            logger.debug(f"忽略未处理的事件类型: {t}")
            return None

    async def _handle_c2c_message(
        self,
        d: dict[str, Any],
        platform: str,
        features_config: Any,
    ) -> MessageEnvelope | None:
        """处理单聊消息 (C2C_MESSAGE_CREATE)"""
        if not getattr(features_config, "enable_c2c_message", True):
            return None

        author = d.get("author", {})
        user_openid = author.get("user_openid", "")
        content = d.get("content", "")
        message_id = d.get("id", "")
        attachments = d.get("attachments") or []
        timestamp = d.get("timestamp", "")

        # 检查封禁列表
        if self._is_user_banned(user_openid, features_config):
            logger.debug(f"用户 {user_openid} 在封禁列表中，跳过消息")
            return None

        # 检查私聊黑白名单
        if not self._check_private_list(user_openid, features_config):
            return None

        # 处理附件（下载图片/语音等）
        segments = await self._build_segments(content, attachments)

        builder = (
            MessageBuilder()
            .direction("incoming")
            .message_id(message_id)
            .from_user(
                user_openid,
                platform=platform,
                nickname=f"QQ用户_{user_openid[:8]}",
                role=UserRole.MEMBER,
            )
        )

        for seg in segments:
            builder.raw_segment(seg)

        envelope = builder.build()

        # 存储原始数据用于被动回复
        if "metadata" not in envelope:
            envelope["metadata"] = {}
        envelope["metadata"]["qq_event_id"] = d.get("id", "")
        envelope["metadata"]["qq_event_type"] = EVENT_C2C_MESSAGE_CREATE
        envelope["metadata"]["qq_user_openid"] = user_openid
        if timestamp:
            envelope["metadata"]["qq_timestamp"] = timestamp

        return envelope

    async def _handle_group_at_message(
        self,
        d: dict[str, Any],
        platform: str,
        features_config: Any,
    ) -> MessageEnvelope | None:
        """处理群聊 @ 消息 (GROUP_AT_MESSAGE_CREATE)"""
        if not getattr(features_config, "enable_group_message", True):
            return None

        author = d.get("author", {})
        member_openid = author.get("member_openid", "")
        content = d.get("content", "").strip()
        group_openid = d.get("group_openid", "")
        message_id = d.get("id", "")
        attachments = d.get("attachments") or []
        timestamp = d.get("timestamp", "")

        # 检查封禁列表
        if self._is_user_banned(member_openid, features_config):
            logger.debug(f"群成员 {member_openid} 在封禁列表中，跳过消息")
            return None

        # 检查群聊黑白名单
        if not self._check_group_list(group_openid, features_config):
            return None

        # 处理附件
        segments = await self._build_segments(content, attachments)

        builder = (
            MessageBuilder()
            .direction("incoming")
            .message_id(message_id)
            .from_user(
                member_openid,
                platform=platform,
                nickname=f"群成员_{member_openid[:8]}",
                role=UserRole.MEMBER,
            )
            .from_group(group_openid, platform=platform)
        )

        for seg in segments:
            builder.raw_segment(seg)

        envelope = builder.build()

        # 存储原始数据用于被动回复
        if "metadata" not in envelope:
            envelope["metadata"] = {}
        envelope["metadata"]["qq_event_id"] = d.get("id", "")
        envelope["metadata"]["qq_event_type"] = EVENT_GROUP_AT_MESSAGE_CREATE
        envelope["metadata"]["qq_group_openid"] = group_openid
        envelope["metadata"]["qq_member_openid"] = member_openid
        if timestamp:
            envelope["metadata"]["qq_timestamp"] = timestamp

        return envelope

    async def _build_segments(
        self,
        content: str,
        attachments: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """构建消息段列表。

        Args:
            content: 文本内容
            attachments: 附件列表

        Returns:
            list[dict]: 消息段列表
        """
        segments: list[dict[str, Any]] = []

        # 文本内容
        if content:
            segments.append({"type": "text", "data": content})

        # 附件（图片/语音/视频/文件）
        for att in attachments:
            content_type = att.get("content_type", "")
            att_url = att.get("url", "")

            if not att_url:
                continue

            if content_type.startswith("image/"):
                # 尝试下载图片并转为 base64
                try:
                    img_base64 = await self._download_as_base64(att_url)
                    if img_base64:
                        segments.append({"type": "image", "data": img_base64})
                    else:
                        segments.append({"type": "image", "data": att_url})
                except Exception:
                    logger.exception("下载图片失败，使用 URL 代替")
                    segments.append({"type": "image", "data": att_url})

            elif content_type.startswith("audio/") or att.get("voice_wav_url"):
                voice_url = att.get("voice_wav_url") or att_url
                try:
                    voice_base64 = await self._download_as_base64(voice_url)
                    if voice_base64:
                        segments.append({"type": "voice", "data": voice_base64})
                    else:
                        segments.append({"type": "voice", "data": voice_url})
                except Exception:
                    logger.exception("下载语音失败，使用 URL 代替")
                    segments.append({"type": "voice", "data": voice_url})

            elif content_type.startswith("video/"):
                segments.append({
                    "type": "video",
                    "data": {
                        "url": att_url,
                        "filename": att.get("filename", "video.mp4"),
                        "size_mb": round(att.get("size", 0) / (1024 * 1024), 2),
                    },
                })

            elif content_type.startswith("file/"):
                segments.append({
                    "type": "file",
                    "data": {
                        "url": att_url,
                        "filename": att.get("filename", "unknown"),
                        "size": att.get("size", 0),
                    },
                })

        return segments

    async def _download_as_base64(self, url: str) -> str | None:
        """下载 URL 内容并转为 base64 字符串。

        Args:
            url: 资源 URL

        Returns:
            str | None: base64 字符串，失败返回 None
        """
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url)
                response.raise_for_status()
                data = response.content
                return base64.b64encode(data).decode("utf-8")
        except Exception:
            logger.exception(f"下载资源失败: {url[:100]}")
            return None

    def _is_user_banned(self, user_id: str, features_config: Any) -> bool:
        """检查用户是否在封禁列表中"""
        ban_list = [str(item) for item in getattr(features_config, "ban_user_id", [])]
        return str(user_id) in ban_list

    def _check_group_list(self, group_openid: str, features_config: Any) -> bool:
        """检查群聊是否应该处理（黑白名单过滤）"""
        group_list_type = getattr(features_config, "group_list_type", "blacklist")
        group_list = [str(item) for item in getattr(features_config, "group_list", [])]

        if not group_list:
            return True

        gid = str(group_openid)
        if group_list_type == "blacklist":
            if gid in group_list:
                logger.debug(f"群聊 {gid} 在黑名单中，跳过")
                return False
            return True
        else:  # whitelist
            if gid not in group_list:
                logger.debug(f"群聊 {gid} 不在白名单中，跳过")
                return False
            return True

    def _check_private_list(self, user_openid: str, features_config: Any) -> bool:
        """检查私聊用户是否应该处理（黑白名单过滤）"""
        private_list_type = getattr(features_config, "private_list_type", "blacklist")
        private_list = [str(item) for item in getattr(features_config, "private_list", [])]

        if not private_list:
            return True

        uid = str(user_openid)
        if private_list_type == "blacklist":
            if uid in private_list:
                logger.debug(f"用户 {uid} 在私聊黑名单中，跳过")
                return False
            return True
        else:  # whitelist
            if uid not in private_list:
                logger.debug(f"用户 {uid} 不在私聊白名单中，跳过")
                return False
            return True