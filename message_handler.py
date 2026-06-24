"""QQ Bot 入站消息处理器：将 QQ Dispatch 事件转换为 MessageEnvelope"""
from __future__ import annotations

import json
import re
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
EVENT_GROUP_MESSAGE_CREATE = "GROUP_MESSAGE_CREATE"
EVENT_FRIEND_ADD = "FRIEND_ADD"
EVENT_FRIEND_DEL = "FRIEND_DEL"
EVENT_GROUP_ADD_ROBOT = "GROUP_ADD_ROBOT"
EVENT_GROUP_DEL_ROBOT = "GROUP_DEL_ROBOT"


class MessageHandler:
    """QQ Bot 入站消息处理器。

    将 QQ 官方 Bot 的 Dispatch 事件 JSON 转换为 Neo-MoFox 的 MessageEnvelope。
    支持单聊 (C2C_MESSAGE_CREATE) 和群聊消息 (GROUP_AT_MESSAGE_CREATE, GROUP_MESSAGE_CREATE)。

    QQ Bot 消息 payload 的 author 字段中包含 username，可直接提取使用。
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
        elif t in (EVENT_GROUP_AT_MESSAGE_CREATE, EVENT_GROUP_MESSAGE_CREATE):
            return await self._handle_group_at_message(d, platform, features_config)
        elif t in (EVENT_FRIEND_ADD, EVENT_GROUP_ADD_ROBOT):
            logger.info(f"收到系统事件: {t}")
            return None
        else:
            # 未处理的事件类型，始终 debug 打印 payload 方便排查
            logger.info(f"未处理事件 (t={t}) 原始 payload:\n{json.dumps(d, ensure_ascii=False, indent=2)}")
            return None

    def _maybe_log_payload(self, d: dict[str, Any], t: str, features_config: Any) -> None:
        """如果开启了 debug_log_raw_payload 配置，打印原始 payload。"""
        if getattr(features_config, "debug_log_raw_payload", False):
            logger.info(f"收到 {t} 原始 payload:\n{json.dumps(d, ensure_ascii=False, indent=2)}")

    def _replace_mentions(self, content: str, mentions: list[dict[str, Any]]) -> str:
        """替换 content 中的 <@OPENID> 为可读格式 @<username：openid>。"""
        if not mentions:
            return content

        mention_map: dict[str, str] = {}
        for m in mentions:
            uid = m.get("id", "") or m.get("member_openid", "")
            username = m.get("username", "") or uid
            if uid:
                mention_map[uid] = f"@<{username}：{uid}>"

        def _replace(match: re.Match[str]) -> str:
            uid = match.group(1)
            return mention_map.get(uid, match.group(0))

        return re.sub(r"<@([A-F0-9]+)>", _replace, content)

    @staticmethod
    def _sanitize_content(content: str) -> str:
        """清理 QQ 消息中的特殊标记。

        - <faceType=N,faceId="M",ext="base64_json"> → 解析 ext 提取描述
        - <emoji:id> → 保留（标准格式）
        - <#channel_id> → [频道:channel_id]
        """
        def _replace_face_tag(match: re.Match[str]) -> str:
            """解析 faceType 标签并返回可读文本。"""
            face_type = match.group("ft")
            face_id = match.group("fid") or ""
            ext_b64 = match.group("ext") or ""

            # 尝试解析 ext 中的 base64 JSON
            ext_text = ""
            if ext_b64:
                try:
                    ext_json = json.loads(base64.b64decode(ext_b64).decode("utf-8"))
                    ext_text = ext_json.get("text", "").strip()
                except Exception:
                    pass

            # 有文本描述的优先使用
            if ext_text:
                return ext_text

            # 有 faceId 但不是 "0"（非默认），说明是已知系统表情
            if face_id and face_id != "0":
                return f"[QQ表情{face_type}#{face_id}]"

            # 无法解析，给通用占位符
            return "[QQ表情]"

        # 替换 <faceType=N,faceId="M",ext="base64"> 格式
        content = re.sub(
            r'<faceType=(?P<ft>\d+),faceId="(?P<fid>[^"]*)",ext="(?P<ext>[^"]*)"\s*>',
            _replace_face_tag,
            content,
        )

        # 替换 <#channel_id> 频道引用
        content = re.sub(r"<#(\d+)>", r"[频道:\1]", content)

        return content

    @staticmethod
    def _prepend_quote_content(content: str, msg_elements: list[dict[str, Any]]) -> str:
        """将引用消息的原文拼接到 content 前。

        QQ Bot 引用消息会在 msg_elements 中包含被引用的消息内容。
        格式化为 "「引用xxx的消息：原文」\n当前消息" 供 LLM 理解上下文。

        Args:
            content: 当前消息文本
            msg_elements: msg_elements 数组

        Returns:
            str: 拼接后的完整内容
        """
        if not msg_elements:
            return content

        for elem in msg_elements:
            author = elem.get("author", {})
            author_name = author.get("username", "未知用户") or "未知用户"
            quoted_content = elem.get("content", "") or "[非文本消息]"
            quote_line = f"「引用{author_name}的消息：{quoted_content}」"

            # 将引用内容放在最前面
            if content:
                content = f"{quote_line}\n{content}"
            else:
                content = quote_line

        return content

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
        username = author.get("username", "") or f"QQ用户_{user_openid[:8]}"
        content = d.get("content", "")
        message_id = d.get("id", "")
        attachments = d.get("attachments") or []
        timestamp = d.get("timestamp", "")
        msg_elements = d.get("msg_elements") or []

        # 调试：打印原始 payload
        self._maybe_log_payload(d, EVENT_C2C_MESSAGE_CREATE, features_config)

        # 引用消息：将引用的原文拼到 content 前
        content = self._prepend_quote_content(content, msg_elements)

        # 清理 QQ 特殊标记（表情等）
        content = self._sanitize_content(content)

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
                nickname=username,
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
        """处理群聊消息 (GROUP_AT_MESSAGE_CREATE / GROUP_MESSAGE_CREATE)"""
        if not getattr(features_config, "enable_group_message", True):
            return None

        author = d.get("author", {})
        member_openid = author.get("member_openid", "")
        username = author.get("username", "") or f"群成员_{member_openid[:8]}"
        content = d.get("content", "").strip()
        group_openid = d.get("group_openid", "")
        message_id = d.get("id", "")
        attachments = d.get("attachments") or []
        timestamp = d.get("timestamp", "")
        mentions = d.get("mentions") or []
        msg_elements = d.get("msg_elements") or []

        # 调试：打印原始 payload
        self._maybe_log_payload(d, EVENT_GROUP_MESSAGE_CREATE, features_config)

        # 引用消息：将引用的原文拼到 content 前
        content = self._prepend_quote_content(content, msg_elements)

        # 替换 <@OPENID> 为 @<username：openid>
        content = self._replace_mentions(content, mentions)

        # 清理 QQ 特殊标记（表情等）
        content = self._sanitize_content(content)

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
                nickname=username,
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
                    logger.debug("下载图片失败，使用 URL 代替")
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
                    logger.debug("下载语音失败，使用 URL 代替")
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

            elif content_type == "file" or content_type.startswith("file/"):
                # QQ 图片可能以 "file" 类型发送（非 "image/xxx"），通过文件名后缀区分
                filename: str = att.get("filename", "")
                if filename.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp")):
                    try:
                        img_base64 = await self._download_as_base64(att_url)
                        if img_base64:
                            segments.append({"type": "image", "data": img_base64})
                        else:
                            segments.append({"type": "image", "data": att_url})
                    except Exception:
                        logger.debug("下载文件图片失败，使用 URL 代替")
                        segments.append({"type": "image", "data": att_url})
                else:
                    segments.append({
                        "type": "file",
                        "data": {
                            "url": att_url,
                            "filename": filename or "unknown",
                            "size": att.get("size", 0),
                        },
                    })

            else:
                # 未知附件类型（如合并转发卡片）：记录类型名方便后续支持
                logger.info(
                    f"未识别的附件类型: content_type={content_type!r}, "
                    f"filename={att.get('filename', '')}, url={att_url[:80]}"
                )
                segments.append({
                    "type": "unknown",
                    "data": {
                        "content_type": content_type,
                        "url": att_url,
                        "filename": att.get("filename", "未知附件"),
                        "size": att.get("size", 0),
                    },
                })

        # 兜底：没有任何段时（纯合并转发等），添加占位文本防止 builder.build() 崩溃
        if not segments:
            segments.append({"type": "text", "data": "[不支持的消息类型]"})

        return segments

    async def _download_as_base64(self, url: str) -> str | None:
        """下载 URL 内容并转为 base64 字符串。

        Args:
            url: 资源 URL

        Returns:
            str | None: base64 字符串，失败返回 None
        """
        try:
            async with httpx.AsyncClient(timeout=30.0, trust_env=False) as client:
                response = await client.get(url)
                response.raise_for_status()
                data = response.content
                return base64.b64encode(data).decode("utf-8")
        except Exception:
            logger.debug(f"下载资源失败: {url[:100]}")
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