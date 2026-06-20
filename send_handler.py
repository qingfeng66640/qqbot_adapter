"""QQ Bot 出站消息处理器：将 MessageEnvelope 转换为 QQ REST API 调用"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx

from mofox_wire import MessageEnvelope
from mofox_wire.types import SegPayload

from src.app.plugin_system.api.log_api import get_logger

logger = get_logger("qqbot_adapter.send_handler")

# REST API 基础 URL
API_BASE_SANDBOX = "https://sandbox.api.sgroup.qq.com"
API_BASE_PRODUCTION = "https://api.sgroup.qq.com"

# 被动回复：5 分钟内有效
PASSIVE_REPLY_WINDOW = 300  # 5 分钟（秒）


class SendHandler:
    """QQ Bot 出站消息处理器。

    将 Neo-MoFox 的 MessageEnvelope 转换为 QQ 官方 REST API 调用。
    优先使用被动回复（在收到消息 5 分钟内），超时降级为主动消息。
    """

    def __init__(
        self,
        token_provider: Any,  # TokenManager
        env: str = "sandbox",
        bot_name: str = "QQBot",
    ) -> None:
        self._token_provider = token_provider
        self._env = env
        self._bot_name = bot_name
        self._base_url = API_BASE_PRODUCTION if env == "production" else API_BASE_SANDBOX

    async def send(self, envelope: MessageEnvelope) -> None:
        """发送 MessageEnvelope 到 QQ 平台。

        Args:
            envelope: 要发送的消息信封
        """
        try:
            await self._send_message(envelope)
        except Exception:
            logger.exception("发送 QQ 消息失败")

    async def _send_message(self, envelope: MessageEnvelope) -> None:
        """核心发送逻辑"""
        message_segment = envelope.get("message_segment", [])
        message_info = envelope.get("message_info", {})
        metadata: dict[str, Any] = envelope.get("metadata", {}) or {}

        # 解析 segment
        segments = message_segment if isinstance(message_segment, list) else [message_segment]

        # 判断目标（群聊 vs 单聊）
        group_info = message_info.get("group_info")
        user_info = message_info.get("user_info")

        # 提取文本内容
        text_content = self._extract_text(segments)

        # 检查是否有图片段
        image_segments = [s for s in segments if s.get("type") == "image"]

        # 提取 msg_id 用于被动回复
        msg_id = metadata.get("qq_event_id", "")
        qq_timestamp = metadata.get("qq_timestamp", "")

        # 判断是否可以使用被动回复
        can_passive_reply = self._can_passive_reply(msg_id, qq_timestamp)

        token = await self._token_provider.ensure_token()
        headers = {
            "Authorization": f"QQBot {token}",
            "Content-Type": "application/json",
        }

        if group_info:
            group_openid = metadata.get("qq_group_openid", "") or group_info.get("group_id", "")
            if not group_openid:
                logger.error("群聊消息缺少 group_openid")
                return

            if image_segments and not text_content:
                # 纯图片消息
                await self._send_media_message(
                    headers, "group", group_openid, image_segments[0], msg_id, can_passive_reply
                )
            else:
                await self._send_text_message(
                    headers, "group", group_openid, text_content, msg_id, can_passive_reply
                )

        elif user_info:
            user_openid = metadata.get("qq_user_openid", "") or user_info.get("user_id", "")
            if not user_openid:
                logger.error("单聊消息缺少 user_openid")
                return

            await self._send_text_message(
                headers, "user", user_openid, text_content, msg_id, can_passive_reply
            )

        else:
            logger.error("消息缺少目标信息（group_info 或 user_info），无法发送")

    async def _send_text_message(
        self,
        headers: dict[str, str],
        target_type: str,
        target_id: str,
        content: str,
        msg_id: str,
        can_passive_reply: bool,
    ) -> None:
        """发送文本消息到 QQ。

        Args:
            headers: HTTP 请求头
            target_type: 目标类型 "group" 或 "user"
            target_id: group_openid 或 user_openid
            content: 文本内容
            msg_id: 原始消息 ID（用于被动回复）
            can_passive_reply: 是否可以使用被动回复
        """
        if not content:
            return

        if target_type == "group":
            url = f"{self._base_url}/v2/groups/{target_id}/messages"
        else:
            url = f"{self._base_url}/v2/users/{target_id}/messages"

        body: dict[str, Any] = {
            "content": content,
            "msg_type": 0,  # 文本消息
        }

        # 被动回复：带上 msg_id
        if can_passive_reply and msg_id:
            body["msg_id"] = msg_id
            target_label = "群聊" if target_type == "group" else "单聊"
            logger.info(f"使用被动回复发送{target_label}消息到 {target_id}")
        else:
            target_label = "群聊" if target_type == "group" else "单聊"
            logger.info(f"使用主动消息发送{target_label}消息到 {target_id}")

        await self._post_api(url, headers, body)

    async def _send_media_message(
        self,
        headers: dict[str, str],
        target_type: str,
        target_id: str,
        image_segment: SegPayload,
        msg_id: str,
        can_passive_reply: bool,
    ) -> None:
        """发送富媒体消息（图片等）到 QQ。

        先上传媒体资源获取 file_uuid，再发送 media 类型消息。
        """
        image_data = image_segment.get("data", "")

        if not isinstance(image_data, str):
            logger.error("图片数据格式不正确")
            return

        # 步骤 1：上传媒体
        if target_type == "group":
            upload_url = f"{self._base_url}/v2/groups/{target_id}/files"
        else:
            upload_url = f"{self._base_url}/v2/users/{target_id}/files"

        # 判断是 URL 还是 base64
        is_base64 = not image_data.startswith("http")

        upload_body: dict[str, Any] = {
            "file_type": 1,  # 图片
            "srv_send_msg": False,  # 仅上传不发送
        }

        if is_base64:
            upload_body["file_data"] = image_data
        else:
            upload_body["url"] = image_data

        try:
            upload_resp = await self._post_api(upload_url, headers, upload_body)
        except Exception:
            logger.exception("上传媒体失败")
            return

        file_uuid = upload_resp.get("file_uuid", "")
        file_info = upload_resp.get("file_info", "")

        if not file_uuid:
            logger.error(f"上传媒体失败，响应中没有 file_uuid: {upload_resp}")
            return

        # 步骤 2：发送 media 消息
        if target_type == "group":
            send_url = f"{self._base_url}/v2/groups/{target_id}/messages"
        else:
            send_url = f"{self._base_url}/v2/users/{target_id}/messages"

        send_body: dict[str, Any] = {
            "msg_type": 7,  # 富媒体消息
            "media": {"file_info": file_info},
        }

        if can_passive_reply and msg_id:
            send_body["msg_id"] = msg_id

        await self._post_api(send_url, headers, send_body)

    async def _post_api(
        self,
        url: str,
        headers: dict[str, str],
        body: dict[str, Any],
    ) -> dict[str, Any]:
        """发送 POST 请求到 QQ REST API。

        包含错误处理和 token 过期重试逻辑。
        """
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(url, headers=headers, json=body)

                if response.status_code == 401:
                    # Token 过期，刷新后重试
                    logger.warning("Token 可能已过期，刷新后重试")
                    await self._token_provider.fetch_token()
                    headers["Authorization"] = f"QQBot {self._token_provider.access_token}"
                    response = await client.post(url, headers=headers, json=body)

                response.raise_for_status()

                if response.status_code == 204 or not response.content:
                    return {}

                return response.json()

        except httpx.HTTPStatusError as e:
            error_body = ""
            try:
                error_body = e.response.text
            except Exception:
                pass

            if e.response.status_code == 429:
                logger.warning(f"QQ API 限频 (429)，响应: {error_body}")
            else:
                logger.error(
                    f"QQ API 错误 ({e.response.status_code}): {error_body[:500]}, URL: {url}"
                )
            raise

    def _can_passive_reply(self, msg_id: str, qq_timestamp: str) -> bool:
        """判断是否可以使用被动回复机制。

        被动回复条件：有原始 msg_id，且距消息时间不超过 5 分钟。

        Args:
            msg_id: 原始消息 ID
            qq_timestamp: QQ 消息时间戳（RFC3339 格式）

        Returns:
            bool: 是否可以使用被动回复
        """
        if not msg_id:
            return False

        # 如果有时间戳，检查是否在 5 分钟窗口内
        if qq_timestamp:
            try:
                # QQ 时间戳格式: "2023-11-06T13:37:18+08:00"
                dt_str = qq_timestamp.replace("Z", "+00:00")
                msg_time = datetime.fromisoformat(dt_str)
                now = datetime.now(timezone.utc)

                if msg_time.tzinfo is None:
                    msg_time = msg_time.replace(tzinfo=timezone.utc)

                elapsed = (now - msg_time).total_seconds()
                return elapsed < PASSIVE_REPLY_WINDOW

            except (ValueError, TypeError):
                logger.debug(f"无法解析时间戳 {qq_timestamp}，默认不使用被动回复")
                return False

        return True

    def _extract_text(self, segments: list[dict[str, Any]]) -> str:
        """从消息段列表中提取文本内容。

        Args:
            segments: 消息段列表

        Returns:
            str: 合并后的文本内容
        """
        texts: list[str] = []
        for seg in segments:
            seg_type = seg.get("type", "")
            data = seg.get("data", "")

            if seg_type == "text" and isinstance(data, str):
                texts.append(data)
            elif seg_type == "seglist" and isinstance(data, list):
                # 递归提取嵌套 seglist 中的文本
                texts.append(self._extract_text(data))

        return "".join(texts)