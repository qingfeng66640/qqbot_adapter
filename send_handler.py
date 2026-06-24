"""QQ Bot 出站消息处理器：将 MessageEnvelope 转换为 QQ REST API 调用"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx

from mofox_wire import MessageEnvelope

from src.app.plugin_system.api.log_api import get_logger

logger = get_logger("qqbot_adapter.send_handler")

# REST API 基础 URL
API_BASE_SANDBOX = "https://sandbox.api.sgroup.qq.com"
API_BASE_PRODUCTION = "https://api.sgroup.qq.com"

# 被动回复：5 分钟内有效
PASSIVE_REPLY_WINDOW = 300  # 5 分钟（秒）

# 富媒体上传 file_type 枚举
FILE_TYPE_IMAGE = 1  # 图片：png, jpg
FILE_TYPE_VIDEO = 2  # 视频：mp4
FILE_TYPE_VOICE = 3  # 语音：silk, wav, mp3, flac
FILE_TYPE_FILE = 4  # 文件（群聊暂不开放）

# segment type → file_type 映射
_SEGMENT_FILE_TYPE_MAP: dict[str, int] = {
    "image": FILE_TYPE_IMAGE,
    "video": FILE_TYPE_VIDEO,
    "voice": FILE_TYPE_VOICE,
    "file": FILE_TYPE_FILE,
}


class SendHandler:
    """QQ Bot 出站消息处理器。

    将 Neo-MoFox 的 MessageEnvelope 转换为 QQ 官方 REST API 调用。
    优先使用被动回复（在收到消息 5 分钟内），超时降级为主动消息。
    支持文本、图片、视频、语音、文件消息的发送。
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
            logger.error("发送 QQ 消息失败", exc_info=True)

    async def _send_message(self, envelope: MessageEnvelope) -> None:
        """核心发送逻辑。

        处理策略：
        1. 先发送文本内容（如有）
        2. 逐个发送富媒体附件（图片/视频/语音/文件）
        """
        message_segment = envelope.get("message_segment", [])
        message_info = envelope.get("message_info", {})
        metadata: dict[str, Any] = envelope.get("metadata", {}) or {}

        segments = message_segment if isinstance(message_segment, list) else [message_segment]

        # 判断目标（群聊 vs 单聊）
        group_info = message_info.get("group_info")
        user_info = message_info.get("user_info")

        # 提取文本内容
        text_content = self._extract_text(segments)

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
            target_type = "group"
            target_id = metadata.get("qq_group_openid", "") or group_info.get("group_id", "")
            if not target_id:
                logger.error("群聊消息缺少 group_openid")
                return
        elif user_info:
            target_type = "user"
            target_id = metadata.get("qq_user_openid", "") or user_info.get("user_id", "")
            if not target_id:
                logger.error("单聊消息缺少 user_openid")
                return
        else:
            logger.error("消息缺少目标信息（group_info 或 user_info），无法发送")
            return

        # 1. 发送文本
        if text_content:
            await self._send_text_message(
                headers, target_type, target_id, text_content, msg_id, can_passive_reply
            )

        # 2. 发送富媒体附件
        for seg in segments:
            seg_type = seg.get("type", "")
            if seg_type not in _SEGMENT_FILE_TYPE_MAP:
                continue

            file_type = _SEGMENT_FILE_TYPE_MAP[seg_type]
            media_data = seg.get("data", "")

            # 群聊不支持发送文件 (file_type=4)
            if file_type == FILE_TYPE_FILE and target_type == "group":
                logger.warning("群聊暂不支持发送文件（QQ 官方限制）")
                continue

            await self._send_media_message(
                headers, target_type, target_id, media_data, file_type,
                msg_id, can_passive_reply,
            )

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
        media_data: Any,
        file_type: int,
        msg_id: str,
        can_passive_reply: bool,
    ) -> None:
        """发送富媒体消息（图片/视频/语音/文件）到 QQ。

        上传 → 获取 file_info → 发送 msg_type=7 媒体消息。
        推荐模式: srv_send_msg=false，先上传再通过 media 字段发送。

        Args:
            headers: HTTP 请求头
            target_type: 目标类型 "group" 或 "user"
            target_id: group_openid 或 user_openid
            media_data: 媒体数据。str = URL 或 base64；dict = {url, filename, size} 结构
            file_type: 1=图片, 2=视频, 3=语音, 4=文件
            msg_id: 原始消息 ID（用于被动回复）
            can_passive_reply: 是否可以使用被动回复
        """
        # 解析媒体数据：dict 结构 (来自 file/video segment) 或 纯字符串 (base64/URL)
        if isinstance(media_data, dict):
            media_url = media_data.get("url", "")
            # 如果没有 url，尝试 data 字段（base64）
            if not media_url:
                media_url = media_data.get("data", "")
        elif isinstance(media_data, str):
            media_url = media_data
        else:
            logger.error(f"媒体数据格式不支持: {type(media_data)}")
            return

        if not media_url:
            logger.error("媒体数据为空，无法发送")
            return

        # 步骤 1：上传媒体
        if target_type == "group":
            upload_url = f"{self._base_url}/v2/groups/{target_id}/files"
        else:
            upload_url = f"{self._base_url}/v2/users/{target_id}/files"

        # 判断是 URL 还是 base64
        is_base64 = not str(media_url).startswith("http")

        upload_body: dict[str, Any] = {
            "file_type": file_type,
            "srv_send_msg": False,  # 推荐模式：仅上传不发送
        }

        if is_base64:
            upload_body["file_data"] = media_url
        else:
            upload_body["url"] = media_url

        try:
            upload_resp = await self._post_api(upload_url, headers, upload_body)
        except Exception:
            logger.error(f"上传媒体失败 (file_type={file_type})", exc_info=True)
            return

        file_uuid = upload_resp.get("file_uuid", "")
        file_info = upload_resp.get("file_info", "")

        if not file_uuid:
            logger.error(f"上传媒体失败，响应中没有 file_uuid: {upload_resp}")
            return

        # 步骤 2：发送 media 消息 (msg_type=7)
        if target_type == "group":
            send_url = f"{self._base_url}/v2/groups/{target_id}/messages"
        else:
            send_url = f"{self._base_url}/v2/users/{target_id}/messages"

        send_body: dict[str, Any] = {
            "msg_type": 7,  # 富媒体消息
            "media": {"file_info": file_info},
        }

        # 群聊 msg_type=7 content 仍为必填，填空字符串
        if target_type == "group":
            send_body["content"] = ""

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
            async with httpx.AsyncClient(timeout=30.0, trust_env=False) as client:
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
                logger.debug(f"无法解析时间戳 {qq_timestamp}，跳过时间窗口检查")
                return True  # 仍尝试被动回复，让服务端判断是否过期

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