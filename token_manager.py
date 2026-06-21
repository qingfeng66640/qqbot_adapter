"""access_token 获取与定时刷新管理器"""
from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx

from src.app.plugin_system.api.log_api import get_logger

logger = get_logger("qqbot_adapter.token")

# API 端点
TOKEN_URL = "https://bots.qq.com/app/getAppAccessToken"

# Token 刷新策略：在过期前 60 秒刷新（利用新旧 token 共存窗口）
REFRESH_BEFORE_EXPIRY = 60


class TokenManager:
    """QQ Bot access_token 管理器。

    负责获取 token 并在后台定时刷新。
    利用 QQ API 的特性：在 token 过期前 60 秒内申请新 token，
    新旧两个 token 可共存使用。
    """

    def __init__(self, app_id: str, app_secret: str) -> None:
        self._app_id = app_id
        self._app_secret = app_secret
        self._access_token: str = ""
        self._expires_at: float = 0  # Unix timestamp
        self._refresh_task: asyncio.Task[Any] | None = None
        self._running = False

    @property
    def access_token(self) -> str:
        """获取当前有效的 access_token"""
        return self._access_token

    def is_token_valid(self) -> bool:
        """检查 token 是否在有效期内"""
        return bool(self._access_token) and time.time() < self._expires_at

    async def fetch_token(self) -> str:
        """从 QQ API 获取新的 access_token。

        Returns:
            str: access_token 字符串

        Raises:
            RuntimeError: API 请求失败时抛出
        """
        logger.info("正在获取 QQ Bot access_token...")

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    TOKEN_URL,
                    json={
                        "appId": self._app_id,
                        "clientSecret": self._app_secret,
                    },
                )
                response.raise_for_status()
                data = response.json()

            token = data.get("access_token", "")
            expires_in_raw = data.get("expires_in", 7200)
            expires_in = int(expires_in_raw) if isinstance(expires_in_raw, str) else expires_in_raw

            if not token:
                raise RuntimeError(f"获取 access_token 失败，响应中没有 access_token: {data}")

            self._access_token = token
            self._expires_at = time.time() + expires_in

            logger.info(
                f"QQ Bot access_token 获取成功，有效期 {expires_in} 秒，"
                f"将在 {time.strftime('%H:%M:%S', time.localtime(self._expires_at))} 过期"
            )
            return token

        except httpx.HTTPStatusError as e:
            raise RuntimeError(f"获取 access_token HTTP 错误 ({e.response.status_code}): {e.response.text}") from e
        except Exception as e:
            raise RuntimeError(f"获取 access_token 失败: {e}") from e

    async def _refresh_loop(self) -> None:
        """后台 token 刷新循环。

        计算距离过期的时间，在过期前 60 秒刷新。
        如果刷新失败，等待 30 秒后重试。
        """
        while self._running:
            try:
                if not self._access_token:
                    # 首次获取
                    await self.fetch_token()
                    continue

                # 计算距离过期的时间
                remaining = self._expires_at - time.time()
                if remaining <= REFRESH_BEFORE_EXPIRY:
                    # 进入刷新窗口，刷新 token
                    logger.info(f"Token 即将过期（剩余 {remaining:.0f} 秒），正在刷新...")
                    await self.fetch_token()
                else:
                    # 等待到刷新窗口
                    wait_time = max(remaining - REFRESH_BEFORE_EXPIRY, 1)
                    logger.debug(f"距离 token 刷新还有 {wait_time:.0f} 秒")
                    await asyncio.sleep(wait_time)
                    continue

            except asyncio.CancelledError:
                break
            except Exception:
                logger.error("Token 刷新失败，30 秒后重试", exc_info=True)
                await asyncio.sleep(30)

    async def start(self) -> None:
        """启动 token 管理器（获取首个 token 并启动后台刷新）"""
        self._running = True

        # 先获取首个 token
        await self.fetch_token()

        # 启动后台刷新任务
        self._refresh_task = asyncio.create_task(self._refresh_loop())
        logger.info("Token 管理器已启动")

    async def stop(self) -> None:
        """停止 token 管理器"""
        self._running = False
        if self._refresh_task and not self._refresh_task.done():
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
            self._refresh_task = None
        logger.info("Token 管理器已停止")

    async def ensure_token(self) -> str:
        """确保有一个有效的 token，必要时刷新。

        Returns:
            str: 有效的 access_token
        """
        if not self.is_token_valid():
            await self.fetch_token()
        return self._access_token