"""Telegram notification sender."""
from __future__ import annotations
import logging
import aiohttp
from pcp_arbitrage.config import TelegramConfig

logger = logging.getLogger(__name__)


async def send_telegram(cfg: TelegramConfig, text: str) -> None:
    """Send a Telegram message. Logs and returns on any error (never raises)."""
    if not cfg.bot_token or not cfg.chat_id:
        return
    url = f"https://api.telegram.org/bot{cfg.bot_token}/sendMessage"
    payload = {"chat_id": cfg.chat_id, "text": text, "parse_mode": "HTML"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning("Telegram send failed status=%d body=%s", resp.status, body[:200])
    except Exception as exc:
        logger.warning("Telegram send error: %s", exc)
