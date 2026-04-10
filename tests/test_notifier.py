"""Tests for src/pcp_arbitrage/notifier.py."""
from __future__ import annotations
import logging
from unittest.mock import AsyncMock, MagicMock, patch

from pcp_arbitrage.config import TelegramConfig
from pcp_arbitrage.notifier import send_telegram


def _make_resp(status: int = 200, text: str = "") -> MagicMock:
    """Build a fake aiohttp response async context manager."""
    resp = AsyncMock()
    resp.status = status
    resp.text = AsyncMock(return_value=text)
    # async context manager support
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__ = AsyncMock(return_value=False)
    return resp


def _make_session(resp: MagicMock) -> MagicMock:
    """Build a fake aiohttp.ClientSession async context manager."""
    session = MagicMock()
    session.post = MagicMock(return_value=resp)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    return session


async def test_sends_correct_url_and_payload():
    """When bot_token and chat_id are set, POST is sent with correct URL and payload."""
    cfg = TelegramConfig(bot_token="123:ABC", chat_id="-100123")
    resp = _make_resp(status=200)
    session = _make_session(resp)

    with patch("aiohttp.ClientSession", return_value=session):
        await send_telegram(cfg, "hello <b>world</b>")

    session.post.assert_called_once()
    call_args = session.post.call_args
    url = call_args.args[0] if call_args.args else call_args.kwargs.get("url")
    assert url == "https://api.telegram.org/bot123:ABC/sendMessage"
    payload = call_args.kwargs.get("json")
    assert payload["chat_id"] == "-100123"
    assert payload["text"] == "hello <b>world</b>"
    assert payload["parse_mode"] == "HTML"


async def test_does_nothing_when_bot_token_empty():
    """No HTTP call is made when bot_token is empty."""
    cfg = TelegramConfig(bot_token="", chat_id="-100123")

    with patch("aiohttp.ClientSession") as mock_session_cls:
        await send_telegram(cfg, "test")

    mock_session_cls.assert_not_called()


async def test_does_nothing_when_chat_id_empty():
    """No HTTP call is made when chat_id is empty."""
    cfg = TelegramConfig(bot_token="123:ABC", chat_id="")

    with patch("aiohttp.ClientSession") as mock_session_cls:
        await send_telegram(cfg, "test")

    mock_session_cls.assert_not_called()


async def test_logs_warning_on_non_200_does_not_raise(caplog):
    """Warning is logged and no exception raised when Telegram returns non-200."""
    cfg = TelegramConfig(bot_token="123:ABC", chat_id="-100123")
    resp = _make_resp(status=400, text='{"ok":false,"description":"Bad Request"}')
    session = _make_session(resp)

    with patch("aiohttp.ClientSession", return_value=session):
        with caplog.at_level(logging.WARNING, logger="pcp_arbitrage.notifier"):
            await send_telegram(cfg, "test")

    assert any("400" in r.message for r in caplog.records)


async def test_logs_warning_on_exception_does_not_raise(caplog):
    """Warning is logged and no exception raised when the HTTP call throws."""
    cfg = TelegramConfig(bot_token="123:ABC", chat_id="-100123")

    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    session.post = MagicMock(side_effect=OSError("network error"))

    with patch("aiohttp.ClientSession", return_value=session):
        with caplog.at_level(logging.WARNING, logger="pcp_arbitrage.notifier"):
            await send_telegram(cfg, "test")

    assert any("network error" in r.message for r in caplog.records)
