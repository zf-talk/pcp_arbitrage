import hmac
import base64
import hashlib
import json
import asyncio
import logging
from datetime import datetime, timezone

import aiohttp

logger = logging.getLogger(__name__)

BASE_REST = "https://www.okx.com"
WS_PUBLIC = "wss://ws.okx.com:8443/ws/v5/public"
WS_PRIVATE = "wss://ws.okx.com:8443/ws/v5/private"


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _sign(secret: str, ts: str, method: str, path: str, body: str = "") -> str:
    msg = ts + method.upper() + path + body
    mac = hmac.new(secret.encode(), msg.encode(), hashlib.sha256)
    return base64.b64encode(mac.digest()).decode()


class OKXRestClient:
    def __init__(self, api_key: str, secret: str, passphrase: str):
        self._api_key = api_key
        self._secret = secret
        self._passphrase = passphrase
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self):
        self._session = aiohttp.ClientSession(base_url=BASE_REST)
        return self

    async def __aexit__(self, *_):
        if self._session:
            await self._session.close()

    def _auth_headers(self, method: str, path: str, body: str = "") -> dict:
        ts = _timestamp()
        sig = _sign(self._secret, ts, method, path, body)
        headers = {
            "OK-ACCESS-KEY": self._api_key,
            "OK-ACCESS-SIGN": sig,
            "OK-ACCESS-TIMESTAMP": ts,
            "OK-ACCESS-PASSPHRASE": self._passphrase,
            "Content-Type": "application/json",
        }
        return headers

    async def _get(self, path: str, params: dict | None = None, auth: bool = False) -> dict:
        assert self._session is not None
        if auth and params:
            from urllib.parse import urlencode
            sign_path = path + "?" + urlencode(params)
            headers = self._auth_headers("GET", sign_path)
        elif auth:
            headers = self._auth_headers("GET", path)
        else:
            headers = {}
        async with self._session.get(path, params=params, headers=headers) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_balance(self) -> dict:
        """Fetch account balance. Returns totalEq, adjEq, availEq, details, imr, mmr."""
        data = await self._get("/api/v5/account/balance", auth=True)
        info = data.get("data", [{}])[0]
        details = info.get("details", [])
        def _f(val, default=0):
            try:
                return float(val) if val != "" else default
            except (TypeError, ValueError):
                return default

        return {
            "totalEq": _f(info.get("totalEq")),
            "adjEq": _f(info.get("adjEq")),
            "availEq": _f(info.get("availEq")),
            "ordFroz": _f(info.get("ordFroz")),
            "imr": _f(info.get("imr")),
            "mmr": _f(info.get("mmr")),
            "details": [
                d
                for d in details
                if _f(d.get("eq")) > 0
                or _f(d.get("eqUsd")) > 0
                or _f(d.get("availEq")) > 0
                or _f(d.get("availBal")) > 0
            ],
        }

    async def get_instruments(self, inst_type: str, uly: str | None = None) -> list[dict]:
        """Fetch instrument list. inst_type: 'OPTION' or 'FUTURES'."""
        params: dict = {"instType": inst_type}
        if uly:
            params["uly"] = uly
        data = await self._get("/api/v5/public/instruments", params=params)
        return data.get("data", [])

    async def get_ticker(self, inst_id: str) -> dict:
        data = await self._get("/api/v5/market/ticker", params={"instId": inst_id})
        return data.get("data", [{}])[0]

    async def get_account_config(self) -> dict:
        """Returns account-level config including fee tier."""
        data = await self._get("/api/v5/account/config", auth=True)
        return data.get("data", [{}])[0]

    async def get_fee_rates(self, inst_type: str, inst_id: str = "", uly: str = "") -> dict:
        """Fetch trading fee rates for given instType."""
        params: dict = {"instType": inst_type}
        if inst_id:
            params["instId"] = inst_id
        if uly:
            params["uly"] = uly
        data = await self._get("/api/v5/account/trade-fee", params=params, auth=True)
        return data.get("data", [{}])[0]


class OKXWebSocketClient:
    """Manages a single OKX WebSocket connection with heartbeat and reconnect."""

    MAX_CHANNELS_PER_CONN = 240
    PING_INTERVAL = 20
    RECONNECT_BASE = 1
    RECONNECT_MAX = 60

    def __init__(self, url: str, on_message, on_reconnect=None):
        self._url = url
        self._on_message = on_message
        self._on_reconnect = on_reconnect
        self._subscriptions: list[dict] = []
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._session: aiohttp.ClientSession | None = None

    def add_subscriptions(self, channels: list[dict]) -> None:
        self._subscriptions.extend(channels)

    async def run(self) -> None:
        retry_delay = self.RECONNECT_BASE
        while True:
            try:
                await self._connect_and_run()
                retry_delay = self.RECONNECT_BASE  # reset on clean exit
            except Exception as exc:
                logger.warning("[ws] Disconnected: %s — reconnecting in %ds", exc, retry_delay)
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, self.RECONNECT_MAX)
                if self._on_reconnect:
                    await self._on_reconnect()

    async def _connect_and_run(self) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self._url) as ws:
                self._ws = ws
                logger.info("[ws] Connected to %s", self._url)
                await self._subscribe(ws)
                ping_task = asyncio.create_task(self._heartbeat(ws))
                try:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            if msg.data == "pong":
                                continue
                            await self._on_message(json.loads(msg.data))
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                finally:
                    ping_task.cancel()

    async def _subscribe(self, ws) -> None:
        # chunk into groups of MAX_CHANNELS_PER_CONN if needed
        for i in range(0, len(self._subscriptions), self.MAX_CHANNELS_PER_CONN):
            chunk = self._subscriptions[i : i + self.MAX_CHANNELS_PER_CONN]
            await ws.send_str(json.dumps({"op": "subscribe", "args": chunk}))

    async def _heartbeat(self, ws) -> None:
        while True:
            await asyncio.sleep(self.PING_INTERVAL)
            await ws.send_str("ping")
