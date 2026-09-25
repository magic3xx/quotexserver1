import asyncio
import logging
import json
import os
import time
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("quotex_bridge")

EMAIL = os.getenv("QUOTEX_EMAIL", "").strip()
PASSWORD = os.getenv("QUOTEX_PASSWORD", "").strip()
SSID = os.getenv("QUOTEX_SSID", "").strip()
COOKIES = os.getenv("QUOTEX_COOKIES", "").strip()
SESSION_JSON = os.getenv("QUOTEX_SESSION_JSON", "").strip()
USER_AGENT = os.getenv(
    "QUOTEX_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
).strip()
HOST = os.getenv("QUOTEX_HOST", "qxbroker.com").strip()
ORIGINS = [
    x.strip()
    for x in os.getenv("ALLOWED_ORIGINS", "http://localhost:5173").split(",")
    if x.strip()
]

app = FastAPI(title="Quotex Live Data Bridge")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def normalize_asset(symbol: str, mode: str) -> str:
    asset = symbol.replace("/", "").replace(" ", "").upper()
    if mode == "OTC" and not asset.lower().endswith("_otc"):
        asset += "_otc"
    return asset


def normalize_candles(raw: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    source = raw.values() if isinstance(raw, dict) else raw if isinstance(raw, list) else []

    for c in source:
        try:
            if isinstance(c, dict):
                t = c.get("time", c.get("from", c.get("timestamp")))
                o = c.get("open")
                close = c.get("close")
                h = c.get("high", c.get("max"))
                l = c.get("low", c.get("min"))
                volume = c.get("volume", 0) or 0
            elif isinstance(c, (list, tuple)) and len(c) >= 5:
                # Some Quotex-compatible candle payloads use:
                # [time, open, close, high, low]
                t, o, close, h, l = c[:5]
                volume = c[5] if len(c) > 5 else 0
            else:
                continue

            if None in (t, o, h, l, close):
                continue

            ts = float(t)
            rows.append(
                {
                    "time": int(ts) * 1000 if ts < 10_000_000_000 else int(ts),
                    "open": float(o),
                    "high": float(h),
                    "low": float(l),
                    "close": float(close),
                    "volume": float(volume or 0),
                }
            )
        except (TypeError, ValueError, IndexError):
            continue

    return sorted(
        {c["time"]: c for c in rows}.values(),
        key=lambda x: x["time"],
    )[-300:]


async def make_client(asset: str):
    session_token = SSID
    session_cookies = COOKIES
    session_user_agent = USER_AGENT

    if SESSION_JSON:
        try:
            saved = json.loads(SESSION_JSON)
            if isinstance(saved, dict):
                # A dedicated QUOTEX_SSID / QUOTEX_COOKIES environment
                # variable is the explicit override. This prevents an older
                # QUOTEX_SESSION_JSON value from silently replacing a fresh
                # browser session copied into Render.
                json_token = str(saved.get("token") or saved.get("ssid") or "").strip()
                json_cookies = str(saved.get("cookies") or "").strip()
                json_user_agent = str(saved.get("user_agent") or "").strip()

                if not session_token and json_token:
                    session_token = json_token
                if not session_cookies and json_cookies:
                    session_cookies = json_cookies
                if json_user_agent:
                    session_user_agent = json_user_agent
        except json.JSONDecodeError as exc:
            raise RuntimeError("QUOTEX_SESSION_JSON is not valid JSON") from exc

    # Never let placeholder/example values become a real session.
    if session_token.upper() in {"YOUR_SSID", "YOUR_TOKEN", "TOKEN", "SSID"}:
        session_token = ""
    if session_cookies.upper() in {"YOUR_COOKIES", "COOKIES"}:
        session_cookies = ""

    # A fresh SSID lets the bridge skip the HTTP sign-in page. This is useful
    # when Quotex/Cloudflare returns HTTP 403 to datacenter-hosted login
    # requests. Keep the SSID private and store it only as a Render secret.
    if not session_token and (not EMAIL or not PASSWORD):
        raise RuntimeError(
            "Configure either QUOTEX_SSID or both QUOTEX_EMAIL and QUOTEX_PASSWORD "
            "on the Render server"
        )

    # pyquotex 1.1.0 expects an aiohttp-style reason_phrase attribute,
    # while its current curl_cffi Response only exposes reason. Add a
    # compatibility property before importing Quotex so HTTP errors (notably
    # Cloudflare/Quotex 403 responses on hosted servers) are reported cleanly
    # instead of crashing with AttributeError.
    try:
        from curl_cffi.requests import Response as CurlResponse

        if not hasattr(CurlResponse, "reason_phrase"):
            CurlResponse.reason_phrase = property(
                lambda response: getattr(response, "reason", "")
            )
    except Exception as exc:
        logger.warning("Could not install curl_cffi response compatibility patch: %s", exc)

    from pyquotex.stable_api import Quotex

    logger.info(
        "Connecting to Quotex: host=%s asset=%s auth=%s",
        HOST,
        asset,
        "SESSION" if session_token else "EMAIL_PASSWORD",
    )
    from pyquotex.qxtypes import ReconnectPolicy

    client = Quotex(
        email=EMAIL,
        password=PASSWORD,
        host=HOST,
        lang="en",
        user_agent=session_user_agent,
        asset_default=asset,
        period_default=60,
        # Do not let a failed authorization create an endless reconnect storm.
        reconnect_policy=ReconnectPolicy(enabled=False),
    )

    if session_token:
        client.set_session(
            user_agent=session_user_agent,
            cookies=session_cookies or None,
            ssid=session_token,
        )
        logger.info(
            "Using configured Quotex SSID session (cookies=%s)",
            bool(session_cookies),
        )

    try:
        ok, reason = await client.connect()
    except Exception as exc:
        # During HTTP login the active response lives on the temporary Login
        # object, not on client.api.browser. Inspect both locations so a
        # Cloudflare/Quotex 403 is reported as the actionable SSID problem.
        candidates = [
            getattr(getattr(client, "api", None), "browser", None),
            getattr(getattr(client, "api", None), "login", None),
        ]
        status_code = None
        for candidate in candidates:
            response = getattr(candidate, "response", None)
            status_code = getattr(response, "status_code", None)
            if status_code:
                break

        if status_code == 403 or "HTTP 403" in str(exc):
            raise RuntimeError(
                "Quotex returned HTTP 403 to the Render server. "
                "Server-side email/password login is blocked. "
                "Configure a fresh QUOTEX_SSID (and, when needed, "
                "QUOTEX_COOKIES) from an authenticated Quotex browser session."
            ) from exc

        raise RuntimeError(f"Quotex connection error: {exc}") from exc

    logger.info("Quotex transport connect result: ok=%s reason=%s", ok, reason)

    if not ok:
        raise RuntimeError(f"Quotex connection failed: {reason}")

    # pyquotex reports the TCP/WebSocket transport as connected before the
    # Quotex authorization handshake has necessarily completed. Wait for the
    # auth state explicitly so the bridge never proceeds to get_candles() or
    # asset discovery on an unauthenticated socket.
    authenticated = await client.check_connect()
    auth_state = getattr(getattr(client, "api", None), "state", None)
    auth_reason = getattr(auth_state, "websocket_error_reason", None)

    logger.info(
        "Quotex authentication result: authenticated=%s auth_reason=%s",
        authenticated,
        auth_reason or "",
    )

    if not authenticated:
        raise RuntimeError(
            "Quotex WebSocket authorization was rejected. "
            "Refresh the QUOTEX_SSID session from an active logged-in Quotex browser "
            "session and update the Render secret. "
            + (f"Server reason: {auth_reason}" if auth_reason else "")
        )

    return client


@app.get("/")
async def root():
    return {"ok": True, "service": "quotex-live-data-bridge", "websocket": "/ws"}


@app.get("/health")
async def health():
    return {
        "ok": True,
        "service": "quotex-live-data-bridge",
        "quotex_credentials_configured": bool(EMAIL and PASSWORD),
        "quotex_ssid_configured": bool(SSID or SESSION_JSON),
        "quotex_cookies_configured": bool(COOKIES),
        "quotex_session_json_configured": bool(SESSION_JSON),
        "host": HOST,
    }


@app.websocket("/ws")
async def websocket_feed(ws: WebSocket):
    await ws.accept()
    client = None
    asset = None
    timeframe = 60

    try:
        logger.info("Browser WebSocket connected")
        request = await ws.receive_json()
        logger.info("Browser subscription request: %s", request)

        if request.get("type") != "subscribe":
            await ws.send_json(
                {"type": "error", "message": "First message must be subscribe"}
            )
            await ws.close()
            return

        mode = str(request.get("mode", "REGULAR")).upper()
        asset = normalize_asset(str(request.get("symbol", "EUR/USD")), mode)
        timeframe = int(request.get("timeframe", 60))

        if timeframe not in (5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600):
            timeframe = 60

        client = await make_client(asset)

        # Resolve the exact asset name used by the current Quotex session.
        resolved_asset, asset_status = await client.get_available_asset(asset, force_open=False)
        logger.info("Asset resolution: requested=%s resolved=%s status=%s", asset, resolved_asset, asset_status)

        if not asset_status or len(asset_status) < 3 or not asset_status[2]:
            resolved_asset, asset_status = await client.get_available_asset(asset, force_open=True)
            logger.info("Asset OTC fallback: resolved=%s status=%s", resolved_asset, asset_status)

        if not asset_status or len(asset_status) < 3 or not asset_status[2]:
            raise RuntimeError(f"Quotex asset is unavailable: {asset}")

        asset = resolved_asset

        # Quotex returns at most 199 candles per history request.
        history = await client.get_candles(
            asset,
            time.time(),
            min(timeframe * 199, 11940),
            timeframe,
        )
        snapshot = normalize_candles(history)
        logger.info("Historical candles received: %d for %s", len(snapshot), asset)

        if not snapshot:
            raise RuntimeError(f"Quotex returned no candles for {asset}")

        await ws.send_json(
            {
                "type": "snapshot",
                "symbol": asset,
                "candles": snapshot,
            }
        )

        # IMPORTANT: pyquotex requires the realtime candle stream to be
        # explicitly subscribed before get_realtime_candles() is read.
        # pyquotex 1.1.0 starts the subscription asynchronously and returns
        # None on success. Do NOT treat a falsy return value as a failure.
        await client.start_candles_stream(asset, timeframe)
        logger.info(
            "Realtime candle subscription requested: asset=%s timeframe=%ss",
            asset,
            timeframe,
        )

        last_signature = None

        while True:
            raw = await client.get_realtime_candles(asset)
            candles = normalize_candles(raw)

            if candles:
                signature = (
                    candles[-1]["time"],
                    candles[-1]["open"],
                    candles[-1]["high"],
                    candles[-1]["low"],
                    candles[-1]["close"],
                )

                if signature != last_signature:
                    last_signature = signature
                    await ws.send_json(
                        {
                            "type": "candles",
                            "symbol": asset,
                            "candles": candles,
                        }
                    )

            await asyncio.sleep(0.25)

    except WebSocketDisconnect:
        logger.info("Browser WebSocket disconnected asset=%s", asset)
    except Exception as exc:
        logger.exception("Quotex bridge error")
        try:
            await ws.send_json({"type": "error", "message": str(exc)})
        except Exception:
            pass
    finally:
        if client is not None and asset is not None:
            try:
                await client.stop_candles_stream(asset)
            except Exception:
                pass
            try:
                await client.close()
            except Exception:
                logger.exception("Error closing Quotex client")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "quotex_bridge:app",
        host=os.getenv("BRIDGE_HOST", "0.0.0.0"),
        port=int(os.getenv("BRIDGE_PORT", "8000")),
        reload=False,
    )
