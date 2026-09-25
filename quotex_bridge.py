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
USER_AGENT = os.getenv("QUOTEX_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36").strip()
HOST = os.getenv("QUOTEX_HOST", "qxbroker.com").strip()

app = FastAPI(title="Quotex Live Data Bridge V2")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def normalize_candles(raw: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    source = raw.values() if isinstance(raw, dict) else raw if isinstance(raw, list) else []

    for c in source:
        try:
            if isinstance(c, dict):
                t = c.get("time", c.get("from", c.get("timestamp")))
                o, close, h, l = c.get("open"), c.get("close"), c.get("high", c.get("max")), c.get("low", c.get("min"))
            elif isinstance(c, (list, tuple)) and len(c) >= 5:
                t, o, close, h, l = c[:5]
            else:
                continue

            if None in (t, o, h, l, close): continue
            ts = float(t)
            rows.append({
                "time": int(ts) * 1000 if ts < 10_000_000_000 else int(ts),
                "open": float(o), "high": float(h), "low": float(l), "close": float(close)
            })
        except Exception:
            continue

    return sorted({c["time"]: c for c in rows}.values(), key=lambda x: x["time"])[-300:]


class BridgeManager:
    """Manages a single Quotex connection and broadcasts data to multiple WebSockets."""
    def __init__(self):
        self.client = None
        self.client_lock = asyncio.Lock()
        self.subscribers = {}      # Format: { "EURUSD": set(websocket1, websocket2) }
        self.active_streams = {}   # Format: { "EURUSD": asyncio.Task }

    async def get_client(self):
        async with self.client_lock:
            if not self.client or not await self.client.check_connect():
                logger.info("Initializing Shared Quotex Client...")
                from pyquotex.stable_api import Quotex
                from pyquotex.qxtypes import ReconnectPolicy
                
                self.client = Quotex(
                    email=EMAIL, password=PASSWORD, host=HOST, user_agent=USER_AGENT,
                    reconnect_policy=ReconnectPolicy(enabled=True)
                )
                if SSID:
                    self.client.set_session(user_agent=USER_AGENT, ssid=SSID)
                
                ok, reason = await self.client.connect()
                if not ok or not await self.client.check_connect():
                    raise RuntimeError(f"Quotex connection failed. Check SSID. Reason: {reason}")
                logger.info("Shared Client Connected Successfully!")
        return self.client

    async def get_payouts(self):
        """Fetches all open assets and their payout percentages."""
        client = await self.get_client()
        try:
            # 1. Force the client to fetch the latest market instruments if empty
            if not client.api.instruments:
                await client.api.get_instruments()
                await asyncio.sleep(2) # Give it 2 seconds to download from Quotex
            
            import inspect
            
            # 2. Call the function
            call_result = client.get_payment()
            
            # 3. Check if it needs to be awaited
            if inspect.isawaitable(call_result):
                result = await call_result
            else:
                result = call_result
            
            # 4. Parse the result
            payouts = {}
            if isinstance(result, tuple) and len(result) == 2:
                payouts = result[1]
            elif isinstance(result, dict):
                payouts = result
                
            logger.info(f"Successfully fetched {len(payouts)} payouts from Quotex.")
            return payouts
            
        except Exception as e:
            logger.error(f"Error fetching payouts: {e}")
            
        return {}

    async def subscribe(self, ws: WebSocket, asset: str, timeframe: int):
        if asset not in self.subscribers:
            self.subscribers[asset] = set()
        self.subscribers[asset].add(ws)

        # If this asset is not currently being streamed from Quotex, start a background task
        if asset not in self.active_streams:
            self.active_streams[asset] = asyncio.create_task(self._stream_asset(asset, timeframe))

    def unsubscribe(self, ws: WebSocket, asset: str):
        if asset in self.subscribers and ws in self.subscribers[asset]:
            self.subscribers[asset].remove(ws)

    async def _stream_asset(self, asset: str, timeframe: int):
        """Background loop that fetches live candles and broadcasts to subscribed WebSockets."""
        client = await self.get_client()
        await client.start_candles_stream(asset, timeframe)
        last_signature = None
        
        try:
            while True:
                await asyncio.sleep(0.5)
                # Stop stream if no one is listening anymore
                if not self.subscribers.get(asset):
                    break
                    
                raw = await client.get_realtime_candles(asset)
                candles = normalize_candles(raw)
                
                if candles:
                    sig = (candles[-1]["time"], candles[-1]["close"])
                    if sig != last_signature:
                        last_signature = sig
                        msg = {"type": "candles", "symbol": asset, "candles": candles}
                        
                        # Broadcast to all connected clients listening to this asset
                        for ws in list(self.subscribers[asset]):
                            try:
                                await ws.send_json(msg)
                            except:
                                pass # Disconnected clients are handled in the websocket route
        finally:
            logger.info(f"Stopping stream for {asset}")
            try:
                await client.stop_candles_stream(asset)
            except: pass
            self.active_streams.pop(asset, None)

bridge = BridgeManager()

@app.websocket("/ws")
async def websocket_feed(ws: WebSocket):
    await ws.accept()
    subscribed_assets = set()
    logger.info("New WebSocket Client Connected.")

    try:
        while True:
            request = await ws.receive_json()
            req_type = request.get("type")

            if req_type == "get_assets":
                # Client wants a list of all open pairs and payouts
                payouts = await bridge.get_payouts()
                await ws.send_json({"type": "assets", "data": payouts})

            elif req_type == "subscribe":
                # Client wants to stream a specific asset
                raw_asset = str(request.get("symbol", "EURUSD")).replace("/", "").upper()
                timeframe = int(request.get("timeframe", 60))
                
                client = await bridge.get_client()
                
                # Fetch history (snapshot)
                history = await client.get_candles(raw_asset, time.time(), timeframe * 199, timeframe)
                snapshot = normalize_candles(history)
                await ws.send_json({"type": "snapshot", "symbol": raw_asset, "candles": snapshot})
                
                # Start Real-Time streaming
                await bridge.subscribe(ws, raw_asset, timeframe)
                subscribed_assets.add(raw_asset)

            elif req_type == "unsubscribe":
                raw_asset = str(request.get("symbol", "")).replace("/", "").upper()
                bridge.unsubscribe(ws, raw_asset)
                if raw_asset in subscribed_assets:
                    subscribed_assets.remove(raw_asset)

    except WebSocketDisconnect:
        logger.info("WebSocket Client Disconnected.")
    finally:
        for asset in subscribed_assets:
            bridge.unsubscribe(ws, asset)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("quotex_bridge:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
