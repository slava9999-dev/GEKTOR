import os
import json
import time
import asyncio
import websockets
import websockets.client
import requests
import logging
import ssl
from collections import defaultdict, deque
from typing import Any, Dict, List, Tuple
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("CryptoSentinelWSS")

# Load environment variables
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(BASE_DIR, ".env"))

SKILL_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(SKILL_DIR, "config_wss.json")
TG_CONFIG_FILE = os.path.join(
    os.path.dirname(SKILL_DIR), "telegram-guard", "config.json"
)


def load_json(filepath: str) -> Dict[str, Any]:
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load {filepath}: {e}")
        return {}


def send_telegram_alert_async(message: str, tg_config: Dict[str, Any]) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN") or tg_config.get("bot_token")
    chat_id = os.getenv("TELEGRAM_CHAT_ID") or tg_config.get("default_chat_id")
    prefix = tg_config.get("alert_prefix", "🚨 ")
    if not token or not chat_id:
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    text = f"{prefix} {message}"

    # Fire and forget request to not block websocket loop
    try:
        proxies = None
        proxy_url = os.getenv("PROXY_URL")
        if proxy_url:
            proxies = {"http": proxy_url, "https": proxy_url}
        # We use sync requests here but run in a separate thread if needed, or simply let it run quickly since it's an alert
        loop = asyncio.get_event_loop()
        loop.run_in_executor(
            None,
            lambda: requests.post(
                url,
                json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
                timeout=5,
                proxies=proxies
            ),
        )
    except Exception as e:
        logger.error(f"Failed to queue Telegram alert: {e}")

class ExponentialBackoff:
    def __init__(self, base=2, max_delay=300, max_attempts_before_fallback=10):
        self.base = base
        self.max_delay = max_delay
        self.max_attempts_before_fallback = max_attempts_before_fallback
        self.attempt = 0
    
    def next_delay(self) -> float:
        self.attempt += 1
        delay = min(self.base * (2 ** (self.attempt - 1)), self.max_delay)
        return float(delay)
    
    def should_try_fallback(self) -> bool:
        return self.attempt >= self.max_attempts_before_fallback
    
    def reset(self):
        self.attempt = 0


class WSSentinel:
    def __init__(self):
        self.config = load_json(CONFIG_FILE)
        self.tg_config = load_json(TG_CONFIG_FILE)

        self.wss_url = "wss://stream.bybit.com/v5/public/linear"
        cfg = self.config.get("screener_wss", {})
        self.top_n = cfg.get("top_symbols_to_monitor", 100)
        self.whale_threshold = cfg.get("whale_trade_usd_threshold", 500000)
        self.rekt_threshold = cfg.get("liquidation_usd_threshold", 100000)
        self.cvd_threshold = cfg.get("cvd_spike_usd_threshold", 3000000)
        self.dom_threshold = cfg.get("dom_wall_usd_threshold", 2000000)
        self.dom_top_n = cfg.get("dom_top_n_symbols", 20)

        self.symbols: List[str] = []

        self.cvd_queues: Dict[str, deque] = defaultdict(lambda: deque(maxlen=2000))
        self.cvd_sums: Dict[str, float] = defaultdict(float)

        # Debounce/Cooldown maps to prevent spamming
        self.last_whale_alert: Dict[str, float] = {}
        self.last_rekt_alert: Dict[str, float] = {}
        self.last_cvd_alert: Dict[str, float] = {}
        self.last_dom_alert: Dict[str, float] = {}

    def get_thresholds(self, symbol: str) -> Tuple[float, float, float, float]:
        if symbol == "BTCUSDT":
            return (
                self.whale_threshold * 10,
                self.rekt_threshold * 5,
                self.cvd_threshold * 5,
                self.dom_threshold * 4,
            )
        elif symbol in ["ETHUSDT", "SOLUSDT"]:
            return (
                self.whale_threshold * 5,
                self.rekt_threshold * 3,
                self.cvd_threshold * 3,
                self.dom_threshold * 3,
            )
        return (
            self.whale_threshold,
            self.rekt_threshold,
            self.cvd_threshold,
            self.dom_threshold,
        )

    def fetch_top_symbols(self) -> List[str]:
        url = "https://api.bybit.com/v5/market/tickers?category=linear"
        try:
            proxies = None
            proxy_url = os.getenv("PROXY_URL")
            if proxy_url:
                proxies = {"http": proxy_url, "https": proxy_url}
            r = requests.get(url, timeout=10, proxies=proxies)
            if r.status_code == 200:
                data = r.json()
                if data.get("retCode") == 0:
                    tickers = data["result"]["list"]
                    valid = [t for t in tickers if t.get("symbol", "").endswith("USDT")]
                    valid.sort(
                        key=lambda x: float(x.get("turnover24h", 0)), reverse=True
                    )
                    return [t["symbol"] for t in valid[: self.top_n]]
        except Exception as e:
            logger.error(f"Failed fetching top symbols: {e}")
        # Default fallback
        return ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

    async def connect(self) -> None:
        self.symbols = self.fetch_top_symbols()
        logger.info(
            f"Targeting Top {len(self.symbols)} symbols by Volume. E.g.: {self.symbols[:5]}"
        )

        ssl_context = ssl.create_default_context()
        backoff = ExponentialBackoff()
        primary_url = self.wss_url
        fallback_url = "wss://stream.bytick.com/v5/public/linear"
        current_url = primary_url

        proxy_url = os.getenv("PROXY_URL")
        # Ensure proxy url looks like http://host:port for proxy library, but websockets library accepts HTTP proxy if requested. 
        # For websockets directly it requires a specific proxy handling or custom connector, but since websockets library in python doesn't officially support HTTP proxy connecting directly via client.connect kwargs smoothly in all versions, we'll try configuring it if supported, or pass aiohttp session. 
        # websockets client does NOT support native proxy argument directly. A helper or different library is usually needed.
        # But wait! websockets.client support proxy since version 14.0 or via aiohttp... Actually, standard websockets only supports HTTP_PROXY environment variables by default. 
        # BUT Bybit websocket is wss://. Let's rely on standard OS env vars which websockets respects if httpx/aiohttp is used, OR if we need raw support we modify env inside script.
        
        # Let's set env vars for proxy so `websockets` may pick it up automatically if compiled with proxy support, or use aiohttp instead if possible... Sentinel uses `websockets` lib.
        if proxy_url:
            os.environ["HTTP_PROXY"] = proxy_url
            os.environ["HTTPS_PROXY"] = proxy_url
            logger.info(f"Using proxy for WSS: {proxy_url}")

        while True:
            try:
                logger.info(f"Connecting to {current_url}...")
                async with websockets.client.connect(
                    current_url, ping_interval=30, ping_timeout=60, ssl=ssl_context
                ) as ws:
                    logger.info(
                        "Connected! Subscribing to publicTrade and liquidation streams..."
                    )
                    backoff.reset()

                    # Split into chunks of 10 for subscription
                    topics = []
                    for sym in self.symbols:
                        topics.append(f"publicTrade.{sym}")
                        topics.append(f"liquidation.{sym}")

                    # Add DOM orderbook only for strictly top N to avoid overloading WSS
                    for sym in self.symbols[: self.dom_top_n]:
                        topics.append(f"orderbook.50.{sym}")

                    for i in range(0, len(topics), 10):
                        req = {"op": "subscribe", "args": topics[i : i + 10]}
                        await ws.send(json.dumps(req))
                        await asyncio.sleep(0.1)  # Avoid rate limit

                    send_telegram_alert_async(
                        "⚡ <b>WSS Sentinel PRO</b> запущен.\nСнайпер слушает:\n🐋 Киты (>$"
                        + str(self.whale_threshold // 1000)
                        + "k)\n💀 REKT (>$"
                        + str(self.rekt_threshold // 1000)
                        + "k)\n📈 CVD Дельту (>$"
                        + str(self.cvd_threshold // 1000000)
                        + "M)\n🧱 DOM Плиты (>$"
                        + str(self.dom_threshold // 1000000)
                        + "M)",
                        self.tg_config,
                    )
                    logger.info("Subscriptions sent. Listening for events...")

                    await self.listen(ws)
            except Exception as e:
                if backoff.should_try_fallback():
                    current_url = fallback_url
                delay = backoff.next_delay()
                logger.error(f"WSS Connection lost or error: {e}. Reconnecting in {delay}s (attempt #{backoff.attempt})...")
                await asyncio.sleep(delay)

    async def listen(self, ws: Any) -> None:
        async for message in ws:
            try:
                data = json.loads(message)
                if "topic" not in data:
                    continue

                topic = data["topic"]

                # 1. WHALE TRADES ( publicTrade ) & CVD Tracker
                if topic.startswith("publicTrade"):
                    processed_symbols = set()
                    now = time.time()
                    for trade in data.get("data", []):
                        price = float(trade["p"])
                        size = float(trade["v"])
                        usd_value = price * size
                        symbol = trade["s"]
                        side = trade["S"]  # "Buy" or "Sell"

                        # Whale Tracking
                        whale_th, _, _, _ = self.get_thresholds(symbol)
                        if usd_value >= whale_th:
                            cache_key = f"{symbol}_{side}"
                            # Prevent alert spam if multiple prints hit instantly (cooldown 2 seconds)
                            if now - self.last_whale_alert.get(cache_key, 0) > 2.0:
                                self.last_whale_alert[cache_key] = now
                                self.trigger_whale_alert(symbol, side, price, usd_value)

                        # CVD Accumulation logic
                        processed_symbols.add(symbol)
                        usd_delta = usd_value if side == "Buy" else -usd_value
                        self.cvd_queues[symbol].append((now, usd_delta))
                        self.cvd_sums[symbol] += usd_delta

                    # CVD Flush & Check
                    for sym in processed_symbols:
                        queue = self.cvd_queues[sym]
                        # Remove items older than 300 seconds (5 mins)
                        while queue and now - queue[0][0] > 300:
                            _, old_delta = queue.popleft()
                            self.cvd_sums[sym] -= old_delta

                        current_cvd = self.cvd_sums[sym]
                        _, _, cvd_th, _ = self.get_thresholds(sym)
                        if abs(current_cvd) >= cvd_th:
                            if (
                                now - self.last_cvd_alert.get(sym, 0) > 300
                            ):  # 5m cooldown per symbol
                                self.last_cvd_alert[sym] = now
                                self.trigger_cvd_alert(sym, current_cvd)

                # 2. LIQUIDATIONS ( liquidation )
                elif topic.startswith("liquidation"):
                    trade = data.get("data", {})
                    if trade:
                        price = float(trade["p"])
                        size = float(trade["v"])
                        usd_value = price * size
                        symbol = trade["s"]

                        _, rekt_th, _, _ = self.get_thresholds(symbol)
                        if usd_value >= rekt_th:
                            # For Liquidations on Bybit:
                            # side "Buy" -> Short was liquidated (forced buy to close short)
                            # side "Sell" -> Long was liquidated (forced sell to close long)
                            side = trade["S"]
                            now = time.time()

                            cache_key = f"{symbol}_{side}"
                            # 2 sec cooldown for cascades
                            if now - self.last_rekt_alert.get(cache_key, 0) > 2.0:
                                self.last_rekt_alert[cache_key] = now
                                self.trigger_rekt_alert(symbol, side, price, usd_value)

                # 3. DOM WALLS ( orderbook )
                elif topic.startswith("orderbook"):
                    data_obj = data.get("data", {})
                    symbol = data_obj.get("s", "")
                    if not symbol:
                        continue

                    bids = data_obj.get("b", [])
                    asks = data_obj.get("a", [])
                    now_dom = time.time()

                    _, _, _, dom_th = self.get_thresholds(symbol)

                    def check_dom(orders, side_name):
                        for price_str, size_str in orders:
                            price_val = float(price_str)
                            size_val = float(size_str)
                            if size_val == 0:
                                continue  # order deleted
                            usd_val = price_val * size_val
                            if usd_val >= dom_th:
                                cache_key = f"{symbol}_{side_name}"
                                if (
                                    now_dom - self.last_dom_alert.get(cache_key, 0)
                                    > 1800
                                ):  # 30 min cooldown per side
                                    self.last_dom_alert[cache_key] = now_dom
                                    self.trigger_dom_alert(
                                        symbol, side_name, price_val, usd_val
                                    )

                    check_dom(bids, "Bid (Покупка)")
                    check_dom(asks, "Ask (Продажа)")

            except Exception as e:
                logger.debug(f"Parsing error: {e}")

    def trigger_whale_alert(
        self, symbol: str, side: str, price: float, usd_val: float
    ) -> None:
        if side == "Buy":
            emoji = "🟩"
            action = "АГРЕССИВНЫЙ ЗАКУП"
            detail = "Маркет-бай по Ask'ам"
        else:
            emoji = "🟥"
            action = "СБРОС В РЫНОК"
            detail = "Маркет-селл по Bid'ам"

        size_str = f"${usd_val/1e6:.2f}M" if usd_val >= 1_000_000 else f"${usd_val/1e3:.0f}K"

        msg = (
            f"{emoji} <b>КИТ │ {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>{size_str}</b> │ {action}\n"
            f"📍 Цена: <code>${price}</code>\n"
            f"📋 {detail}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>Gerald Sentinel 🔭</i>"
        )
        logger.info(f"WHALE: {symbol} {side} ${usd_val:.0f} @ {price}")
        send_telegram_alert_async(msg, self.tg_config)

    def trigger_rekt_alert(
        self, symbol: str, side: str, price: float, usd_val: float
    ) -> None:
        if side == "Buy":
            emoji = "🩳"
            rekt_type = "SHORT SQUEEZE"
            victim = "Шортисты"
        else:
            emoji = "🩸"
            rekt_type = "LONG LIQUIDATION"
            victim = "Лонгисты"

        size_str = f"${usd_val/1e6:.2f}M" if usd_val >= 1_000_000 else f"${usd_val/1e3:.0f}K"

        msg = (
            f"{emoji} <b>REKT │ {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"💀 <b>{size_str}</b> │ {rekt_type}\n"
            f"📍 Цена: <code>${price}</code>\n"
            f"👥 {victim} ликвидированы\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>Gerald Sentinel 🔭</i>"
        )
        logger.info(f"REKT: {symbol} {rekt_type} ${usd_val:.0f} @ {price}")
        send_telegram_alert_async(msg, self.tg_config)

    def trigger_cvd_alert(self, symbol: str, cvd_val: float) -> None:
        if cvd_val > 0:
            emoji = "🟢"
            direction = "ПОКУПАТЕЛИ ДАВЯТ"
        else:
            emoji = "🔴"
            direction = "ПРОДАВЦЫ ДАВЯТ"

        msg = (
            f"{emoji} <b>CVD │ {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>${cvd_val/1e6:+.2f}M</b> │ {direction}\n"
            f"⏱ Окно: 5 минут\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>Gerald Sentinel 🔭</i>"
        )
        logger.info(f"CVD SPIKE: {symbol} {cvd_val:+.0f}")
        send_telegram_alert_async(msg, self.tg_config)

    def trigger_dom_alert(
        self, symbol: str, side: str, price: float, usd_val: float
    ) -> None:
        if "Bid" in side:
            emoji = "🟢"
            wall_type = "СТЕНА ПОКУПОК"
        else:
            emoji = "🔴"
            wall_type = "СТЕНА ПРОДАЖ"

        msg = (
            f"🧱 <b>DOM │ {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"{emoji} <b>${usd_val/1e6:.2f}M</b> │ {wall_type}\n"
            f"📍 Уровень: <code>${price}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>Gerald Sentinel 🔭</i>"
        )
        logger.info(f"DOM WALL: {symbol} {side} ${usd_val:.0f} @ {price}")
        send_telegram_alert_async(msg, self.tg_config)


if __name__ == "__main__":
    sentinel = WSSentinel()
    try:
        asyncio.run(sentinel.connect())
    except KeyboardInterrupt:
        logger.info("WSSentinel stopped manually.")
