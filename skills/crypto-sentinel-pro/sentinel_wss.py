import os
import json
import time
import asyncio
import websockets
import requests
import logging
from collections import defaultdict, deque

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("CryptoSentinelWSS")

SKILL_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(SKILL_DIR, 'config_wss.json')
TG_CONFIG_FILE = os.path.join(os.path.dirname(SKILL_DIR), 'telegram-guard', 'config.json')

def load_json(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load {filepath}: {e}")
        return {}

def send_telegram_alert_async(message, tg_config):
    if not tg_config: return
    token = tg_config.get("bot_token")
    chat_id = tg_config.get("default_chat_id")
    prefix = tg_config.get("alert_prefix", "🚨 ")
    if not token or not chat_id: return
    
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    text = f"{prefix} {message}"
    
    # Fire and forget request to not block websocket loop
    try:
        # We use sync requests here but run in a separate thread if needed, or simply let it run quickly since it's an alert
        loop = asyncio.get_event_loop()
        loop.run_in_executor(None, lambda: requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"}, timeout=5))
    except Exception as e:
        logger.error(f"Failed to queue Telegram alert: {e}")

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
        
        self.symbols = []
        
        self.cvd_queues = defaultdict(deque) 
        self.cvd_sums = defaultdict(float)
        
        # Debounce/Cooldown maps to prevent spamming
        self.last_whale_alert = {}
        self.last_rekt_alert = {}
        self.last_cvd_alert = {}
        self.last_dom_alert = {}

    def get_thresholds(self, symbol):
        if symbol == "BTCUSDT":
            return self.whale_threshold * 10, self.rekt_threshold * 5, self.cvd_threshold * 5, self.dom_threshold * 4
        elif symbol in ["ETHUSDT", "SOLUSDT"]:
            return self.whale_threshold * 5, self.rekt_threshold * 3, self.cvd_threshold * 3, self.dom_threshold * 3
        return self.whale_threshold, self.rekt_threshold, self.cvd_threshold, self.dom_threshold

    def fetch_top_symbols(self):
        url = "https://api.bybit.com/v5/market/tickers?category=linear"
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                data = r.json()
                if data.get('retCode') == 0:
                    tickers = data['result']['list']
                    valid = [t for t in tickers if t.get('symbol', '').endswith('USDT')]
                    valid.sort(key=lambda x: float(x.get('turnover24h', 0)), reverse=True)
                    return [t['symbol'] for t in valid[:self.top_n]]
        except Exception as e:
            logger.error(f"Failed fetching top symbols: {e}")
        # Default fallback
        return ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

    async def connect(self):
        self.symbols = self.fetch_top_symbols()
        logger.info(f"Targeting Top {len(self.symbols)} symbols by Volume. E.g.: {self.symbols[:5]}")
        
        while True:
            try:
                logger.info(f"Connecting to {self.wss_url}...")
                async with websockets.connect(self.wss_url, ping_interval=20, ping_timeout=10) as ws:
                    logger.info("Connected! Subscribing to publicTrade and liquidation streams...")
                    
                    # Split into chunks of 10 for subscription
                    topics = []
                    for sym in self.symbols:
                        topics.append(f"publicTrade.{sym}")
                        topics.append(f"liquidation.{sym}")
                    
                    # Add DOM orderbook only for strictly top N to avoid overloading WSS
                    for sym in self.symbols[:self.dom_top_n]:
                        topics.append(f"orderbook.50.{sym}")
                        
                    for i in range(0, len(topics), 10):
                        req = {
                            "op": "subscribe",
                            "args": topics[i:i+10]
                        }
                        await ws.send(json.dumps(req))
                        await asyncio.sleep(0.1) # Avoid rate limit
                    
                    send_telegram_alert_async("⚡ <b>WSS Sentinel PRO</b> запущен.\nСнайпер слушает:\n🐋 Киты (>$" + str(self.whale_threshold//1000) + "k)\n💀 REKT (>$" + str(self.rekt_threshold//1000) + "k)\n📈 CVD Дельту (>$" + str(self.cvd_threshold//1000000) + "M)\n🧱 DOM Плиты (>$" + str(self.dom_threshold//1000000) + "M)", self.tg_config)
                    logger.info("Subscriptions sent. Listening for events...")
                    
                    await self.listen(ws)
            except Exception as e:
                logger.error(f"WSS Connection lost or error: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)

    async def listen(self, ws):
        async for message in ws:
            try:
                data = json.loads(message)
                if 'topic' not in data: continue
                
                topic = data['topic']
                
                # 1. WHALE TRADES ( publicTrade ) & CVD Tracker
                if topic.startswith("publicTrade"):
                    processed_symbols = set()
                    now = time.time()
                    for trade in data.get('data', []):
                        price = float(trade['p'])
                        size = float(trade['v'])
                        usd_value = price * size
                        symbol = trade['s']
                        side = trade['S'] # "Buy" or "Sell"
                        
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
                            if now - self.last_cvd_alert.get(sym, 0) > 300: # 5m cooldown per symbol
                                self.last_cvd_alert[sym] = now
                                self.trigger_cvd_alert(sym, current_cvd)
                                
                # 2. LIQUIDATIONS ( liquidation )
                elif topic.startswith("liquidation"):
                    trade = data.get('data', {})
                    if trade:
                        price = float(trade['p'])
                        size = float(trade['v'])
                        usd_value = price * size
                        symbol = trade['s']
                        
                        _, rekt_th, _, _ = self.get_thresholds(symbol)
                        if usd_value >= rekt_th:
                            # For Liquidations on Bybit:
                            # side "Buy" -> Short was liquidated (forced buy to close short)
                            # side "Sell" -> Long was liquidated (forced sell to close long)
                            side = trade['S'] 
                            now = time.time()
                            
                            cache_key = f"{symbol}_{side}"
                            # 2 sec cooldown for cascades
                            if now - self.last_rekt_alert.get(cache_key, 0) > 2.0:
                                self.last_rekt_alert[cache_key] = now
                                self.trigger_rekt_alert(symbol, side, price, usd_value)

                # 3. DOM WALLS ( orderbook )
                elif topic.startswith("orderbook"):
                    data_obj = data.get('data', {})
                    symbol = data_obj.get('s', '')
                    if not symbol: continue
                    
                    bids = data_obj.get('b', [])
                    asks = data_obj.get('a', [])
                    now_dom = time.time()
                    
                    _, _, _, dom_th = self.get_thresholds(symbol)
                    
                    def check_dom(orders, side_name):
                        for price_str, size_str in orders:
                            price_val = float(price_str)
                            size_val = float(size_str)
                            if size_val == 0: continue # order deleted
                            usd_val = price_val * size_val
                            if usd_val >= dom_th:
                                cache_key = f"{symbol}_{side_name}"
                                if now_dom - self.last_dom_alert.get(cache_key, 0) > 1800: # 30 min cooldown per side
                                    self.last_dom_alert[cache_key] = now_dom
                                    self.trigger_dom_alert(symbol, side_name, price_val, usd_val)
                    
                    check_dom(bids, "Bid (Покупка)")
                    check_dom(asks, "Ask (Продажа)")

            except Exception as e:
                # Silently catch parsing errors so we don't crash
                pass

    def trigger_whale_alert(self, symbol, side, price, usd_val):
        emoji_side = "🟩 ЖЕСТКИЙ ЗАКУП (Вдарили по Ask'ам)" if side == "Buy" else "🟥 КРАСНАЯ СОПЛЯ (Ударили по Bid'ам)"
        msg = f"<b>🐋 КТО-ТО ЖИРНЫЙ ВЛЕТЕЛ В {symbol}</b>\nДвиж: {emoji_side}\nКотлета: <b>${usd_val/1e6:.2f}M</b>\nЗашел по: ${price}"
        logger.info(f"WHALE: {symbol} {side} ${usd_val:.0f} @ {price}")
        send_telegram_alert_async(msg, self.tg_config)

    def trigger_rekt_alert(self, symbol, side, price, usd_val):
        if side == "Buy":
            rekt_type = "🩳 Шортистов несут вперед ногами (Short Squeeze)"
        else:
            rekt_type = "🩸 Лонгустов сбрили (Long Liquidation)"
            
        msg = f"<b>💀 КЛАДБИЩЕ ЛИКВИДАЦИЙ: {symbol}</b>\nСуета: {rekt_type}\nРынок сожрал: <b>${usd_val/1e3:.0f}K</b>\nУбило об цену: ${price}"
        logger.info(f"REKT: {symbol} {rekt_type} ${usd_val:.0f} @ {price}")
        send_telegram_alert_async(msg, self.tg_config)

    def trigger_cvd_alert(self, symbol, cvd_val):
        direction = "🚀 Пылесосят стакан вверх (CVD ПАМП)" if cvd_val > 0 else "🩸 Дампят безжалостно (CVD ДАМП)"
        msg = f"<b>{direction}</b>\nПациент: <b>{symbol}</b>\nПеревес по маркету (5м): <b>${cvd_val/1e6:+.2f}M</b>"
        logger.info(f"CVD SPIKE: {symbol} {cvd_val:+.0f}")
        send_telegram_alert_async(msg, self.tg_config)

    def trigger_dom_alert(self, symbol, side, price, usd_val):
        emoji_side = "🟢 Вёдра на подбор" if "Bid" in side else "🔴 Стена лимиток"
        msg = f"<b>🧱 БЕТОННАЯ ПЛИТА В СТАКАНЕ</b>\nАктив: <b>{symbol}</b>\nТип: {emoji_side} {side}\nРазмер завала: <b>${usd_val/1e6:.2f}M</b>\nНа уровне: ${price}"
        logger.info(f"DOM WALL: {symbol} {side} ${usd_val:.0f} @ {price}")
        send_telegram_alert_async(msg, self.tg_config)

if __name__ == "__main__":
    sentinel = WSSentinel()
    try:
        asyncio.run(sentinel.connect())
    except KeyboardInterrupt:
        logger.info("WSSentinel stopped manually.")
