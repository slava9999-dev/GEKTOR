import os
import json
import time
import requests
import logging
import pandas as pd
import mplfinance as mpf
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("FormationScanner")

# Load environment variables
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(BASE_DIR, ".env"))

executor = ThreadPoolExecutor(max_workers=5)
SKILL_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(SKILL_DIR, "config.json")
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


def _send_telegram_worker(
    url: str,
    type_req: str,
    data: Optional[Dict[str, Any]] = None,
    json_data: Optional[Dict[str, Any]] = None,
    files: Optional[Dict[str, Any]] = None,
) -> None:
    try:
        if type_req == "photo":
            requests.post(url, data=data, files=files, timeout=15)
        else:
            requests.post(url, json=json_data, timeout=5)
    except Exception as e:
        logger.error(f"Failed to send Telegram alert: {e}")


def send_telegram_alert(
    message: str, tg_config: Dict[str, Any], photo_buf: Optional[BytesIO] = None
) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN") or tg_config.get("bot_token")
    chat_id = os.getenv("TELEGRAM_CHAT_ID") or tg_config.get("default_chat_id")
    if not token or not chat_id:
        return

    if photo_buf:
        url = f"https://api.telegram.org/bot{token}/sendPhoto"
        files = {"photo": ("chart.png", photo_buf.getvalue(), "image/png")}
        data = {"chat_id": chat_id, "caption": message, "parse_mode": "HTML"}
        executor.submit(_send_telegram_worker, url, "photo", data, None, files)
    else:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        executor.submit(
            _send_telegram_worker,
            url,
            "text",
            None,
            {"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            None,
        )


def fetch_klines(symbol: str, interval: str = "5", limit: int = 100) -> List[Any]:
    url = f"https://api.bybit.com/v5/market/kline?category=linear&symbol={symbol}&interval={interval}&limit={limit}"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            data = r.json()
            if data.get("retCode") == 0:
                result = data["result"]["list"]
                # Bybit returns descending, we want ascending for analysis
                return result[::-1]
    except Exception as e:
        logger.error(f"Error fetching klines for {symbol}: {e}")
    return []


def generate_chart_from_klines(
    symbol: str, klines: List[Any], level: Optional[float] = None
) -> Optional[BytesIO]:
    if not klines:
        return None
    try:
        df = pd.DataFrame(
            klines,
            columns=["timestamp", "open", "high", "low", "close", "volume", "turnover"],
        )
        df["timestamp"] = pd.to_numeric(df["timestamp"])
        df["Datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
        df.set_index("Datetime", inplace=True)

        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col])

        buf = BytesIO()
        mc = mpf.make_marketcolors(
            up="green", down="red", edge="i", wick="i", volume="in", ohlc="i"
        )
        s = mpf.make_mpf_style(
            marketcolors=mc,
            gridstyle=":",
            y_on_right=True,
            base_mpl_style="dark_background",
        )

        title = f"{symbol} (5m)"

        # Add horizontal line for the level
        hline = (
            dict(hlines=[level], colors=["cyan"], linewidths=[1.5], linestyle="--")
            if level
            else None
        )

        mpf.plot(
            df,
            type="candle",
            volume=True,
            style=s,
            title=title,
            hlines=hline,
            savefig=buf,
            figsize=(8, 5),
            tight_layout=True,
        )
        buf.seek(0)
        
        # Prevent profound memory leak in long-running jobs!
        import matplotlib.pyplot as plt
        plt.close('all')
        
        return buf
    except Exception as e:
        logger.error(f"Chart generation error: {e}")
        return None


class MarketSentinel:
    def __init__(self) -> None:
        self.config = load_json(CONFIG_FILE)
        self.tg_config = load_json(TG_CONFIG_FILE)

        # Pull parameters from config or use defaults
        cfg = self.config.get("scanner_daemon", {})
        self.poll_interval = cfg.get("poll_interval", 180)
        self.min_24h_turnover = cfg.get("min_24h_turnover", 15000000)
        self.top_n_symbols = cfg.get("top_n_symbols", 60)
        self.compression_threshold = cfg.get("compression_threshold", 0.38)

        self.last_alert_time: Dict[str, float] = {}  # symbol -> timestamp

    def fetch_tickers(self) -> List[Dict[str, Any]]:
        url = "https://api.bybit.com/v5/market/tickers?category=linear"
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                data = r.json()
                if data.get("retCode") == 0:
                    return data["result"]["list"]
        except Exception as e:
            logger.error(f"Error fetching tickers: {e}")
        return []

    def analyze_formation(
        self, symbol: str, klines: List[Any], current_price: float
    ) -> Optional[Dict[str, Any]]:
        if len(klines) < 50:
            return None

        # Exclude current forming candle to find true historical levels
        historical = klines[:-1]

        highs = [float(k[2]) for k in historical]
        lows = [float(k[3]) for k in historical]

        max_high = max(highs)
        min_low = min(lows)

        # Volatility filter: skip dead/flat markets (less than 3.5% move in window)
        range_pct = (max_high - min_low) / min_low * 100
        if range_pct < 3.5:
            return None

        # Clustering filter: the last 3 closed candles must consolidate near the extreme
        closes = [float(k[4]) for k in historical]
        avg_close_last_3 = sum(closes[-3:]) / 3

        dist_to_high_pct = (max_high - current_price) / current_price * 100
        dist_to_low_pct = (current_price - min_low) / current_price * 100

        formation = None
        touches = 0
        level: float = 0.0

        if 0 <= dist_to_high_pct <= self.compression_threshold:
            close_dist_high = (max_high - avg_close_last_3) / avg_close_last_3 * 100
            if close_dist_high <= 0.4:  # The base is clustered right under the high
                touch_zone = max_high * 0.9985
                touches = sum(1 for h in highs if h >= touch_zone)
                if touches >= 5:
                    formation = "ПРОБОЙ ХАЯ (В ЛОНГ)"
                    level = max_high

        elif 0 <= dist_to_low_pct <= self.compression_threshold:
            close_dist_low = (avg_close_last_3 - min_low) / avg_close_last_3 * 100
            if close_dist_low <= 0.4:  # The base is clustered right above the low
                touch_zone = min_low * 1.0015
                touches = sum(1 for low_val in lows if low_val <= touch_zone)
                if touches >= 5:
                    formation = "ПРОБОЙ ЛОЯ (В ШОРТ)"
                    level = min_low

        if formation:
            return {
                "type": formation,
                "level": level,
                "touches": touches,
                "dist": dist_to_high_pct if "ЛОНГ" in formation else dist_to_low_pct,
            }
        return None

    def cycle(self) -> None:
        tickers = self.fetch_tickers()
        if not tickers:
            return

        valid_tickers = [
            t
            for t in tickers
            if t.get("symbol", "").endswith("USDT")
            and float(t.get("turnover24h", 0)) >= self.min_24h_turnover
        ]
        sorted_by_vol = sorted(
            valid_tickers, key=lambda x: float(x.get("turnover24h", 0)), reverse=True
        )

        top_symbols = [t["symbol"] for t in sorted_by_vol[: self.top_n_symbols]]
        now = time.time()

        logger.info(f"Scanning top {len(top_symbols)} symbols for formations...")

        for symbol in top_symbols:
            try:
                # To avoid rate limits (Bybit allows 120 req/s, but we still shouldn't burst too fast)
                time.sleep(0.12)

                klines = fetch_klines(symbol, interval="5", limit=80)  # last ~6.5 hours
                if not klines:
                    continue

                current_price = float(klines[-1][4])  # Close of the latest candle

                formation = self.analyze_formation(symbol, klines, current_price)

                if formation:
                    # Cooldown for alerts (60 minutes per symbol)
                    last_time = self.last_alert_time.get(symbol, 0)
                    if now - last_time > 3600:
                        self.last_alert_time[symbol] = now
                        self.fire_alert(symbol, current_price, formation, klines)
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")

    def fire_alert(
        self,
        symbol: str,
        current_price: float,
        formation: Dict[str, Any],
        klines: List[Any],
    ) -> None:
        lvl = formation["level"]
        tchs = formation["touches"]
        dist = formation["dist"]

        emoji = "📈" if "ЛОНГ" in formation["type"] else "📉"

        msg = f"{emoji} <b>ФОРМАЦИЯ: {formation['type']}</b>\n\n"
        msg += f"🔥 <b>Монета:</b> #{symbol}\n"
        msg += f"📍 <b>Уровень (Таргет):</b> ${lvl:.4f}\n"
        msg += f"🎯 <b>Касаний уровня:</b> {tchs} раз\n"
        msg += f"📏 <b>До пробоя:</b> {dist:.2f}%\n"
        msg += f"💲 <b>Текущая цена:</b> ${current_price:.4f}\n\n"
        msg += "<i>Поджатие сформировано. Готовь лимитки! Скринер продолжает мониторинг.</i>"

        logger.info(f"FORMATION ALERT: {symbol} - {formation['type']} @ {lvl}")

        chart_buf = generate_chart_from_klines(symbol, klines, level=lvl)
        send_telegram_alert(msg, self.tg_config, photo_buf=chart_buf)

    def run(self) -> None:
        logger.info("Starting Formation Scanner PRO...")
        send_telegram_alert(
            "👁️‍🗨️ <b>Formation Scanner PRO</b> запущен!\nИщу горизонтальные уровни, поджатия и формации на пробой для топ альтов.",
            self.tg_config,
        )
        while True:
            try:
                self.cycle()
            except Exception as e:
                logger.error(f"Cycle error: {e}")
            time.sleep(self.poll_interval)


if __name__ == "__main__":
    print("=" * 60)
    print("⛔ DEPRECATED: sentinel_daemon.py is DISABLED.")
    print("")
    print("This module has been superseded by Gerald Sniper")
    print("(skills/gerald-sniper/main.py) which provides:")
    print("  ✅ WebSocket real-time data (not REST polling)")
    print("  ✅ KDE-based mathematical level detection")
    print("  ✅ Multi-trigger scoring (compression + volume + breakout)")
    print("  ✅ BTC macro filter")
    print("  ✅ Position sizing & risk management")
    print("  ✅ Exponential backoff & bytick.com fallback")
    print("")
    print("To run the active scanner:")
    print("  python skills/gerald-sniper/main.py")
    print("=" * 60)
    sys.exit(0)
