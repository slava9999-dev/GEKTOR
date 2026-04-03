"""
Gerald Sniper v4 — Unified Telegram Alert Formatter (RU).

Design Spec: GERALD SNIPER — TELEGRAM ALERT DESIGN SPEC (RU) v1.0

Principles:
1. Every alert answers 3 questions: Что? Насколько сильно? Что делать?
2. Maximum 12-14 lines
3. NO technical junk: no detector names, no raw ratios, no debug data
4. Full Russian localization
5. Consistent branding: Gerald Sniper 🎯
6. Inline keyboard buttons (Bybit, TradingView)
7. Confidence score on every alert

Alert Types:
🚀 СИЛЬНЫЙ ИМПУЛЬС       — momentum breakout
⚡ РАННЕЕ УСКОРЕНИЕ       — early velocity/acceleration
💥 ПРОБОЙ УРОВНЯ          — level break
🧲 РЕАКЦИЯ ОТ УРОВНЯ      — level proximity/retest
🔥 АНОМАЛЬНЫЙ ОБЪЁМ       — volume explosion
⚠️ РИСКОВАННОЕ ДВИЖЕНИЕ   — OB imbalance / liquidation cascade
"""
import json
from datetime import datetime
from typing import Optional, Dict, Any, List, Set
from core.events.events import SignalEvent

from core.alerts.models import AlertEvent, AlertType


# ─────────────────────────────────────────────────
#  Utility helpers
# ─────────────────────────────────────────────────

def _format_price(price: float) -> str:
    """Smart price formatting for crypto."""
    if price >= 10_000:
        return f"{price:,.1f}"
    elif price >= 100:
        return f"{price:,.2f}"
    elif price >= 1:
        return f"{price:,.4f}"
    elif price >= 0.01:
        return f"{price:.5f}"
    else:
        return f"{price:.6f}"


def _confidence_label(conf: int) -> str:
    """Human-readable confidence category."""
    if conf >= 90:
        return "экстремальное движение 🔥"
    elif conf >= 80:
        return "сильный сигнал 💪"
    elif conf >= 70:
        return "хороший сигнал ✅"
    elif conf >= 60:
        return "слабый сигнал ⚠️"
    else:
        return "наблюдение 👀"


def _btc_context_text(btc_change: float) -> str:
    """BTC market context as human text."""
    if btc_change > 2.0:
        return "BTC растёт 📈"
    elif btc_change > 0.5:
        return "BTC нейтрально-позитив"
    elif btc_change > -0.5:
        return "BTC нейтрально"
    elif btc_change > -2.0:
        return "BTC слабость 📉"
    else:
        return "BTC падает ⚠️"


def _chart_buttons(symbol: str) -> str:
    """
    Generate inline keyboard JSON for Telegram.
    Buttons: Bybit Chart, TradingView
    """
    # Strip USDT suffix for TradingView
    base = symbol.replace("USDT", "")
    
    keyboard = {
        "inline_keyboard": [[
            {
                "text": "📊 Bybit",
                "url": f"https://www.bybit.com/trade/usdt/{symbol}"
            },
            {
                "text": "📈 TradingView",
                "url": f"https://www.tradingview.com/chart/?symbol=BYBIT:{symbol}.P"
            }
        ]]
    }
    return json.dumps(keyboard)


# ─────────────────────────────────────────────────
#  Main entry point
# ─────────────────────────────────────────────────

def format_alert_to_telegram(event: AlertEvent) -> tuple[str, Optional[str]]:
    """
    Unified alert formatter.
    
    Returns:
        (message_text, reply_markup_json | None)
        
    All formatting follows TELEGRAM ALERT DESIGN SPEC (RU) v1.0.
    """
    try:
        if event.type == AlertType.LEVEL_PROXIMITY:
            return _format_proximity_v4(event)
        elif event.type == AlertType.TRIGGER_FIRED:
            return _format_trigger_v4(event)
        elif event.type == AlertType.LEVEL_ARMED:
            return _format_armed_v4(event)
        elif event.type == AlertType.RADAR_UPDATE:
            return _format_radar_v4(event), None
        elif event.type == AlertType.SYSTEM_DEGRADED:
            return _format_system_v4(event, degraded=True), None
        elif event.type == AlertType.SYSTEM_RECOVERED:
            return _format_system_v4(event, degraded=False), None

        return f"❓ Неизвестное событие: {event.type}", None
    except Exception as e:
        from loguru import logger
        logger.error(f"Formatter error for {event.symbol}: {e}")
        return f"⚠️ Ошибка форматирования ({event.symbol})", None


# ─────────────────────────────────────────────────
#  Signal Alerts (from SignalEngine confluence)
# ─────────────────────────────────────────────────

def format_signal_alert(event: SignalEvent) -> tuple[str, str]:
    """
    Mobile Sniper Interface v5.2 (Pilot Spec).
    Optimized for Russian mobile trading with Deep Links & Latency Guards.
    
    Returns: (message_text, reply_markup_json)
    """
    m = event.metadata or {}
    now = time.time()
    generated_at = event.timestamp
    age_sec = int(now - generated_at)
    
    # 1. Visual Coding (Emojis)
    score = int(event.confidence * 100) if event.confidence <= 1.0 else int(event.confidence)
    color_emoji = "🔥" if score >= 95 else "✅" if score >= 85 else "⚠️"
    
    # 2. Latency Guard
    time_str = datetime.fromtimestamp(generated_at).strftime("%H:%M:%S")
    age_warn = ""
    if age_sec > 60:
        age_warn = " ⚠️ <b>ВХОД ОПАСЕН (Late)</b>"
    
    # 3. Deep Link Generation
    bybit_link = f"https://www.bybit.com/trade/usdt/{event.symbol}"
    
    # 4. Metrics Translation
    spike = m.get('spike', 1.0)
    velocity = m.get('velocity', 1.0)
    momentum = m.get('momentum', 0.0)
    imbalance = m.get('imbalance', 1.0)
    
    # 4.1 Critical Volume Header (Audit 19.4)
    critical_header = ""
    if imbalance > 3.0:
        critical_header = "🚨 <b>[КРИТИЧЕСКИЙ ОБЪЕМ]</b>\n"

    # Main Header & Quick Stats
    msg = (
        f"{critical_header}"
        f"{color_emoji} <b>#{event.symbol} | СКОР: {score}</b>\n"
        f"\n"
        f"🟢 <b>ВХОД (VWAP):</b> <code>{_format_price(event.price)}</code>\n"
        f"🛑 <b>СТОП (SL):</b> <code>{_format_price(m.get('sl', 0))}</code>\n"
        f"💰 <b>ЦЕЛЬ (TP):</b> <code>{_format_price(m.get('tp', 0))}</code>\n"
        f"\n"
        f"📊 <b>ПОЧЕМУ:</b>\n"
        f"• 🧨 <b>Объем:</b> x{spike:.1f} ({'Кит зашел' if spike > 5.0 else 'Рост'})\n"
        f"• ⚡ <b>Скорость:</b> {velocity:.1f} ({'Импульс' if velocity > 2.5 else 'Активность'})\n"
        f"• ⚖️ <b>Дисбаланс:</b> {imbalance:.2f} ({int(imbalance)} пок. на 1 прод.)\n"
        f"• 📈 <b>Импульс:</b> {momentum:+.1f}%\n"
        f"\n"
        f"⏰ {time_str} ({age_sec}с назад){age_warn}\n"
        f"🔗 <a href='{bybit_link}'>ОТКРЫТЬ НА BYBIT</a>\n"
        f"\n"
        f"<i>Gerald Sniper v5.2</i>"
    )
    
    # 5. Interactive Buttons
    markup = {
        "inline_keyboard": [[
            {
                "text": "⚡ ВХОД (Limit IOC)", 
                "callback_data": f"exec|{event.symbol}|{event.price:.6f}|{event.signal_id}"
            },
            {
                "text": "🚫 ПАС", 
                "callback_data": f"pass|{event.signal_id}"
            }
        ]]
    }
    
    return msg, json.dumps(markup)


def _build_market_description(signal_type: str, factors: set, meta: dict) -> str:
    """Generates human-readable market description."""
    
    if signal_type == "MOMENTUM_BREAKOUT":
        lines = []
        if "VELOCITY_SHOCK" in factors and "VOLUME_EXPLOSION" in factors:
            lines.append("На рынке зафиксировано резкое ускорение.")
            lines.append("Количество сделок и объём резко выросли.")
        elif "VELOCITY_SHOCK" in factors:
            lines.append("Резко увеличилось количество сделок.")
        elif "VOLUME_EXPLOSION" in factors:
            lines.append("Объём торгов резко вырос.")
        
        if "MICRO_MOMENTUM" in factors or "ACCELERATION" in factors:
            lines.append("Цена начала быстрое движение вверх.")
        else:
            lines.append("Цена начала набирать импульс.")
        
        return "\n".join(lines) if lines else "Обнаружено сильное движение на рынке."
    
    elif signal_type == "ORDERFLOW_PRESSURE":
        if "ORDERBOOK_IMBALANCE" in factors:
            side = meta.get("ob_side", "")
            if side == "BIDS":
                return "Появляется мощное давление покупателей.\nСтакан показывает сильный перевес быков."
            elif side == "ASKS":
                return "Появляется давление продавцов.\nСтакан показывает перевес медведей."
        return "Обнаружен перекос потока ордеров.\nАктивность участников рынка растёт."
    
    elif signal_type == "VOLATILE_REVERSAL":
        return "Обнаружено раннее ускорение движения.\nЦена начинает набирать импульс."
    
    return "Обнаружена аномальная активность на рынке."


def _build_metrics_lines(factors: set, meta: dict) -> list[str]:
    """Build clean metric lines without technical junk."""
    lines = []
    details = meta.get("details", [])
    
    # Extract useful data from hit details
    for hit in details if isinstance(details, list) else []:
        h_type = hit.get("type", "")
        
        if h_type == "MICRO_MOMENTUM":
            change = hit.get("change", 0)
            if change > 0:
                lines.append(f"Изменение цены (30с): <b>+{change:.1f}%</b>")
        
        elif h_type == "VELOCITY_SHOCK":
            ratio = hit.get("ratio", 0)
            if ratio > 0:
                # Display as human-friendly "ускорение сделок"
                label = "сильное" if ratio > 5 else "заметное"
                lines.append(f"Ускорение сделок: <b>{label} ({ratio:.1f}x)</b>")
        
        elif h_type == "VOLUME_EXPLOSION":
            ratio = hit.get("ratio", 0)
            if ratio > 0:
                lines.append(f"Всплеск объёма: <b>{ratio:.1f}x</b>")
        
        elif h_type == "ACCELERATION":
            accel = hit.get("accel", 0)
            if accel != 0:
                sign = "+" if accel > 0 else ""
                lines.append(f"Ускорение цены: <b>{sign}{accel:.2f}%</b>")
        
        elif h_type == "ORDERBOOK_IMBALANCE":
            ratio = hit.get("ratio", 0)
            side = hit.get("side", "")
            if ratio > 0:
                direction = "покупателей" if side == "BIDS" else "продавцов"
                lines.append(f"Давление {direction}: <b>{ratio:.1f}x</b>")
        
        elif h_type == "LIQUIDATION_CLUSTER":
            vol = hit.get("vol", 0)
            if vol > 0:
                lines.append(f"Ликвидации (60с): <b>${vol:,.0f}</b>")
    
    return lines


# ─────────────────────────────────────────────────
#  Single Detector Alert (low-priority)
# ─────────────────────────────────────────────────

def format_early_detection_alert(
    hit: dict,
    price: float = 0.0,
    confidence: int = 0,
) -> tuple[str, str]:
    """
    Formats a single detector hit as early warning.
    Lower priority than confluence signals.
    
    Returns: (message_text, reply_markup_json)
    """
    symbol = hit.get("symbol", "UNKNOWN")
    h_type = hit.get("type", "UNKNOWN")
    
    # Map to user-facing header
    header_map = {
        "MICRO_MOMENTUM": "⚡ РАННЕЕ УСКОРЕНИЕ",
        "VELOCITY_SHOCK": "⚡ РАННЕЕ УСКОРЕНИЕ",
        "VOLUME_EXPLOSION": "🔥 АНОМАЛЬНЫЙ ОБЪЁМ",
        "ACCELERATION": "⚡ РАННЕЕ УСКОРЕНИЕ",
        "ORDERBOOK_IMBALANCE": "⚠️ РИСКОВАННОЕ ДВИЖЕНИЕ",
        "LIQUIDATION_CLUSTER": "⚠️ РИСКОВАННОЕ ДВИЖЕНИЕ",
    }
    header = header_map.get(h_type, "⚡ РАННЕЕ УСКОРЕНИЕ")
    
    # Description
    desc_map = {
        "MICRO_MOMENTUM": "Обнаружено раннее ускорение движения.\nЦена начинает набирать импульс.",
        "VELOCITY_SHOCK": "Обнаружен резкий рост количества сделок.\nАктивность рынка растёт.",
        "VOLUME_EXPLOSION": "Объём торгов резко вырос.\nВозможно начало сильного движения.",
        "ACCELERATION": "Обнаружено ускорение ценового движения.\nИмпульс набирает силу.",
        "ORDERBOOK_IMBALANCE": "Стакан показывает сильный перекос.\nВозможно давление крупного участника.",
        "LIQUIDATION_CLUSTER": "Зафиксирован каскад ликвидаций.\nВозможно резкое движение цены.",
    }
    description = desc_map.get(h_type, "Обнаружена аномальная активность.")
    
    # Build one metric line from hit data
    metric_line = ""
    if h_type == "MICRO_MOMENTUM":
        metric_line = f"Изменение цены (30с): <b>+{hit.get('change', 0):.1f}%</b>"
    elif h_type == "VELOCITY_SHOCK":
        metric_line = f"Активность сделок: <b>растёт</b>"
    elif h_type == "VOLUME_EXPLOSION":
        r = hit.get("ratio", 0)
        metric_line = f"Всплеск объёма: <b>{r:.1f}x</b>" if r else ""
    elif h_type == "ACCELERATION":
        accel = hit.get('accel', 0)
        sign = "+" if accel > 0 else ""
        metric_line = f"Ускорение цены: <b>{sign}{accel:.2f}%</b>"
    elif h_type == "ORDERBOOK_IMBALANCE":
        side = hit.get("side", "")
        direction = "покупатели" if side == "BIDS" else "продавцы"
        metric_line = f"Давление: <b>{direction}</b>"
    elif h_type == "LIQUIDATION_CLUSTER":
        vol = hit.get("vol", 0)
        metric_line = f"Ликвидации (60с): <b>${vol:,.0f}</b>" if vol else ""
    
    # Confidence Scaling (Fix 0.75 -> 75)
    if confidence == 0:
        confidence = _estimate_single_hit_confidence(hit)
    
    display_conf = confidence if confidence > 1.0 else int(confidence * 100)
    conf_label = _confidence_label(display_conf)
    price_str = _format_price(price) if price > 0 else "—"
    
    msg = (
        f"<b>{header}</b>\n"
        f"\n"
        f"<b>{symbol}</b>\n"
        f"Цена: <code>{price_str}</code>\n"
        f"\n"
        f"{description}\n"
        f"\n"
    )
    
    if metric_line:
        msg += f"{metric_line}\n"
    
    msg += (
        f"\n"
        f"Уверенность сигнала: <b>{display_conf} / 100</b>\n"
        f"<i>{conf_label}</i>"
    )
    
    buttons = _chart_buttons(symbol)
    return msg, buttons


def _estimate_single_hit_confidence(hit: dict) -> int:
    """Estimate confidence from a single detector hit."""
    h_type = hit.get("type", "")
    ratio = hit.get("ratio", 1.0)
    
    # Base confidence for single factor = 40-65
    base = 45
    
    if h_type == "VELOCITY_SHOCK":
        base += min(15, int(ratio * 2))
    elif h_type == "VOLUME_EXPLOSION":
        base += min(15, int(ratio * 2))
    elif h_type == "MICRO_MOMENTUM":
        change = hit.get("change", 0)
        base += min(15, int(change * 5))
    elif h_type == "ACCELERATION":
        base += min(10, int(hit.get("accel", 0) * 5))
    elif h_type == "ORDERBOOK_IMBALANCE":
        base += min(10, int(ratio))
    elif h_type == "LIQUIDATION_CLUSTER":
        base += 10
    
    return min(65, base)  # Cap at 65 for single factor


# ─────────────────────────────────────────────────
#  Level Break / Proximity Alerts
# ─────────────────────────────────────────────────

def _format_proximity_v4(event: AlertEvent) -> tuple[str, Optional[str]]:
    """Level proximity alert — compact version."""
    p = event.payload
    symbol = event.symbol or "UNKNOWN"
    
    level_price = float(p.get("level_price", 0) or 0)
    distance_pct = float(p.get("distance_pct", 0) or 0)
    side = str(p.get("side", "")).upper()
    touches = int(p.get("touches", 0) or 0)
    score = int(p.get("score", 0) or 0)
    
    side_text = "поддержки" if "SUPPORT" in side else "сопротивления"
    
    price_str = _format_price(level_price)
    
    msg = (
        f"<b>🧲 РЕАКЦИЯ ОТ УРОВНЯ</b>\n"
        f"\n"
        f"<b>{symbol}</b>\n"
        f"Цена подошла к сильному уровню {side_text}.\n"
    )
    
    # Activity context
    intensity = float(p.get("trade_intensity", 1.0) or 1.0)
    if intensity > 1.8:
        msg += "Появляется давление покупателей.\n"
    elif intensity > 1.2:
        msg += "Активность сделок: растёт.\n"
    else:
        msg += "Активность сделок: нормальная.\n"
    
    atr_pct = float(p.get("atr_pct", 0) or 0)
    confluence = float(p.get("confluence_score", 0) or 0)
    
    msg += (
        f"\n"
        f"Уровень: <code>{price_str}</code>\n"
        f"Дистанция: <b>{distance_pct:.2f}%</b>\n"
    )
    
    if atr_pct > 0:
        msg += f"ATR (24h): <b>{atr_pct:.1f}%</b>\n"
        
    if confluence > 1:
        msg += f"Конфлюэнс: <b>{confluence:.1f}</b>\n"

    if touches > 1:
        msg += f"Касаний: <b>{touches}</b>\n"
    
    # Confidence
    confidence = min(100, max(40, score))
    msg += (
        f"\n"
        f"Уверенность: <b>{confidence}/100</b>\n"
        f"<i>{_confidence_label(confidence)}</i>"
    )
    
    buttons = _chart_buttons(symbol)
    return msg, buttons


def _format_trigger_v4(event: AlertEvent) -> tuple[str, Optional[str]]:
    """Level break / trigger fired alert."""
    p = event.payload
    symbol = event.symbol or "UNKNOWN"
    
    level_price = float(p.get("level_price", 0) or 0)
    score = int(p.get("score", 0) or 0)
    side = str(p.get("side", "")).upper()
    raw_pattern = str(p.get("pattern", p.get("structure_type", ""))).upper()
    volume_ratio = p.get("volume_ratio") or p.get("volume_spike")
    
    # Determine alert type by pattern
    if raw_pattern in ("BREAK", "MOMENTUM_BREAKOUT"):
        header = "💥 ПРОБОЙ УРОВНЯ"
        if "SUPPORT" in side or "SHORT" in side or "SELL" in side:
            desc = "Цена пробивает уровень поддержки.\nДавление продавцов усиливается."
        else:
            desc = "Цена пробивает сильный уровень сопротивления.\nРастёт объём и давление покупателей."
    elif raw_pattern in ("SQUEEZE",):
        header = "🚀 СИЛЬНЫЙ ИМПУЛЬС"
        desc = "После длительного сжатия цена начинает\nрезкое направленное движение."
    elif raw_pattern in ("RETEST",):
        header = "🧲 РЕАКЦИЯ ОТ УРОВНЯ"
        desc = "Цена ретестирует пробитый уровень.\nВозможно продолжение движения."
    elif raw_pattern in ("SWEEP",):
        header = "⚠️ РИСКОВАННОЕ ДВИЖЕНИЕ"
        desc = "Цена сняла ликвидность за уровнем.\nВозможен разворот или ложный пробой."
    elif raw_pattern in ("VOLUME_SPIKE",):
        header = "🔥 АНОМАЛЬНЫЙ ОБЪЁМ"
        desc = "Зафиксирован аномальный всплеск объёма\nвблизи ключевого уровня."
    elif "SWARM_SYNC" in raw_pattern:
        header = "🌊 СИНХРОНИЗАЦИЯ РОЯ"
        desc = "Все таймфреймы бьют в одном направлении.\nАгрессивный залив объема (x3 Risk)."
    elif "SWARM_CONFLICT" in raw_pattern:
        header = "⚡ СКАЛЬП УЯЗВИМОСТИ"
        desc = "Фреймы конфликтуют. Идеальный момент\nдля контр-трендового скальпинга (Tight Stop)."
    else:
        header = "🚀 СИЛЬНЫЙ ИМПУЛЬС"
        desc = "Обнаружено сильное движение\nвблизи ключевого уровня."
    
    price_str = _format_price(level_price)
    
    msg = (
        f"<b>{header}</b>\n"
        f"\n"
        f"<b>{symbol}</b>\n"
        f"Цена: <code>{price_str}</code>\n"
        f"\n"
        f"{desc}\n"
        f"\n"
        f"Уровень: <code>{price_str}</code>\n"
    )
    
    if volume_ratio:
        msg += f"Объём: <b>{float(volume_ratio):.1f}x</b>\n"
    
    atr_pct = float(p.get("atr_pct", 0) or 0)
    if atr_pct > 0:
        msg += f"ATR: <b>{atr_pct:.1f}%</b>\n"

    squeeze_bars = p.get("squeeze_bars")
    if squeeze_bars:
        msg += f"Сжатие: <b>{squeeze_bars} свечей</b>\n"
    
    confidence = min(100, max(50, score))
    conf_label = _confidence_label(confidence)
    
    msg += (
        f"\n"
        f"Уверенность: <b>{confidence}/100</b>\n"
        f"<i>{conf_label}</i>"
    )
    
    buttons = _chart_buttons(symbol)
    return msg, buttons


# ─────────────────────────────────────────────────
#  Info Alerts (armed, radar, system)
# ─────────────────────────────────────────────────

def _format_armed_v4(event: AlertEvent) -> tuple[str, Optional[str]]:
    """Level armed — minimal notification."""
    p = event.payload
    symbol = event.symbol or "UNKNOWN"
    price = _format_price(float(p.get("level_price", 0) or 0))
    side = str(p.get("side", "")).upper()
    
    side_text = "поддержки" if "SUPPORT" in side else "сопротивления"
    
    source_raw = str(p.get("source", ""))
    source_map = {
        "KDE": "плотность", "SWING": "разворот",
        "ROUND_NUMBER": "круглое число", "WEEK_EXTREME": "экстремум недели",
    }
    source_parts = source_raw.replace("+", " + ").split(" + ")
    # Translate and deduplicate sources for readability
    translated = [source_map.get(s.strip().split(":")[0], s.strip()) for s in source_parts]
    # Count duplicates: ["разворот","разворот","плотность"] → "разворот ×2 + плотность"
    from collections import Counter
    counts = Counter(translated)
    deduped_parts = []
    for name, cnt in counts.items():
        deduped_parts.append(f"{name} ×{cnt}" if cnt > 1 else name)
    source_human = " + ".join(deduped_parts)
    
    msg = (
        f"<b>💎 НОВЫЙ УРОВЕНЬ</b>\n"
        f"\n"
        f"<b>{symbol}</b> — уровень {side_text}\n"
        f"Цена: <code>{price}</code>\n"
        f"Источник: {source_human}\n"
        f"\n"
        f"<i>Gerald Sniper 🎯 На мушке</i>"
    )
    
    return msg, None


def _format_radar_v4(event: AlertEvent) -> str:
    """Radar digest — top symbols."""
    p = event.payload
    results = p.get("top_results", [])
    
    msg = f"<b>📡 РАДАР: ГОРЯЧИЕ МОНЕТЫ</b>\n━━━━━━━━━━━━━━━━━━━━━\n"
    for i, r in enumerate(results[:7], 1):
        sym = r.get("symbol", "?")
        score = r.get("score", 0)
        atr = r.get("atr_pct", 0)
        
        if atr > 3.0:
            icon = "🌶"
        elif atr > 1.5:
            icon = "⚡"
        else:
            icon = " "
        
        msg += f"{i}. <b>{sym}</b>  │  {score:.0f} pts  │  {atr:.1f}% {icon}\n"
    
    msg += f"━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"<i>Gerald Sniper 🎯 Радар</i>"
    return msg


def _format_system_v4(event: AlertEvent, degraded: bool) -> str:
    """System status alert."""
    p = event.payload
    now = datetime.now().strftime("%H:%M:%S")
    
    if degraded:
        header = "⚠️ <b>СВЯЗЬ НЕСТАБИЛЬНА</b>"
    else:
        header = "✅ <b>СВЯЗЬ ВОССТАНОВЛЕНА</b>"
    
    msg_detail = p.get("msg", "Нет данных")
    
    msg = (
        f"{header}\n"
        f"{msg_detail}\n"
        f"Время: {now}\n"
        f"<i>Gerald Sniper 🎯</i>"
    )
    return msg


# ─────────────────────────────────────────────────
#  Momentum Breakout (radar-based)
# ─────────────────────────────────────────────────

def format_momentum_alert(event: AlertEvent) -> tuple[str, str]:
    """Radar momentum breakout alert."""
    p = event.payload
    symbol = event.symbol or "UNKNOWN"
    price = float(p.get("level_price", 0) or 0)
    vol_spike = float(p.get("volume_spike", 0) or 0)
    momentum = float(p.get("momentum", 0) or 0)
    velocity = float(p.get("velocity", 0) or 0)
    score = int(p.get("score", 0) or 0)
    
    price_str = _format_price(price)
    
    # Signal Confidence from score
    display_conf = score if score > 1 else int(score)
    conf_label = _confidence_label(display_conf)
    
    msg = (
        f"<b>🚀 СИЛЬНЫЙ ИМПУЛЬС</b>\n"
        f"\n"
        f"<b>{symbol}</b>\n"
        f"Цена: <code>{price_str}</code>\n"
        f"\n"
        f"На рынке зафиксировано резкое ускорение.\n"
        f"Количество сделок и объём резко выросли.\n"
        f"\n"
    )
    
    if momentum > 0:
        msg += f"Изменение цены (5м): <b>+{momentum:.1f}%</b>\n"
    if velocity > 0:
        msg += f"Ускорение сделок: <b>{velocity:.1f}x</b>\n"
    if vol_spike > 0:
        msg += f"Всплеск объёма: <b>{vol_spike:.1f}x</b>\n"
    
    msg += (
        f"\n"
        f"Уверенность сигнала: <b>{display_conf} / 100</b>\n"
        f"<i>{conf_label}</i>"
    )
    
    buttons = _chart_buttons(symbol)
    return msg, buttons

# ─────────────────────────────────────────────────
#  Execution Alerts (v4 RU)
# ─────────────────────────────────────────────────

def format_approved_alert(signal) -> str:
    """Formats the 'SIGNAL APPROVED' alert in Russian."""
    dir_ru = "ЛОНГ 📈" if signal.direction == "LONG" else "ШОРТ 📉"
    price_str = _format_price(signal.entry_price or 0.0)
    
    # Translate detectors to Russian
    factors_ru = _translate_detectors(signal.detectors)
    
    # Translate BTC trend to Russian
    btc_ru = _translate_btc_trend(signal.btc_trend_1h)
    
    # Confidence display
    conf = signal.confidence_score or 0
    display_conf = int(conf * 100) if conf <= 1.0 else int(conf)
    conf_label = _confidence_label(display_conf)
    
    msg = (
        f"✅ <b>СИГНАЛ ОДОБРЕН</b>\n\n"
        f"Монета: <b>{signal.symbol}</b>\n"
        f"Направление: <b>{dir_ru}</b>\n"
        f"Цена входа: <code>{price_str}</code>\n"
        f"Причина: {factors_ru}\n"
        f"Дист. до уровня: <b>{signal.level_distance_pct or 0.0:.2f}%</b>\n"
        f"Рынок: <b>{btc_ru}</b>\n"
        f"Уверенность: <b>{display_conf}/100</b>\n"
        f"<i>{conf_label}</i>\n\n"
        f"<i>Gerald Sniper 🎯 Подготовка к входу</i>"
    )
    return msg

def format_execution_alert(signal, alert_id: str, ghost: bool = False) -> str:
    """Formats the 'POSITION OPENED' alert in Russian (Pilot Spec)."""
    dir_emoji = "✅" if signal.direction == "LONG" else "🔻"
    price_str = _format_price(signal.entry_price or 0.0)
    
    msg = (
        f"<b>Вход:</b> {dir_emoji} Позиция по <b>#{signal.symbol}</b> открыта по цене <code>{price_str}</code>.\n"
        f"<i>Gerald Sniper v5.2</i>"
    )
    return msg

def format_tp_alert(symbol: str, profit_pct: float, profit_usd: float) -> str:
    """Formats 'Take Profit' alert."""
    return (
        f"💰 <b>Профит зафиксирован!</b> по <b>#{symbol}</b>\n"
        f"Результат: <b>+{profit_pct:.2f}% (${profit_usd:.2f})</b>\n"
        f"<i>Gerald Sniper v5.2</i>"
    )

def format_sl_alert(symbol: str, loss_pct: float, loss_usd: float) -> str:
    """Formats 'Stop Loss' alert."""
    return (
        f"🛑 <b>Стоп-лосс сработал</b> по <b>#{symbol}</b>\n"
        f"Результат: <b>-{abs(loss_pct):.2f}% (-${abs(loss_usd):.2f})</b>\n"
        f"<i>Gerald Sniper v5.2</i>"
    )

def format_rejection_alert(symbol: str, reason: str) -> str:
    """Formats 'Order Rejected' alert."""
    return (
        f"⚠️ <b>Вход по #{symbol} отменен:</b>\n"
        f"Причина: {reason}\n"
        f"<i>Gerald Sniper v5.2</i>"
    )



# ─────────────────────────────────────────────────
#  Translation helpers (v5.1)
# ─────────────────────────────────────────────────

def _translate_detectors(detectors: list) -> str:
    """Translates detector names to human-readable Russian."""
    detector_map = {
        "VELOCITY_SHOCK": "всплеск сделок",
        "VOLUME_EXPLOSION": "взрыв объёма",
        "MICRO_MOMENTUM": "ранний импульс",
        "ACCELERATION": "ускорение цены",
        "ORDERBOOK_IMBALANCE": "перекос стакана",
        "LIQUIDATION_CLUSTER": "каскад ликвидаций",
        "SQUEEZE_FIRE": "выход из сжатия",
        "COMPRESSION": "компрессия",
        "BREAK": "пробой уровня",
        "RETEST": "ретест уровня",
        "SWEEP": "снятие ликвидности",
        "swarm": "hive mind (мульти-таймфрейм)",
    }
    
    translated = []
    for d in detectors:
        if d.startswith("SWARM_SYNC"):
            translated.append("синхронизация роя")
        elif d.startswith("SWARM_CONFLICT"):
            translated.append("излом таймфреймов")
        else:
            translated.append(detector_map.get(d, d))
            
    return ", ".join(translated)


def _translate_btc_trend(trend: str) -> str:
    """Translates BTC trend string to Russian."""
    trend_map = {
        "STRONG_UP": "BTC сильно растёт 📈📈",
        "UP": "BTC растёт 📈",
        "FLAT": "BTC нейтрально ➡️",
        "DOWN": "BTC падает 📉",
        "STRONG_DOWN": "BTC сильно падает 📉📉",
    }
    return trend_map.get(str(trend).upper(), f"BTC: {trend}")
