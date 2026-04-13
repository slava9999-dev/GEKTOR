# src/domain/macro_regime.py
from dataclasses import dataclass
from typing import Dict, Optional
from loguru import logger

@dataclass(slots=True)
class MacroHealth:
    is_panic: bool = False
    btc_vpin: float = 0.0
    btc_delta_pct: float = 0.0
    reason: str = ""

class MacroRegimeFilter:
    """[GEKTOR APEX] MARKET BASELINE (Поводырь)."""
    def __init__(self, panic_vpin_threshold: float = 0.85, panic_delta_threshold: float = -0.02):
        self.panic_vpin_threshold = panic_vpin_threshold
        self.panic_delta_threshold = panic_delta_threshold
        self.current_health = MacroHealth()
        self._last_btc_price: Optional[float] = None

    def update_baseline(self, symbol: str, vpin: float, price: float):
        """Обновление состояния глобального рынка по Поводырю (BTC)."""
        if symbol != "BTCUSDT": return

        # 1. Расчет дельты (на барсетку)
        delta_pct = 0.0
        if self._last_btc_price:
            delta_pct = (price - self._last_btc_price) / self._last_btc_price
        self._last_btc_price = price

        # 2. Детекция Паники
        is_panic = False
        reason = ""
        
        if vpin > self.panic_vpin_threshold:
            is_panic = True
            reason = f"BTC TOXIC FLOW (VPIN: {vpin:.4f})"
        elif delta_pct < self.panic_delta_threshold:
            is_panic = True
            reason = f"BTC FLASH CRASH (Delta: {delta_pct:.2%})"
        
        self.current_health = MacroHealth(
            is_panic=is_panic,
            btc_vpin=vpin,
            btc_delta_pct=delta_pct,
            reason=reason
        )
        
        if is_panic:
            logger.warning(f"🚨 [MACRO] Market PANIC detected! {reason}. Altcoin signals will be MUTED.")
        elif self.current_health.is_panic:
             logger.success("🟢 [MACRO] Market stabilized. Resuming normal radar operation.")

    def should_mute(self, symbol: str) -> bool:
        """Подавлять ли сигнал для данного актива."""
        if symbol == "BTCUSDT": return False # Поводырь никогда не мутится
        return self.current_health.is_panic
