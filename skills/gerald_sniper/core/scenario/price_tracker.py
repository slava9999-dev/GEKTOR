from collections import deque
import statistics
from loguru import logger

class PriceTracker:
    """
    [P1] Median Price Benchmark (State Alignment)
    Maintains a rolling window of recent prices to provide a stable reference_price.
    Prevents the OutlierFilter from freezing during sudden, legitimate market dumps.
    """
    def __init__(self, window_size: int = 20):
        self._price_windows = {}  # symbol -> deque
        self._window_size = window_size

    def add_price(self, symbol: str, price: float):
        """Adds a new price tick to the rolling window."""
        if symbol not in self._price_windows:
            self._price_windows[symbol] = deque(maxlen=self._window_size)
        
        self._price_windows[symbol].append(price)

    def get_reference_price(self, symbol: str) -> float:
        """
        Calculates the Median reference price from the window.
        If the window is empty or has only 1 price, returns that price.
        """
        window = self._price_windows.get(symbol)
        if not window:
            return 0.0
        
        if len(window) < 5: # Use last price if window is too small
            return window[-1]
            
        try:
            return statistics.median(window)
        except Exception as e:
            logger.error(f"❌ [PriceTracker] Median calculation error for {symbol}: {e}")
            return window[-1]

    def reset(self, symbol: str):
        """Resets the window for a specific symbol."""
        if symbol in self._price_windows:
            self._price_windows[symbol].clear()
