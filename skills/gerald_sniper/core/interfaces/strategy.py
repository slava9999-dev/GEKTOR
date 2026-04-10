from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any

class IAlphaStrategy(ABC):
    """
    [GEKTOR v12.5] Public Interface Contract.
    Defining the "Protocol" for Alpha analysis without revealing its implementation.
    The Public Core only knows *how to call* the brain, but not *what's inside*.
    """
    
    @abstractmethod
    def analyze_anomaly(self, symbol: str, data_snapshot: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Input: Data snapshot (Dollar Bars, L2 deltas, CVD).
        Output: Signal payload (Z-Score, Direction, etc.) or None.
        """
        pass

    @abstractmethod
    def get_version(self) -> str:
        """Returns the strategy version for tracking."""
        pass
