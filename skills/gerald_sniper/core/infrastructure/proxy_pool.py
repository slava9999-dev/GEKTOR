# core/infrastructure/proxy_pool.py
import random
from typing import List, Set
from loguru import logger

class ProxyPool:
    """
    [GEKTOR v21.11] Resilient Proxy Rotator.
    Manages a set of SOCKS5/HTTPS addresses to bypass regional blocking.
    Implements a temporary quarantine for failing nodes.
    """
    def __init__(self, proxies: List[str]):
        self.proxies = proxies
        self.quarantine: Set[str] = set()

    def get_proxy(self) -> str:
        """Fetch a healthy proxy. Resets quarantine if all nodes fail."""
        available = [p for p in self.proxies if p not in self.quarantine]
        
        if not available:
            logger.warning("💠 [ProxyPool] ALL PROXIES QUARANTINED. Resetting pool.")
            self.quarantine.clear()
            available = self.proxies

        return random.choice(available)

    def mark_bad(self, proxy: str):
        """Put a failing proxy in quarantine."""
        if proxy in self.proxies:
            logger.error(f"💠 [ProxyPool] QUARANTINE: {proxy} is unresponsive.")
            self.quarantine.add(proxy)
