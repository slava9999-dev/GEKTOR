from dataclasses import dataclass, field
import json
import os
import time
from typing import List, Optional, Dict
from loguru import logger

BLACKLIST_PATH = "proxy_blacklist.json"

@dataclass
class ProxyNode:
    """[GEKTOR v21.58] Institutional Proxy Node with dynamic scoring."""
    url: str
    last_rtt: float = 0.0
    is_active: bool = True
    fails: int = 0
    gaps: List[float] = field(default_factory=list)  # Timestamps of gap recoveries in the last 60s

    def report_gap(self):
        """[GEKTOR v21.58] Detects 'Zombie Proxy' via Gap frequency."""
        now = time.time()
        self.gaps.append(now)
        # Clean up old gaps (> 60s)
        self.gaps = [g for g in self.gaps if now - g < 60]
        
        if len(self.gaps) > 5:
            self.is_active = False
            logger.critical(f"🧟 [ProxyProvider] Node detected as ZOMBIE (High Packet Loss/Gap Frequency): {self.url}")

class GlobalCircuitBreaker:
    """[GEKTOR v21.59] Protects against 'Cloud Provider Blackout' and cascade failures."""
    def __init__(self, failure_threshold: float = 0.8, window: int = 45):
        self.failure_threshold = failure_threshold
        self.window = window
        self._attempts: List[tuple] = [] # (timestamp, success)
        self._is_open = False
        self._open_until = 0.0

    def record_attempt(self, success: bool):
        now = time.time()
        self._attempts.append((now, success))
        self._attempts = [a for a in self._attempts if now - a[0] < self.window]
        
        if len(self._attempts) < 10: return # Require significant sample
        
        fails = sum(1 for a in self._attempts if not a[1])
        fail_rate = fails / len(self._attempts)
        
        if fail_rate >= self.failure_threshold:
            logger.critical(f"🚨 [GlobalCB] CIRCUIT OPENED (Fail Rate: {fail_rate:.1%}). Blocking all traffic for 60s.")
            self._is_open = True
            self._open_until = now + 60.0

    @property
    def can_attempt(self) -> bool:
        if not self._is_open: return True
        if time.time() > self._open_until:
            logger.info("♻️ [GlobalCB] Circuit reset (Half-Open). Testing recovery...")
            self._is_open = False
            return True
        return False

class ProfessionalProxyProvider:
    """[GEKTOR v21.63] Institutional Proxy Pool with Reputation Score & Exponential Ban."""
    def __init__(self, raw_list: List[str]):
        self.blacklist = self._load_blacklist()
        now = time.time()
        self.nodes = []
        for u in raw_list:
            url = u.strip()
            if not url: continue
            
            entry = self.blacklist.get(url, {"until": 0, "count": 0})
            # [GEKTOR v21.63.1] Migration: handle legacy flat-timestamp format
            if isinstance(entry, (int, float)):
                entry = {"until": entry, "count": 1}
                self.blacklist[url] = entry # Update local in-memory representation
                
            ban_until = entry.get("until", 0)
            is_banned = ban_until > now
            
            node = ProxyNode(url=url, is_active=not is_banned)
            if is_banned:
                logger.warning(f"🚫 [ProxyProvider] Node BANNED (Reputation Rank: {entry.get('count',0)}, Expires in {int(ban_until-now)}s): {url}")
            self.nodes.append(node)
            
        self.cb = GlobalCircuitBreaker()
        if not self.nodes:
            logger.warning("⚠️ [ProxyProvider] EMPTY pool. Critical vulnerability detected.")

    def _load_blacklist(self) -> Dict[str, dict]:
        if os.path.exists(BLACKLIST_PATH):
            try:
                with open(BLACKLIST_PATH, 'r') as f:
                    return json.load(f)
            except Exception: return {}
        return {}

    def _save_blacklist(self):
        try:
            with open(BLACKLIST_PATH, 'w') as f:
                json.dump(self.blacklist, f)
        except Exception as e:
            logger.error(f"Failed to save blacklist: {e}")

    def report_zombie_persistent(self, url: str):
        """[GEKTOR v21.63] Exponential Reputation Penalty."""
        entry = self.blacklist.get(url, {"until": 0, "count": 0})
        count = entry.get("count", 0)
        
        # [GEKTOR v21.63.1] Exponential backoff: 1h, 2h, 4h... capped at 24h
        max_ban = 86400  # 24 hours
        ban_duration = min(max_ban, 3600 * (2 ** count))
        expires = time.time() + ban_duration
        
        self.blacklist[url] = {
            "until": expires,
            "count": count + 1
        }
        self._save_blacklist()
        
        for node in self.nodes:
            if node.url == url:
                node.is_active = False
                break
        logger.critical(f"🚫 [ProxyProvider] Node BANNED for {ban_duration/3600:.1f}h (Ban #{count+1}): {url}")

    @property
    def total_nodes(self) -> int:
        return len(self.nodes)

    def get_best_node(self, exclude: Optional[List[str]] = None) -> Optional[str]:
        """Returns the healthiest node, subject to Circuit Breaker and exclusion list."""
        if not self.cb.can_attempt:
            return None

        exclude = exclude or []
        valid_nodes = [n for n in self.nodes if n.is_active and n.url not in exclude]
        
        if not valid_nodes:
            # If we requested a node but NONE are active/excluded, record a failure for the CB
            self.cb.record_attempt(False)
            return None
        
        return min(valid_nodes, key=lambda x: x.last_rtt).url

    def update_rtt(self, url: str, rtt: float):
        """Standard update with CB success recording."""
        for node in self.nodes:
            if node.url == url:
                node.last_rtt = rtt
                self.cb.record_attempt(True)
                break

    def report_failure(self, url: str):
        """Hard failure reporting with node quarantine and CB impact."""
        self.cb.record_attempt(False)
        for node in self.nodes:
            if node.url == url:
                node.fails += 1
                if node.fails > 3:
                    node.is_active = False
                    logger.critical(f"🚫 [ProxyProvider] Node QUARANTINED (Max Fails): {url}")
                break

    def report_gap(self, url: str):
        """Microstructural integrity violation (Zombie Proxy detection)."""
        for node in self.nodes:
            if node.url == url:
                node.report_gap()
                if not node.is_active:
                    self.report_zombie_persistent(url)
                break
