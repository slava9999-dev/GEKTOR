from typing import List, Dict, Set, Tuple
from loguru import logger

import time

# Global state for persistence between cycles
_addition_times: Dict[str, float] = {}

def select_watchlist(scored_results: List[Dict], current_watchlist: Set[str], max_size: int = 8) -> List[str]:
    """
    Gerald v5.2 Watchlist Selection with Hysteresis & TTL (T-02):
    - ENTRY_THRESHOLD = 65
    - EXIT_THRESHOLD  = 45
    - MIN_TTL_SEC     = 600 (10 minutes)
    - MAX_REMOVALS    = 3 per cycle
    """
    global _addition_times
    now = time.time()
    scored_dict = {r['symbol']: r['score'] for r in scored_results}
    
    # 1. Maintain timestamps for current symbols
    for sym in current_watchlist:
        if sym not in _addition_times:
            _addition_times[sym] = now # Fallback if missing

    # 2. Determine Keepers based on Hysteresis and TTL
    keepers = []
    to_remove_candidates = []
    
    for symbol in current_watchlist:
        score = scored_dict.get(symbol, 0)
        age = now - _addition_times.get(symbol, now)
        
        should_exit = score < 45
        ttl_expired = age >= 1200  # IMP-07: Extended from 600s
        
        if not (should_exit and ttl_expired):
            keepers.append(symbol)
            if should_exit: # Saved by TTL
                logger.info(f"🛡️ [Hysteresis] Saved {symbol} from removal (Score: {score}, Age: {age:.0f}s < TTL: 1200s)")
        else:
            to_remove_candidates.append(symbol)
            
    # 3. Handle maximum churn (Max 3 removals)
    CHURN_PROTECTION_MAX_SEC = 1200
    if len(to_remove_candidates) > 3:
        # Filter candidates: only those within protection window can be 'saved'
        protected_candidates = []
        for sym in to_remove_candidates:
            age = now - _addition_times.get(sym, now)
            if age < CHURN_PROTECTION_MAX_SEC:
                protected_candidates.append(sym)
            else:
                logger.info(f"⏰ [Hysteresis] Churn protection expired for {sym} (Age: {age:.0f}s)")

        # Sort protected by score descending
        protected_candidates.sort(key=lambda s: scored_dict.get(s, 0), reverse=True)
        
        # We can save up to N symbols to limit churn to 3 removals, 
        # but only if they are within the protection window.
        num_can_be_saved = max(0, len(to_remove_candidates) - 3)
        saved_from_churn = protected_candidates[:num_can_be_saved]
        
        for sym in saved_from_churn:
            keepers.append(sym)
            logger.info(f"🛡️ [Hysteresis] Saved {sym} from removal to limit churn (Score: {scored_dict.get(sym,0)})")
        
        # Final set of symbols to remove are those not in keepers
        # (This is handled implicitly by not adding them to final_watchlist)

    # 4. Fill remaining slots with new candidates (ENTRY_THRESHOLD = 65)
    candidates = sorted(
        [r for r in scored_results if r['symbol'] not in current_watchlist and r['score'] >= 65],
        key=lambda x: x['score'], reverse=True
    )
    
    room = max_size - len(keepers)
    final_watchlist = keepers
    if room > 0:
        new_picks = candidates[:room]
        for p in new_picks:
            sym = p['symbol']
            final_watchlist.append(sym)
            _addition_times[sym] = now
            logger.info(f"📡 Radar Decision | {sym} | Added (Score: {p['score']})")
            
    # 5. Rule 4 Fallback
    if not final_watchlist and scored_results:
        logger.warning("⚠️ Radar v5.2: Watchlist empty. Falling back to Top 5.")
        for r in scored_results[:5]:
            sym = r['symbol']
            final_watchlist.append(sym)
            _addition_times[sym] = now

    # Cleanup _addition_times for symbols no longer in final_watchlist
    final_set = set(final_watchlist)
    _addition_times = {s: t for s, t in _addition_times.items() if s in final_set}

    return final_watchlist[:max_size]

def calculate_watchlist_diff(old_watchlist: List[str], new_watchlist: List[str]) -> Tuple[List[str], List[str], List[str]]:
    """
    added = new - old
    removed = old - new
    kept = intersection
    """
    old_set = set(old_watchlist)
    new_set = set(new_watchlist)
    
    added = list(new_set - old_set)
    removed = list(old_set - new_set)
    kept = list(new_set & old_set)
    
    return added, removed, kept
