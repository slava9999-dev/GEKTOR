import numpy as np
import hashlib
from typing import List
from core.levels.models import HTFLevel, LevelCluster, LevelSide


def cluster_levels(
    levels: List[HTFLevel],
    tolerance_pct: float = 0.005
) -> List[LevelCluster]:
    """
    Joins levels from various timeframes into multi-factor clusters.
    Calculates Digash Confluence score.
    """
    if not levels:
        return []

    # Separate by side
    supports    = [l for l in levels if l.side == LevelSide.SUPPORT]
    resistances = [l for l in levels if l.side == LevelSide.RESISTANCE]

    clusters = []
    for group in [supports, resistances]:
        clusters.extend(_cluster_group(group, tolerance_pct))

    # Sort: highest confluence and total strength first
    return sorted(clusters, key=lambda c: (-c.confluence_score, -c.strength))


def _cluster_group(levels: List[HTFLevel], tolerance: float) -> List[LevelCluster]:
    if not levels:
        return []

    # Filter out invalid levels (price <= 0)
    valid_levels = [l for l in levels if l.price > 0]
    if not valid_levels:
        return []

    # Sort levels by price to find adjacent groups
    sorted_l = sorted(valid_levels, key=lambda l: l.price)
    groups: List[List[HTFLevel]] = [[sorted_l[0]]]

    for lvl in sorted_l[1:]:
        ref = groups[-1][0].price
        # Cluster levels if they are within tolerance
        # ref is guaranteed to be > 0 here due to filter above
        if abs(lvl.price - ref) / ref < tolerance:
            groups[-1].append(lvl)
        else:
            groups.append([lvl])

    result = []
    for group in groups:
        prices = [l.price for l in group]
        median_price = float(np.median(prices))
        touches = sum(l.touches for l in group)
        # Score normalization to prevent inflation
        # Cap individual strengths to max 80 before averaging
        raw_strength = sum(min(l.strength, 80.0) for l in group)
        avg_strength = raw_strength / len(group)
        # Reward confluence: +15% score per additional confirming source
        strength = min(100.0, avg_strength * (1.0 + 0.15 * (len(group) - 1)))
        
        sources = [f"{l.source}:{l.timeframe}" for l in group]
        
        # Confluence = how many distinct TFs confirmed this zone
        confluence = len(set(l.timeframe for l in group))
        # Relative depth of the zone
        tol_pct = (max(prices) - min(prices)) / median_price if median_price > 0 else tolerance

        # Cluster ID stable for given symbol and side/price
        key = f"{group[0].symbol}|{group[0].side}|{round(median_price, 6)}"
        cluster_id = hashlib.blake2s(key.encode(), digest_size=8).hexdigest()

        result.append(LevelCluster(
            symbol=group[0].symbol,
            price=median_price,
            side=group[0].side,
            sources=sources,
            touches_total=touches,
            confluence_score=confluence,
            strength=strength,
            tolerance_pct=max(tol_pct, 0.002), # min zone width
            level_id=cluster_id
        ))

    return result
