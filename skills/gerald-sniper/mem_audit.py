import os
import psutil
import gc
import objgraph
from loguru import logger

def audit_memory():
    """
    [Audit 25.19] Deep Memory Profiler (Task 18.4).
    Identifies top-N objects consuming RAM in the Sniper process.
    """
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    rss_mb = mem_info.rss / 1024 / 1024
    
    logger.info(f"📊 [MemAudit] Current Process RSS: {rss_mb:.2f} MB")
    
    # Trigger full GC before audit
    gc.collect()
    
    logger.info("🕵️ [MemAudit] Object Statistics (Top 10):")
    objgraph.show_most_common_types(limit=10)
    
    # Catching common leaks
    leaks = objgraph.find_backref_chain(
        objgraph.by_type('dict')[0],
        objgraph.is_proper_module
    ) if objgraph.by_type('dict') else None
    
    if leaks:
        logger.warning(f"⚠️ [MemAudit] Potential Leak detected in Dict chain.")

if __name__ == "__main__":
    audit_memory()
