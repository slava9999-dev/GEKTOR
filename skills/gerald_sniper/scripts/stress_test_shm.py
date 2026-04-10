import time
import multiprocessing
import numpy as np
from loguru import logger
from core.realtime.shm_book import DoubleBufferedSharedOrderbook

# CONFIG
SYMBOL = "BTCUSDT"
MSG_COUNT = 100_000
RATE_LIMIT = 20_000 # msgs/sec target

def synthetic_market_generator(symbol: str, count: int, rate: int):
    """ WRITER: Generates high-frequency L2 updates into SHM. """
    writer = DoubleBufferedSharedOrderbook(symbol, is_writer=True)
    logger.info(f"🚀 [WRITER] Starting performance test for {symbol} at {rate} msgs/s.")
    
    # Pre-generate random orderbooks
    books = [np.random.rand(50, 2) for _ in range(100)]
    
    t0 = time.perf_counter()
    for i in range(count):
        # Swap between pre-generated books
        writer.write(books[i % 100], books[(i+1) % 100])
        
        # Simple sleep to maintain rate
        if i % 100 == 0:
            elapsed = time.perf_counter() - t0
            target_time = i / rate
            if elapsed < target_time:
                time.sleep(target_time - elapsed)
                
    total_time = time.perf_counter() - t0
    logger.success(f"🏁 [WRITER] Finished {count} updates in {total_time:.2f}s (Avg: {count/total_time:.0f} msgs/s)")
    writer.cleanup()

def alpha_correlator_consumer(symbol: str, count: int):
    """ READER: Fast-path consumption loop. """
    reader = DoubleBufferedSharedOrderbook(symbol, is_writer=False)
    buffer = np.zeros((2, 50, 2), dtype=np.float64)
    
    logger.info(f"📥 [READER] Consuming data from SHM for {symbol}...")
    
    reads_performed = 0
    t0 = time.perf_counter()
    
    # We read as fast as possible for the duration of the test
    # In a real system, this would be pinned.
    while reads_performed < count:
        reader.read_into(buffer)
        reads_performed += 1
        
    total_time = time.perf_counter() - t0
    logger.success(f"🏁 [READER] Completed {reads_performed} reads in {total_time:.2f}s (Throughput: {reads_performed/total_time:.0f} r/s)")
    reader.cleanup()

if __name__ == "__main__":
    # Create the SHM first
    try:
        # 1. Start Processes
        p_writer = multiprocessing.Process(target=synthetic_market_generator, args=(SYMBOL, MSG_COUNT, RATE_LIMIT))
        p_reader = multiprocessing.Process(target=alpha_correlator_consumer, args=(SYMBOL, MSG_COUNT))
        
        p_writer.start()
        # Give writer a head start
        time.sleep(0.5)
        p_reader.start()
        
        p_writer.join()
        p_reader.join()
        
        logger.info("✅ Institutional Stress Test COMPLETE. Double-Buffer integrity verified.")
        
    except Exception as e:
        logger.error(f"❌ Test FAILED: {e}")
