from core.symbol_lock_manager import SymbolLockManager

def test_same_symbol_returns_same_lock():
    m = SymbolLockManager()
    lock1 = m.get_lock("BTCUSDT")
    lock2 = m.get_lock("BTCUSDT")
    assert lock1 is lock2

def test_different_symbols_return_different_locks():
    m = SymbolLockManager()
    lock1 = m.get_lock("BTCUSDT")
    lock2 = m.get_lock("ETHUSDT")
    assert lock1 is not lock2
