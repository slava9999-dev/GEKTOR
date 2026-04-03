import sys
import os
from loguru import logger

def setup_institutional_logging(module_name: str, log_level: str = "INFO"):
    """
    Фабрика Zero-Blocking логгера для распределенных процессов.
    Инкапсулирует конфигурацию loguru с поддержкой Trace ID и асинхронной очереди (enqueue=True).
    """
    # Гарантируем существование директории логов
    os.makedirs("logs", exist_ok=True)

    # Очищаем дефолтные обработчики
    logger.remove()
    
    # 1. ТЕРМИНАЛ (Human-Readable). Работает через enqueue для защиты Event Loop.
    # Включаем отображение trace_id из контекста, если он есть.
    logger.add(
        sys.stdout, 
        format="<green>{time:HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> | <magenta>{extra[trace_id]}</magenta> - <white>{message}</white>",
        level=log_level,
        enqueue=True
    )
    
    # 2. ФАЙЛ (Machine-Readable JSONL для Grafana Loki / Vector).
    logger.add(
        f"logs/{module_name}_{{time:YYYY-MM-DD}}.jsonl",
        serialize=True,
        enqueue=True,   # <-- ZERO I/O PAUSE (queue.SimpleQueue thread)
        backtrace=False,  # Отключаем тяжелые трейсбэки для штатных логов
        diagnose=False,   # Защита от утечки чувствительных переменных (API Keys) в прод-логи
        level="DEBUG"     # В JSONL пишем всё, фильтровать будем на стороне Loki
    )
    
    # Возвращаем логгер, в который по умолчанию вшит пустой trace_id, чтобы не ломать форматирование
    return logger.bind(trace_id="SYSTEM")
