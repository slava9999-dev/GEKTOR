import sys
import os
from loguru import logger
from src.shared.config import config

def setup_logger():
    # Remove default handler
    logger.remove()
    
    # Console handler
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level=config.system.log_level if config else "INFO"
    )
    
    # File handler with rotation
    log_path = config.paths.logs if config else "logs/gerald_sys.log"
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    
    logger.add(
        log_path,
        rotation="10 MB",
        retention="10 days",
        compression="zip",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level=config.system.log_level if config else "INFO"
    )

# Initialize logger
setup_logger()
