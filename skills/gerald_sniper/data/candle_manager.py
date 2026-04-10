from loguru import logger
import asyncio

class CandleManager:
    def __init__(self, db_manager):
        self.db = db_manager

    async def handle_ws_message(self, data: dict):
        """Processes incoming WS data."""
        topic = data.get("topic", "")
        if topic.startswith("kline"):
            await self._process_kline(data)
        elif topic.startswith("tickers"):
            # Update real-time price info (can be used for triggering alerts)
            pass
        elif topic.startswith("liquidation"):
            # Queue liquidation events to the trigger engine
            pass
            
    async def _process_kline(self, data: dict):
        # ws payload format for kline
        # {"topic": "kline.5.BTCUSDT", "data": [{"start": 16723232...}]}
        topic_parts = data["topic"].split(".")
        if len(topic_parts) != 3:
            return
            
        timeframe = topic_parts[1]
        symbol = topic_parts[2]
        
        for k in data.get("data", []):
            if k.get("confirm"): # If candle is closed, save it to DB
                # TODO: Implement saving closed candle to DB using aiosqlite in background task
                pass
