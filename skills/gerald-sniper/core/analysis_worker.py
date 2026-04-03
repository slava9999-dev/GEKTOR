import asyncio
from loguru import logger
from typing import Dict, Any
from utils.telegram_bot import send_telegram_alert

class AnalysisWorker:
    """
    Message Queue worker for offloading AI LLM tasks so they don't block
    or crash the main event loop. Implements Context Window Management
    and Stateless Agents principles.
    """
    def __init__(self):
        self.queue = asyncio.Queue()
        self.running = False

    async def start(self):
        self.running = True
        logger.info("🧠 AI Analysis Worker started (Queue mode)")
        while self.running:
            try:
                task_data = await self.queue.get()
                await self._process_task(task_data)
                self.queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Analysis worker error: {e}")
                await asyncio.sleep(1)

    async def stop(self):
        self.running = False

    async def add_task(self, msg_id: int, alert_id: int | None, symbol: str, level: dict, trigger: dict, score: int, direction: str, btc_ctx: dict, risk_data: dict, atr_pct: float):
        if self.queue.qsize() > 10:
            if score < 80:
                logger.warning(f"📉 AI Queue is full ({self.queue.qsize()}), dropping low priority task for {symbol} (score={score})")
                return

        payload = {
            'msg_id': msg_id,
            'alert_id': alert_id,
            'symbol': symbol,
            'level': level,
            'trigger': trigger,
            'score': score,
            'direction': direction,
            'btc_ctx': btc_ctx,
            'risk_data': risk_data,
            'atr_pct': atr_pct
        }
        await self.queue.put(payload)

    async def _process_task(self, payload: dict):
        from core.ai_analyst import analyst
        
        try:
            # We explicitly pass ONLY the essential stateless payload
            # Mocking sym_data explicitly for backwards compatibility without heavy candles
            minimal_sym_data = {
                'radar': type('MockRadar', (), {'atr_pct': payload['atr_pct']})(),
                'm5': [], 
                'm15': []
            }
            
            reply_msg, markup = await analyst.analyze_signal(
                payload['symbol'], 
                payload['level'], 
                payload['trigger'], 
                payload['score'], 
                payload['direction'], 
                minimal_sym_data, 
                payload['btc_ctx'], 
                payload['alert_id']
            )
            
            if reply_msg:
                try:
                    await send_telegram_alert(reply_msg, reply_to_message_id=payload['msg_id'], reply_markup=markup)
                except Exception as e:
                    logger.error(f"Failed to send AI reply for {payload['symbol']}: {e}")
        except Exception as e:
            logger.error(f"Failed to process AI task for {payload['symbol']}: {e}")

analysis_worker = AnalysisWorker()
