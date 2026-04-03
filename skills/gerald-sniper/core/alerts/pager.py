# core/alerts/pager.py

import aiohttp
import asyncio
from loguru import logger
import os

class EmergencyPager:
    """
    Out-of-Band (OOB) escalation for Ultra-Critical failures.
    Uses Twilio Voice API to break through "Do Not Disturb" (DND) mode.
    [Gektor v6.25] Armor-grade Reliability.
    """
    def __init__(self, config: dict = None):
        cfg = config or {}
        # We look for these in env or passed config
        self.account_sid = os.getenv("TWILIO_ACCOUNT_SID")
        self.auth_token = os.getenv("TWILIO_AUTH_TOKEN")
        self.from_number = os.getenv("TWILIO_PHONE_NUMBER")
        self.to_number = os.getenv("OPERATOR_PHONE_NUMBER")
        
        self.base_url = f"https://api.twilio.com/2010-04-01/Accounts/{self.account_sid}/Calls.json" if self.account_sid else None
        self._last_call_ts = 0
        self._call_cooldown = 300 # 5 minutes between manual calls to avoid spamming self

    async def trigger_nuclear_alarm(self, reason: str):
        """
        Initiates a physical cellular phone call to the operator.
        Used ONLY for P0 events (Decapitation, Nuke Fail, Critical Breach).
        """
        now = asyncio.get_event_loop().time()
        if now - self._last_call_ts < self._call_cooldown:
            logger.warning(f"📞 [OOB PAGER] Call requested but suppressed by cooldown ({reason})")
            return

        if not all([self.account_sid, self.auth_token, self.to_number, self.from_number]):
            logger.error(f"🚨 [OOB PAGER] Twilio NOT configured. Reason: {reason}. Operator will sleep through the crash!")
            return

        # Institutional Alert Payload
        twiml = f"<Response><Say voice='alice'>Critical Alert. Tekton Alpha execution authority lost. Reason: {reason}. Manual intervention required immediately.</Say></Response>"
        
        data = {
            "To": self.to_number,
            "From": self.from_number,
            "Twiml": twiml
        }
        
        auth = aiohttp.BasicAuth(self.account_sid, self.auth_token)
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.base_url, data=data, auth=auth, timeout=10.0) as resp:
                    if resp.status in (200, 201):
                        logger.critical("📞 [OOB PAGER] P0 ALERT ESCALATED! Sending voice call to operator...")
                        self._last_call_ts = now
                    else:
                        resp_text = await resp.text()
                        logger.error(f"⚠️ [OOB PAGER] Call execution failed: {resp_text}")
        except Exception as e:
            logger.error(f"⚠️ [OOB PAGER] Network failure during OOB escalation: {e}")

# Global Instance
pager = EmergencyPager()
