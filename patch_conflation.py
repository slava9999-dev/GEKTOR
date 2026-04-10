import os

path = r"c:\Gerald-superBrain\skills\gerald-sniper\core\risk_manager\portfolio.py"
try:
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()

    # 1. Update __init__ to include Conflation structures
    init_start = -1
    for i, line in enumerate(lines):
        if "def __init__(self, redis_client" in line:
            init_start = i
            break
    
    if init_start != -1:
        # Find the end of __init__
        init_end = -1
        for j in range(init_start+1, len(lines)):
            if "async def" in lines[j]:
                init_end = j
                break
        
        conflation_init = [
            "        self._dirty_event = asyncio.Event()\n",
            "        self._is_syncing = False\n",
            "        self._shadow_total_beta = 0.0\n",
            "        self._shadow_gross = 0.0\n"
        ]
        lines[init_end:init_end] = conflation_init

    # 2. Add the Conflation Loop method
    new_loop = """
    async def run_shadow_sync_loop(self):
        \"\"\"
        [GEKTOR v14.8.5] Event Conflation Engine.
        Protects the system from 'Event Storms' by batching risk recalculations
        during micro-structural volatility (Rule of David Beazley).
        \"\"\"
        self._is_syncing = True
        logger.info("⚡ [RISK] Conflation Barrier INITIALIZED (50ms gate).")
        while self._is_syncing:
            await self._dirty_event.wait()
            self._dirty_event.clear()
            
            # [COALESCENCE] Wait for the storm to settle (50ms bucket)
            await asyncio.sleep(0.050) 
            
            try:
                # One single trip to Redis to refresh the SHADOW state
                pipe = self.redis.pipeline()
                pipe.hget(self.h_key, "total_beta")
                pipe.hget(self.h_key, "total_gross")
                res = await pipe.execute()
                
                self._shadow_total_beta = float(res[0] or 0)
                self._shadow_gross = float(res[1] or 0)
                
                logger.debug(f"⚖️ [CONFLATED] Shadow State Updated: Beta {self._shadow_total_beta:.4f}")
            except Exception as e:
                logger.error(f"❌ [CONFLATION_FAIL] Shadow sync error: {e}")
"""
    # 3. Trigger dirty flag in handle_execution
    # After 'delta_applied > 0' logic... 
    # I'll modify the handle_execution during the same patch.

    # This is getting complex for a simple string replacement. 
    # I'll write a full replacement of the class IF I can. 
    # But for now, I'll just add the loop and the trigger.

    with open(path, 'w', encoding='utf-8') as f:
        f.writelines(lines)
    
    # Second pass for handle_execution trigger
    with open(path, 'r', encoding='utf-8') as f:
        text = f.read()
    
    text = text.replace("logger.info(f\"✅ [IDEMPOTENT] {symbol} Order {cl_ord_id}: Applied true delta +{delta_applied:.4f}\")",
                        "logger.info(f\"✅ [IDEMPOTENT] {symbol} Order {cl_ord_id}: Applied true delta +{delta_applied:.4f}\")\n                self._dirty_event.set()")
    
    text = text + new_loop
    
    with open(path, 'w', encoding='utf-8') as f:
        f.write(text)

    print("✅ RiskAllocator updated with Event Conflation logic.")

except Exception as e:
    print(f"❌ Error patching: {e}")
