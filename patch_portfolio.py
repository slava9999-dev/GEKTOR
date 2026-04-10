import os

path = r"c:\Gerald-superBrain\skills\gerald-sniper\core\risk_manager\portfolio.py"
try:
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()

    new_methods = [
        "\n",
        "    async def handle_execution(self, cl_ord_id: str, symbol: str, cum_qty: float, total_qty: float, original_beta: float, is_final: bool):\n",
        "        \"\"\"\n",
        "        [GEKTOR v14.8.5] Atomic Idempotent Update (Lua).\n",
        "        Martin Kleppmann Standard: Strictly only applies positive deltas.\n",
        "        \"\"\"\n",
        "        if not self._fill_sha:\n",
        "             self._fill_sha = await self.redis.script_load(self._lua_fill)\n",
        "        try:\n",
        "            res = await self.redis.evalsha(\n",
        "                self._fill_sha, 3, \n",
        "                cl_ord_id, self.h_key, self.orders_key,\n",
        "                str(cum_qty), str(original_beta), str(total_qty or 1.0), symbol\n",
        "            )\n",
        "            delta_applied = float(res)\n",
        "            if delta_applied > 0:\n",
        "                from loguru import logger\n",
        "                logger.info(f\"✅ [IDEMPOTENT] {symbol} Order {cl_ord_id}: Applied true delta +{delta_applied:.4f}\")\n",
        "            if is_final:\n",
        "                await self.redis.delete(f\"{self.fly_prefix}{cl_ord_id}\")\n",
        "        except Exception as e:\n",
        "            from loguru import logger\n",
        "            logger.error(f\"❌ [IDEMPOTENT_FAIL] {cl_ord_id} execution sync error: {e}\")\n",
        "\n",
        "    async def handle_limbo_resolution(self, event: Any):\n",
        "        await self.handle_execution(\n",
        "            cl_ord_id=event.cl_ord_id,\n",
        "            symbol=event.symbol,\n",
        "            cum_qty=event.cum_qty,\n",
        "            total_qty=event.cum_qty if event.cum_qty > 0 else 1.0, \n",
        "            original_beta=1.0, \n",
        "            is_final=True\n",
        "        )\n",
        "\n",
        "    async def push_to_limbo(self, item_dict: dict):\n",
        "        import json\n",
        "        await self.redis.lpush(self.limbo_key, json.dumps(item_dict))\n",
        "\n",
        "    async def pop_from_limbo(self) -> Optional[dict]:\n",
        "        import json\n",
        "        res = await self.redis.rpop(self.limbo_key)\n",
        "        return json.loads(res.decode()) if res else None\n"
    ]

    found_pos = -1
    for i, line in enumerate(lines):
        if "async def critical_write_barrier" in line:
            for j in range(i+1, len(lines)):
                if "async def" in lines[j] or "class" in lines[j]:
                    found_pos = j
                    break
            if found_pos == -1: found_pos = len(lines)
            break
    
    if found_pos != -1:
        # Check if already patched to prevent duplication
        if not any("handle_limbo_resolution" in line for line in lines):
            lines[found_pos:found_pos] = new_methods
            with open(path, 'w', encoding='utf-8') as f:
                f.writelines(lines)
            print("✅ RiskAllocator updated with Idempotent logic.")
        else:
            print("⚠️  RiskAllocator already patched.")
    else:
        print("❌ Could not find insertion point in RiskAllocator.")
except Exception as e:
    print(f"❌ Error patching RiskAllocator: {e}")
