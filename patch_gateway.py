import os

path = r"c:\Gerald-superBrain\skills\gerald-sniper\core\execution\gateway.py"
try:
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()

    new_lines = []
    for line in lines:
        if "from dataclasses import dataclass, field" in line:
            new_lines.append("from dataclasses import dataclass, field, asdict\n")
        elif "self._queue = asyncio.Queue()" in line:
            new_lines.append("        # [GEKTOR v14.8.5] Using persistent Redis queue via risk_allocator\n")
        elif "await self._queue.put(item)" in line:
            new_lines.append("        await self._risk_allocator.push_to_limbo(asdict(item))\n")
        elif "item = await self._queue.get()" in line:
            # We need to handle the pop with loop
            new_lines.append("            it = await self._risk_allocator.pop_from_limbo()\n")
            new_lines.append("            if not it: await asyncio.sleep(1.0); continue\n")
            new_lines.append("            item = LimboItem(**it)\n")
        else:
            new_lines.append(line)

    with open(path, 'w', encoding='utf-8') as f:
        f.writelines(new_lines)
    print("✅ Gateway updated with persistent Limbo logic.")
except Exception as e:
    print(f"❌ Error patching Gateway: {e}")
