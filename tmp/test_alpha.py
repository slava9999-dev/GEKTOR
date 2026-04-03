import asyncio
import os
import sys

# Correct path logic: Root must be in path for 'src' to be found
BASE_DIR = r"c:\Gerald-superBrain"
SNIPER_DIR = os.path.join(BASE_DIR, "skills", "gerald-sniper")

if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
if SNIPER_DIR not in sys.path:
    sys.path.insert(0, SNIPER_DIR)

async def test_alpha():
    try:
        from src.application.tools.gerald_tools import tool_analyze_market_alpha
        
        # Test with 1 hour window
        print("🚀 Running Alpha Analysis v2.0.7...")
        result = await tool_analyze_market_alpha({"hours": 1})
        
        if result.success:
            print("✅ TEST SUCCESS")
            print(result.output)
        else:
            print("❌ TEST FAILED")
            print(f"Error: {result.error}")
            
    except Exception as e:
        print(f"Test Execution Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_alpha())
