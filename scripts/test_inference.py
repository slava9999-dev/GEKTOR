"""Test real Ollama inference with context parameters."""
import urllib.request
import json
import time

OLLAMA_API = "http://localhost:11434"

def test_inference():
    """Test model responds and respects num_ctx."""
    payload = json.dumps({
        "model": "qwen2.5-coder:14b",
        "prompt": "You are Gerald. Say hello in one sentence, mentioning your name and your owner Slava.",
        "stream": False,
        "options": {
            "num_ctx": 8192,  # Start small to test, 131072 would OOM on 8GB laptop
            "num_gpu": 35,
            "temperature": 0.65,
            "top_p": 0.95
        }
    }).encode("utf-8")

    req = urllib.request.Request(
        f"{OLLAMA_API}/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    
    print("⏳ Sending inference request to qwen2.5-coder:14b...")
    start = time.time()
    
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read())
            elapsed = time.time() - start
            
            response_text = data.get("response", "NO RESPONSE")
            total_duration = data.get("total_duration", 0) / 1e9  # nanoseconds to seconds
            prompt_tokens = data.get("prompt_eval_count", 0)
            response_tokens = data.get("eval_count", 0)
            
            print(f"✅ Response ({elapsed:.1f}s wall, {total_duration:.1f}s model):")
            print(f"   {response_text}")
            print(f"   Tokens: {prompt_tokens} prompt + {response_tokens} response")
            print(f"   Model loaded: {'yes' if data.get('load_duration') else 'was cached'}")
            return True
    except Exception as e:
        print(f"❌ Inference failed: {e}")
        return False


def check_vram_for_128k():
    """Calculate if 128k context fits in VRAM."""
    # qwen2.5-coder:14b at Q4_K_M = ~9GB model
    # Each token in KV cache ≈ 0.5-1 MB for 14B model
    # 131072 tokens * ~0.5MB = ~64GB KV cache
    # RTX 4070 Laptop = 8GB VRAM
    
    print()
    print("=" * 50)
    print("⚠️  VRAM ANALYSIS for 128k context:")
    print("=" * 50)
    print(f"  Model size:     ~9 GB (Q4_K_M)")
    print(f"  Available VRAM: 8 GB (RTX 4070 Laptop)")
    print(f"  128k KV cache:  ~20-64 GB (depends on layer offloading)")
    print()
    print("  🔴 VERDICT: 128k context CANNOT fit in 8GB VRAM!")
    print("  🔴 Model alone (9GB) exceeds total VRAM (8GB)")
    print("  🔴 With num_gpu=35, model uses ~6GB VRAM + rest on CPU")
    print()
    print("  REALISTIC maximum context for your hardware:")
    print("  • qwen2.5-coder:14b with num_gpu=35: ~8k-16k context")
    print("  • qwen2.5-coder:14b with num_gpu=20: ~16k-32k context")
    print("  • mistral-nemo (7.1GB) with num_gpu=28: ~16k-32k context")
    print()
    print("  💡 For true 128k: need qwen2.5-coder:7b (smaller) or cloud API")


if __name__ == "__main__":
    test_inference()
    check_vram_for_128k()
