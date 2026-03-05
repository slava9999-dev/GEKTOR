"""
Gerald-SuperBrain: Realistic Context Window Calculator
Calculates feasible context sizes for your hardware.
"""

import subprocess


def get_gpu_info():
    """Get GPU memory info."""
    try:
        result = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=memory.total,memory.free",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        parts = result.stdout.strip().split(", ")
        return int(parts[0]), int(parts[1])
    except Exception:

        return 8192, 0  # Default RTX 4070 Laptop


def get_system_ram():
    """Get total system RAM."""
    try:
        # Windows WMIC
        result = subprocess.run(
            ["wmic", "memorychip", "get", "capacity"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        lines = [
            line.strip() for line in result.stdout.strip().split("\n") if line.strip().isdigit()
        ]
        total = sum(int(line) for line in lines) / (1024**3)  # GB
        return total
    except Exception:

        return 16  # Assume 16GB


def calculate_context_limits():
    """Calculate realistic context window limits."""
    vram_total, vram_free = get_gpu_info()
    system_ram = get_system_ram()

    print("=" * 60)
    print("🧠 Gerald-SuperBrain: Context Window Reality Check")
    print("=" * 60)
    print()
    print(f"  GPU VRAM:     {vram_total} MiB ({vram_total/1024:.1f} GB)")
    print(f"  System RAM:   {system_ram:.0f} GB")
    print()

    # Model sizes (approximate, at Q4_K_M quantization)
    models = {
        "qwen2.5-coder:14b": {
            "model_size_gb": 9.0,
            "hidden_size": 5120,
            "num_layers": 48,
            "num_kv_heads": 8,  # GQA
            "head_dim": 128,
        },
        "mistral-nemo:latest": {
            "model_size_gb": 7.1,
            "hidden_size": 5120,
            "num_layers": 40,
            "num_kv_heads": 8,  # GQA
            "head_dim": 128,
        },
        "qwen2.5-coder:7b": {  # Alternative smaller model
            "model_size_gb": 4.7,
            "hidden_size": 3584,
            "num_layers": 28,
            "num_kv_heads": 4,  # GQA
            "head_dim": 128,
        },
    }

    print("Context Window Estimates (GPU + RAM hybrid):")
    print("-" * 60)

    for name, info in models.items():
        # KV cache size per token (bytes):
        # 2 (K+V) * num_layers * num_kv_heads * head_dim * 2 (FP16)
        kv_per_token = (
            2 * info["num_layers"] * info["num_kv_heads"] * info["head_dim"] * 2
        )
        kv_per_token_mb = kv_per_token / (1024 * 1024)

        # Available memory for KV cache (using system RAM since model overflows VRAM)
        # Assume 70% of system RAM available (OS + other apps need ~30%)
        available_ram_gb = system_ram * 0.7
        available_kv_gb = (
            available_ram_gb - info["model_size_gb"]
        )  # model weights first

        if available_kv_gb > 0:
            max_ctx_ram = int(available_kv_gb * 1024 / kv_per_token_mb)
        else:
            max_ctx_ram = 0

        # Conservative estimate (leave headroom)
        safe_ctx = int(max_ctx_ram * 0.7)

        # Round down to nearest power of 2 for cleanliness
        import math

        if safe_ctx > 0:
            safe_ctx_rounded = 2 ** int(math.log2(safe_ctx))
        else:
            safe_ctx_rounded = 2048

        print(f"\n  {name}:")
        print(f"    Model: {info['model_size_gb']} GB | KV/token: {kv_per_token} bytes")
        print(f"    Max theoretical ctx: {max_ctx_ram:,}")
        print(f"    Safe recommended:    {safe_ctx_rounded:,}")

        if name == "qwen2.5-coder:14b":
            recommended_ctx = safe_ctx_rounded

    print()
    print("=" * 60)
    print("💡 RECOMMENDATION for qwen2.5-coder:14b:")
    print(f"   Set num_ctx to: {recommended_ctx:,}")
    print(f"   (Current config: 131,072 — UNREALISTIC for {system_ram:.0f}GB RAM)")
    print()
    print("💡 For TRUE 128k context, consider:")
    print("   • qwen2.5-coder:7b (smaller model, more room for KV cache)")
    print("   • Cloud API (Anthropic, OpenAI) via OpenClaw gateway")
    print("   • Upgrade to 32GB+ RAM system")
    print("=" * 60)

    return recommended_ctx


if __name__ == "__main__":
    ctx = calculate_context_limits()
