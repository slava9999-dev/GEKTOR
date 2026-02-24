from llama_cpp import Llama
import os

model_path = "models/qwen2.5-7b-instruct-q4_k_m.gguf"
if not os.path.exists(model_path):
    print(f"ERROR: Model not found at {model_path}")
else:
    print(f"Found model, attempting to load...")
    try:
        llm = Llama(
            model_path=model_path,
            n_ctx=2048, # Small ctx for test
            n_gpu_layers=35,
            verbose=True
        )
        print("SUCCESS: Model loaded successfully!")
        del llm
    except Exception as e:
        print(f"FAILURE: Could not load model: {e}")
