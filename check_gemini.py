import os
import google.generativeai as genai
from dotenv import load_dotenv
from pathlib import Path

def run_api_recon():
    print("[Recon] Initiating Gemini API Scan...")
    
    # 1. Жесткая загрузка окружения
    env_path = Path(__file__).parent / '.env'
    load_dotenv(dotenv_path=env_path, override=True)
    
    # 2. Подхватываем твой прокси из .env
    proxy_url = os.getenv("PROXY_URL")
    if proxy_url:
        print(f"[Proxy] Proxy detected: {proxy_url.split('@')[-1]}") # Прячем пароль
        os.environ['http_proxy'] = proxy_url
        os.environ['https_proxy'] = proxy_url
        # Also lowercase variants for some libraries
        os.environ['HTTP_PROXY'] = proxy_url
        os.environ['HTTPS_PROXY'] = proxy_url

    # 3. Инициализация ключа
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("[ERROR] GEMINI_API_KEY not found in .env")
        return

    genai.configure(api_key=api_key)

    # 4. Запрос списка
    print("\n[Models] Available Gemini Models for structured generation (generateContent):")
    try:
        models_found = 0
        for m in genai.list_models():
            if 'generateContent' in m.supported_generation_methods:
                print(f" [OK] {m.name}")
                models_found += 1
        
        if models_found == 0:
            print("[WARNING] No models found supporting 'generateContent'. Check API key permissions.")
        else:
            print(f"\n[Total] Total viable models found: {models_found}")
            
    except Exception as e:
        print(f"[API ERROR] Connection failed: {e}")

if __name__ == "__main__":
    run_api_recon()
