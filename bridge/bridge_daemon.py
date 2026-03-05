import requests
import os
import sys
import time
import json
import subprocess
import signal
from datetime import datetime
from dotenv import load_dotenv

# Portable path resolution
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(CURRENT_DIR)
sys.path.append(BASE_DIR)

from src.shared.logger import logger  # noqa: E402
from src.shared.gpu_monitor import GPUMonitor  # noqa: E402

load_dotenv(os.path.join(BASE_DIR, ".env"))

# Configuration
BRIDGE_DIR = os.path.join(BASE_DIR, "bridge")
INBOX = os.path.join(BRIDGE_DIR, "inbox")
OUTBOX = os.path.join(BRIDGE_DIR, "outbox")
STATUS_FILE = os.path.join(BRIDGE_DIR, "status.json")


class GeraldBridge:
    def __init__(self):
        self.gpu_monitor = GPUMonitor()
        self.config = self._get_tg_config()
        self.last_maintenance = 0
        self.running = True
        self._ensure_dirs()
        self._setup_signals()

    def _ensure_dirs(self):
        for d in [INBOX, OUTBOX]:
            os.makedirs(d, exist_ok=True)

    def _setup_signals(self):
        def handler(signum, frame):
            logger.info(f"Received signal {signum}, shutting down gracefully...")
            self.running = False

        # Windows supports limited signals via Python
        if sys.platform != "win32":
            signal.signal(signal.SIGTERM, handler)
        signal.signal(signal.SIGINT, handler)

    def _get_tg_config(self):
        return {
            "bot_token": os.getenv("GERALD_BOT_TOKEN"),
            "default_chat_id": int(os.getenv("TELEGRAM_CHAT_ID", 0)),
        }

    def update_status(self, inbox_count, outbox_count, status="running", error=None):
        try:
            if os.path.exists(STATUS_FILE):
                with open(STATUS_FILE, "r") as f:
                    data = json.load(f)
            else:
                data = {"bridge_version": "1.2.1", "errors": [], "tg_offset": 0}

            data.update(
                {
                    "status": status,
                    "last_sync": datetime.now().isoformat(),
                    "inbox_pending": inbox_count,
                    "outbox_pending": outbox_count,
                    "gpu_stats": self.gpu_monitor.get_stats(),
                }
            )

            if error:
                data["errors"].append(f"[{datetime.now().isoformat()}] {error}")
                data["errors"] = data["errors"][-10:]

            with open(STATUS_FILE, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Status update failed: {e}")

    def check_telegram(self):
        if not self.config.get("bot_token"):
            return

        offset = 0
        try:
            if os.path.exists(STATUS_FILE):
                with open(STATUS_FILE, "r") as f:
                    offset = json.load(f).get("tg_offset", 0)
        except Exception as e:
            logger.debug(f"Failed reading tg_offset from status file: {e}")

        url = f"https://api.telegram.org/bot{self.config['bot_token']}/getUpdates"
        try:
            resp = requests.get(
                url, params={"offset": offset, "timeout": 5}, timeout=10
            )
            updates = resp.json().get("result", [])

            for update in updates:
                new_offset = update["update_id"] + 1
                self._update_tg_offset(new_offset)

                if "message" in update and "text" in update["message"]:
                    msg = update["message"]
                    if msg["chat"]["id"] == self.config.get("default_chat_id"):
                        msg_id = f"tg_{update['update_id']}"
                        with open(
                            os.path.join(INBOX, f"{msg_id}.msg"), "w", encoding="utf-8"
                        ) as f:
                            f.write(msg["text"])
                        logger.info(f"Telegram message received: {msg_id}")
        except Exception as e:
            logger.warning(f"Telegram poll error: {e}")

    def _update_tg_offset(self, offset):
        try:
            with open(STATUS_FILE, "r") as f:
                data = json.load(f)
            data["tg_offset"] = offset
            with open(STATUS_FILE, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.debug(f"Failed to update tg_offset: {e}")

    def deliver_responses(self):
        if not self.config.get("bot_token"):
            return

        files = [
            f for f in os.listdir(OUTBOX) if f.startswith("tg_") and f.endswith(".resp")
        ]
        url = f"https://api.telegram.org/bot{self.config['bot_token']}/sendMessage"

        for file in files:
            path = os.path.join(OUTBOX, file)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    text = f.read()

                resp = requests.post(
                    url,
                    json={
                        "chat_id": self.config["default_chat_id"],
                        "text": f"🧠 GERALD:\n{text}",
                    },
                    timeout=10,
                )

                if resp.status_code == 200:
                    os.remove(path)
                    logger.info(f"Delivered TG response: {file}")
            except Exception as e:
                logger.error(f"Failed to deliver TG response {file}: {e}")

    def process_inbox(self):
        files = [f for f in os.listdir(INBOX) if f.endswith(".msg")]
        for file in files:
            path = os.path.join(INBOX, file)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    prompt = f.read().strip()

                if not prompt:
                    os.remove(path)
                    continue

                logger.info(f"Executing OpenClaw for: {file}")
                result = subprocess.run(
                    [
                        "npx",
                        "--yes",
                        "openclaw",
                        "agent",
                        "--agent",
                        "main",
                        "--session-id",
                        "gerald_tg",
                        "--message",
                        prompt,
                    ],
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    cwd=BASE_DIR,
                    shell=True,
                    timeout=300,
                )

                resp_path = os.path.join(OUTBOX, file.replace(".msg", ".resp"))
                with open(resp_path, "w", encoding="utf-8") as f:
                    f.write(
                        result.stdout
                        if result.returncode == 0
                        else f"⚠️ Error: {result.stderr}"
                    )

                os.remove(path)
            except Exception as e:
                logger.error(f"Processing error for {file}: {e}")

    def run_maintenance(self):
        if time.time() - self.last_maintenance > 3600:
            m_path = os.path.join(BASE_DIR, "scripts", "maintenance.py")
            if os.path.exists(m_path):
                subprocess.run([sys.executable, m_path], capture_output=True)
            self.last_maintenance = time.time()

    def step(self):
        self.run_maintenance()

        is_safe, msg = self.gpu_monitor.check_safety()
        if not is_safe:
            logger.warning(f"Bridge paused: {msg}")
            time.sleep(10)
            return

        self.check_telegram()
        self.process_inbox()
        self.deliver_responses()

        in_count = len([f for f in os.listdir(INBOX) if f.endswith(".msg")])
        out_count = len([f for f in os.listdir(OUTBOX) if f.endswith(".resp")])
        self.update_status(in_count, out_count)


def main():
    bridge = GeraldBridge()
    logger.info("Gerald Bridge v1.2.1 (Portable) started")

    while bridge.running:
        try:
            bridge.step()
            time.sleep(1)
        except Exception as e:
            logger.error(f"Fatal bridge loop error: {e}")
            time.sleep(5)

    logger.info("Bridge daemon exit.")


if __name__ == "__main__":
    main()
