"""
Gerald-SuperBrain: Reflector Engine
Analyzes system logs and errors, suggests improvements via the bridge.
"""

import os
import sys
import subprocess
from datetime import datetime

BASE_DIR = r"c:\Gerald-superBrain"
LOG_PATH = os.path.join(BASE_DIR, "gerald-setup.log")
BRIDGE_INBOX = os.path.join(BASE_DIR, "bridge", "inbox")


def analyze_logs():
    if not os.path.exists(LOG_PATH):
        return None

    with open(LOG_PATH, "r", encoding="utf-8") as f:
        content = f.read()

    # Simple error extraction logic
    errors = []
    for line in content.split("\n"):
        if (
            "ERROR" in line.upper()
            or "EXCEPTION" in line.upper()
            or "FAIL" in line.upper()
        ):
            errors.append(line)

    if not errors:
        return None

    return "\n".join(errors[-10:])  # last 10 errors


def main():
    print(f"[{datetime.now().isoformat()}] 🧠 Reflector: Starting analysis...")

    recent_errors = analyze_logs()

    if recent_errors:
        print(
            f"[{datetime.now().isoformat()}] ⚠️ Errors found! Notifying Gerald and Slava..."
        )

        # 1. Notify Slava via Telegram (Real-time alert)
        try:
            guard_path = os.path.join(BASE_DIR, "skills", "telegram-guard", "guard.py")
            subprocess.run(
                [
                    sys.executable,
                    guard_path,
                    "System Errors Detected! Check logs for details.",
                ],
                capture_output=True,
            )
        except Exception:

            pass

        # 2. Notify Gerald via Bridge (Self-healing request)
        improvement_msg = f"""Gerald, я обнаружил следующие системные ошибки в логах:
{recent_errors}
..."""
        msg_path = os.path.join(
            BRIDGE_INBOX, f"auto_fix_{int(datetime.now().timestamp())}.msg"
        )
        with open(msg_path, "w", encoding="utf-8") as f:
            f.write(improvement_msg)
        print("✅ Reflector: Improvement request sent to inbox.")
    else:
        print("✅ Reflector: System healthy, no critical errors found.")


if __name__ == "__main__":
    main()
