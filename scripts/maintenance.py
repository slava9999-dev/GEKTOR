"""
Gerald-SuperBrain: Auto-Maintenance & Self-Healing v2.0
Checks health, prunes logs, cleans sandboxes, and ensures all services are optimal.
"""

import os
import sys
import shutil
import subprocess
import time
from loguru import logger

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.shared.gpu_monitor import GPUMonitor

BASE_DIR = r"c:\Gerald-superBrain"
LOG_FILES = [
    os.path.join(BASE_DIR, "gerald-setup.log"),
    os.path.join(BASE_DIR, "logs", "gerald_sys.log"),
]
TMP_SANDBOX = os.path.join(BASE_DIR, "gerald_workspace", "tmp_run")


def check_docker_chroma():
    """Ensure ChromaDB container is running and healthy."""
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=chroma", "--format", "{{.Status}}"],
            capture_output=True,
            text=True,
        )
        if "Up" not in result.stdout:
            logger.warning("ChromaDB container down. Attempting restart...")
            subprocess.run(["docker", "start", "chroma"])
            return False
        return True
    except Exception as e:
        logger.error(f"Docker check failed: {e}")
        return False


def prune_logs(max_lines=2000):
    """Keep log files at a manageable size."""
    for log_path in LOG_FILES:
        if os.path.exists(log_path):
            try:
                with open(log_path, "r", encoding="utf-8") as f:
                    lines = f.readlines()

                if len(lines) > max_lines:
                    logger.info(f"Pruning log: {os.path.basename(log_path)}")
                    with open(log_path, "w", encoding="utf-8") as f:
                        f.writelines(lines[-max_lines:])
            except Exception as e:
                logger.error(f"Error pruning {log_path}: {e}")


def clean_sandboxes():
    """Cleanup temporary execution files to prevent disk bloat."""
    if os.path.exists(TMP_SANDBOX):
        try:
            now = time.time()
            for f in os.listdir(TMP_SANDBOX):
                f_path = os.path.join(TMP_SANDBOX, f)
                # Remove if older than 1 hour
                if os.path.getmtime(f_path) < now - 3600:
                    if os.path.isfile(f_path):
                        os.remove(f_path)
                    elif os.path.isdir(f_path):
                        shutil.rmtree(f_path)
            logger.info("Sandbox cleanup completed.")
        except Exception as e:
            logger.error(f"Sandbox cleanup error: {e}")


def sync_identity():
    """Sync canonical identity files to OpenClaw workspace."""
    workspace_path = os.path.expanduser(r"~\.openclaw\workspace")
    if not os.path.exists(workspace_path):
        return

    files_to_sync = ["SOUL.md", "USER.md"]
    for f in files_to_sync:
        src = os.path.join(BASE_DIR, f)
        dst = os.path.join(workspace_path, f)
        if os.path.exists(src):
            try:
                shutil.copy2(src, dst)
            except Exception:

                pass


def main():
    logger.info("Starting Gerald Maintenance v2.0...")

    # 1. Hardware Guard
    monitor = GPUMonitor()
    safe, msg = monitor.check_safety()
    if not safe:
        logger.warning(f"Hardware Alert: {msg}")

    # 2. Cleanup
    prune_logs()
    clean_sandboxes()

    # 3. Docker Services
    check_docker_chroma()

    # 4. Identity Sync
    sync_identity()

    # 5. External Reflector
    try:
        reflector_path = os.path.join(BASE_DIR, "scripts", "reflector.py")
        subprocess.run([sys.executable, reflector_path], capture_output=True)
    except Exception:

        pass

    logger.info("Maintenance complete.")


if __name__ == "__main__":
    main()
