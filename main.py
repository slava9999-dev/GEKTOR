import os
import sys

# Ensure project root is in PYTHONPATH
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.presentation.cli.main import GeraldCLI
import asyncio

if __name__ == "__main__":
    cli = GeraldCLI()
    try:
        asyncio.run(cli.run())
    except KeyboardInterrupt:
        pass
