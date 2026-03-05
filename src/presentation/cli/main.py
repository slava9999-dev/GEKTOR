import asyncio
import signal
import sys
from src.shared.logger import logger
from src.infrastructure.llm.llama_engine import LlamaEngine
from src.application.services.agent import GeraldAgent

from src.application.services.indexer import BackgroundIndexer

from src.infrastructure.database.backup import BackupSystem


class GeraldCLI:
    """
    CLI Presentation Layer.
    """

    def __init__(self):
        self.llm = LlamaEngine()
        self.agent = GeraldAgent(self.llm)
        self.indexer = BackgroundIndexer(self.agent.vector_db)
        self.backup_system = BackupSystem()
        self._stop_event = asyncio.Event()

    async def run(self):
        print("\n🧠 Gerald-SuperBrain V2.0 (GGUF Mode)")
        print("Type 'exit' to quit.\n")

        # Start Background Indexing
        asyncio.create_task(self.indexer.scan_and_index())

        # Setup signal handling for Graceful Shutdown
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.shutdown)

        try:
            while not self._stop_event.is_set():
                user_input = await loop.run_in_executor(None, input, "Slava > ")

                if user_input.lower() in ("exit", "quit", "bye"):
                    self.shutdown()
                    break

                response = await self.agent.chat(user_input)
                print(f"Gerald > {response}\n")

        except asyncio.CancelledError:
            pass
        finally:
            await self._cleanup()

    def shutdown(self):
        logger.info("Shutdown signal received")
        self._stop_event.set()

    async def _cleanup(self):
        # TS Requirement: Graceful Shutdown
        logger.info("Performing final cleanup...")
        # 0. Backup
        self.backup_system.create_backup()
        # 1. Unload model (VRAM release)
        self.llm.unload()
        # 2. Wait a bit for logs/sessions
        await asyncio.sleep(1)
        logger.info("Gerald-SuperBrain safely shut down.")
        sys.exit(0)


if __name__ == "__main__":
    cli = GeraldCLI()
    asyncio.run(cli.run())
