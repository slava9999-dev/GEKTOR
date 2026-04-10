import pickle
import logging
import os
import asyncio
from typing import Any, Optional

logger = logging.getLogger("GEKTOR.APEX.Checkpoint")

class CheckpointManager:
    """ 
    [KLEPPMAN v21.1] Атомарный хранитель 'Памяти' с защитой от Torn Writes. 
    """
    def __init__(self, storage_path: str = "data/checkpoints/"):
        self.storage_path = storage_path
        os.makedirs(storage_path, exist_ok=True)

    def _atomic_save(self, component_name: str, state: Any):
        """ Синхронная атомарная запись (выполняется в thread). """
        file_path = os.path.join(self.storage_path, f"{component_name}.pkl")
        tmp_path = f"{file_path}.tmp"
        
        try:
            # 1. Пишем во временный файл
            with open(tmp_path, "wb") as f:
                pickle.dump(state, f, protocol=pickle.HIGHEST_PROTOCOL)
            
            # 2. Атомарно подменяем старый файл новым (POSIX rename)
            # Если питание пропадет до этой строки, старый чекпойнт останется целым.
            os.replace(tmp_path, file_path) 
            logger.info(f"Checkpoint: Atomic save for '{component_name}' successful.")
        except Exception as e:
            logger.error(f"Failed to save checkpoint for {component_name}: {e}")
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

    async def save_checkpoint_async(self, component_name: str, state: Any):
        """ [BEAZLEY] Неблокирующий фасад для Event Loop. """
        await asyncio.to_thread(self._atomic_save, component_name, state)

    def load_checkpoint(self, component_name: str) -> Optional[Any]:
        """ Восстановление стейта (выполняется синхронно при старте). """
        file_path = os.path.join(self.storage_path, f"{component_name}.pkl")
        if not os.path.exists(file_path):
            return None
            
        try:
            with open(file_path, "rb") as f:
                return pickle.load(f)
        except EOFError:
            logger.critical(f"Checkpoint '{component_name}' corrupted! Purging.")
            try:
                os.remove(file_path)
            except OSError:
                pass
            return None
        except Exception as e:
            logger.error(f"Failed to load checkpoint for {component_name}: {e}")
            return None
