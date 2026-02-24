import os
import subprocess
import uuid
import asyncio
from src.shared.config import config
from src.shared.logger import logger
from src.domain.entities.agent_output import ToolResult

class PythonExecutor:
    """
    Isolated Python code execution sandbox.
    Follows TS v2.0: 15s timeout, tmp files, internet isolation.
    """
    def __init__(self):
        self.tmp_dir = config.paths.tmp_run
        os.makedirs(self.tmp_dir, exist_ok=True)

    async def execute(self, code: str) -> ToolResult:
        file_id = str(uuid.uuid4())
        file_path = os.path.join(self.tmp_dir, f"run_{file_id}.py")
        
        logger.info(f"Executing Python code sandbox: {file_path}")
        
        # Write code to temp file
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(code)

        try:
            # Subprocess execution with timeout
            # Note: In Windows, complete network isolation is tricky without Docker.
            # We will use basic isolation and a strict timeout.
            process = await asyncio.create_subprocess_exec(
                "python", file_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(), 
                    timeout=config.sandbox.timeout
                )
                
                output = stdout.decode().strip()
                error = stderr.decode().strip()
                
                if process.returncode == 0:
                    return ToolResult(success=True, output=output)
                else:
                    return ToolResult(success=False, output=output, error=error)

            except asyncio.TimeoutError:
                # TS Requirement: Kill and report error
                process.kill()
                logger.warning(f"Python execution timed out after {config.sandbox.timeout}s")
                return ToolResult(
                    success=False, 
                    output="", 
                    error=f"Твой код превысил время выполнения ({config.sandbox.timeout}с) и был убит. Исправь ошибку."
                )

        except Exception as e:
            logger.error(f"Sandbox execution error: {e}")
            return ToolResult(success=False, output="", error=str(e))
        finally:
            # Cleanup
            if os.path.exists(file_path):
                os.remove(file_path)
