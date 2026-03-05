import os
import uuid
import asyncio
import ast
from typing import Tuple
from src.shared.config import config
from src.shared.logger import logger
from src.domain.entities.agent_output import ToolResult


class PythonExecutor:
    """
    Isolated Python code execution sandbox with AST-based safety filtering.
    Follows TS v2.0: 15s timeout, tmp files, internet isolation.
    """

    def __init__(self):
        self.tmp_dir = config.paths.tmp_run
        os.makedirs(self.tmp_dir, exist_ok=True)

        # Blacklisted modules that could lead to host compromise
        self.BANNED_MODULES = {
            "os",
            "subprocess",
            "shutil",
            "sys",
            "socket",
            "requests",
            "urllib",
            "http",
            "ftplib",
            "smtplib",
            "pty",
            "platform",
            "ctypes",
            "winreg",
            "pickle",
            "importlib",
            "builtins",
        }

        # Banned built-in functions
        self.BANNED_BUILTINS = {
            "eval",
            "exec",
            "open",
            "compile",
            "getattr",
            "setattr",
            "delattr",
            "__import__",
        }

    def _is_safe(self, code: str) -> Tuple[bool, str]:
        """Performs static analysis to detect dangerous constructs."""
        try:
            tree = ast.parse(code)
            for node in ast.walk(tree):
                # Check for banned imports
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name.split(".")[0] in self.BANNED_MODULES:
                            return (
                                False,
                                f"Использование модуля '{alias.name}' запрещено в целях безопасности.",
                            )

                if isinstance(node, ast.ImportFrom):
                    if node.module and node.module.split(".")[0] in self.BANNED_MODULES:
                        return False, f"Использование модуля '{node.module}' запрещено."

                # Check for banned function calls
                if isinstance(node, ast.Call):
                    if (
                        isinstance(node.func, ast.Name)
                        and node.func.id in self.BANNED_BUILTINS
                    ):
                        return (
                            False,
                            f"Вызов функции '{node.func.id}()' заблокирован системой безопасности.",
                        )

            return True, ""
        except SyntaxError as e:
            return False, f"Ошибка синтаксиса: {e}"
        except Exception as e:
            return False, f"Ошибка при проверке кода: {e}"

    async def execute(self, code: str) -> ToolResult:
        # Pre-execution safety check
        safe, msg = self._is_safe(code)
        if not safe:
            logger.warning(f"Blocked dangerous Python code: {msg}")
            return ToolResult(
                success=False, output="", error=f"🛡 ОШИБКА БЕЗОПАСНОСТИ: {msg}"
            )

        file_id = str(uuid.uuid4())
        file_path = os.path.join(self.tmp_dir, f"run_{file_id}.py")

        logger.info(f"Executing secure sandbox for: run_{file_id}.py")

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(code)

        try:
            process = await asyncio.create_subprocess_exec(
                "python",
                "-S",
                file_path,  # -S to skip site-packages for more isolation
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(), timeout=config.sandbox.timeout
                )

                output = stdout.decode().strip()
                error = stderr.decode().strip()

                if process.returncode == 0:
                    return ToolResult(success=True, output=output)
                else:
                    return ToolResult(success=False, output=output, error=error)

            except asyncio.TimeoutError:
                process.kill()
                return ToolResult(
                    success=False,
                    output="",
                    error=f"Твой код превысил время выполнения ({config.sandbox.timeout}с). Исправь ошибку.",
                )

        except Exception as e:
            logger.error(f"Sandbox execution error: {e}")
            return ToolResult(success=False, output="", error=str(e))
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)
