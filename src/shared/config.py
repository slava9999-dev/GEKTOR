import os
from typing import List, Optional
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
import yaml

class SystemConfig(BaseModel):
    device: str = "cuda"
    vram_limit_gb: float = 7.8
    log_level: str = "INFO"
    language: str = "ru"

class ModelParams(BaseModel):
    path: str
    n_ctx: int = 32768
    n_gpu_layers: int = 35
    temperature: float = 0.7

class MemoryConfig(BaseModel):
    vector_store_path: str = "memory/gerald_v2.lance"
    embedding_model: str = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    chunk_size: int = 1500
    chunk_overlap: int = 200

class PathsConfig(BaseModel):
    workspace: str = "gerald_workspace"
    tmp_run: str = "gerald_workspace/tmp_run"
    logs: str = "logs/gerald_sys.log"

class SandboxConfig(BaseModel):
    timeout: int = 15

class GeraldConfig(BaseSettings):
    system: SystemConfig
    models: dict[str, ModelParams]
    memory: MemoryConfig
    paths: PathsConfig
    sandbox: SandboxConfig

    @classmethod
    def load(cls, config_path: str = "config.yaml") -> "GeraldConfig":
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_path, "r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f)
            
        return cls(**config_data)

# Global config instance
try:
    config = GeraldConfig.load()
except Exception:
    # Fallback for initialization or if file doesn't exist yet during build
    config = None
