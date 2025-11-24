import os
import yaml
from typing import List
from pydantic import BaseModel, Field, FilePath, field_validator

class ServerConfig(BaseModel):
    host: str
    ssh_port: int = Field(ge=1, le=65535)
    api_port: int = Field(ge=1, le=65535)
    username: str
    key_filename: FilePath
    remote_path: str
    auxiliary_remote_path: str

    @field_validator("remote_path", "auxiliary_remote_path")
    def validate_remote_paths(cls, path: str):
        if not path or not isinstance(path, str):
            raise ValueError("path is empty")
        if not path.startswith("/"):
            raise ValueError("path must be absolute (start with '/')")
        return path

class StatusCheckConfig(BaseModel):
    process_name: str
    min_uptime_seconds: float = Field(ge=0)
    retries: int = Field(default=5, ge=1)
    delay_seconds: float = Field(default=10.0, ge=0)

class AppConfig(BaseModel):
    watch_dir: str
    auxiliary_watch_dir: str
    debounce_seconds: float = Field(ge=0.1, le=30)
    servers: List[ServerConfig]
    status_check: StatusCheckConfig

    @field_validator("watch_dir", "auxiliary_watch_dir")
    def validate_local_directories(cls, path: str):
        if not os.path.isdir(path):
            raise ValueError(f"directory does not exist: {path}")
        return os.path.abspath(path)

def load_config(path: str = "config.yaml") -> AppConfig:
    if not os.path.exists(path):
        raise FileNotFoundError(f"config not found: {path}")

    with open(path, "r") as fp:
        raw = yaml.safe_load(fp)

    return AppConfig.model_validate(raw)
