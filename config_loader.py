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
    def validate_remote_paths(cls, v):
        if not v or not isinstance(v, str):
            raise ValueError("Path cannot be empty")
        if not v.startswith("/"):
            raise ValueError("Remote paths must be absolute (start with '/')")
        return v

class StatusCheckConfig(BaseModel):
    process_name: str
    min_uptime_seconds: float = Field(ge=0)

class AppConfig(BaseModel):
    watch_dir: str
    auxiliary_watch_dir: str
    debounce_seconds: float = Field(ge=0.1, le=30)
    servers: List[ServerConfig]
    status_check: StatusCheckConfig

    @field_validator("watch_dir", "auxiliary_watch_dir")
    def validate_local_directories(cls, v):
        if not os.path.isdir(v):
            raise ValueError(f"Directory does not exist: {v}")
        return os.path.abspath(v)

def load_config(path: str = "config.yaml") -> AppConfig:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, "r") as f:
        raw = yaml.safe_load(f)

    return AppConfig.model_validate(raw)
