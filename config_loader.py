import os
import yaml
from dataclasses import dataclass
from typing import List

@dataclass
class ServerConfig:
    host: str
    port: int
    username: str
    key_filename: str
    remote_path: str
    auxiliary_remote_path: str

@dataclass
class AppConfig:
    watch_dir: str
    auxiliary_watch_dir: str
    debounce_seconds: float
    servers: List[ServerConfig]

def load_config(path: str = "config.yaml") -> AppConfig:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, "r") as f:
        raw = yaml.safe_load(f)

    required_top = ["watch_dir", "auxiliary_watch_dir", "debounce_seconds", "servers"]
    for k in required_top:
        if k not in raw:
            raise ValueError(f"Missing `{k}` in config.yaml")

    watch_dir = raw["watch_dir"]
    auxiliary_watch_dir = raw["auxiliary_watch_dir"]
    debounce_seconds = float(raw["debounce_seconds"])

    servers = []
    for s in raw["servers"]:
        for key in ["host", "port", "username", "key_filename", "remote_path", "auxiliary_remote_path"]:
            if key not in s:
                raise ValueError(f"Missing `{key}` in server entry")

        servers.append(
            ServerConfig(
                host=s["host"],
                port=int(s["port"]),
                username=s["username"],
                key_filename=s["key_filename"],
                remote_path=s["remote_path"],
                auxiliary_remote_path=s["auxiliary_remote_path"]
            )
        )

    return AppConfig(
        watch_dir=watch_dir,
        auxiliary_watch_dir=auxiliary_watch_dir,
        debounce_seconds=debounce_seconds,
        servers=servers,
    )
