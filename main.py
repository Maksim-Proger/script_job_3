import os
from logging_formatter import logging_formatter
from watcher import start_watcher
from config_loader import load_config

def main():
    logging_formatter()
    config = load_config("config.yaml")

    os.makedirs(config.watch_dir, exist_ok=True)
    os.makedirs(config.auxiliary_watch_dir, exist_ok=True)

    start_watcher(
        watch_dir=config.watch_dir,
        auxiliary_watch_dir=config.auxiliary_watch_dir,
        servers=config.servers,
        debounce_seconds=config.debounce_seconds,
        status_check=config.status_check
    )

if __name__ == "__main__":
    main()
