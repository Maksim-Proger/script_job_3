import os
import time
import logging
from queue import Queue
from threading import Thread
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from status import check_service_status
from sync import sync_to_server
from sync import delete_from_server
from api_client import send_api_request

logger = logging.getLogger("watcher")
task_queue = Queue()

def worker():
    while True:
        action, yaml_path, servers = task_queue.get()
        try:
            filename = os.path.basename(yaml_path)
            logger.info("action=worker_start file=%s action=%s", filename, action)

            for server in servers:
                try:
                    if action in ("new", "update"):
                        sync_to_server(yaml_path, server)
                        send_api_request(server.host, server.api_port, action, yaml_path)
                    elif action == "delete":
                        delete_from_server(yaml_path, server)
                        send_api_request(server.host, server.api_port, action, yaml_path)
                except Exception as e:
                    logger.error(
                        "action=worker_error server=%s file=%s error=%s",
                        server.host, filename, e, exc_info=True
                    )

            logger.info("action=worker_done file=%s", filename)

        finally:
            task_queue.task_done()

def start_workers(num_workers: int = 2):
    for _ in range(num_workers):
        t = Thread(target=worker, daemon=True)
        t.start()

class ConfigChangeHandler(FileSystemEventHandler):
    def __init__(self, servers, debounce_seconds, watch_dir, auxiliary_watch_dir, status_check):
        super().__init__()
        self.servers = servers
        self.debounce_seconds = debounce_seconds
        self.watch_dir = os.path.abspath(watch_dir)
        self.auxiliary_watch_dir = os.path.abspath(auxiliary_watch_dir)
        self.status_check = status_check
        self.last_sync_time = {}

        os.makedirs(self.auxiliary_watch_dir, exist_ok=True)

    def _debounce_check(self, path):
        now = time.time()
        if now - self.last_sync_time.get(path, 0) < self.debounce_seconds:
            logger.debug("action=debounced path=%s", os.path.basename(path))
            return True
        self.last_sync_time[path] = now
        return False

    def _handle_change(self, path, event_type):
        if not path.endswith(".save"):
            return

        if not path.startswith(self.watch_dir + os.sep):
            return

        if path.startswith(self.auxiliary_watch_dir + os.sep):
            return

        if os.path.isdir(path):
            return

        if self._debounce_check(path):
            return

        time.sleep(0.15)

        if not os.path.isfile(path):
            return

        yaml_path = path[:-5] + ".yaml"
        filename = os.path.basename(yaml_path)

        action = "new" if event_type == "created" else "update"

        logger.info("action=%s path=%s event_type=%s", action, filename, event_type)

        if not check_service_status(
                process_name=self.status_check.process_name,
                min_uptime=self.status_check.min_uptime_seconds):
            logger.error("action=service_check_failed")
            return

        logger.info("action=enqueue path=%s", filename)

        task_queue.put((action, yaml_path, self.servers))

    def _handle_delete(self, path):
        yaml_path = path[:-5] + ".yaml" if path.endswith(".save") else path

        logger.info("action=file_deleted path=%s", os.path.basename(yaml_path))

        task_queue.put(("delete", yaml_path, self.servers))

    def on_created(self, event): self._on_event(event)
    def on_modified(self, event): self._on_event(event)
    def on_moved(self, event):    self._on_event(event)
    def on_deleted(self, event):  self._on_delete(event)

    def _on_event(self, event):
        if event.is_directory:
            return
        path = getattr(event, "dest_path", event.src_path)
        self._handle_change(os.path.abspath(path), event.event_type)

    def _on_delete(self, event):
        if event.is_directory:
            return
        self._handle_delete(os.path.abspath(event.src_path))

def start_watcher(watch_dir, auxiliary_watch_dir, servers, debounce_seconds, status_check):
    logger.info("action=start_watcher dir=%s", watch_dir)

    start_workers()  # запускаем очередь ВАЖНО: раньше observer

    event_handler = ConfigChangeHandler(
        servers, debounce_seconds, watch_dir, auxiliary_watch_dir, status_check
    )

    observer = Observer()
    observer.schedule(event_handler, watch_dir, recursive=True)
    observer.start()

    logger.info("action=observer_started thread_alive=%s", observer.is_alive())

    try:
        observer.join()
    except KeyboardInterrupt:
        logger.info("action=watcher_keyboard_interrupt")
        observer.stop()
    finally:
        observer.join()
        task_queue.join()
        logger.info("action=watcher_stopped")
