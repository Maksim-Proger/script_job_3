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

# Очередь для задач
task_queue = Queue()

NUM_WORKERS = 4  # количество воркеров

def wait_for_file_ready(path, timeout=2.0, interval=0.05):
    """Ждём, пока файл появится и станет ненулевого размера"""
    start = time.time()
    while time.time() - start < timeout:
        if os.path.exists(path) and os.path.getsize(path) > 0:
            return True
        time.sleep(interval)
    return False

def worker():
    while True:
        task = task_queue.get()
        if task is None:
            task_queue.task_done()
            break

        action, yaml_path, servers, task_type = task  # task_type: 'sync' или 'delete'

        for server in servers:
            try:
                if task_type == "sync":
                    sync_to_server(yaml_path, server)
                else:
                    delete_from_server(yaml_path, server)

                send_api_request(server.host, server.api_port, action, yaml_path)
            except Exception as e:
                logger.error(
                    "action=task_failed path=%s target=%s error=%s",
                    yaml_path, getattr(server, "host", "<no-host>"), e, exc_info=True
                )
        task_queue.task_done()

# Запуск воркеров
for _ in range(NUM_WORKERS):
    Thread(target=worker, daemon=True).start()


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

    def _handle_event_path(self, src, event_type):
        if not src:
            return

        path = os.path.abspath(src)
        if path.startswith(self.auxiliary_watch_dir + os.sep):
            return
        if not path.startswith(self.watch_dir + os.sep):
            return
        if os.path.isdir(path):
            return
        if not path.endswith(".save"):
            return
        if self._debounce_check(path):
            return

        yaml_path = path[:-5] + ".yaml"

        if event_type in ("created", "modified", "moved"):
            # Ждём, пока файл реально появится и будет готов
            if not wait_for_file_ready(path):
                logger.warning("action=file_not_ready path=%s", path)
                return
            action = "new" if event_type == "created" else "update"
            task_type = "sync"
        else:
            return

        if not check_service_status(
            process_name=self.status_check.process_name,
            min_uptime=self.status_check.min_uptime_seconds
        ):
            logger.error("action=service_check_failed")
            return

        # ставим задачу в очередь
        task_queue.put((action, yaml_path, self.servers, task_type))
        logger.info("action=task_queued path=%s type=%s", yaml_path, task_type)

    def on_created(self, event):
        self._handle_event_path(getattr(event, "dest_path", event.src_path), "created")

    def on_modified(self, event):
        self._handle_event_path(getattr(event, "dest_path", event.src_path), "modified")

    def on_moved(self, event):
        self._handle_event_path(getattr(event, "dest_path", event.src_path), "moved")

    def on_deleted(self, event):
        path = os.path.abspath(event.src_path)
        if path.endswith(".save"):
            yaml_path = path[:-5] + ".yaml"
        else:
            yaml_path = path

        # Удаляем локальный YAML если существует
        if os.path.exists(yaml_path):
            try:
                os.remove(yaml_path)
            except OSError as e:
                logger.error("action=delete_master_yaml_failed path=%s error=%s", yaml_path, e)

        task_queue.put(("delete", yaml_path, self.servers, "delete"))
        logger.info("action=task_queued path=%s type=delete", yaml_path)


def start_watcher(watch_dir, auxiliary_watch_dir, servers, debounce_seconds, status_check):
    logger.info("action=start_watcher path=%s", watch_dir)

    event_handler = ConfigChangeHandler(
        servers, debounce_seconds, watch_dir, auxiliary_watch_dir, status_check
    )
    observer = Observer()
    observer.schedule(event_handler, path=watch_dir, recursive=True)
    observer.start()
    logger.info("action=observer_started thread_alive=%s", observer.is_alive())

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("action=watcher_keyboard_interrupt")
    finally:
        observer.stop()
        observer.join()
        # остановка воркеров
        for _ in range(NUM_WORKERS):
            task_queue.put(None)
        task_queue.join()
        logger.info("action=watcher_stopped")
