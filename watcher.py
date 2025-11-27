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
NUM_WORKERS = 2  # количество воркеров

# Очередь задач
task_queue = Queue()

def worker():
    while True:
        task = task_queue.get()
        if task is None:
            task_queue.task_done()
            break
        action, yaml_path, servers = task
        try:
            for server in servers:
                if action == "delete":
                    delete_from_server(yaml_path, server)
                else:
                    ConfigChangeHandler._sync_file(yaml_path, server)
                send_api_request(server.host, server.api_port, action, yaml_path)
        except Exception as e:
            logger.exception(
                "worker_error action=%s path=%s error=%s",
                action, yaml_path, e
            )
        finally:
            task_queue.task_done()

def wait_for_file(path, timeout=2.0, interval=0.05):
    """Ждём пока файл станет доступен"""
    start = time.time()
    while time.time() - start < timeout:
        if os.path.exists(path) and os.path.getsize(path) > 0:
            return True
        time.sleep(interval)
    return False

# Запуск воркеров
for _ in range(NUM_WORKERS):
    Thread(target=worker, daemon=True).start()


class ConfigChangeHandler(FileSystemEventHandler):
    def __init__(self, servers, debounce_seconds: float, watch_dir: str, auxiliary_watch_dir: str, status_check):
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

    @staticmethod
    def _sync_file(local_file, server):
        try:
            sync_to_server(local_file, server)
        except Exception as e:
            logger.error(
                "action=sync path=%s target=%s:%s error=%s",
                local_file, server.host, getattr(server, "ssh_port", "<no-port>"), e, exc_info=True
            )

    def _handle_event_path(self, src: str, event_type: str):
        if not src:
            return

        path = os.path.abspath(src)

        if not path.endswith(".save"):
            logger.debug("action=skip path=%s reason=not_save", path)
            return

        if not path.startswith(self.watch_dir + os.sep):
            logger.debug("action=skip path=%s reason=outside_watch_dir", path)
            return

        if path.startswith(self.auxiliary_watch_dir + os.sep):
            logger.debug("action=skip path=%s reason=inside_aux_dir", path)
            return

        if os.path.isdir(path):
            return

        if self._debounce_check(path):
            return

        time.sleep(0.15)

        if not os.path.isfile(path):
            return

        yaml_path = path[:-5] + ".yaml"

        if event_type == "created":
            action = "new"
        elif event_type == "modified" or event_type == "moved":
            action = "update"
        else:
            return

        logger.info("action=%s path=%s event_type=%s", action, yaml_path, event_type)

        if not check_service_status(process_name=self.status_check.process_name,
                                    min_uptime=self.status_check.min_uptime_seconds):
            logger.error("action=service_check_failed")
            return

        filename = os.path.basename(yaml_path)
        logger.info("action=change_detected path=%s triggered_by=.save", filename)

        if not wait_for_file(path):
            logger.warning("action=file_not_ready path=%s", path)
            return

        # Кладём задачу в очередь
        task_queue.put((action, yaml_path, self.servers))

    def _file_event(self, event):
        if event.is_directory:
            return
        src = getattr(event, "dest_path", getattr(event, "src_path", None))
        event_type = event.event_type
        self._handle_event_path(src, event_type)

    on_modified = on_created = on_moved = _file_event

    def _file_deleted(self, event):
        if event.is_directory:
            return
        path = os.path.abspath(event.src_path)
        if not path.startswith(self.watch_dir + os.sep):
            return
        yaml_path = path[:-5] + ".yaml" if path.endswith(".save") else path
        # удаляем локально
        if os.path.exists(yaml_path):
            try:
                os.remove(yaml_path)
                logger.info("action=deleted_master_yaml path=%s", yaml_path)
            except OSError as e:
                logger.error("action=delete_master_yaml_failed path=%s error=%s", yaml_path, e)
        # кладём удаление в очередь
        task_queue.put(("delete", yaml_path, self.servers))

    on_deleted = _file_deleted


def start_watcher(watch_dir: str, auxiliary_watch_dir: str, servers, debounce_seconds: float, status_check):
    logger.info("action=start_watcher path=%s", watch_dir)

    event_handler = ConfigChangeHandler(servers, debounce_seconds, watch_dir, auxiliary_watch_dir, status_check)
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

