import logging
import os
import threading
import time
from queue import Queue
from threading import Thread

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from api_client import send_api_request
from status import check_service_status
from sync import sync_to_server
from sync import delete_from_server

logger = logging.getLogger("watcher")

task_queue = Queue()
active_tasks = set()
active_tasks_lock = threading.Lock()

def worker(watch_dir: str):
    while True:
        try:
            action, yaml_path, servers = task_queue.get()
            try:
                for server in servers:
                    sync_ok = False
                    try:
                        if action == "delete":
                            delete_from_server(yaml_path, server, watch_dir)
                        else:
                            sync_to_server(yaml_path, server, action, watch_dir)
                        sync_ok = True
                    except Exception as e:
                        logger.error(
                            "action=sync path=%s target=%s error=%s",
                            yaml_path, server.host, e, exc_info=True
                        )

                    if sync_ok:
                        try:
                            send_api_request(server.host, server.api_port, action, yaml_path)
                        except Exception as e:
                            logger.error(
                                "action=api_request_failed target=%s error=%s",
                                server.host, e, exc_info=True
                            )
            finally:
                task_queue.task_done()
                with active_tasks_lock:
                    active_tasks.discard(yaml_path)
        except Exception:
            logger.exception("action=worker_fatal_error")

def is_yaml_consistent(yaml_path: str) -> bool:
    save_path = yaml_path + ".save"

    if not os.path.isfile(yaml_path):
        logger.error(
            "action=consistency_check_failed reason=yaml_missing path=%s",
            yaml_path
        )
        return False

    if not os.path.isfile(save_path):
        logger.error(
            "action=consistency_check_failed reason=save_missing path=%s",
            save_path
        )
        return False

    try:
        with open(yaml_path, "rb") as f1, open(save_path, "rb") as f2:
            if f1.read() != f2.read():
                logger.error(
                    "action=consistency_check_failed reason=content_mismatch path=%s",
                    yaml_path
                )
                return False
    except OSError as e:
        logger.error(
            "action=consistency_check_failed reason=io_error path=%s error=%s",
            yaml_path, e
        )
        return False

    logger.info(
        "action=consistency_check_ok path=%s",
        yaml_path
    )
    return True

class ConfigChangeHandler(FileSystemEventHandler):
    def __init__(self,
                 servers,
                 debounce_seconds: float,
                 watch_dir: str,
                 status_check):
        super().__init__()
        self.servers = servers
        self.debounce_seconds = debounce_seconds
        self.watch_dir = os.path.abspath(watch_dir)
        self.status_check = status_check
        self.last_sync_time = {}

    def _debounce_check(self, path):
        now = time.time()
        if now - self.last_sync_time.get(path, 0) < self.debounce_seconds:
            logger.debug(
                "action=debounced path=%s",
                os.path.basename(path)
            )
            return True
        self.last_sync_time[path] = now
        return False

    def _handle_event_path(self, src: str, event_type: str):

        if not src:
            return

        path = os.path.abspath(src)

        if not path.endswith(".yaml"):
            logger.debug(
                "action=skip path=%s reason=not_yaml",
                path
            )
            return

        if not path.startswith(self.watch_dir + os.sep):
            logger.debug(
                "action=skip path=%s reason=outside_watch_dir",
                path
            )
            return

        if os.path.isdir(path):
            return

        if self._debounce_check(path):
            return

        time.sleep(0.15)

        if not os.path.isfile(path):
            return

        yaml_path = path

        if event_type == "created":
            action = "new"
        else:
            action = "update"

        logger.info(
            "action=%s path=%s event_type=%s",
            action, yaml_path, event_type
        )

        if not check_service_status(
                process_name=self.status_check.process_name,
                min_uptime=self.status_check.min_uptime_seconds
        ):
            logger.error("action=service_check_failed")
            return

        if not is_yaml_consistent(yaml_path):
            logger.error(
                "action=sync_skipped reason=inconsistent_yaml path=%s",
                yaml_path
            )
            return

        with active_tasks_lock:
            if yaml_path not in active_tasks:
                active_tasks.add(yaml_path)
                task_queue.put((action, yaml_path, self.servers))
            else:
                logger.debug(
                    "action=skip_duplicate_task path=%s",
                    yaml_path
                )

    def _file_event(self, event):
        if event.is_directory:
            return

        if hasattr(event, "dest_path") and getattr(event, "dest_path"):
            src = event.dest_path
        else:
            src = event.src_path

        event_type = event.event_type

        self._handle_event_path(src, event_type)

    on_modified = on_created = on_moved = _file_event

    def _file_deleted(self, event):
        if event.is_directory:
            return

        path = os.path.abspath(event.src_path)

        if not path.startswith(self.watch_dir + os.sep):
            return

        if path.endswith(".yaml.save"):
            yaml_path = path[:-5]
        elif path.endswith(".yaml"):
            yaml_path = path
        else:
            return

        logger.info(
            "action=file_deleted yaml_path=%s",
            yaml_path
        )

        if os.path.exists(yaml_path):
            try:
                os.remove(yaml_path)
                logger.info(
                    "action=deleted_master_yaml path=%s",
                    yaml_path
                )
            except OSError as e:
                logger.error(
                    "action=delete_master_yaml_failed path=%s error=%s",
                    yaml_path, e
                )
                return

        current_dir = os.path.dirname(yaml_path)

        while (
                current_dir.startswith(self.watch_dir + os.sep)
                and current_dir != self.watch_dir
        ):
            try:
                if os.listdir(current_dir):
                    break
                os.rmdir(current_dir)
                logger.info(
                    "action=deleted_empty_dir path=%s",
                    current_dir
                )
                current_dir = os.path.dirname(current_dir)
            except OSError:
                break

        with active_tasks_lock:
            if yaml_path not in active_tasks:
                active_tasks.add(yaml_path)
                task_queue.put(("delete", yaml_path, self.servers))

    on_deleted = _file_deleted

def start_watcher(
        watch_dir: str,
        servers,
        debounce_seconds: float,
        status_check
):
    logger.info(
        "action=start_watcher path=%s",
        watch_dir
    )

    event_handler = ConfigChangeHandler(
        servers,
        debounce_seconds,
        watch_dir,
        status_check
    )

    observer = Observer()
    observer.schedule(event_handler, path=watch_dir, recursive=True)
    observer.start()
    logger.info(
        "action=observer_started thread_alive=%s",
        observer.is_alive()
    )

    Thread(target=worker, args=(watch_dir,), daemon=True).start()

    try:
        observer.join()
    except KeyboardInterrupt:
        observer.stop()
        logger.info("action=watcher_keyboard_interrupt")
    finally:
        observer.join()
        logger.info("action=watcher_stopped")
