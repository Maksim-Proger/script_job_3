import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from threading import Thread

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from api_client import send_api_request
from status import check_service_status
from sync import sync_to_server

logger = logging.getLogger("watcher")

task_queue = Queue()

def worker():
    while True:
        action, yaml_path, servers = task_queue.get()
        try:
            for server in servers:
                try:
                    logger.info(
                        "action=action",
                        action
                    )
                    sync_to_server(yaml_path, server)
                except Exception as e:
                    logger.error(
                        "action=sync path=%s target=%s error=%s",
                        yaml_path, server.host, e, exc_info=True
                    )

                try:
                    send_api_request(server.host, server.api_port, action, yaml_path)
                except Exception as e:
                    logger.error(
                        "action=api_request_failed target=%s error=%s",
                        server.host, e, exc_info=True
                    )
        finally:
            task_queue.task_done()

# создаём 2 фоновых worker-а
for _ in range(2):
    Thread(target=worker, daemon=True).start()

class ConfigChangeHandler(FileSystemEventHandler):
    def __init__(self,
                 servers,
                 debounce_seconds: float,
                 watch_dir: str,
                 auxiliary_watch_dir: str,
                 status_check):
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
            logger.debug(
                "action=debounced path=%s",
                os.path.basename(path)
            )
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
                local_file, server.host,
                getattr(server, "ssh_port", "<no-port>"), e, exc_info=True
            )

    def _handle_event_path(self, src: str, event_type: str):
        if not src:
            return

        path = os.path.abspath(src)

        if not path.endswith(".save"):
            logger.debug(
                "action=skip path=%s reason=not_save",
                path
            )
            return

        if not path.startswith(self.watch_dir + os.sep):
            logger.debug(
                "action=skip path=%s reason=outside_watch_dir",
                path
            )
            return

        if path.startswith(self.auxiliary_watch_dir + os.sep):
            logger.debug(
                "action=skip path=%s reason=inside_aux_dir",
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

        yaml_path = path[:-5] + ".yaml"

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

        filename = os.path.basename(yaml_path)
        logger.info(
            "action=change_detected path=%s triggered_by=.save",
            filename
        )

        task_queue.put((action, yaml_path, self.servers))

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
        logger.info(
            "action=file_deleted_event_received event=%s",
            event
        )
        if event.is_directory:
            return

        path = os.path.abspath(event.src_path)
        logger.info(
            "action=file_deleted_path path=%s",
            path
        )

        if not path.startswith(self.watch_dir + os.sep):
            logger.info(
                "action=delete_skip path=%s reason=outside_watch_dir",
                path
            )
            return

        filename = os.path.basename(path)
        logger.info(
            "action=file_deleted path=%s",
            filename
        )

        if path.endswith(".save"):
            yaml_path = path[:-5] + ".yaml"
        else:
            yaml_path = path

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
        task_queue.put(("delete", yaml_path, self.servers))

        # for server in self.servers:
        #     logger.info(
        #         "action=submit_delete path=%s target=%s",
        #         yaml_path, server.host
        #     )
        #     self.executor.submit(delete_from_server, yaml_path, server)
        #     self.executor.submit(send_api_request, server.host, server.api_port, "delete", yaml_path)

    on_deleted = _file_deleted

def start_watcher(watch_dir: str,
                  auxiliary_watch_dir: str,
                  servers,
                  debounce_seconds: float,
                  status_check):
    logger.info(
        "action=start_watcher path=%s",
        watch_dir
    )

    event_handler = ConfigChangeHandler(
        servers,
        debounce_seconds,
        watch_dir,
        auxiliary_watch_dir,
        status_check
    )
    observer = Observer()
    observer.schedule(event_handler, path=watch_dir, recursive=True)
    observer.start()
    logger.info(
        "action=observer_started thread_alive=%s",
        observer.is_alive()
    )

    try:
        observer.join()
    except KeyboardInterrupt:
        logger.info("action=watcher_keyboard_interrupt")
        observer.stop()
    finally:
        observer.join()
        logger.info("action=watcher_stopped")
