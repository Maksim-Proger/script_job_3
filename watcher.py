import os
import time
import logging
from queue import Queue
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
from typing import NamedTuple, Any
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from status import check_service_status
from sync import sync_to_server
from sync import delete_from_server
from api_client import send_api_request

logger = logging.getLogger("watcher")
logger.setLevel(logging.INFO)

class SyncTask(NamedTuple):
    action: str
    yaml_path: str
    server: Any

class ConfigChangeHandler(FileSystemEventHandler):
    def __init__(self, servers, debounce_seconds: float, watch_dir: str,
                 auxiliary_watch_dir: str, status_check):
        super().__init__()
        self.servers = servers
        self.debounce_seconds = debounce_seconds
        self.watch_dir = os.path.abspath(watch_dir)
        self.auxiliary_watch_dir = os.path.abspath(auxiliary_watch_dir)
        self.status_check = status_check

        self.task_queue: Queue[SyncTask | None] = Queue()
        self.last_save_trigger = {}  # ключ: путь к .yaml (без .save)
        self.workers = []

        os.makedirs(self.auxiliary_watch_dir, exist_ok=True)

        for i in range(4):
            t = Thread(target=self._worker_loop, daemon=True, name=f"SyncWorker-{i+1}")
            t.start()
            self.workers.append(t)

    def _debounce(self, yaml_path: str) -> bool:
        now = time.time()
        last = self.last_save_trigger.get(yaml_path, 0)
        if now - last < self.debounce_seconds:
            return True
        self.last_save_trigger[yaml_path] = now
        return False

    def _worker_loop(self):
        while True:
            task = self.task_queue.get()
            if task is None:
                break
            action, yaml_path, server = task.action, task.yaml_path, task.server
            try:
                if action == "delete":
                    delete_from_server(yaml_path, server)
                else:
                    sync_to_server(yaml_path, server)
                send_api_request(server.host, server.api_port, action, yaml_path)
                logger.info("sync_ok action=%s file=%s server=%s", action, os.path.basename(yaml_path), server.host)
            except Exception as e:
                logger.error("sync_failed action=%s file=%s server=%s error=%s", action, os.path.basename(yaml_path), server.host, e, exc_info=True)
            finally:
                self.task_queue.task_done()

    def _enqueue(self, yaml_path: str, action: str = "update"):
        if not check_service_status(self.status_check.process_name, self.status_check.min_uptime_seconds):
            logger.warning("service_not_ready skip file=%s", os.path.basename(yaml_path))
            return
        for server in self.servers:
            self.task_queue.put(SyncTask(action, yaml_path, server))
        logger.info("enqueued action=%s file=%s", action, os.path.basename(yaml_path))

    def _trigger_from_save_file(self, save_path: str):
        yaml_path = save_path[:-5] + ".yaml"  # file.yaml.save → file.yaml
        if not yaml_path.startswith(self.watch_dir + os.sep):
            return
        if yaml_path.startswith(self.auxiliary_watch_dir + os.sep):
            return

        if self._debounce(yaml_path):
            return

        # Ждём, пока редактор закончит замену
        for _ in range(8):
            time.sleep(0.08)
            if os.path.isfile(yaml_path):
                logger.info("save_completed file=%s (via .save trigger)", os.path.basename(yaml_path))
                self._enqueue(yaml_path, "update")
                return
        logger.warning("save_timeout file=%s not appeared after .save", os.path.basename(yaml_path))

    # === ГЛАВНОЕ: ловим ТОЛЬКО изменение .save файла ===
    def on_modified(self, event):
        if event.is_directory:
            return
        path = os.path.abspath(event.src_path)
        if path.endswith(".save") and path.startswith(self.watch_dir + os.sep):
            if path.startswith(self.auxiliary_watch_dir + os.sep):
                return
            logger.debug("save_file_modified path=%s", path)
            Thread(target=self._trigger_from_save_file, args=(path,), daemon=True).start()

    # === Создание нового файла .yaml (например, через git, cp, или первый раз без .save) ===
    def on_created(self, event):
        if event.is_directory:
            return
        path = os.path.abspath(event.src_path)
        if path.endswith(".yaml") and path.startswith(self.watch_dir + os.sep):
            if path.startswith(self.auxiliary_watch_dir + os.sep):
                return
            if self._debounce(path):
                return
            time.sleep(0.1)
            if os.path.isfile(path):
                logger.info("new_file_created file=%s", os.path.basename(path))
                self._enqueue(path, "update")

    # === Удаление ===
    def on_deleted(self, event):
        if event.is_directory:
            return
        path = os.path.abspath(event.src_path)
        if not path.startswith(self.watch_dir + os.sep):
            return

        if path.endswith(".yaml"):
            yaml_path = path
        elif path.endswith(".save"):
            yaml_path = path[:-5] + ".yaml"
        else:
            return

        if os.path.exists(yaml_path):
            try:
                os.remove(yaml_path)
                logger.info("local_yaml_removed file=%s", os.path.basename(yaml_path))
            except OSError:
                pass

        logger.info("file_deleted file=%s", os.path.basename(yaml_path))
        self._enqueue(yaml_path, "delete")

    def stop(self):
        for _ in self.workers:
            self.task_queue.put(None)
        for t in self.workers:
            t.join(timeout=10)
        self.task_queue.join()
        logger.info("all_workers_stopped")

def start_watcher(watch_dir: str,
                  auxiliary_watch_dir: str,
                  servers,
                  debounce_seconds: float = 0.9,
                  status_check=None):
    logger.info("=== CONFIG WATCHER STARTED ===")
    handler = ConfigChangeHandler(servers, debounce_seconds, watch_dir, auxiliary_watch_dir, status_check)
    observer = Observer()
    observer.schedule(handler, watch_dir, recursive=True)
    observer.start()
    logger.info("watching directory=%s", watch_dir)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("shutting down...")
    finally:
        observer.stop()
        observer.join()
        handler.stop()
        logger.info("=== WATCHER STOPPED ===")

















# logger = logging.getLogger("watcher")
#
# class ConfigChangeHandler(FileSystemEventHandler):
#     def __init__(self,
#                  servers,
#                  debounce_seconds: float,
#                  watch_dir: str,
#                  auxiliary_watch_dir: str,
#                  status_check):
#         super().__init__()
#         self.servers = servers
#         self.debounce_seconds = debounce_seconds
#         self.watch_dir = os.path.abspath(watch_dir)
#         self.auxiliary_watch_dir = os.path.abspath(auxiliary_watch_dir)
#         self.status_check = status_check
#         self.executor = ThreadPoolExecutor(max_workers=4)
#         self.last_sync_time = {}
#
#         os.makedirs(self.auxiliary_watch_dir, exist_ok=True)
#
#     def _debounce_check(self, path):
#         now = time.time()
#         if now - self.last_sync_time.get(path, 0) < self.debounce_seconds:
#             logger.debug(
#                 "action=debounced path=%s",
#                 os.path.basename(path)
#             )
#             return True
#         self.last_sync_time[path] = now
#         return False
#
#     @staticmethod
#     def _sync_file(local_file, server):
#         try:
#             sync_to_server(local_file, server)
#         except Exception as e:
#             logger.error(
#                 "action=sync path=%s target=%s:%s error=%s",
#                 local_file, server.host,
#                 getattr(server, "ssh_port", "<no-port>"), e, exc_info=True
#             )
#
#     def _handle_event_path(self, src: str, event_type: str):
#         if not src:
#             return
#
#         path = os.path.abspath(src)
#
#         if not path.endswith(".save"):
#             logger.debug(
#                 "action=skip path=%s reason=not_save",
#                 path
#             )
#             return
#
#         if not path.startswith(self.watch_dir + os.sep):
#             logger.debug(
#                 "action=skip path=%s reason=outside_watch_dir",
#                 path
#             )
#             return
#
#         if path.startswith(self.auxiliary_watch_dir + os.sep):
#             logger.debug(
#                 "action=skip path=%s reason=inside_aux_dir",
#                 path
#             )
#             return
#
#         if os.path.isdir(path):
#             return
#
#         if self._debounce_check(path):
#             return
#
#         time.sleep(0.15)
#
#         if not os.path.isfile(path):
#             return
#
#         yaml_path = path[:-5] + ".yaml"
#
#         if event_type == "created":
#             action = "new"
#         else:
#             action = "update"
#
#         logger.info(
#             "action=%s path=%s event_type=%s",
#             action, yaml_path, event_type
#         )
#
#         if not check_service_status(
#                 process_name=self.status_check.process_name,
#                 min_uptime=self.status_check.min_uptime_seconds
#         ):
#             logger.error("action=service_check_failed")
#             return
#
#         filename = os.path.basename(yaml_path)
#         logger.info(
#             "action=change_detected path=%s triggered_by=.save",
#             filename
#         )
#
#         for server in self.servers:
#             try:
#                 self.executor.submit(self._sync_file, yaml_path, server)
#                 self.executor.submit(send_api_request, server.host, server.api_port, action, yaml_path)
#             except RuntimeError as e:
#                 logger.exception(
#                     "action=submit_failed path=%s target=%s error=%s",
#                     yaml_path,
#                     getattr(server, "host", "<no-host>"), e
#                 )
#
#     def _file_event(self, event):
#         if event.is_directory:
#             return
#
#         if hasattr(event, "dest_path") and getattr(event, "dest_path"):
#             src = event.dest_path
#         else:
#             src = event.src_path
#
#         event_type = event.event_type
#
#         self._handle_event_path(src, event_type)
#
#     on_modified = on_created = on_moved = _file_event
#
#     def _file_deleted(self, event):
#         logger.info(
#             "action=file_deleted_event_received event=%s",
#             event
#         )
#         if event.is_directory:
#             return
#
#         path = os.path.abspath(event.src_path)
#         logger.info(
#             "action=file_deleted_path path=%s",
#             path
#         )
#
#         if not path.startswith(self.watch_dir + os.sep):
#             logger.info(
#                 "action=delete_skip path=%s reason=outside_watch_dir",
#                 path
#             )
#             return
#
#         filename = os.path.basename(path)
#         logger.info(
#             "action=file_deleted path=%s",
#             filename
#         )
#
#         if path.endswith(".save"):
#             yaml_path = path[:-5] + ".yaml"
#         else:
#             yaml_path = path
#
#         if os.path.exists(yaml_path):
#             try:
#                 os.remove(yaml_path)
#                 logger.info(
#                     "action=deleted_master_yaml path=%s",
#                     yaml_path
#                 )
#             except OSError as e:
#                 logger.error(
#                     "action=delete_master_yaml_failed path=%s error=%s",
#                     yaml_path, e
#                 )
#
#         for server in self.servers:
#             logger.info(
#                 "action=submit_delete path=%s target=%s",
#                 yaml_path, server.host
#             )
#             self.executor.submit(delete_from_server, yaml_path, server)
#             self.executor.submit(send_api_request, server.host, server.api_port, "delete", yaml_path)
#
#     on_deleted = _file_deleted
#
# def start_watcher(watch_dir: str,
#                   auxiliary_watch_dir: str,
#                   servers,
#                   debounce_seconds: float,
#                   status_check):
#     logger.info(
#         "action=start_watcher path=%s",
#         watch_dir
#     )
#
#     event_handler = ConfigChangeHandler(
#         servers,
#         debounce_seconds,
#         watch_dir,
#         auxiliary_watch_dir,
#         status_check
#     )
#     observer = Observer()
#     observer.schedule(event_handler, path=watch_dir, recursive=True)
#     observer.start()
#     logger.info(
#         "action=observer_started thread_alive=%s",
#         observer.is_alive()
#     )
#
#     try:
#         while True:
#             time.sleep(1)
#     except KeyboardInterrupt:
#         logger.info("action=watcher_keyboard_interrupt")
#     finally:
#         observer.stop()
#         observer.join()
#         event_handler.executor.shutdown(wait=True)
#         logger.info("action=watcher_stopped")
