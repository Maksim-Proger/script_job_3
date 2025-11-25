import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from status import wait_for_service_healthy
from status import check_service_status
from sync import sync_to_server
from sync import delete_from_server
from api_client import send_api_request

logger = logging.getLogger("watcher")

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
        self.executor = ThreadPoolExecutor(max_workers=4)
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

    # def _create_local_symlink(self, file_path: str):
    #     filename = os.path.basename(file_path)
    #     link_path = os.path.join(self.auxiliary_watch_dir, filename)
    #     link_dir = os.path.dirname(link_path)
    #
    #     os.makedirs(link_dir, exist_ok=True)
    #
    #     target = os.path.relpath(file_path, start=link_dir)
    #
    #     try:
    #         if os.path.islink(link_path):
    #             existing = os.readlink(link_path)
    #             if existing == target:
    #                 logger.debug(
    #                     "action=symlink_skip path=%s target=%s",
    #                     link_path, target
    #                 )
    #                 return
    #             else:
    #                 logger.info(
    #                     "action=symlink_replace path=%s old_target=%s new_target=%s",
    #                     link_path, existing, target
    #                 )
    #                 os.remove(link_path)
    #         elif os.path.exists(link_path):
    #             logger.warning(
    #                 "action=symlink_failed path=%s reason=exists_not_symlink",
    #                 link_path
    #             )
    #             return
    #     except OSError as e:
    #         logger.error(
    #             "action=symlink_check_failed path=%s error=%s",
    #             link_path, e
    #         )
    #         return
    #
    #     try:
    #         os.symlink(target, link_path)
    #         logger.info(
    #             "action=symlink_created path=%s target=%s",
    #             link_path, target
    #         )
    #     except OSError as e:
    #         logger.error(
    #             "action=symlink_failed path=%s target=%s error=%s",
    #             link_path, target, e
    #         )

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

        # try:
        #     self._create_local_symlink(yaml_path)
        # except OSError as e:
        #     logger.exception(
        #         "action=symlink_failed path=%s error=%s",
        #         yaml_path, e
        #     )

        # Двойная проверка с попытками
        # if not wait_for_service_healthy(
        #         process_name=self.status_check.process_name,
        #         min_uptime=self.status_check.min_uptime_seconds,
        #         retries=self.status_check.retries,
        #         delay=self.status_check.delay_seconds
        # ):
        #     logger.error("action=service_check_failed")
        #     return
        # Простая разовая проверка
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

        for server in self.servers:
            try:
                self.executor.submit(self._sync_file, yaml_path, server)
                self.executor.submit(send_api_request, server.host, server.api_port, action, yaml_path)
            except RuntimeError as e:
                logger.exception(
                    "action=submit_failed path=%s target=%s error=%s",
                    yaml_path,
                    getattr(server, "host", "<no-host>"), e
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

    # def _delete_local_symlink(self, file_path: str):
    #     filename = os.path.basename(file_path)
    #     link_path = os.path.join(self.auxiliary_watch_dir, filename)
    #     logger.info(
    #         "action=delete_local_symlink_attempt path=%s",
    #         link_path
    #     )
    #
    #     if os.path.islink(link_path):
    #         try:
    #             os.remove(link_path)
    #             logger.info(
    #                 "action=delete_local_symlink_success path=%s",
    #                 link_path
    #             )
    #         except OSError as e:
    #             logger.error(
    #                 "action=delete_local_symlink_failed path=%s error=%s",
    #                 link_path, e
    #             )
    #     elif os.path.exists(link_path):
    #         logger.warning(
    #             "action=delete_local_symlink_failed path=%s reason=exists_not_symlink",
    #             link_path
    #         )
    #     else:
    #         logger.info(
    #             "action=delete_local_symlink_skip path=%s reason=not_exist",
    #             link_path
    #         )

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

        # self._delete_local_symlink(yaml_path)

        for server in self.servers:
            logger.info(
                "action=submit_delete path=%s target=%s",
                yaml_path, server.host
            )
            self.executor.submit(delete_from_server, yaml_path, server)
            self.executor.submit(send_api_request, server.host, server.api_port, "delete", yaml_path)

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
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("action=watcher_keyboard_interrupt")
    finally:
        observer.stop()
        observer.join()
        event_handler.executor.shutdown(wait=True)
        logger.info("action=watcher_stopped")
