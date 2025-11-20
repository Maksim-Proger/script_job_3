import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from sync import sync_to_server
from sync import delete_from_server

logger = logging.getLogger("watcher")

class ConfigChangeHandler(FileSystemEventHandler):
    def __init__(self,
                 servers,
                 debounce_seconds: float,
                 watch_dir: str,
                 auxiliary_watch_dir: str):
        super().__init__()
        self.servers = servers
        self.debounce_seconds = debounce_seconds
        self.watch_dir = os.path.abspath(watch_dir)
        self.auxiliary_watch_dir = os.path.abspath(auxiliary_watch_dir)
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.last_sync_time = {}

        os.makedirs(self.auxiliary_watch_dir, exist_ok=True)

    def _debounce_check(self, path):
        now = time.time()
        if now - self.last_sync_time.get(path, 0) < self.debounce_seconds:
            logger.debug("Skip %s — debounced", os.path.basename(path))
            return True
        self.last_sync_time[path] = now
        return False

    @staticmethod
    def _sync_file(local_file, server):
        try:
            sync_to_server(local_file, server)
        except Exception as e:
            logger.error("Failed → %s:%s | %s", server.host, local_file, e, exc_info=True)

    def _create_local_symlink(self, file_path: str):
        filename = os.path.basename(file_path)
        link_path = os.path.join(self.auxiliary_watch_dir, filename)
        link_dir = os.path.dirname(link_path)

        os.makedirs(link_dir, exist_ok=True)

        target = os.path.relpath(file_path, start=link_dir)

        try:
            if os.path.islink(link_path):
                existing = os.readlink(link_path)
                if existing == target:
                    logger.debug("Local symlink already correct: %s -> %s", link_path, target)
                    return
                else:
                    logger.info("Replacing local symlink %s (was %s -> %s)", link_path, existing, target)
                    os.remove(link_path)
            elif os.path.exists(link_path):
                logger.warning("Cannot create symlink, path exists and is not a symlink: %s", link_path)
                return
        except OSError as e:
            logger.error("Error checking existing link %s: %s", link_path, e)
            return

        try:
            os.symlink(target, link_path)
            logger.info("Local symlink created: %s -> %s", link_path, target)
        except Exception as e:
            logger.error("Failed to create local symlink %s -> %s: %s", link_path, target, e)

    def _handle_event_path(self, src: str):
        if not src:
            return

        path = os.path.abspath(src)

        if not path.endswith(".save"):
            logger.debug("Not a .save file, skipping: %s", path)
            return

        if not path.startswith(self.watch_dir + os.sep):
            logger.debug("Event outside watch_dir, skipping: %s", path)
            return

        if path.startswith(self.auxiliary_watch_dir + os.sep):
            logger.debug("Event inside auxiliary dir, skipping: %s", path)
            return

        if os.path.isdir(path):
            return

        time.sleep(0.15)

        if not os.path.isfile(path):
            return

        if self._debounce_check(path):
            return

        # convert .save → .yaml
        yaml_path = path[:-5] + ".yaml"

        if not os.path.isfile(yaml_path):
            logger.warning("YAML counterpart does not exist for %s (expected %s)", path, yaml_path)
            return

        # create symlink for .yaml, not .save
        try:
            self._create_local_symlink(yaml_path)
        except Exception:
            logger.exception("Failed to create local symlink for %s", yaml_path)

        filename = os.path.basename(yaml_path)
        logger.info("Change detected (triggered by .save) → %s", filename)

        for server in self.servers:
            try:
                self.executor.submit(self._sync_file, yaml_path, server)
            except Exception:
                logger.exception("Failed to submit sync job for %s -> %s", yaml_path,
                                 getattr(server, "host", "<no-host>"))

    def _file_event(self, event):
        if event.is_directory:
            return

        if hasattr(event, "dest_path") and getattr(event, "dest_path"):
            src = event.dest_path
        else:
            src = event.src_path

        self._handle_event_path(src)

    on_modified = on_created = on_moved = _file_event

    def _delete_local_symlink(self, file_path: str):
        filename = os.path.basename(file_path)
        link_path = os.path.join(self.auxiliary_watch_dir, filename)
        logger.info("Attempting to remove local symlink: %s", link_path)

        if os.path.islink(link_path):
            try:
                os.remove(link_path)
                logger.info("Removed local symlink: %s", link_path)
            except Exception as e:
                logger.error("Failed to remove local symlink %s: %s", link_path, e)
        elif os.path.exists(link_path):
            logger.warning("Cannot remove %s — exists but is not a symlink", link_path)
        else:
            logger.info("Symlink does not exist: %s", link_path)

    def _file_deleted(self, event):
        logger.info("on_deleted event received: %s", event)
        if event.is_directory:
            return

        path = os.path.abspath(event.src_path)
        logger.info("Deleted file path: %s", path)

        if not path.startswith(self.watch_dir + os.sep):
            logger.info("Deleted file outside watch_dir, skipping: %s", path)
            return

        filename = os.path.basename(path)
        logger.info("File deleted → %s", filename)

        self._delete_local_symlink(path)

        for server in self.servers:
            logger.info("Submitting delete_from_server for %s -> %s", path, server.host)
            self.executor.submit(delete_from_server, path, server)

    on_deleted = _file_deleted

def start_watcher(watch_dir: str,
                  auxiliary_watch_dir: str,
                  servers,
                  debounce_seconds: float):
    logger.info("Starting watcher → %s", watch_dir)

    event_handler = ConfigChangeHandler(
        servers,
        debounce_seconds,
        watch_dir,
        auxiliary_watch_dir
    )
    observer = Observer()
    observer.schedule(event_handler, path=watch_dir, recursive=True)
    observer.start()
    logger.info("Observer started (thread alive=%s)", observer.is_alive())

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received — stopping watcher")
    finally:
        observer.stop()
        observer.join()
        event_handler.executor.shutdown(wait=True)
        logger.info("Watcher stopped")
