import os
import logging
import paramiko
import shlex

from config_loader import ServerConfig
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger("sync")

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    retry=retry_if_exception_type((paramiko.SSHException, OSError, ConnectionError)),
    reraise=True,
)
def sync_to_server(local_file: str, server: ServerConfig, action: str, watch_dir: str):
    rel_path = os.path.relpath(local_file, start=watch_dir)
    remote_full_path = os.path.join(server.remote_path, rel_path)
    remote_link_path = os.path.join(server.auxiliary_remote_path, rel_path)

    remote_dirs = os.path.dirname(remote_full_path)
    link_dirs = os.path.dirname(remote_link_path)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    sftp = None

    try:
        logger.info(
            "action=connect target=%s:%s user=%s",
            server.host, server.ssh_port, server.username
        )

        ssh.connect(
            hostname=server.host,
            port=server.ssh_port,
            username=server.username,
            key_filename=str(server.key_filename),
            timeout=15,
        )

        sftp = ssh.open_sftp()

        def mkdirs(path):
            parts = path.split('/')
            current = ""
            for part in parts:
                if not part:
                    continue
                current += f"/{part}"
                try:
                    sftp.stat(current)
                except IOError:
                    sftp.mkdir(current)
                    logger.info(
                        "action=mkdir path=%s target=%s",
                        current, server.host
                    )

        mkdirs(remote_dirs)
        mkdirs(link_dirs)

        sftp.put(local_file, remote_full_path)
        logger.info(
            "action=upload path=%s target=%s:%s",
            local_file, server.host, remote_full_path
        )

        if action == "new":
            target_relative = os.path.relpath(remote_full_path, start=link_dirs)

            try:
                sftp.remove(remote_link_path)
            except IOError:
                pass

            try:
                sftp.symlink(target_relative, remote_link_path)
            except (IOError, AttributeError):
                cmd = (
                    f"ln -sf -- "
                    f"{shlex.quote(target_relative)} "
                    f"{shlex.quote(remote_link_path)}"
                )
                ssh.exec_command(cmd)

    finally:
        if sftp:
            sftp.close()
        ssh.close()

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    retry=retry_if_exception_type((paramiko.SSHException, OSError, ConnectionError)),
    reraise=True,
)
def delete_from_server(file_path: str, server: ServerConfig, watch_dir: str):
    if file_path.endswith(".yaml.save"):
        relative_file = file_path[:-5]
    else:
        relative_file = file_path

    rel_path = os.path.relpath(relative_file, start=watch_dir)

    remote_file = os.path.join(server.remote_path, rel_path)
    remote_link = os.path.join(server.auxiliary_remote_path, rel_path)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    sftp = None

    try:
        logger.info(
            "action=connect target=%s:%s user=%s",
            server.host, server.ssh_port, server.username
        )

        ssh.connect(
            hostname=server.host,
            port=server.ssh_port,
            username=server.username,
            key_filename=str(server.key_filename),
            timeout=15,
        )

        sftp = ssh.open_sftp()

        deleted_dirs = set()

        for path in (remote_file, remote_link):
            try:
                sftp.remove(path)
                logger.info(
                    "action=deleted path=%s target=%s",
                    path, server.host
                )
                deleted_dirs.add(os.path.dirname(path))
            except IOError:
                pass

        # === УДАЛЕНИЕ ПУСТЫХ ДИРЕКТОРИЙ ===
        for dir_path in deleted_dirs:
            if dir_path.startswith(server.remote_path):
                _cleanup_empty_dirs(
                    sftp,
                    dir_path,
                    server.remote_path,
                    server
                )

            if dir_path.startswith(server.auxiliary_remote_path):
                _cleanup_empty_dirs(
                    sftp,
                    dir_path,
                    server.auxiliary_remote_path,
                    server
                )

    finally:
        if sftp:
            sftp.close()
        ssh.close()

def _cleanup_empty_dirs(sftp, start_dir: str, stop_dir: str, server: ServerConfig):
    current = start_dir.rstrip("/")

    while current.startswith(stop_dir.rstrip("/")):
        try:
            if sftp.listdir(current):
                break

            sftp.rmdir(current)
            logger.info(
                "action=remove_empty_dir path=%s target=%s",
                current, server.host
            )
        except IOError:
            break

        parent = os.path.dirname(current)
        if parent == current:
            break

        current = parent
