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
    remote_aux_dir = server.auxiliary_remote_path
    remote_link_path = os.path.join(remote_aux_dir, rel_path)

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
                    logger.info("action=mkdir path=%s target=%s", current, server.host)

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
                logger.debug(
                    "action=remove path=%s target=%s",
                    remote_link_path, server.host
                )
            except IOError:
                pass

            try:
                sftp.symlink(target_relative, remote_link_path)
                logger.info(
                    "action=symlink path=%s target=%s:%s",
                    remote_link_path, server.host, target_relative
                )
            except (IOError, AttributeError) as e:
                logger.debug("sftp.symlink failed (%s), trying ssh ln -s", e)
                cmd = f"ln -sf -- {shlex.quote(target_relative)} {shlex.quote(remote_link_path)}"
                stdin, stdout, stderr = ssh.exec_command(cmd)
                err = stderr.read().decode().strip()
                if err:
                    logger.warning(
                        "action=symlink_via_ssh path=%s target=%s error=%s",
                        remote_link_path, target_relative, err
                    )
                else:
                    logger.info(
                        "action=symlink_via_ssh path=%s target=%s",
                        remote_link_path, target_relative
                    )

    except (paramiko.SSHException, OSError, ConnectionError) as e:
        logger.error(
            "action=upload path=%s target=%s:%s error=%s",
            local_file, server.host, server.ssh_port, e
        )
        raise
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
    if file_path.endswith(".save"):
        if file_path.endswith(".yaml.save"):
            relative_file = file_path[:-5]
        else:
            root, _ = os.path.splitext(file_path)
            relative_file = root + ".yaml"
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

        for path in [remote_file, remote_link]:
            try:
                logger.info(
                    "action=delete path=%s target=%s:%s",
                    path, server.host, server.ssh_port
                )
                sftp.remove(path)
                logger.info(
                    "action=deleted path=%s target=%s:%s",
                    path, server.host, server.ssh_port
                )
            except IOError:
                logger.debug(
                    "action=delete path=%s target=%s:%s not found",
                    path, server.host, server.ssh_port
                )

    except (paramiko.SSHException, OSError) as e:
        logger.error(
            "action=delete path=%s target=%s:%s error=%s",
            relative_file, server.host, server.ssh_port, e
        )
    finally:
        if sftp:
            sftp.close()
        ssh.close()
