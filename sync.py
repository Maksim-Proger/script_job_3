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
def sync_to_server(local_file: str, server: ServerConfig, action: str):
    filename = os.path.basename(local_file)
    remote_full_path = os.path.join(server.remote_path, filename)
    remote_aux_dir = server.auxiliary_remote_path
    remote_link_path = os.path.join(remote_aux_dir, filename)

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

        try:
            sftp.stat(server.remote_path)
        except IOError:
            logger.info(
                "action=mkdir path=%s target=%s:%s",
                server.remote_path, server.host, server.ssh_port
            )
            sftp.mkdir(server.remote_path)

        try:
            sftp.stat(remote_aux_dir)
        except IOError:
            logger.info(
                "action=mkdir path=%s target=%s:%s",
                remote_aux_dir, server.host, server.ssh_port
            )
            sftp.mkdir(remote_aux_dir)

        sftp.put(local_file, remote_full_path)
        logger.info(
            "action=upload path=%s target=%s:%s",
            filename, server.host, remote_full_path
        )

        target_relative = os.path.relpath(remote_full_path, start=remote_aux_dir)

        logger.info("action=info path=%s target=%s:%s", action)
        if action == "new_configs":
            try:
                sftp.remove(remote_link_path)
                logger.debug(
                    "action=remove path=%s target=%s:%s",
                    remote_link_path, server.host, server.ssh_port
                )

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
                            "action=symlink_via_ssh path=%s target=%s:%s error=%s",
                            remote_link_path, server.host, target_relative, err
                        )
                    else:
                        logger.info(
                            "action=symlink_via_ssh path=%s target=%s:%s",
                            remote_link_path, server.host, target_relative
                        )
            except IOError:
                pass

        else:
            logger.debug(
                "action=skip_symlink_update reason=update_event path=%s",
                remote_link_path
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
def delete_from_server(file_path: str, server: ServerConfig):
    if file_path.endswith(".save"):
        filename = os.path.basename(file_path[:-5] + ".yaml")
    else:
        filename = os.path.basename(file_path)

    remote_file = os.path.join(server.remote_path, filename)
    remote_link = os.path.join(server.auxiliary_remote_path, filename)

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
            filename, server.host, server.ssh_port, e
        )
    finally:
        if sftp:
            sftp.close()
        ssh.close()
