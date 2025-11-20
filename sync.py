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
def sync_to_server(local_file: str, server: ServerConfig):
    filename = os.path.basename(local_file)
    remote_full_path = os.path.join(server.remote_path, filename)
    remote_aux_dir = server.auxiliary_remote_path
    remote_link_path = os.path.join(remote_aux_dir, filename)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    sftp = None

    try:
        logger.debug(f"SSH → {server.username}@{server.host}:{server.port}")
        ssh.connect(
            hostname=server.host,
            port=server.port,
            username=server.username,
            key_filename=server.key_filename,
            timeout=15,
        )

        sftp = ssh.open_sftp()

        try:
            sftp.stat(server.remote_path)
        except IOError:
            logger.warning(f"mkdir {server.remote_path} on {server.host}")
            sftp.mkdir(server.remote_path)

        try:
            sftp.stat(remote_aux_dir)
        except IOError:
            logger.info("mkdir %s on %s", remote_aux_dir, server.host)
            sftp.mkdir(remote_aux_dir)

        sftp.put(local_file, remote_full_path)
        logger.info(f"Uploaded {os.path.basename(local_file)} → {server.host}:{remote_full_path}")

        target_relative = os.path.relpath(remote_full_path, start=remote_aux_dir)

        try:
            # st = sftp.lstat(remote_link_path)
            logger.debug("Remote link exists (will replace): %s on %s", remote_link_path, server.host)
            sftp.remove(remote_link_path)
        except IOError:
            pass

        try:
            sftp.symlink(target_relative, remote_link_path)
            logger.info("Created symlink on %s: %s → %s", server.host, remote_link_path, target_relative)
        except (IOError, AttributeError) as e:
            logger.debug("sftp.symlink failed (%s), trying ssh ln -s", e)
            cmd = f"ln -sf -- {shlex.quote(target_relative)} {shlex.quote(remote_link_path)}"
            stdin, stdout, stderr = ssh.exec_command(cmd)
            err = stderr.read().decode().strip()
            if err:
                logger.warning("ssh ln created stderr: %s", err)
            else:
                logger.info("Created symlink on %s via ssh: %s → %s", server.host, remote_link_path, target_relative)

    except Exception as e:
        logger.error(f"Upload failed {local_file} → {server.host} | {e}")
        raise
    finally:
        if sftp is not None:
            try:
                sftp.close()
            except Exception:
                pass
        try:
            ssh.close()
        except Exception:
            pass

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    retry=retry_if_exception_type((paramiko.SSHException, OSError, ConnectionError)),
    reraise=True,
)
def delete_from_server(file_path: str, server: ServerConfig):
    # Тестируем доработку логики удаления
    # filename = os.path.basename(file_path)
    if file_path.endswith(".save"):
        filename = os.path.basename(file_path[:-5] + ".yaml")
    else:
        filename = os.path.basename(file_path)
    # Тестируем доработку логики удаления

    remote_file = os.path.join(server.remote_path, filename)
    remote_link = os.path.join(server.auxiliary_remote_path, filename)
    logger.info("delete_from_server called for %s on %s", filename, server.host)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    sftp = None

    try:
        ssh.connect(
            hostname=server.host,
            port=server.port,
            username=server.username,
            key_filename=server.key_filename,
            timeout=15,
        )
        sftp = ssh.open_sftp()

        for path in [remote_file, remote_link]:
            try:
                logger.info("Attempting to remove %s on %s", path, server.host)
                sftp.remove(path)
                logger.info("Removed from server %s: %s", server.host, path)
            except IOError:
                logger.debug("File not found on server %s: %s", server.host, path)

    except Exception as e:
        logger.error("Failed to delete from server %s: %s", server.host, e)
    finally:
        if sftp:
            sftp.close()
        ssh.close()
