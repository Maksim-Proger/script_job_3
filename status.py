import subprocess
import logging

logger = logging.getLogger("status")

def check_service_status(process_name: str, min_uptime: float) -> bool:
    try:
        # Проверяем статус сервиса
        active = subprocess.check_output(
            ["systemctl", "show", "-p", "ActiveState", "--value", process_name],
            text=True
        ).strip()

        if active != "active":
            logger.error("Service %s is not active", process_name)
            return False

        # Проверяем uptime
        start_ts = subprocess.check_output(
            ["systemctl", "show", "-p", "ExecMainStartTimestampMonotonic", "--value", process_name],
            text=True
        ).strip()

        uptime_sec = int(start_ts) / 1_000_000  # systemd monotonic timestamp в секундах
        if uptime_sec < min_uptime:
            logger.error("Service %s uptime %.2fs < %.2fs", process_name, uptime_sec, min_uptime)
            return False

        logger.info("Service %s is healthy (uptime %.2fs)", process_name, uptime_sec)
        return True

    except Exception as e:
        logger.error("Failed to check service %s: %s", process_name, e)
        return False
