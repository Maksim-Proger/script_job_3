import subprocess
import logging
import time

logger = logging.getLogger("status")

def check_service_status(
        process_name: str,
        min_uptime: float
) -> bool:
    try:
        active = subprocess.check_output(
            ["systemctl", "show", "-p", "ActiveState", "--value", process_name],
            text=True
        ).strip()

        if active != "active":
            logger.error("service=%s state=%s expected=active", process_name, active)
            return False

        start_ts = subprocess.check_output(
            ["systemctl", "show", "-p", "ExecMainStartTimestampMonotonic", "--value", process_name],
            text=True
        ).strip()

        if not start_ts.isdigit():
            logger.error(
                "service=%s invalid_start_timestamp=%s",
                process_name, start_ts
            )
            return False

        uptime_sec = int(start_ts) / 1_000_000

        if uptime_sec < min_uptime:
            logger.error("service=%s uptime=%.2f min_required=%.2f", process_name, uptime_sec, min_uptime)
            return False

        logger.info("service=%s healthy uptime=%.2f", process_name, uptime_sec)
        return True

    except (subprocess.CalledProcessError, FileNotFoundError, OSError) as e:
        logger.error("service=%s check_failed=%s", process_name, e)
        return False

def wait_for_service_healthy(
    process_name: str,
    min_uptime: float,
    retries: int = 5,
    delay: float = 10.0
) -> bool:

    logger.info(
        "healthcheck_start service=%s retries=%d delay=%.1f uptime_required=%.2f",
        process_name, retries, delay, min_uptime
    )

    for attempt in range(1, retries + 1):
        if check_service_status(process_name, min_uptime):
            logger.info(
                "healthcheck_ok service=%s attempt=%d",
                process_name, attempt, retries
            )
            return True

        if attempt < retries:
            logger.warning(
                "healthcheck_retry service=%s attempt=%d/%d delay=%.1f",
                process_name, attempt, retries, delay
            )
            time.sleep(delay)

    logger.error(
        "healthcheck_failed service=%s attempts=%d",
        process_name, retries
    )
    return False
