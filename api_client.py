import requests
import json
import os
import logging

logger = logging.getLogger("api")

def send_api_request(server_host: str, server_port: int, action: str, file_path: str):
    url = f"http://{server_host}:{server_port}/managed-object/config"
    headers = {"Content-Type": "application/json"}

    data = {}

    if action == "update":
        data = {"update": {"update-configs": [{"path": file_path}]}}
    elif action == "delete":
        mo_name = os.path.splitext(os.path.basename(file_path))[0]
        data = {"update": {"delete": [{"name": mo_name}]}}
    elif action == "new":
        data = {"update": {"new-configs": [{"path": file_path}]}}
    else:
        logger.error("invalid action: %s", action)
        return

    try:
        response = requests.post(url, headers=headers, data=json.dumps(data), timeout=5)
        response.raise_for_status()
        mo_name = os.path.splitext(os.path.basename(file_path))[0]
        log_data = None

        if action in ("update", "new"):

            log_data = {
                "commit": [{
                    "path": file_path,
                    "mo": mo_name,
                    "status": True,
                    "status-text": "success"
                }]
            }
        elif action == "delete":
            log_data = {
                "delete": [{
                    "mo": mo_name,
                    "status": True,
                    "status-text": "success"
                }]
            }

        logger.info(
            "action=%s result=%s target=%s:%s",
            action, json.dumps(log_data), server_host, server_port
        )

    except requests.HTTPError as e:
        logger.error(
            "action=%s http_error=%s status_code=%s target=%s:%s",
            action, e, getattr(e.response, "status_code", None), server_host, server_port
        )

    except Exception as e:
        logger.error(
            "action=%s exception=%s target=%s:%s",
            action, e, server_host, server_port, exc_info=True
        )
