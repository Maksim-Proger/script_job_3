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
        requests.post(url, headers=headers, data=json.dumps(data), timeout=5)
        logger.info(
            "action=%s path=%s target=%s:%s",
            action, file_path, server_host, server_port
        )
    except Exception as e:
        logger.error(
            "action=%s path=%s target=%s:%s error=%s",
            action, file_path, server_host, server_port, e
        )
