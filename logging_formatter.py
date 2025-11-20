import logging
import sys

def logging_formatter(level=logging.INFO):
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(level)

    if not root.handlers:
        root.addHandler(handler)
