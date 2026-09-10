# -*- coding: utf-8 -*-

import os
import threading

from . import utils


_lock = threading.Lock()
_package_config_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config")


def _get_storage_dir() -> str:
    """Return the directory used for queue files."""
    configured_dir = os.environ.get("QUEUE_DIR")
    if configured_dir:
        return utils.normalise_path(configured_dir)

    if utils.CONTAINER and os.path.isdir("/config"):
        return "/config"

    from .config import get_default_configs

    for path in get_default_configs():
        normal_path = utils.normalise_path(path)
        if normal_path and os.path.isfile(normal_path):
            return os.path.dirname(normal_path)

    return _package_config_dir


STORAGE_DIR = _get_storage_dir()
QUEUE_FILE = os.path.join(STORAGE_DIR, "queue.txt")
ERROR_FILE = os.path.join(STORAGE_DIR, "queue_errors.txt")
WORKER_LOG_FILE = os.path.join(STORAGE_DIR, "worker.log")

os.makedirs(STORAGE_DIR, exist_ok=True)
for _path in (QUEUE_FILE, ERROR_FILE, WORKER_LOG_FILE):
    open(_path, "a", encoding="utf-8").close()


def add_to_queue(url: str):
    """Append a URL to the queue."""
    with _lock:
        with open(QUEUE_FILE, "a", encoding="utf-8") as file:
            file.write(url + "\n")


def pop_from_queue() -> str | None:
    """Remove and return the first queued URL."""
    with _lock:
        with open(QUEUE_FILE, "r+", encoding="utf-8") as file:
            lines = file.readlines()
            if not lines:
                return None

            first_line = lines[0].strip()
            file.seek(0)
            file.writelines(lines[1:])
            file.truncate()
            return first_line or None
