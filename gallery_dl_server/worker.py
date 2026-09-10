# -*- coding: utf-8 -*-

import logging
import multiprocessing
import queue
import threading
import time

from multiprocessing.queues import Queue

from . import download as download_module, output
from .queue import ERROR_FILE, WORKER_LOG_FILE, pop_from_queue


_worker_thread: threading.Thread | None = None
_worker_lock = threading.Lock()


def _get_logger() -> logging.Logger:
    logger = logging.getLogger("gallery_dl_server.worker")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not logger.handlers:
        handler = logging.FileHandler(WORKER_LOG_FILE, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(handler)

    return logger


def start_worker():
    """Start the queue worker once."""
    global _worker_thread

    with _worker_lock:
        if _worker_thread is None or not _worker_thread.is_alive():
            _worker_thread = threading.Thread(
                target=worker_loop,
                name="gallery-dl-worker",
                daemon=True,
            )
            _worker_thread.start()


def worker_loop():
    """Process queued URLs one at a time."""
    logger = _get_logger()
    logger.info("Worker started")

    while True:
        url = pop_from_queue()
        if url:
            try:
                download(url)
            except Exception as error:
                logger.exception("Download failed for %s", url)
                with open(ERROR_FILE, "a", encoding="utf-8") as file:
                    file.write(f"{url}: {type(error).__name__}: {error}\n")
        else:
            time.sleep(2)


def download(url: str):
    """Run the project's download job in a separate process."""
    logger = _get_logger()
    logger.info("Starting download: %s", url)

    log_queue: Queue[dict] = multiprocessing.Queue()
    return_status: Queue[int] = multiprocessing.Queue()
    process = multiprocessing.Process(
        target=download_module.run,
        args=(url, {"video-options": "none-selected"}, log_queue, return_status, output.args),
    )
    process.start()

    while True:
        if log_queue.empty() and not process.is_alive():
            break
        try:
            record = output.dict_to_record(log_queue.get(timeout=1))
            logger.log(record.levelno, record.getMessage())
        except queue.Empty:
            continue

    process.join()

    try:
        exit_code = return_status.get(block=False)
    except queue.Empty:
        exit_code = process.exitcode

    if exit_code != 0:
        raise RuntimeError(f"Download failed with exit code: {exit_code}")

    logger.info("Download completed: %s", url)
