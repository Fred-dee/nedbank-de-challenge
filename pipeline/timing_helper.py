import logging
import os
import time

logger = logging.getLogger("pipeline.timer")


class Timer:
    def __init__(self, label: str):
        self.label = label
        self.start_time = None

    def __enter__(self):
        self.start_time = time.perf_counter()
        logger.info(">>> STAGE START: %s", self.label)
        return self

    def __exit__(self, exc_type, exc, tb):
        elapsed = time.perf_counter() - self.start_time

        # Simple diagnostic for 2GB RAM tracking
        # rss = Resident Set Size (actual physical memory used)
        try:
            import psutil
            process = psutil.Process(os.getpid())
            mem_mb = process.memory_info().rss / 1024 / 1024
            mem_status = f" | RAM: {mem_mb:.2f}MB"
        except ImportError:
            mem_status = ""

        if exc_type:
            logger.error(">>> STAGE FAILED: %s (%.3f sec)%s",
                         self.label, elapsed, mem_status)
        else:
            logger.info(">>> STAGE COMPLETE: %s (%.3f sec)%s",
                        self.label, elapsed, mem_status)
