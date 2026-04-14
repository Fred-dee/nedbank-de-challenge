import logging
import os
import pwd
import sys
from pathlib import Path

from pipeline.pipeline_config import PipelineConfig


def initialize_output_directories(config: PipelineConfig):
    """
    Load pipeline config and ensure all configured output directories exist.

    This is intended to run once at startup before any pipeline stage writes files.
    """

    bronze_output = config.get("output.bronze_path")
    silver_output = config.get("output.silver_path")
    gold_output = config.get("output.gold_path")


    for path in (bronze_output, silver_output, gold_output):
        if not os.path.exists(path):
            print(f"Creating output directory: {path}")
            Path(path).mkdir(parents=True, exist_ok=True)
            print(f"Created output directory: {path}")



def setup_performance_logging():
    # Use a specific format to make timing logs stand out
    log_format = '%(asctime)s [%(levelname)s] [%(name)s] %(message)s'

    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    # Prevent library noise (like DuckDB/Boto3) from drowning out your timers
    logging.getLogger("duckdb").setLevel(logging.WARNING)


def debug_container_permissions():
    print("--- CONTAINER DEBUG START ---")
    # 1. Who am I?
    uid = os.getuid()
    user = pwd.getpwuid(uid).pw_name
    print(f"Current User: {user} (UID: {uid})")

    # 2. Check path accessibility
    target_path = "/data/output"
    print(f"Checking path: {target_path}")

    if os.path.exists(target_path):
        stats = os.stat(target_path)
        print(f"Path Permissions: {oct(stats.st_mode)}")
        print(f"Path Owner UID: {stats.st_uid}")
        print(f"Writable by current user? {os.access(target_path, os.W_OK)}")
    else:
        print(f"CRITICAL: {target_path} does not exist!")
    print("--- CONTAINER DEBUG END ---")
