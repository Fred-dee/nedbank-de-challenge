"""
Pipeline entry point.

Orchestrates the three medallion architecture stages in order:
  1. Ingest  — reads raw source files into Bronze layer Delta tables
  2. Transform — cleans and conforms Bronze into Silver layer Delta tables
  3. Provision — joins and aggregates Silver into Gold layer Delta tables

The scoring system invokes this file directly:
  docker run ... python pipeline/run_all.py

Do not add interactive prompts, argument parsing that blocks execution,
or any code that reads from stdin. The container has no TTY attached.
"""

from pipeline import bootstrap
from pipeline.bootstrap import setup_performance_logging, debug_container_permissions
from pipeline.ingest import run_ingestion
from pipeline.provision import run_provisioning
from pipeline.timing_helper import Timer
from pipeline.transform import run_transformation

if __name__ == "__main__":
    debug_container_permissions()
    setup_performance_logging()
    with Timer("Pipeline run time"):
        config = bootstrap.initialize_output_directories()
        with Timer("Ingest time"):
            run_ingestion()
        with Timer("Transform time"):
            run_transformation()
        with Timer("Provision time"):
            run_provisioning()
