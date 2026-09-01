import subprocess
import sys
import logging
import os
import time

os.makedirs('logs', exist_ok=True)

main_logger = logging.getLogger('Ecommerce-ETL.Main')
main_logger.setLevel(logging.INFO)
main_logger.propagate = False

if not main_logger.handlers:
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    file_handler = logging.FileHandler("logs/main.log", mode="w")
    file_handler.setFormatter(formatter)
    main_logger.addHandler(stream_handler)
    main_logger.addHandler(file_handler)

logger = main_logger


def run_stage(module_name):
    logger.info(f'Starting stage: {module_name}')
    start_time = time.time()
    result = subprocess.run(["python", "-m", module_name])
    if result.returncode != 0:
        logger.critical(f'Stage {module_name} failed with exit code {result.returncode}')
        sys.exit(1)
    end_time = time.time()
    logger.info(f'Completed stage: {module_name} (Time taken: {end_time - start_time:.2f} seconds)')


if __name__ == "__main__":
    stages = ["src.ingest", "src.clean", "src.transform", "src.load"]
    
    logger.info("Starting full ETL pipeline run...")
    pipeline_start_time = time.time()
    for stage in stages:
        run_stage(stage)
    pipeline_end_time = time.time()
    logger.info(f"Full ETL pipeline completed successfully. (Total time: {pipeline_end_time - pipeline_start_time:.2f} seconds)")