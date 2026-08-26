# Ecommerce ETL with PySpark

A simple end-to-end data pipeline for processing online retail transaction data using PySpark. The project follows a standard ETL workflow: ingest raw data, clean and validate it, transform it into a usable analytical format, and load the final results into a target destination.

## Overview

This repository demonstrates how to build a batch ETL pipeline using Spark for a retail dataset. It is designed to be easy to follow, extend, and run locally for learning or prototyping.

## Dataset

The source data is the Online Retail dataset from Kaggle:

- https://www.kaggle.com/datasets/vijayuv/onlineretail

This dataset contains online retail transactions and is used as the input for the ingestion and ETL process in this project.

## Project Structure

- `src/1_ingest.py` — loads the raw CSV file into a Spark DataFrame.
- `src/2_clean.py` — cleans invalid, duplicate, and inconsistent records.
- `src/3_transform.py` — creates derived fields and prepares the data for analysis.
- `src/4_load.py` — writes the transformed output to the target storage layer.
- `main.py` — entry point for orchestrating the pipeline.
- `data/raw/online_retail.csv` — source dataset used by the pipeline.
- `logs/` — runtime logs generated during execution.

## Prerequisites

- Python 3.10+
- Java Runtime Environment (JRE) compatible with PySpark
- Spark-compatible environment

## Setup

1. Clone the repository.
2. Create and activate a virtual environment:

   ```bash
   python -m venv venv
   source venv/bin/activate
   ```

3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. Run the ingestion step:

   ```bash
   python src/1_ingest.py
   ```

## Pipeline Flow

1. Ingest raw CSV data into Spark.
2. Clean missing or malformed values.
3. Transform the dataset into analysis-friendly columns and metrics.
4. Load the output to a destination for reporting or downstream processing.

## Notes

- Raw data files under `data/raw/` are excluded from version control.
- Log files generated in `logs/` are also excluded from Git.
- The project structure is intentionally modular to make each ETL stage easier to debug and expand.

## License

This project is provided for educational and demonstration purposes.
