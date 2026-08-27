# Ecommerce ETL with PySpark

A simple end-to-end data pipeline for processing online retail transaction data using PySpark. The project follows a standard ETL workflow: ingest raw data, clean and validate it, transform it into a usable analytical format, and load the final results into a target destination.

## Overview

This repository demonstrates how to build a batch ETL pipeline using Spark for a retail dataset. It is designed to be easy to follow, extend, and run locally for learning or prototyping.

## Dataset

The source data is the Online Retail dataset from Kaggle:

- https://www.kaggle.com/datasets/vijayuv/onlineretail

This dataset contains online retail transactions and is used as the input for the ingestion and ETL process in this project.

## Project Structure

- `src/ingest.py` — loads the raw CSV file into a Spark DataFrame.
- `src/clean.py` — casts fields, trims text, checks nulls, and removes rows with required null values.
- `src/transform.py` — creates derived fields and prepares the data for analysis.
- `src/load.py` — writes the transformed output to the target storage layer.
- `main.py` — entry point for orchestrating the pipeline.
- `data/raw/online_retail.csv` — source dataset used by the pipeline.
- `logs/` — runtime logs generated during execution.

## Prerequisites

- Python 3.10+
- Java Runtime Environment (JRE) compatible with PySpark
- Spark-compatible environment

## Setup

Run the commands below from the repository root.

1. Clone the repository and change into it:

   ```bash
   git clone <repository-url>
   cd ecommerce-etl-pyspark
   ```

2. Create and activate a virtual environment:

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. Install dependencies:

   ```bash
   python3 -m pip install -r requirements.txt
   ```

## Running the Pipeline

Run individual ETL stages as Python modules:

```bash
python3 -m src.ingest
python3 -m src.clean
python3 -m src.transform
python3 -m src.load
```

The cleaning stage reads `data/raw/online_retail.csv`, writes its file output to `logs/clean.log`, and uses its own logger without propagating records to the ingestion logger.

To run the orchestrated pipeline:

```bash
python3 main.py
```

## Pipeline Flow

1. Ingest raw CSV data into Spark.
2. Clean missing or malformed values.
3. Transform the dataset into analysis-friendly columns and metrics.
4. Load the output to a destination for reporting or downstream processing.

## Notes

- The raw input file under `data/raw/` is required for local execution and is ignored by default to avoid committing datasets accidentally. To intentionally commit the bundled CSV, run `git add -f data/raw/online_retail.csv`.
- Log files generated in `logs/` are runtime artifacts and should generally remain uncommitted.
- The project structure is intentionally modular to make each ETL stage easier to debug and expand.

## License

This project is provided for educational and demonstration purposes.
