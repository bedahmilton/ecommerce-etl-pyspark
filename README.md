# Ecommerce ETL with PySpark

A simple end-to-end data pipeline for processing online retail transaction data using PySpark. The project follows a standard ETL workflow: ingest raw data, clean and validate it, transform it into a usable analytical format, and load the final results into a target destination.

## Overview

This repository demonstrates how to build a batch ETL pipeline using Spark for a retail dataset. It is designed to be easy to follow, extend, and run locally for learning or prototyping.

## Dataset

The source data is the Online Retail dataset from Kaggle:

- https://www.kaggle.com/datasets/vijayuv/onlineretail

This dataset contains online retail transactions and is used as the input for the ingestion and ETL process in this project.
To be used on the raw input file under `data/raw/`

## Project Structure

- `src/ingest.py` — loads the raw CSV file into a Spark DataFrame.
- `src/clean.py` — casts fields, trims text, checks nulls, removes invalid/duplicate rows, and saves cleaned output as Parquet (`data/processed/cleaned_data.parquet`).
- `src/transform.py` — reads cleaned Parquet, derives analytical columns, computes revenue aggregations, and saves each result as Parquet.
- `src/load.py` — writes the final transformed output to its destination. *(pending)*
- `main.py` — entry point for orchestrating the pipeline.
- `data/raw/online_retail.csv` — source dataset used by the pipeline.
- `data/processed/` — intermediate Parquet outputs from cleaning and transformation stages (not committed to git).
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

1. **Ingest** — load raw CSV data into a Spark DataFrame.
2. **Clean** — validate and correct field types, remove invalid or incomplete records. Saves cleaned output to `data/processed/cleaned_data.parquet`.
3. **Transform** — derive analysis-ready columns and metrics, then compute aggregations. Reads from the cleaned Parquet file; saves each result to `data/processed/`.
4. **Load** — write the final output to a destination for reporting or downstream processing. *(pending)*

Each stage is decoupled: rather than importing functions from the previous stage, every stage reads the previous stage's saved Parquet output. This keeps stages independently testable and mirrors how real pipelines hand off data through storage.

## Data Cleaning Summary

Cleaning steps applied in `src/clean.py`, in order:

- Cast `InvoiceDate` string → timestamp
- Trim whitespace in `Description` and `Country`
- Drop rows with null `CustomerID` or `Description`
- Filter out `Quantity <= 0`
- Filter out `UnitPrice <= 0`
- Remove duplicate rows

**Results:**

| Metric | Value |
|---|---|
| Starting rows | 541,909 |
| Removed — null `CustomerID`/`Description` | 135,080 |
| Removed — invalid `Quantity` | 8,905 |
| Removed — invalid `UnitPrice` | 40 |
| Removed — duplicates | 5,192 |
| Final rows | 392,692 |
| Rows kept | 72.46% |

## Data Transformation Summary

Transformation steps applied in `src/transform.py`, reading from `data/processed/cleaned_data.parquet`:

- `TotalPrice` — derived column: `Quantity * UnitPrice`
- `Year`, `Month`, `YearMonth` — extracted from `InvoiceDate`
- **Revenue by country** — total `TotalPrice` grouped by `Country`, sorted highest first
- **Revenue by month** — total `TotalPrice` grouped by `YearMonth`
- **Top products by revenue** — total `TotalPrice` grouped by `Description`, sorted highest first

Each result is saved individually as Parquet:

| Output | File |
|---|---|
| Transaction-level data (with `TotalPrice`, `Year`, `Month`) | `data/processed/transactions.parquet` |
| Revenue by country | `data/processed/country_revenue.parquet` |
| Revenue by month | `data/processed/monthly_revenue.parquet` |
| Top products by revenue | `data/processed/product_revenue.parquet` |

## Notes

- The raw input file under `data/raw/` is required for local execution.
- `data/processed/` is excluded from version control (see `.gitignore`) — these are generated intermediate outputs from `clean.py` and `transform.py`. Running the pipeline stages locally will recreate them.
- Log files generated in `logs/` are runtime artifacts and should generally remain uncommitted.
- The project structure is intentionally modular to make each ETL stage easier to debug and expand.

## License

This project is provided for educational and demonstration purposes.