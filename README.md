# Ecommerce ETL with PySpark

This project loads raw online retail data into a PySpark-based ETL pipeline for ingestion, cleaning, transformation, and loading.

## Project structure

- `src/1_ingest.py` reads the raw CSV file into a Spark DataFrame.
- `src/2_clean.py` prepares and cleans the raw dataset.
- `src/3_transform.py` applies transformation logic to produce analytic-ready data.
- `src/4_load.py` writes the processed data to its target destination.
- `main.py` serves as the entry point for the pipeline.

## Setup

1. Create and activate a virtual environment.
2. Install dependencies:
   `pip install -r requirements.txt`
3. Run the ingestion step:
   `python src/1_ingest.py`

## Notes

- Raw data under `data/raw/` is ignored by Git.
- Logs are written to `logs/` and are also ignored by Git.
