import logging
import os
import sys
from pyspark.sql import SparkSession

# Ensure logs folder exists before initializing FileHandler
os.makedirs('logs', exist_ok=True)
 
# configure basic logging format
logging.basicConfig(
    level= logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers= [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/ingest.log", mode="a")
    ],
)

logger = logging.getLogger('Ecommerce-ETL.Ingest')

# Raw Data file Path
data_path = 'data/raw/online_retail.csv'


def get_spark_session():
    logger.info('Spark Session Initialized ...')

    spark = SparkSession.builder \
        .appName("EcommerceETL") \
        .getOrCreate()
    return spark



# Reading the raw data with permissive mode 
def read_csv(spark, data_path):
    logger.info(f'Loading Raw CSV from data path : {data_path}')

    #check if file path exists 
    if not os.path.exists(data_path):
        logger.error(f'File path provided does not exist : {data_path}')

    try:
        df = (
            spark.read.format("csv")
            .option("header", "True")
            .option("inferSchema", "True")
            .option("mode", "PERMISSIVE")
            .load(data_path)
        )
        return df

    except Exception as e:
        # Save a detailed error message to the log file.
        # 'exc_info=True' adds the exact file and line number where the crash happened.
        logger.error(f"Failed to load CSV file at '{data_path}': {str(e)}", exc_info=True)
        
        # Stop the program and show a clear message about what went wrong.
        # 'from e' links this new error to the original error so we can trace both.
        raise RuntimeError(f"Spark read error on path '{data_path}': {e}") from e
    
#Reads raw csv into a spark dataframe
if __name__ == "__main__":
    spark = get_spark_session()
    try:
        # Try to read the file
        df = read_csv(spark, data_path)

        # Print the data structure (columns and data types) to the terminal
        logging.info(f'Schema is : {df.schema.treeString()}')

        # Generate and display basic math stats (mean, min, max) for numeric columns
        logger.info('Calculating summary statistics 📊 ...')
        df.describe().show()

        # Log basic data properties like names of columns and total number of records
        logger.info(f"DataFrame columns: {df.columns}")
        logger.info(f"Total row count: {df.count()}")

        # Print the first 5 rows of the dataset to verify it looks correct
        df.show(5)

    except (FileNotFoundError, RuntimeError) as err:
        # Catch any critical pipeline errors, log the failure, and exit with an error code
        logger.critical(f'Ingestion pipeline execution failed ❌ : {err}')
        sys.exit(1)
    finally:
        # Always close down Spark to clean up system memory, even if the code crashed
        spark.stop()
        logger.info('SparkSession stopped 🛑.')