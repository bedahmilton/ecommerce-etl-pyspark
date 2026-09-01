from src.ingest import get_spark_session
from pyspark.sql.functions import *
import logging, sys, os

# Reading paths for each parquet
transactions_path = 'data/processed/transactions.parquet'
country_revenue_path = 'data/processed/country_revenue.parquet'
monthly_revenue_path = 'data/processed/monthly_revenue.parquet'
product_revenue_path = 'data/processed/product_revenue.parquet'



output_dir = 'data/output'

#Output Paths for each parquet
transactions_output_path = f'{output_dir}/transactions.parquet'        # partitioned, so it's a folder not a single file
country_revenue_output_path = f'{output_dir}/country_revenue.parquet'
monthly_revenue_output_path = f'{output_dir}/monthly_revenue.parquet'
product_revenue_output_path = f'{output_dir}/product_revenue.parquet'

# OS check for logs directory and create if not exists
os.makedirs('logs', exist_ok=True)

# Create a logger specific to this file with no bubbling to root
load_logger = logging.getLogger('Ecommerce-ETL.Load')
load_logger.setLevel(logging.INFO)
load_logger.propagate = False #  stops messages from bubbling up to root logger

if not load_logger.handlers:
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    
    file_handler = logging.FileHandler("logs/load.log", mode="w")
    file_handler.setFormatter(formatter)
    
    load_logger.addHandler(stream_handler)
    load_logger.addHandler(file_handler)

logger = load_logger

#loading transaction file
def load_transactions (spark, logger):
    logger.info(f'Loading from {transactions_path} processed Parquet...')
    df = spark.read.parquet(transactions_path)

    logger.info(f'Writing transactions to {transactions_output_path}, partitioned by Year and Month...')
    df.write.mode('overwrite').partitionBy('Year', 'Month').parquet(transactions_output_path)
    logger.info(f'Transactions successfully written to final output {transactions_output_path}')


# Loading Summary data
def load_summary_tables (spark, logger):
    # Country_Revenue Reading & Writing
    logger.info(f'Loading from {country_revenue_path} Parquet...')
    country_revenue = spark.read.parquet(country_revenue_path)

    logger.info(f'Writing into {country_revenue_output_path} ...')
    country_revenue.write.mode('overwrite').parquet(country_revenue_output_path)
    logger.info(f'{country_revenue_path} successfully written to final output on {country_revenue_output_path}')

    # Monthly_Revenue Reading & Writing
    logger.info(f'Loading from {monthly_revenue_path} Parquet...')
    monthly_revenue = spark.read.parquet(monthly_revenue_path)
    
    logger.info(f'Writing into {monthly_revenue_output_path} ...')
    monthly_revenue.write.mode('overwrite').parquet(monthly_revenue_output_path)
    logger.info(f'{monthly_revenue_path} successfully written to final output on {monthly_revenue_output_path}')

    # Product_Revenue Reading & Writing
    logger.info(f'Loading from {product_revenue_path} Parquet...')
    product_revenue = spark.read.parquet(product_revenue_path)
    
    logger.info(f'Writing into {product_revenue_output_path} ...')
    product_revenue.write.mode('overwrite').parquet(product_revenue_output_path)
    logger.info(f'{product_revenue_path} successfully written to final output on {product_revenue_output_path}')

def load_data(spark, logger):
    logger.info('Starting load stage...')
    load_transactions(spark, logger)
    load_summary_tables(spark, logger)
    logger.info('Load stage complete.')

if __name__ == "__main__":
    spark = get_spark_session()
    try:
        # Read the cleaned data and load it to the final output
        load_data(spark, logger)

    except (FileNotFoundError, RuntimeError) as err:
        logger.critical(f'Loading pipeline execution failed ❌ : {err}')
        sys.exit(1)
    finally:
        spark.stop()
        logger.info('SparkSession stopped 🛑.')


