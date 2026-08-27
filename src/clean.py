from src.ingest import get_spark_session, read_csv, data_path
from pyspark.sql.functions import *
import logging, sys, os

# OS check for logs directory and create if not exists
os.makedirs('logs', exist_ok=True)

# Create a logger specific to this file with no bubbling to root

clean_logger = logging.getLogger('Ecommerce-ETL.Clean')
clean_logger.setLevel(logging.INFO)
clean_logger.propagate = False #  stops messages from bubbling up to root logger

if not clean_logger.handlers:
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    
    file_handler = logging.FileHandler("logs/clean.log", mode="w")
    file_handler.setFormatter(formatter)
    
    clean_logger.addHandler(stream_handler)
    clean_logger.addHandler(file_handler)

logger = clean_logger


# Schema casting 

#Invoice data type from string to timestamp
def cast_invoice_date(df):
    df = df.withColumn('InvoiceDate', to_timestamp(col('InvoiceDate'), 'M/d/yyyy H:mm'))
    return df


# Trim white Spaces in  Description and country
def trim_white_space(df):
    columns_to_trim = ['Description', 'Country']
    for c in columns_to_trim:
          df = df.withColumn(c, trim(col(c)))
  
    return df

# Null Values

def check_null_counts(df, logger):
    for c in df.columns:
        null_count = df.filter(col(c).isNull()).count()
        logger.info(f'{c} has : {null_count} nulls')

def drop_null_columns(df,logger):
    logger.info(f'Dropping Null Columns ...')
    df = df.dropna(subset=['CustomerID', 'Description'])
    for c in df.columns:
        null_counts = df.filter(col(c).isNull()).count()
        logger.info(f'total null values in {c} is : {null_counts} ')
    return df

if __name__ == "__main__":
    spark = get_spark_session()
    try:
        df = read_csv(spark, data_path)
        logger.info(f'\nNumber of rows in the DataFrame after reading CSV: {df.count()}')

        # ==========================================
        # Schema casting
        # ==========================================
        
        logger.info(f'Schema before casting InvoiceDate:\n {df.schema.treeString()}')

        # Call your casting function
        df = cast_invoice_date(df)
        logger.info(f'Schema after casting InvoiceDate:\n {df.schema.treeString()}')

        # ==========================================
        # WhiteSpace check & Trimming
        # ==========================================

        #checks leading and trailing spaces in the specified column
        whitespace_count_description= df.filter(col('Description') != trim(col('Description'))).count()
        logger.info(f'Rows with whitespace in Description before trimming : {whitespace_count_description}')

        # Trimming the whitespace
        df = trim_white_space(df)

        #Sanity Check
        whitespace_count_description_after= df.filter(col('Country') != trim(col('Country'))).count()
        logger.info(f'Rows with whitespace in Description after trimming : {whitespace_count_description_after}\n')

        # ==========================================
        # Check and Drop Null Values
        # ==========================================
        logger.info(f'Number of rows in the DataFrame before removing nulls: {df.count()}')
        logger.info('Checking null counts in each column ...')
        check_null_counts(df, logger)
        df  = drop_null_columns(df,logger)
        logger.info(f'Number of rows in the DataFrame after removing nulls: {df.count()}\n')

    except (FileNotFoundError, RuntimeError) as err:
        logger.critical(f'Cleaning pipeline execution failed ❌ : {err}')
        sys.exit(1)
    finally:
        spark.stop()
        logger.info('SparkSession stopped 🛑.')