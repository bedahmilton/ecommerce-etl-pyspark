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

#Filtering

#Filter Invalid Data in quantity Column
def filter_invalid_quantity(df, logger):
    logger.info(f'Number of rows in Quantity is : {df.count()}')

    invalid_quantity = df.filter(df.Quantity <= 0)
    logger.info(f'Invalid Data in Quantity is : {invalid_quantity.count()} rows')

    df = df.filter(df.Quantity > 0)
    logger.info(f'Filtered Quantity is : {df.count()} rows')

    return df

# Filter Invalid Data in Unit Price
def filter_invalid_unit_price (df, logger):
    logger.info(f'Number of rows in UnitPrice is : {df.count()}')

    invalid_unit_price = df.filter(df.UnitPrice <= 0)
    logger.info(f'Invalid Data in UnitPrice is {invalid_unit_price.count()} rows')

    df = df.filter(df.UnitPrice > 0)
    logger.info (f'Filtered UnitPrice is {df.count()} rows \n')

    return df

# Duplicated Data
def remove_duplicates (df, logger):
    initial_count = df.count()
    logger.info(f'Number of rows before removal of duplicates : {initial_count}')

    cleaned_df = df.dropDuplicates()
    logger.info(f'Duplicated rows were : {initial_count - cleaned_df.count()} rows')

    logger.info(f'Number of Rows after Removing Duplicates : {cleaned_df.count()} \n')
    return cleaned_df
    
#
def clean_data (df, logger):

    logger.info(f'Schema before casting InvoiceDate:\n {df.schema.treeString()}')
    # CASTING
    df = cast_invoice_date(df)
    logger.info(f'Schema after casting InvoiceDate:\n {df.schema.treeString()}')

    # WHITE SPACES TRIMMING
    whitespace_count_description= df.filter(col('Description') != trim(col('Description'))).count()
    logger.info(f'Rows with whitespace in Description before trimming : {whitespace_count_description}')
    df = trim_white_space(df)
    #Sanity Check
    whitespace_count_description_after= df.filter(col('Description') != trim(col('Description'))).count()
    logger.info(f'Rows with whitespace in Description after trimming : {whitespace_count_description_after}\n')

    # NULL CHECKS AND DROPPING
    logger.info(f'Number of rows in the DataFrame before removing nulls: {df.count()}')
    logger.info('Checking null counts in each column ...')
    check_null_counts(df, logger)
    df  = drop_null_columns(df,logger)
    logger.info(f'Number of rows in the DataFrame after removing nulls: {df.count()}\n')

    # CHECK INVALID DATA AND FILTERING WITH A CONDIDITION
    logger.info('Checking Invalid Data ...')
    df = filter_invalid_quantity(df, logger)
    df = filter_invalid_unit_price(df,logger)

    # DUPLICATES REMOVAL
    logger.info('Checking Duplicates ... ')
    df = remove_duplicates(df, logger)

    return df

output_path = 'data/processed/cleaned_data.parquet'
def save_cleaned_data(df, logger, output_path):
    logger.info(f'Writing cleaned data to {output_path} ... ')
    df.coalesce(1).write.mode('overwrite').parquet(output_path)
    logger.info(f'Cleaned data saved successfully.')


if __name__ == "__main__":
    spark = get_spark_session()
    try:
        df = read_csv(spark, data_path)
        logger.info(f'\nNumber of rows in the DataFrame after reading CSV: {df.count()}')
        original_count = df.count()
        df = clean_data(df, logger)
        final_count = df.count()
        rows_kept_percentage = (final_count / original_count) * 100
        logger.info(f'Cleaning complete: {final_count}/{original_count} rows retained ({rows_kept_percentage:.2f}%)')

        #save data
        save_cleaned_data(df, logger, output_path)


    except (FileNotFoundError, RuntimeError) as err:
        logger.critical(f'Cleaning pipeline execution failed ❌ : {err}')
        sys.exit(1)
    finally:
        spark.stop()
        logger.info('SparkSession stopped 🛑.')