from src.ingest import get_spark_session, read_csv, data_path
from pyspark.sql.functions import *
import logging, sys, os

clean_data_path = 'data/processed/cleaned_data.parquet'
        
# OS check for logs directory and create if not exists
os.makedirs('logs', exist_ok=True)

# Create a logger specific to this file with no bubbling to root
transform_logger = logging.getLogger('Ecommerce-ETL.Transform')
transform_logger.setLevel(logging.INFO)
transform_logger.propagate = False #  stops messages from bubbling up to root logger

if not transform_logger.handlers:
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    
    file_handler = logging.FileHandler("logs/transform.log", mode="w")
    file_handler.setFormatter(formatter)
    
    transform_logger.addHandler(stream_handler)
    transform_logger.addHandler(file_handler)

logger = transform_logger

# Check if the cleaned data path exists before reading
if not os.path.exists(clean_data_path):
    logger.error(f'File path provided does not exist : {clean_data_path}')
    raise FileNotFoundError(f'File path provided does not exist: {clean_data_path}')

# ---- TRANSFORMATION LOGIC ---- #

# Total Price column 
def add_total_price (df, logger):
    logger.info('Total Price Column been added and calculated ...')
    df = df.withColumn('TotalPrice', round(col('Quantity') * col('UnitPrice'), 2))
    logger.info(f'Successfully added the TotalPrice Column.')

    return df

#Extract Year Month on Timestamp
def extract_year_month (df, logger):
    logger.info(f'Extracting the Year, Month on the data ...')
    df = df.withColumns({
        'Month' : month(col('InvoiceDate')),
        'Year' : year(col('InvoiceDate')),
        'YearMonth' : date_format(col('InvoiceDate'), 'yyyy-MM')
    })
    logger.info('Year & Month columns added.')

    return df

# Revenue By Country
def revenue_by_country (df, logger):
    logger.info('Getting revenue by Country ... ')

    country_revenue = df.groupBy('Country').agg(
        sum('TotalPrice').alias('Revenue')
    ).sort(col('Revenue').desc())

    logger.info(f'Top Country by revenue is {country_revenue.first()}')
    logger.info('Revenue per Country Successfully added.')

    return country_revenue

# Revenue by Month in ascending order
def revenue_by_month(df, logger):
    logger.info('Getting Revenue By Month ... ')

    monthly_revenue = df.groupBy('YearMonth').agg(
        sum('TotalPrice').alias('RevenueByMonth')
    ).sort(col('YearMonth').asc())

    logger.info('Revenue per Month Successfully added.')
    return monthly_revenue

# Perfoming products By revenue
def top_products_by_revenue (df, logger):
    logger.info('Getting top products by total revenue...')

    product_revenue = df.groupBy('Description').agg(
        sum('TotalPrice').alias('TotalRevenuePerProduct')
    ).sort(col('TotalRevenuePerProduct').desc())


    logger.info('Top products by revenue calculated successfully.')
    return product_revenue

# Wrapper function
def transform_data(df, logger):
    logger.info('Applying Transformation Logic on cleaned data ... ')

    #total price
    df = add_total_price(df, logger)
    df.select('Quantity', 'UnitPrice', 'TotalPrice').show(5)

    # Year Month
    df = extract_year_month(df, logger)
    df.select('InvoiceDate', 'Month', 'Year', 'YearMonth').show(5)

    #Revenue By Country
    country_revenue = revenue_by_country(df, logger)
    country_revenue.show(10)

    #Revenue By Month
    monthly_revenue = revenue_by_month(df, logger)
    monthly_revenue.show(10)

    #Performing Products each year 
    product_revenue = top_products_by_revenue(df, logger)
    product_revenue.show(truncate=False)

    return {
        'transactions' : df,
        'country_revenue': country_revenue,
        'monthly_revenue': monthly_revenue,
        'product_revenue': product_revenue
    }

def save_transformed_data(results, logger):
    output_dir = "data/processed"
    
    for name, df in results.items():
        output_path = f'{output_dir}/{name}.parquet'
        logger.info(f"Saving '{name}' to {output_path} ...")
        df.write.mode("overwrite").parquet(output_path)
        logger.info(f"Saved '{name}' successfully.")

if __name__ == "__main__":
    spark = get_spark_session()
    try:
        # Read the cleaned data from Parquet
        df = spark.read.parquet(clean_data_path)
        results = transform_data(df, logger)
        save_transformed_data (results, logger)
        logger.info(f'Transformation pipeline executed successfully, Keys available: {list(results.keys())}')

    except (FileNotFoundError, RuntimeError) as err:
        logger.critical(f'Transform pipeline execution failed ❌ : {err}')
        sys.exit(1)
    finally:
        spark.stop()
        logger.info('SparkSession stopped 🛑.')

