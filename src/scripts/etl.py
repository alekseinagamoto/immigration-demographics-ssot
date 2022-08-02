import configparser
import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, DateType

import create_tables 
import helpers

logger = logging.getLogger()
logger.setLevel(logging.INFO)

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

READ_S3_BUCKET = config['S3']['READ_S3_BUCKET']
WRITE_S3_BUCKET = config['S3']['WRITE_S3_BUCKET']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def process_immigration_data(spark, input_data, output_data):
    """Process immigration data to create fact_immigration 
    and dim_immigrant_demographics tables.
    
    Args:
        spark {object}: SparkSession object
        input_data {object}: Source S3 endpoint
        output_data {object}: Target S3 endpoint
    """
    logging.info("Start processing immigration data..")
    # Get filepath to data file
    file_path = os.path.join(input_data + '/immigration_data/')

    # Read data 
    # df_raw = spark.read.format('com.github.saurfang.sas.spark').load(file_path)
    df_raw = spark.read.parquet(file_path)

    # Preprocess data
    logging.info("Preprocessing immigration data..")
    df_preprocessed = helpers.preprocess_immigration_data(df_raw)
    
    # Create fact_immigration table
    logging.info("Creating fact_immigration table..")
    df = create_tables.create_immigration_fact_table(df_preprocessed, output_data)

    # Run data quality checks
    expected_schema = StructType(
        [
            StructField('cic_id', DoubleType(),True), StructField('year', DoubleType(),True), StructField('month', DoubleType(),True), 
            StructField('city_code', StringType(),True), StructField('state_code', StringType(),True), StructField('arrival_date', StringType(),True), 
            StructField('departure_date', StringType(),True), StructField('mode', DoubleType(),True), StructField('visa', DoubleType(),True), 
            StructField('visa_type', StringType(),True), StructField('immigration_id', LongType(),False)
        ]
    )
    helpers.run_table_schema_check(df, 'fact_immigration', expected_schema)
    helpers.run_data_completeness_check(df, 'fact_immigration')

    # Create dim_immigrant_demographics table
    logging.info("Creating dim_immigrant_demographics table..")
    df = create_tables.create_immi_demographics_dim_table(df_preprocessed, output_data)

     # Run data quality checks
    expected_schema = StructType(
        [
            StructField('immigration_cic_id', DoubleType(),True), StructField('country_of_birth', DoubleType(),True), StructField('country_of_residence', DoubleType(),True), 
            StructField('year_of_birth', DoubleType(),True), StructField('gender', StringType(),True), StructField('immi_demographics_id', LongType(),False)
        ]
    )
    helpers.run_table_schema_check(df, 'dim_immigrant_demographics', expected_schema)
    helpers.run_data_completeness_check(df, 'dim_immigrant_demographics')

    logging.info("Finished processing immigration data.")


def process_city_demographics_data(spark, input_data, output_data):
    """Process U.S. city demographics data to create dim_city_demographics table.
    
    Args:
        spark {object}: SparkSession object
        input_data {object}: Source S3 endpoint
        output_data {object}: Destination S3 endpoint
    """
    logging.info("Start processing U.S. city demographics data..")
    # Get filepath to data file
    file_path = os.path.join(input_data + 'us-cities-demographics.csv')

    # Read data 
    df_raw = spark.read.format('csv').options(header=True, delimiter=';').load(file_path)

     # Preprocess data
    logging.info("Preprocessing us city demographics data..")
    df_preprocessed = helpers.preprocess_demographics_data(df_raw)

    # Create table
    df = create_tables.create_city_demographics_dimension_table(df_preprocessed, output_data)

    # Run data quality checks
    expected_schema = StructType(
        [
            StructField('city_code',StringType(),True), StructField('state_code',StringType(),True), StructField('median_age',DoubleType(),True), StructField('male_population',IntegerType(),True), 
            StructField('female_population',IntegerType(),True), StructField('total_population',IntegerType(),True), StructField('number_of_veterans',IntegerType(),True), 
            StructField('foreign_born_num',IntegerType(),True), StructField('avg_household_size',DoubleType(),True), StructField('race',StringType(),True), 
            StructField('count',IntegerType(),True), StructField('id',LongType(),False)
        ]
    )
    helpers.run_table_schema_check(df, 'dim_city_demographics', expected_schema)
    helpers.run_data_completeness_check(df, 'dim_city_demographics')

    logging.info("Finished processing U.S. city demographics data.")
    

def process_temperature_data(spark, input_data, output_data):
    """Process global temparture data to create dim_temperature table.
    
    Args:
        spark {object}: SparkSession object
        input_data {object}: Source S3 endpoint
        output_data {object}: Destination S3 endpoint
    """
    logging.info("Start processing global temperature data..")
    # Get filepath to data file
    file_path = os.path.join(input_data + 'GlobalLandTemperatureByCity.csv')

    # Read data 
    df_raw = spark.read.csv(file_path, header=True)

     # Preprocess data
    logging.info("Preprocessing global temperature data..")
    df_preprocessed = helpers.preprocess_temperature_data(df_raw)

    # Create table
    df = create_tables.create_temperature_dimension_table(df_preprocessed, output_data)

    # Run data quality checks
    expected_schema = StructType(
        [
            StructField('dt',DateType(),True),StructField('city_name',StringType(),True),StructField('country_name',StringType(),True),StructField('avg_temperature',DoubleType(),True),
            StructField('avg_temperature_delta',DoubleType(),True),StructField('year',IntegerType(),True),StructField('month',IntegerType(),True)
        ]
    )
    helpers.run_table_schema_check(df, 'dim_temperature', expected_schema)
    helpers.run_data_completeness_check(df, 'dim_temperature')
    
    logging.info("Finished processing global temperature data.")


def process_i94_label_descriptions(spark, input_data, output_data):
    """Process I94 Label Descriptions to create dim_country, dim_city and
    dim_state dimension tables.
    
    Args:
        spark {object}: SparkSession object
        input_data {object}: Source S3 endpoint
        output_data {object}: Destination S3 endpoint
    """
    # Get filepath to data file
    file_path = os.path.join(input_data + "I94_SAS_Labels_Descriptions.SAS")

    # Read data 
    logging.info("Start processing label descriptions..")
    with open(file_path) as f:
        contents = f.readlines()
    
    # 1. Create dim_country table 
    logging.info("Creating country dimension table..")
    df = create_tables.create_dim_country_table(spark, contents, output_data)
    # Run data quality checks
    expected_schema = StructType([StructField('country_code', StringType(), True), StructField('country_name', StringType(), True)])
    helpers.run_table_schema_check(df, 'dim_country', expected_schema)
    helpers.run_data_completeness_check(df, 'dim_country')

    # 2. Create dim_city table 
    logging.info("Creating city dimension table..")
    df = create_tables.create_dim_city_table(spark, contents, output_data)
    # Run data quality checks
    expected_schema = StructType([StructField('city_code',StringType(),True), StructField('city_name',StringType(),True)])
    helpers.run_table_schema_check(df, 'dim_city', expected_schema)
    helpers.run_data_completeness_check(df, 'dim_city')

    # 3. Create dim_state table 
    logging.info("Creating state dimension table..")
    df = create_tables.create_dim_state_table(spark, contents, output_data)
    # Run data quality checks
    expected_schema = StructType([StructField('state_code', StringType(),True), StructField('state_name', StringType(),True)])
    helpers.run_table_schema_check(df, 'dim_state', expected_schema)
    helpers.run_data_completeness_check(df, 'dim_state')

    logging.info("Finished processing label descriptions.")
    

def main():
    spark = create_spark_session()

    process_i94_label_descriptions(spark, READ_S3_BUCKET, WRITE_S3_BUCKET)
    process_immigration_data(spark, READ_S3_BUCKET, WRITE_S3_BUCKET)    
    process_city_demographics_data(spark, READ_S3_BUCKET, WRITE_S3_BUCKET)
    process_temperature_data(spark, READ_S3_BUCKET, WRITE_S3_BUCKET)


if __name__ == "__main__":
    main()
