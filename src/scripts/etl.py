import configparser
import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

import create_tables 
import helpers

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
    file_path = os.path.join(input_data + 'immigration/18-83510-I94-Data-2016/*.sas7bdat')

    # Read data 
    df_raw = spark.read.format('com.github.saurfang.sas.spark').load(file_path)
    
    # Preprocess data
    logging.info("Preprocessing immigration data..")
    df_preprocessed = helpers.preprocess_immigration_data(df_raw)
    
    # Create fact_immigration table
    logging.info("Creating fact_immigration table..")
    create_tables.create_immigration_fact_table(df_preprocessed, output_data)

    # Create dim_immigrant_demographics table
    logging.info("Creating dim_immigrant_demographics table..")
    create_tables.create_immi_demographics_dim_table(df_preprocessed, output_data)

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
    file_path = os.path.join(input_data + 'demography/us-cities-demographics.csv')

    # Read data 
    df_raw = spark.read.format('csv').options(header=True, delimiter=';').load(file_path)

     # Preprocess data
    logging.info("Preprocessing us city demographics data..")
    df_preprocessed = helpers.preprocess_demographics_data(df_raw)

    # Create table
    create_tables.create_city_demographics_dimension_table(df_preprocessed, output_data)
    
    logging.info("Finished processing U.S. city demographics data.")
    

def process_temperature_data(spark, input_data, output_data):
    """Process song data and create songs and artists table.
    
    Args:
        spark {object}: SparkSession object
        input_data {object}: Source S3 endpoint
        output_data {object}: Destination S3 endpoint
    """
    logging.info("Start processing global temperature data..")
    # Get filepath to data file
    file_path = os.path.join(input_data + 'temperature/GlobalLandTemperaturesByCity.csv')

    # Read data 
    df_raw = spark.read.csv(file_path, header=True)

     # Preprocess data
    logging.info("Preprocessing global temperature data..")
    df_preprocessed = helpers.preprocess_temperature_data(df_raw)

    # Create table
    create_tables.create_temperature_dimension_table(df_preprocessed, output_data)
    
    logging.info("Finished processing global temperature data.")




def process_i94_label_descriptions(spark, input_data, output_data)
    # Get filepath to data file
    file_path = os.path.join(input_data + "I94_SAS_Labels_Descriptions.SAS")

    # Read data 
    logging.info("Start processing label descriptions..")
    with open(file_path) as f:
        contents = f.readlines()
    
    # 1. Create dim_country table 
    create_tables.create_dim_country_table(contents, output_data)

    # 2. Create dim_city table 
    create_tables.create_dim_city_table(contents, output_data)

    # 3. Create dim_state table 
    create_tables.create_dim_state_table(contents, output_data)

    logging.info("Finished processing label descriptions.")
    

def main():
    spark = create_spark_session()

    process_i94_sas_label_descriptions(spark, READ_S3_BUCKET, WRITE_S3_BUCKET)
    process_immigration_data(spark, READ_S3_BUCKET, WRITE_S3_BUCKET)    
    process_city_demographics_data(spark, READ_S3_BUCKET, WRITE_S3_BUCKET)
    process_temparature_data(spark, READ_S3_BUCKET, WRITE_S3_BUCKET)
    

if __name__ == "__main__":
    main()