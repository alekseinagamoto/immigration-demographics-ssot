import datetime as dt
import pandas as pd


from pyspark.sql.functions import udf, monotonically_increasing_id, to_date, col, year, month

def create_immigration_fact_table(df, output_data):
    """Creates an immigration fact table from  I94 Immigration data.
    
    Args:
        df {object}: spark dataframe with preprocessed immigration data
        output_data {str}: write path

    Return:
        df {object}: spark dataframe with preprocessed immigration data
    """    
    # UDF to convert SAS date format to datetime object
    get_datetime = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)
    
    df = df.select('cicid', 'i94yr', 'i94mon', 'i94port', 'i94addr', 'arrdate', 'depdate', 'i94mode', 'i94visa', 'visatype').distinct() \
        .withColumn('immigration_id', monotonically_increasing_id()) \
        .withColumnRenamed('cicid','cic_id') \
        .withColumnRenamed('i94yr', 'year') \
        .withColumnRenamed('i94mon', 'month') \
        .withColumnRenamed('i94port', 'city_code') \
        .withColumnRenamed('i94addr', 'state_code') \
        .withColumnRenamed('arrdate', 'arrival_date') \
        .withColumnRenamed('depdate', 'departure_date') \
        .withColumnRenamed('i94mode', 'mode') \
        .withColumnRenamed('i94visa', 'visa') \
        .withColumnRenamed('visatype', 'visa_type')
    
    # convert dates into datetime objects
    df = df.withColumn('arrival_date', get_datetime(df.arrival_date))
    df = df.withColumn('departure_date', get_datetime(df.departure_date))
        
    # write fact table to parquet file partioned by state
    df.write.mode('overwrite').partitionBy('state_code').parquet(path=output_data + "fact_immigration")

    return df


def create_immi_demographics_dim_table(df, output_data):
    """Creates an immigrant demographics dim table from  I94 Immigration data.
    
    Args:
        df {object}: spark dataframe of immigration data
        output_data {str}: write path

    Return:
        df {object}: spark dataframe with preprocessed immigrant demographics data
    """    
    df = df.select('cicid', 'i94cit', 'i94res', 'biryear', 'gender').distinct() \
    .withColumn('immi_demographics_id', monotonically_increasing_id()) \
    .withColumnRenamed('cicid','immigration_cic_id') \
    .withColumnRenamed('i94cit', 'country_of_birth') \
    .withColumnRenamed('i94res', 'country_of_residence') \
    .withColumnRenamed('biryear', 'year_of_birth')
    
    # write dimension table to parquet file
    df.write.mode('overwrite').parquet(path=output_data + "dim_immigrant_demographics")

    return df


def create_city_demographics_dimension_table(df, output_data):
    """Creates a us city demographics dimension table from the U.S. City Demographic dataset.
    
    Args:
        df {object}: spark dataframe of us city demographics data
        output_data {str}: write path

    Return:
        df {object}: spark dataframe with preprocessed city demographics data
    """
    df = df.withColumnRenamed('City', 'city_code') \
        .withColumnRenamed('State', 'state_code') \
        .withColumnRenamed('Median Age','median_age') \
        .withColumnRenamed('Male Population', 'male_population') \
        .withColumnRenamed('Female Population', 'female_population') \
        .withColumnRenamed('Total Population', 'total_population') \
        .withColumnRenamed('Number of Veterans', 'number_of_veterans') \
        .withColumnRenamed('Foreign-born', 'foreign_born_num') \
        .withColumnRenamed('Average Household Size', 'avg_household_size') \
        .withColumnRenamed('Race', 'race') \
        .withColumnRenamed('Count', 'count')

    df = df.withColumn('id', monotonically_increasing_id())
    
    # write dimension table to parquet file
    df.write.mode('overwrite').parquet(path=output_data + "dim_city_demographics")

    return df


def create_temperature_dimension_table(df, output_data):
    """Creates a global temperature dimension table from the Global Average Temperature dataset.
    
    Args:
        df {object}: spark dataframe of global average temperature by city data
        output_data {str}: write path

    Return:
        df {object}: spark dataframe with preprocessed temperature data
    """
    df = df.select('dt', 'City', 'Country', 'AverageTemperature', 'AverageTemperatureUncertainty').distinct() \
           .withColumn('dt', to_date(col('dt'))) \
           .withColumnRenamed('City', 'city_name') \
           .withColumnRenamed('Country', 'country_name') \
           .withColumnRenamed('AverageTemperature','avg_temperature') \
           .withColumnRenamed('AverageTemperatureUncertainty', 'avg_temperature_delta')    
    
    # Derive month and year from datetime column 
    df = df.withColumn('year', year(df['dt']))
    df = df.withColumn('month', month(df['dt']))

    # write dimension table to parquet file
    df.write.mode('overwrite').parquet(path=output_data + "dim_temperature")
    
    return df


def create_dim_country_table(spark, data, output_data):
    """Creates a country dim table from I94 SAS labels descriptions. 

    Args:
        spark {object}: SparkSession object
        data {object}: I94 SAS labels descriptions
        output_data {str}: write path

    Return:
        df {object}: spark dataframe with country data
    """  
    i94cit_i94res = {}
    for countries in data[9:298]:
        pair = countries.split('=')
        country_code, country_name = pair[0].strip(), pair[1].strip().strip("'")
        i94cit_i94res[country_code] = country_name

    df = spark.createDataFrame(i94cit_i94res.items(), ['country_code', 'country_name'])
    df.write.mode("overwrite").parquet(path=output_data + 'dim_country.parquet')

    return df
        


def create_dim_city_table(spark, data, output_data):
    """Creates a city dim table from I94 SAS labels descriptions. 

    Args:
        spark {object}: SparkSession object
        data {object}: I94 SAS labels descriptions
        output_data {str}: write path

    Return:
        df {object}: spark dataframe with city data
    """  
    i94port = {}
    for cities in data[302:962]:
        pair = cities.split('=')
        city_code, city_name = pair[0].strip("\t").strip().strip("'"), pair[1].strip('\t').strip().strip("''")
        i94port[city_code] = city_name

    df_i94port = pd.DataFrame(list(i94port.items()), columns=['city_code', 'city_name'])
    df_i94port[['city_name', 'state_code']] = df_i94port['city_name'].str.split(',', 1, expand=True)
    df_i94port['city_name'] = df_i94port['city_name'].str.title()
    df_i94port.drop(columns='state_code', inplace=True)

    df = spark.createDataFrame(df_i94port)
    df.write.mode("overwrite").parquet(path=output_data + 'dim_city.parquet')

    return df


def create_dim_state_table(spark, data, output_data):
    """Creates a state dim table from I94 SAS labels descriptions. 

    Args:
        spark {object}: SparkSession object
        data {object}: I94 SAS labels descriptions
        output_data {str}: write path

    Return:
        df {object}: spark dataframe with state data
    """ 
    i94addr = {}
    for states in data[981:1036]:
        pair = states.split('=')
        state_code, state_name = pair[0].strip('\t').strip("'"), pair[1].strip().strip("'")
        i94addr[state_code] = state_name.title()

    df_i94addr = pd.DataFrame(list(i94addr.items()), columns=['state_code', 'state_name'])
    df = spark.createDataFrame(df_i94addr)
    df.write.mode("overwrite").parquet(path=output_data + 'dim_state.parquet')

    return df
