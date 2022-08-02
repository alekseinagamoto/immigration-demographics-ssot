import logging 


def preprocess_immigration_data(df):
    """Preprocess immigration dataframe

    Args:
        df {object}: spark dataframe with immigration data
        
    Return:
        df {object}: spark dataframe with preprocessed immigration data
    """    
    logging.info(f'Total records in raw dataframe: {df.count()}')
    
    # Remove columns with +85% missing values as identified during EDA
    drop_columns = ['entdepu', 'occup', 'insnum']    
    df = df.drop(*drop_columns)
    
    # Remove duplicate records on 'cicid'
    df = df.dropDuplicates(['cicid'])
    
    # Remove empty records
    df = df.dropna(how='all')
    
    logging.info(f'Total records in preprocessed dataframe: {df.count()}')
    
    return df


def preprocess_temperature_data(df):
    """Preprocess world temperature dataset to remove records with missing values and duplicates
    
    Args:
        df {object}: spark dataframe with world temperature data
        
    Return:
        df {object}: spark dataframe with preprocessed world temperature data
    """
    logging.info(f'Total records in raw dataframe: {df.count()}')
    
    # Remove records with missing average temperature
    df = df.dropna(subset=['AverageTemperature'])
    
    # Remove duplicate records on date, city and country
    df = df.dropDuplicates(subset=['dt', 'City', 'Country'])
    
    # Remove empty rows
    df = df.dropna(how='all')
    
    logging.info(f'Total records in preprocessed dataframe: {df.count()}')
    
    return df


def preprocess_demographics_data(df):
    """Preprocess US demographics dataset to remove records with missing values and duplicates
    
    Args:
    df {object}: spark dataframe with us demograpgics data
    
    Return:
    df {object}: spark dataframe with processed us demograpgics data
    """
    logging.info(f'Total records in raw dataframe: {df.count()}')
    
    # Remove duplicate records on city, state and race
    df = df.dropDuplicates(subset=['City', 'State', 'Race'])
    
    # Remove empty rows
    df.dropna(how="all")
    
    logging.info(f'Total records in preprocessed dataframe: {df.count()}')
    
    return df

def run_data_completeness_check(df, table_name):
    """Check for non-empty fact and dimension tables.
    Args:
        df {object}: spark dataframe
        table_name {str}: table name
    """
    total_count = df.count()

    if total_count == 0:
        logging.warning(f"Data completeness check FAILED for {table_name}: no records found!")
    else:
        logging.info(f"Data completeness check PASSED for {table_name}: {total_count} records found!")


def run_table_schema_check(df, table_name, expected_schema):
    """Check for schema correctness of fact and dimension tables.
    Args:
        df {object}: spark dataframe
        table_name {str}: table name
        schema {object}: schema
    """
    if df.schema != expected_schema:
        logging.warning(f"Table schema check FAILED for {table_name}: unexpected schema!")
    else:
        logging.info(f"Table schema check PASSED for {table_name}: expected schema!")
