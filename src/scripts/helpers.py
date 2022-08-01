def preprocess_immigration_data(df):
    """Preprocess immigration dataframe

    Args:
        df {object}: spark dataframe with immigration data
        
    Return:
        df {object}: spark dataframe with preprocessed immigration data
    """    
    logging.info(f'Total records in raw dataframe: {df.count()}')
    print(f'Total records in raw dataframe: {df.count()}')
    
    # Remove columns with +85% missing values as identified during EDA
    drop_columns = ['entdepu', 'occup', 'insnum']    
    df = df.drop(*drop_columns)
    
    # Remove duplicate records on 'cicid'
    df = df.dropDuplicates(['cicid'])
    
    # Remove empty records
    df = df.dropna(how='all')
    
    logging.info(f'Total records in preprocessed dataframe: {df.count()}')
    print(f'Total records in preprocessed dataframe: {df.count()}')
    
    return df


def preprocess_temperature_data(df):
    """Preprocess world temperature dataset to remove records with missing values and duplicates
    
    Args:
        df {object}: spark dataframe with world temperature data
        
    Return:
        df {object}: spark dataframe with preprocessed world temperature data
    """
    logging.info(f'Total records in raw dataframe: {df.count()}')
    print(f'Total records in raw dataframe: {df.count()}')
    
    # Remove records with missing average temperature
    df = df.dropna(subset=['AverageTemperature'])
    
    # Remove duplicate records on date, city and country
    df = df.dropDuplicates(subset=['dt', 'City', 'Country'])
    
    # Remove empty rows
    df = df.dropna(how='all')
    
    logging.info(f'Total records in preprocessed dataframe: {df.count()}')
    print(f'Total records in preprocessed dataframe: {df.count()}')
    
    return df


def preprocess_demographics_data(df):
    """Preprocess US demographics dataset to remove records with missing values and duplicates
    
    Args:
    df {object}: spark dataframe with us demograpgics data
    
    Return:
    df {object}: spark dataframe with processed us demograpgics data
    """
    logging.info(f'Total records in raw dataframe: {df.count()}')
    print(f'Total records in raw dataframe: {df.count()}')
    
    # Remove duplicate records on city, state and race
    df = df.dropDuplicates(subset=['City', 'State', 'Race'])
    
    # Remove empty rows
    df.dropna(how="all")
    
    logging.info(f'Total records in preprocessed dataframe: {df.count()}')
    print(f'Total records in preprocessed dataframe: {df.count()}')
    
    return df