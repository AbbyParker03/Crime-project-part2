#Introduction 
#This is a pipeline that reads in a number of files associated with crime in Essex, Avon and Somerset, Cleveland and the West Midlands
#from June 2022 to June 2024. It is designed to process and analyze crime and housing data in a strucutred manner. It is divided into three distinct layers:
#Staging, Primary and Reporting. Each layer performs specific tasks to clean, transform, and aggregate data to support detailed reporting and analysis. 

#The staging layer is responsible for the initial ingestion and preparation of raw data. This involves: Ingesting data, cleaning data and saving data. 

#In the primary layer the staged data is transformed for deeper analysis. This involves: Data transformation, Feature extraction and saving the transformed data #

#The reporting layer is the final stage where the processed data is used to generate actionable insights. This involves: Merging data, Aggregating data and producing CSV's that can be used to make visulisations. 

#The pipeline can be executed in full or specific stages based on the users needs. Each stage builds upon the previous one, ensuring a strucutred flow from raw data ingestion to insightful reporting. 

#The folders needed to run this code is in the submission folder in teams, please downloads all folders and files there. 

#please import the following packages

import os 
import pandas as pd 
import numpy as np
import logging

logging.basicConfig(
    filename='pipeline.log',  # Specifies the log file name
    filemode='a',             # Opens the log file in append mode  
    format='%(asctime)s %(levelname)s:%(message)s',  # Specifies the format of log messages
    datefmt='%Y-%m-%d %H:%M:%S',  # Specifies the date and time format
    level=logging.INFO # Sets the logging level to INFO
)


# Constants for file paths
LOCAL_DATA_PATH = './'
OUTCOMES_FOLDER = 'outcomes'
STOP_AND_SEARCH_FOLDERS = {'Avon and Somerset': 'stop and search/avon and somerset',
                           'Cleveland': 'stop and search/cleveland',
                           'Essex': 'stop and search/essex',
                           'West Midlands': 'stop and search/west midlands'
                           }
STREET_FOLDER = 'street'
HOUSE_PRICES_FOLDER = 'Avon and Somerset house prices'
POSTCODES_FILE = 'postcodes.csv'
ESSEX_HOUSE_PRICE_FILE = 'Essex house pricing.csv'
POPULATION_FILE = 'crime_population.csv'


STAGED_OUTCOMES_FILE = os.path.join(LOCAL_DATA_PATH, 'staged_outcomes.csv')
STAGED_STREET_FILE = os.path.join(LOCAL_DATA_PATH, 'staged_street.csv')
STAGED_STOP_SEARCH_FILE = os.path.join(LOCAL_DATA_PATH, 'staged_stop_search.csv')
STAGED_AVON_AND_SOMERSET_HOUSE_PRICE_FILE = os.path.join(LOCAL_DATA_PATH, 'staged_avon_and_somerset_house_price.csv')
STAGED_POSTCODES_FILE = os.path.join(LOCAL_DATA_PATH, 'staged_postcodes.csv')
STAGED_ESSEX_HOUSE_PRICE_FILE = os.path.join(LOCAL_DATA_PATH, 'staged_essex_house_price.csv')
STAGED_POPULATION_FILE = os.path.join(LOCAL_DATA_PATH, 'staged_population.csv')

PRIMARY_OUTCOMES_FILE = os.path.join(LOCAL_DATA_PATH, 'primary_outcomes.csv')
PRIMARY_STREET_FILE = os.path.join(LOCAL_DATA_PATH, 'primary_street.csv')
PRIMARY_STOP_SEARCH_FILE = os.path.join(LOCAL_DATA_PATH, 'primary_stop_search.csv')
PRIMARY_POSTCODES_FILE = os.path.join(LOCAL_DATA_PATH, 'primary_postcodes.csv')
PRIMARY_ESSEX_HOUSE_PRICE_ALL_FILE = os.path.join(LOCAL_DATA_PATH, 'primary_essex_house_price.csv')
PRIMARY_AVON_AND_SOMERSET_HOUSE_PRICE_ALL_FILE = os.path.join(LOCAL_DATA_PATH, 'primary_avon_and_somerset_house_price.csv')
PRIMARY_ESSEX_HOUSE_PRICING_BY_HOUSE_TYPE_FILE = os.path.join(LOCAL_DATA_PATH, 'primary_essex_house_pricing_by_house_type.csv')
PRIMARY_AVON_AND_SOMERSET_HOUSE_PRICING_BY_HOUSE_TYPE_FILE = os.path.join(LOCAL_DATA_PATH, 'primary_avon_and_somerset_house_pricing_by_house_type.csv')


REPORTING_POPULATION_STOP_SEARCH_FILE = os.path.join(LOCAL_DATA_PATH, 'stop_and_search_with_population.csv')
REPORTING_COUNT_OF_OBJECT_OF_SEARCH_FILE = os.path.join(LOCAL_DATA_PATH, 'count_of_object_of_search.csv')
REPORTING_STOP_AND_SEARCH_PER_1000_FILE = os.path.join(LOCAL_DATA_PATH, 'stop_and_search_per_1000_people.csv')
REPORTING_SPECIFIC_CRIMES_FILE = os.path.join(LOCAL_DATA_PATH, 'specific_crimes.csv')
REPORTING_POPULATION_SPECIFIC_CRIME_FILE = os.path.join(LOCAL_DATA_PATH, 'specific_crimes_with_population.csv')
REPORTING_SPECIFIC_CRIMES_PER_1000_FILE = os.path.join(LOCAL_DATA_PATH, 'specific_crimes_per_1000_people.csv')
REPORTING_SPECIFIC_CRIMES_PER_1000_OVER_TIME_FILE = os.path.join(LOCAL_DATA_PATH, 'specific_crimes_per_1000_people_over_time.csv')
REPORTING_OUTCOME_WITH_FINAL_OUTCOME_FILE = os.path.join(LOCAL_DATA_PATH, 'outcomes_with_final_outcome.csv')
REPORTING_OUTCOME_SPECIFIC_WITH_BROAD_CATEGORY_FILE = os.path.join(LOCAL_DATA_PATH, 'outcome_specific_with_broad_category.csv')
REPORTING_ESSEX_AND_BURGLARY_FILE = os.path.join(LOCAL_DATA_PATH, 'essex_and_burglary.csv')
REPORTING_AVON_AND_SOMERSET_AND_BURGLARY_FILE = os.path.join(LOCAL_DATA_PATH, 'avon_and_somerset_and_burglary.csv')
REPORTING_MERGE_AVON_SOMERSET_POSTCODES_FILE = os.path.join(LOCAL_DATA_PATH, 'avon_and_somerset_with_postcodes.csv')
REPORTING_MERGE_ESSEX_POSTCODE_FILE = os.path.join(LOCAL_DATA_PATH, 'essex_with_postcodes.csv')
REPORTING_ESSEX_BURGLARY_LINE_FILE = os.path.join(LOCAL_DATA_PATH, 'essex_burglary_count_for_line_graph.csv')
REPORTING_AVON_SOMERSET_BURGLARY_LINE_FILE = os.path.join(LOCAL_DATA_PATH, 'avon_and_somerset_burglary_count_for_line_graph.csv')
REPORTING_ESSEX_HOUSE_PRICE_LINE_FILE= os.path.join(LOCAL_DATA_PATH, 'Essex house price for line graph.csv')
REPORTING_AVON_AND_SOMERSET_HOUSE_PRICE_LINE_FILE = os.path.join(LOCAL_DATA_PATH, 'Avon and Somerset house price for line graph.csv')
REPORTING_AVERAGE_ESSEX_HOUSE_TYPE_FILE = os.path.join(LOCAL_DATA_PATH, 'average_essex_house_type.csv')
REPORTING_AVERAGE_AVON_AND_SOMERSET_HOUSE_TYPE_FILE = os.path.join(LOCAL_DATA_PATH, 'average_avon_and_somerset_house_type.csv')



#function to read in csv files 
def ingest_data(file_path: str)-> pd.DataFrame:
    """
    Ingest raw data from a CSV file. 

    Parameters:
    file_path(str): File path to the csv file 

    Returns:
    pd.DataFrame: A dataframe containing the raw data.

    """
    logging.info(f"Starting data ingestion from {file_path}")
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        return None

    try:
        df = pd.read_csv(file_path)
        logging.info(f"Data ingestion from {file_path} completed successfully")
        return df
    except ValueError as e:
        logging.error(f"Error reading the CSV file {file_path}: {e}")
        raise





#function to combine all the csv files 
def combine_csv_files(folder_path, output_file=None):
    """
    Reads all CSV files from a given folder, combines them into a single dataframe, and optionally saves it to a CSV file. 

    Parameters:
    folder_path(str): The path to the folder containing the CSV files. 
    output_file(str, optional): The file path where the combined dataframe will be saved as a CSV. If None, it won't save the file. 

    Returns:
    pd.DataFrame: The combined dataframe containing all the data from the CSV files. 

    """
    #log the start of the process 
    logging.info(f'Starting to combine CSV files from folder:{folder_path}')

    try: 
        #get all CSV files in the folder 
        csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]

        if not csv_files:
            logging.info('No CSV files found in the folder')
            return pd.DataFrame() #return an empty dataframe if no csv files are found 

        #initialize an empty list to store individual dataframes
        dataframes = []

        #loop through each file, read it into a dataframe, and append it to the list 
        for file in csv_files:
            file_path = os.path.join(folder_path, file)
            df = pd.read_csv(file_path)
            dataframes.append(df)


        #Concatenate all dataframes in the list into a single dataframe 
        combined_df = pd.concat(dataframes, ignore_index = True)

        #if an output file is specified, save the combined dataframe as a CSV
        if output_file:
            logging.info(f'Saving combined dataframe to {output_file}')
            combined_df.to_csv(output_file, index = False)
        else:
            logging.info('No ouput file specified, returning the combined dataframe.')

        return combined_df

    except Exception as e:
        logging.error(f'An error occurred: {e}')
        raise


#function to add an area column and its values 
def add_area_column (df, area_name):
    """
    Adds an 'Area' column to the given dataframe and assigns the provided area name to all rows. 

    Parameters:
    df(pd.DataFrame): The dataframe to which the 'Area' column will be added. 
    area_name(str): The name of the area to be assigned to the 'Area' column.

    Returns:
    pd.DataFrame: The dataframe with the new 'Area' column. 

    """
    try:
        logging.info(f"Adding 'Area' column to the dataframe")
        df['Area'] = area_name
        logging.info("'Area' column successfully added.")
        return df
    except Exception as e:
        logging.error(f"Error adding 'Area' column: {e}", exc_info=True)
        raise


#function to concat dataframes together
def concat_dataframes(dfs, axis = 0, ignore_index = False):
    """
    Concatenates a list of dataframes together.

    Parameters:
    dfs(list of pd.DataFrame): List of dataframes to concatenate 
    axis(int): The axis to concatenate along. 0 for rows, 1 for columns. Default is 0
    ignore_index(bool): If True, do not use the index values on the concatenation axis. Default is False 

    Returns:
    pd.DataFrame: The concatenated DataFrame. 

    """
    try:
        logging.info('Concatenating dataframes')
        return pd.concat(dfs, axis=axis, ignore_index=ignore_index)
    except Exception as e:
        logging.error(f'Error concatenating dataframes: {e}', exc_info=True)
        raise



#function to check nulls in a dataframe
def check_nulls(df):
    """
    Checks for null values in a DataFrame and provides a summary. 

    Parameters:
    df(pd.DataFrame): The dataframe to check for null values. 

    Returns 
    pd.Series: A series indicating the count of null values for each column. 

    """
    try:
        logging.info('Checking for null values in the dataframe')
        null_count = df.isnull().sum()

        if null_count.any():
            logging.warning(f'Columns with null values:\n{null_count[null_count > 0]}')
            print(null_count[null_count > 0])
        else:
            logging.info('No null values found in the DataFrame:')

        return null_count
    except Exception as e:
        logging.error(f'Error checking for null values: {e}', exc_info=True)
        raise 





#function to replace nulls in a dataframe
def replace_nulls(df, column, fill):
    """
    Replace null values in a specified column of a DataFrame with a given fill value 

    Parameters:
    df (pd.DataFrame):The dataframe where null values need to be replaced 
    column (str): The name of the column in which to replace null values 
    fill: The value used to replace null values in the specified column. 
        This can be a scalar,  such as a number of a string, or a method like 'ffill' or 'bfill'.
    df[column] = df[column].fillna(fill)
    return df

    Returns:
    pd.DataFrame: The dataframe with null values replaced in the specified column

    """
    try:
        logging.info('Replacing null values in column')
        df[column] = df[column].fillna(fill)
        logging.info('Null values successfully replaced')
        return df
    except Exception as e:
        logging.error(f"Error replacing null values in column '{column}': {e}", exc_info=True)
        raise


#function to remove the rolls of columns with nulls in
def remove_nulls_in_columns(df, columns):
    """
    Removes all rows with null values in the specified columns. 

    Parameters:
    df(pd.DataFrame): The DataFrame to process
    columns (list): A list of column names to check for null values. 

    Returns:
    pd.DataFrame: The DataFrame with rows containing null values in the specified columns removed. 
    """
    try:
        logging.info('Removing rows with null values in columns')
        df = df.dropna(subset=columns)
        logging.info('Rows with null values successfully removed')
        return df
    except Exception as e:
        logging.error(f'Error removing rows with null values: {e}', exc_info=True)
        raise


#function to drop specific columns 
def drop_columns(df, columns):
    """
    Drops specified columns from the DataFrame

    Parameters:
    df(pd.DataFrame): The dataframe to process
    columns (list): A list of column names to drop

    Returns:
    pd.DataFrame: The dataframe with the specified columns removed

    """
    try:
        logging.info(f'Dropping columns: {columns}.')
        df = df.drop(columns=columns, errors='ignore')
        logging.info('Columns successfully dropped')
        return df
    except Exception as e:
        logging.error(f'Error dropping columns: {e}', exc_info=True)
        raise


#function to separate the date column into month and year
def process_date_column(df, old_column_name, new_column_name):
    """
    Renames a specified column and splits its string values into separate 'Year' and 'Month' columns

    Parameters:
    df(pd.DataFrame): The dataframe to process
    old_column_name(str): The name of the column to rename 
    new_column_name(str): The new name for the column

    Returns:
    pd.DataFrame: The dataframe with the renamed column and new 'Year' and 'Month' columns.
    
    """
    try:
        logging.info('Processing date column')
        #convert the new column to a string to avoid errors
        df[old_column_name] = pd.to_datetime(df[old_column_name])

    
        #rename the column
        df.rename(columns={old_column_name: new_column_name}, inplace = True)


        #split the new column into 'Year' and 'Month'
        df['Year'] = df[new_column_name].dt.year
        df['Month'] = df[new_column_name].dt.month

        logging.info('Date column successfully processed')
        return df
    except Exception as e:
        logging.error(f"Error processing data column '{old_column_name}' : {e}", exc_info=True)
        raise


#function to merge dataframes 
def merge_dataframes(left_df, right_df, left_on, right_on, how):
    """

    Merges two dataframes together togther based on specified columns 

    Parameters:
    left_df (pd.DataFrame): The dataframe to merge on the left 
    right_df (pd.DataFrame): The dataframe to merge on the right 
    left_on (str, optional): The column name in the left dataframe to merge on
    right_on (str, optional): The column name in the right dataframe to merge on 
    how (str): The type of merge to perform

    Returns:
    pd.DataFrame: The merged dataframe with the left and right dataframe combined 

    """
    try:
        logging.info('Merging dataframes')
        merged_df = pd.merge(left=left_df, right=right_df, left_on=left_on, right_on=right_on, how=how)
        logging.info('Dataframes successfully merged')
        return merged_df
    except Exception as e:
        logging.error(f"Error merging dataframes: {e}", exc_info=True)
        raise


#function that groups columns and counts values in a specific column
def group_and_count(df, group_by_cols, count_col):
    """
    Groups a dataframe by the specified columns and counts the occurences of values in the specified column

    Parameters:
    df(pd.DataFrame): The dataframe to group and count
    group_by_cols (list or str): The column(s) to group by
    count_col (str): The column to count occurences of

    Returns:
    pd.DataFrame: A dataframe with grouped data and a count column 

    """
    logging.info(f"Starting to group dataframe by columns: {group_by_cols} and count occurrences of column: {count_col}")

    try:
        #group by the specified columns and count the occurences of the count_col
        grouped_df = df.groupby(group_by_cols).agg({count_col: 'count'})

        #rename the count column to 'count'
        grouped_df = grouped_df.rename(columns={count_col: 'count'})

        logging.info(f"Successfully grouped dataframe by {group_by_cols} and counted occurrences of {count_col}")
        return grouped_df
    
    except Exception as e:
        logging.error(f"An error occurred while grouping and counting: {e}")
        raise 

   

#function to calculate the crime rate per 1000 people 
def calculate_crimes_per_1000(df, group_by_cols, count_col, pop_df, df_merge_col, pop_merge_col, pop_col):
    """

    Groups the dataframe by specified columns, counts occurrences of a specified column, merges population data, 
    and calculates the crime rate per 1,000 people

    Parameters:
    df (pd.DataFrame): The dataframe to group and count
    group_by_cols (list or str): The column(s) to group by
    count_col (str): The column to count occurences of
    pop_df (pd.DataFrame): The population dataframe to merge with
    df_merge_col (str): The column to merge on from the original dataframe
    pop_merge_col (str): The column to merge on from the population dataframe 
    pop_col (str): The column in the populaton dataframe representing the population size

    Returns:
    pd.DataFrame: A dataframe with the count of crimes and the calculated crime rate per 1,000 people

    """
    try:
        logging.info('Grouping data and counting occurrences')
        #group by the specified columns and count the occurences of the count_col
        grouped_df = df.groupby(group_by_cols).agg({count_col: 'count'})

        #rename the count column to 'count'
        grouped_df = grouped_df.rename(columns={count_col: 'count'})

        #merge with the population dataframe on the specified column
        merged_df = grouped_df.merge(pop_df[[pop_merge_col, pop_col]].drop_duplicates(),
                                 left_on=df_merge_col, right_on=pop_merge_col)

        #calculate the crimes per 1,000 people and create a new column 
        merged_df['crimes per 1000'] = (merged_df['count'] / merged_df[pop_col]) * 1000
        logging.info('Crime rates per 1,000 people successfully calculated')
        return merged_df
    except Exception as e:
        logging.error(f"Error in calculating crimes per 1,000 people: {e}", exc_info=True)
        raise


#function to filter rows with the specified values in the column
def filter_by_column_values(df, column_name, values):
    """
    Filters the given dataframe by selecting rows where the specified column's values are in the provided list of values

    Parameters:
    df (pd.DataFrame): The dataframe to filter 
    column_name (str): The name if the column to filer by
    values (list): The list of values to filter for in the specified column 

    Returns:
    pd.DataFrame: A filtered dataframe containing only the rows with the specified values in the column
    """
    try:
        logging.info('Filtering dataframe by values in column')
        return df[df[column_name].isin(values)]
    except Exception as e:
        logging.error(f"Error in filtering dataframe by {column_name}: {e}", exc_info=True)
        raise


#calculate crime per 1000 people and merging dataframes with different column names 
def calculate_crime_statistics(df, group_by_cols, count_col, pop_df, df_merge_col, pop_merge_col, pop_col):
    """
    Groups the dataframe by specified columns, counts occurrences of a specified column, merges population data, 
    and calculates the crime rate per 1,000 people.

    Parameters:
    df (pd.DataFrame): The dataframe containing crime data.
    group_by_cols (list or str): The column(s) to group by (e.g., 'Date' and 'Region').
    count_col (str): The column to count occurrences of (e.g., 'Crime type').
    pop_df (pd.DataFrame): The population dataframe to merge with.
    df_merge_col (str): The column to merge on from the crime dataframe.
    pop_merge_col (str): The column to merge on from the population dataframe.
    pop_col (str): The column in the population dataframe representing the population size.

    Returns:
    pd.DataFrame: A dataframe with crime count, average population, crimes per 1,000 people, and the grouped columns.

    """
    try:
        logging.info('Grouping by columns and counting column')
        # Group by the specified columns and count occurrences of the count_col
        grouped_df = df.groupby(group_by_cols).agg({count_col: 'count'}).reset_index()

        # Rename the count column to 'count'
        grouped_df = grouped_df.rename(columns={count_col: 'count'})

        # Merge the crime data with the population data
        merged_df = grouped_df.merge(pop_df[[pop_merge_col, pop_col]].drop_duplicates(), 
                                 left_on=df_merge_col, right_on=pop_merge_col)

        # Calculate crimes per 1,000 people and create a new column
        merged_df['crimes per 1000'] = (merged_df['count'] / merged_df[pop_col]) * 1000
        logging.info('Crime statistics successfully calculated')
        return merged_df[['Date', df_merge_col, 'count', pop_col, 'crimes per 1000']]
    except Exception as e:
        logging.error(f"Error calcualting crime statistics: {e}", exc_info=True)
        raise


#merging dataframes and keeping only specified columns on the right dataframe 
def merge_specific_columns(df1, df2, merge_on, columns_to_keep, how='left'):
    """
    Merges two dataframes on a specified column and keeps only specific columns from the second dataframe.

    Parameters:
    df1 (pd.DataFrame): The first dataframe.
    df2 (pd.DataFrame): The second dataframe from which specific columns will be selected.
    merge_on (str): The column on which to merge the dataframes.
    columns_to_keep (list): The list of columns to keep from the second dataframe (including the merge column).
    how (str): The type of merge to perform. Default is 'left'.

    Returns:
    pd.DataFrame: The merged dataframe with the selected columns from the second dataframe.

    """
    try:
        logging.info('Merging dataframes and keepiny columns')
        # Ensure the merge column is included in the columns to keep
        if merge_on not in columns_to_keep:
            columns_to_keep.insert(0, merge_on)

        # Merge the dataframes on the specified column
        merged_df = pd.merge(df1, df2[columns_to_keep], how=how, on=merge_on)
        logging.info('Dataframes successfully merged')
        return merged_df
    except Exception as e:
        logging.error(f"Error merging dataframes: {e}", exc_info=True)
        raise

#function to create a final outcome column
def create_final_outcome(df, primary_col, fallback_col, new_col_name= 'Final outcome'):
    """
    Creates a new column in the dataframe where values are based on the primary column unless it is null,
    in which case values from the fallback column are used. 

    Parameters:
    df (pd.DataFrame): The dataframe on which to create the new column
    primary_col (str): The column whose values will be prioritized unless null
    fallback_col (str): The column whose values will be used if the primary coolumn is null 
    new_col_name (str): The name of the new column to be created. Default is 'Final outcome'

    Returns:
    pd.DataFrame: The dataframe with the new column added 
    """
    try:
        logging.info('Creating new column')
        df[new_col_name] = np.where(df[primary_col].notnull(), df[primary_col], df[fallback_col])
        logging.info('New column successfully created')
        return df
    except Exception as e:
        logging.error(f"Error creating final outcome column: {e}", exc_info=True)#
        raise


#categorizing and grouping the outcomes 
def categorize_and_group_outcomes(df, area_col, outcome_col, new_col_name='Broad outcome category'):
    """
    Categorizes the outcomes into smaller groups, creates a new column based on these categories,
    and then groups the dataframe by area and the newly categorized outcome, providing the count of each category.

    Parameters:
    df (pd.DataFrame): The dataframe to operate on.
    area_col (str): The column representing the area (e.g., 'Falls within').
    outcome_col (str): The column containing outcome types to categorize (e.g., 'Outcome type').
    new_col_name (str): The name for the new column with categorized outcomes. Default is 'Broad outcome category'.

    Returns:
    pd.DataFrame: A dataframe grouped by area and broad outcome category, with the count of each category.

    """
    try:
        logging.info('Categorizing outcomes and grouping')
        def categorize_outcome(outcome):
            """
            Internal function to categorize outcome types into broader groups.
            """
            if outcome in ['Unable to prosecute suspect', 'Investigation complete; no suspect identified']:
                return 'No Further Action'
            elif outcome in ['Local resolution', 'Offender given a caution', 'Action to be taken by another organisation', 'Awaiting court outcome']:
                return 'Non-criminal Outcome'
            elif outcome in ['Further investigation is not in the public interest', 'Further action is not in the public interest', 'Formal action is not in the public interest']:
                return 'Public Interest Consideration'
            elif outcome in ['Suspect charged', 'Suspect charged as part of another case']:
                return 'Suspect charged'
            elif outcome in ['Offender given a drugs possession warning', 'Offender given penalty notice']:
                return 'Offender given warning'
            else:
                return 'Unknown'  # Default category for unlisted outcomes

        # Apply the categorization function to create a new column
        df[new_col_name] = df[outcome_col].apply(categorize_outcome)

        # Group by the specified area and broad outcome category, and count occurrences
        grouped_df = df.groupby([area_col, new_col_name]).size().reset_index(name='Count')
        logging.info('Outcomes successfully categorized and grouped')
        return grouped_df
    except Exception as e:
        logging.error(f"Error in categorizing and grouping outcomes: {e}", exc_info=True)
        raise


#function to filter the dataframe 
def filter_dataframe(df, conditions):
    """
    Filters the dataframe based on specified conditions and returns the filtered dataframe.

    Parameters:
    df (pd.DataFrame): The dataframe to filter.
    conditions (dict): A dictionary of column names and their corresponding values to filter by. 
                       Example: {'Falls within': 'Essex Police', 'Crime type': 'Burglary'}.

    Returns:
    pd.DataFrame: The filtered dataframe.
    """
    try:
        logging.info('Filtering dataframes with conditions')
        #reset the index to avoid index misalignment issues 
        df = df.reset_index(drop = True)

        # Construct the filtering conditions dynamically from the dictionary
        condition = pd.Series([True] * len(df))  # Start with all rows as True
        for column, value in conditions.items():
            if column in df.columns:
                condition &= (df[column] == value)
            else:
                raise KeyError(f"Column '{column}' does not exist in the dataframe")

        # Apply the filter to the dataframe
        filtered_df = df[condition]
        logging.info('Dataframe successfully filtered')
        return filtered_df
    except KeyError as e:
        logging.error(f"Key error while filtering dataframe: {e}", exc_info=True)
        raise
    except Exception as e:
        logging.error(f"Error filtering dataframe: {e}", exc_info=True)
        raise


#function to group dataframe by specified columns and count values and reset index 
def group_and_count_reset_index(df, group_by_cols, count_col):
    """
    Groups a dataframe by the specified columns and counts the occurences of values in the specified column

    Parameters:
    df(pd.DataFrame): The dataframe to group and count
    group_by_cols (list or str): The column(s) to group by
    count_col (str): The column to count occurences of

    Returns:
    pd.DataFrame: A dataframe with grouped data and a count column 

    """
    try:
        logging.info('Grouping by and counting')
        #group by the specified columns and count the occurences of the count_col
        grouped_df = df.groupby(group_by_cols).agg({count_col: 'count'}).reset_index()

        #rename the count column to 'count'
        grouped_df = grouped_df.rename(columns={count_col: 'count'})
        logging.info('Data successfully grouped and counted')
        return grouped_df#
    except Exception as e:
        logging.error(f"Error grouping and counting data: {e}", exc_info=True)
        raise



def calculate_column_averages(df, columns, output_file=None):
    """
    Calculate the average of specified columns in a DataFrame and save the results to a new DataFrame.

    Parameters:
    df(pd.DataFrame): The input DataFrame containing the data.
    columns (list of str): List of column names for which the averages are to be calculated.
    output_file (str, optional): If provided, the resulting DataFrame will be saved to this CSV file.

    Returns:
    - pd.DataFrame
        A DataFrame containing the average values for the specified columns.
    """
    # Dictionary to store the average values
    averages = {}
    
    # Calculate the average for each specified column
    for column in columns:
        if column in df.columns:
            averages[column] = df[column].mean()
        else:
            logging.info(f"Warning: Column '{column}' not found in DataFrame.")
    
    # Create a new DataFrame to store the averages
    averages_df = pd.DataFrame(list(averages.items()), columns=['Column', 'Average'])
    
    # Save to CSV file if an output file is specified
    if output_file:
        averages_df.to_csv(output_file, index=False)
        logging.info(f"Results saved to {output_file}")
    
    return averages_df


# Staging layer 

def staging():
    """
    Ingest the data, apply cleaning, and store to CSV files for staging. 

    """
    logging.info('Start staging layer')
    #ingest the files
    combined_outcomes_df = combine_csv_files(OUTCOMES_FOLDER , 'combined_outcomes.csv')


    #combining the stop and search files and adding the area column 
    combined_stop_search = []
    for area, folder in STOP_AND_SEARCH_FOLDERS.items():
        df = combine_csv_files(folder, f'combined_{area.lower().replace(" ", "_")}_stop_search.csv')
        df = add_area_column(df, f'{area} Constabulary')
        combined_stop_search.append(df)


    combined_street_df = combine_csv_files(STREET_FOLDER, 'combined_street_df.csv')
    #combine all the files in the avon and somerset house prices folder 
    avon_and_somerset_house_price = combine_csv_files(HOUSE_PRICES_FOLDER, 'avon_and_somerset_house_price.csv')

    #ingesting other csv files 
    postcodes = pd.read_csv(POSTCODES_FILE , low_memory = False)
    essex_house_price = pd.read_csv(ESSEX_HOUSE_PRICE_FILE)
    population = pd.read_csv(POPULATION_FILE)
    
    
    #concat the stop and search dataframes 
    combined_stop_search = concat_dataframes(combined_stop_search,
                                         axis = 0, ignore_index = True)
    


    try:
        check_nulls(combined_outcomes_df)
        check_nulls(combined_stop_search)
        check_nulls(combined_street_df)
        check_nulls(avon_and_somerset_house_price)
        check_nulls(postcodes)
        check_nulls(essex_house_price)
        check_nulls(population)

        #replace nulls in the combined outcomes df
        replace_nulls(combined_outcomes_df, 'LSOA code', 'No LSOA code')
        replace_nulls(combined_outcomes_df, 'LSOA name', 'No LSOA name')

        #replace nulls in the combined stop and search df
        replace_nulls(combined_stop_search, 'Part of a policing operation', 'Unknown')
        replace_nulls(combined_stop_search, 'Gender', 'Unknown')
        replace_nulls(combined_stop_search, 'Age range', 'Unknown')
        replace_nulls(combined_stop_search, 'Self-defined ethnicity', 'Unknown')
        replace_nulls(combined_stop_search, 'Officer-defined ethnicity', 'Unknown')
        replace_nulls(combined_stop_search, 'Legislation', 'Unknown')
        replace_nulls(combined_stop_search, 'Object of search', 'Unknown')
        replace_nulls(combined_stop_search, 'Outcome', 'Unknown')
        replace_nulls(combined_stop_search, 'Removal of more than just outer clothing', 'Unknown')

        #replace nulls in the combined street df 
        replace_nulls(combined_street_df, 'LSOA code', 'No LSOA code')
        replace_nulls(combined_street_df, 'LSOA name', 'No LSOA name')
        replace_nulls(combined_street_df, 'Crime ID', 'No crime ID')
        replace_nulls(combined_street_df, 'Last outcome category', 'No last outcome category')

        #remove the rows with nulls in the stated columns for all dataframes 
        combined_outcomes_df = remove_nulls_in_columns(combined_outcomes_df, ['Latitude', 'Longitude'])
        combined_stop_search = remove_nulls_in_columns(combined_stop_search, ['Latitude', 'Longitude'])
        combined_street_df = remove_nulls_in_columns(combined_street_df, ['Latitude', 'Longitude'])



        #save staging files to CSV
        combined_outcomes_df.to_csv(STAGED_OUTCOMES_FILE, index = False)
        combined_street_df.to_csv(STAGED_STREET_FILE, index = False)
        combined_stop_search.to_csv(STAGED_STOP_SEARCH_FILE, index = False)
        avon_and_somerset_house_price.to_csv(STAGED_AVON_AND_SOMERSET_HOUSE_PRICE_FILE, index = False)
        postcodes.to_csv(STAGED_POSTCODES_FILE, index = False)
        essex_house_price.to_csv(STAGED_ESSEX_HOUSE_PRICE_FILE, index = False)
        population.to_csv(STAGED_POPULATION_FILE, index = False)

        

        logging.info('Staging layer complete')

    except Exception as e:
        logging.error(f'Error during data staging: {e}')




#Primary layer
def primary():
    """ 
    Transform the staged data, further pre-processing 

    """
    logging.info('Start primary layer')
    staged_outcomes = pd.read_csv(STAGED_OUTCOMES_FILE)
    staged_street = pd.read_csv(STAGED_STREET_FILE)
    staged_stop_search = pd.read_csv(STAGED_STOP_SEARCH_FILE)
    staged_postcodes = pd.read_csv(STAGED_POSTCODES_FILE, low_memory = False)
    staged_essex_house_price = pd.read_csv(STAGED_ESSEX_HOUSE_PRICE_FILE)
    staged_avon_and_somerset_house_price = pd.read_csv(STAGED_AVON_AND_SOMERSET_HOUSE_PRICE_FILE)

    try:
    
        #drop the columns from stop and search has they contain many nulls 
        staged_stop_search = drop_columns(staged_stop_search, ['Policing operation' , 'Outcome linked to object of search'])

        #drop the column from combined street has this was filled with nulls 
        staged_street = drop_columns(staged_street, ['Context'])

        #processing the date column in the street df to separate month and year 
        process_date_column(staged_street, 'Month', 'Date')

        #processing the postcode dataframe to select only the needed columns and filter for just england 
        staged_postcodes = staged_postcodes[['Postcode', 'Latitude', 'Longitude', 'Country', 'LSOA Code', 'Police force', 'Average Income']]
        staged_postcodes = staged_postcodes[staged_postcodes['Country'] == 'England']

        #selecting the columns needed from the essex house pricing dataframe
        staged_essex_house_price_all = staged_essex_house_price[['Name', 'Period', 'Average price All property types', 'Percentage change (yearly) All property types', 
                           'Percentage change (monthly) All property types', 'House price index All property types', 'Sales volume All property types']]

        #processing the date column to split it into months and years 
        staged_essex_house_price_all = process_date_column(staged_essex_house_price_all, 'Period', 'Date')


        #selecting the columns needed from the avon and somerset pricing dataframe 
        staged_avon_and_somerset_house_price_all = staged_avon_and_somerset_house_price[['Name', 'Period' , 'Average price All property types', 'Percentage change (yearly) All property types', 
                           'Percentage change (monthly) All property types', 'House price index All property types', 'Sales volume All property types']]

        #processing the date column to split it into months and years 
        staged_avon_and_somerset_house_price_all = process_date_column(staged_avon_and_somerset_house_price_all, 'Period', 'Date')


        #selecting the columns from the essex house price selecting all the different house types and pricing 
        staged_essex_house_pricing_by_house_type = staged_essex_house_price[['Name' ,'Period', 'Average price Detached houses','Percentage change (yearly) Detached houses',
                                                                      'Percentage change (monthly) Detached houses','House price index Detached houses','Average price Semi-detached houses',
                                                                      'Percentage change (yearly) Semi-detached houses','Percentage change (monthly) Semi-detached houses','House price index Semi-detached houses',
                                                                      'Average price Terraced houses','Percentage change (yearly) Terraced houses','Percentage change (monthly) Terraced houses',
                                                                      'House price index Terraced houses','Average price Flats and maisonettes','Percentage change (yearly) Flats and maisonettes',
                                                                      'Percentage change (monthly) Flats and maisonettes','House price index Flats and maisonettes']]
        
    
        #processing the date column to split it into months and years 
        staged_essex_house_pricing_by_house_type = process_date_column(staged_essex_house_pricing_by_house_type, 'Period', 'Date')


        #selecting the columns from the avon and somerset house price selecting all the different house types and pricing 
        staged_avon_and_somerset_house_pricing_by_house_type = staged_avon_and_somerset_house_price[['Name' ,'Period', 'Average price Detached houses','Percentage change (yearly) Detached houses',
                                                                                                     'Percentage change (monthly) Detached houses','House price index Detached houses','Average price Semi-detached houses',
                                                                                                     'Percentage change (yearly) Semi-detached houses','Percentage change (monthly) Semi-detached houses',
                                                                                                     'House price index Semi-detached houses','Average price Terraced houses','Percentage change (yearly) Terraced houses',
                                                                                                     'Percentage change (monthly) Terraced houses','House price index Terraced houses','Average price Flats and maisonettes',
                                                                                                     'Percentage change (yearly) Flats and maisonettes','Percentage change (monthly) Flats and maisonettes',
                                                                                                     'House price index Flats and maisonettes']]
        


        #processing the date column to split it into months and years 
        staged_avon_and_somerset_house_pricing_by_house_type = process_date_column(staged_avon_and_somerset_house_pricing_by_house_type, 'Period', 'Date')



        #save primary files to csv
        staged_outcomes.to_csv(PRIMARY_OUTCOMES_FILE, index = False)
        staged_street.to_csv(PRIMARY_STREET_FILE, index = False)
        staged_stop_search.to_csv(PRIMARY_STOP_SEARCH_FILE, index = False)
        staged_postcodes.to_csv(PRIMARY_POSTCODES_FILE, index = False)
        staged_essex_house_price_all.to_csv(PRIMARY_ESSEX_HOUSE_PRICE_ALL_FILE, index = False)
        staged_avon_and_somerset_house_price_all.to_csv(PRIMARY_AVON_AND_SOMERSET_HOUSE_PRICE_ALL_FILE, index = False)
        staged_essex_house_pricing_by_house_type.to_csv(PRIMARY_ESSEX_HOUSE_PRICING_BY_HOUSE_TYPE_FILE, index = False)
        staged_avon_and_somerset_house_pricing_by_house_type.to_csv(PRIMARY_AVON_AND_SOMERSET_HOUSE_PRICING_BY_HOUSE_TYPE_FILE, index = False)



        logging.info('Primary layer complete')

    except Exception as e:
        logging.error(f'Error during primary layer: {e}')




#Reporting layer 

def reporting():
    """
    Reporting layer: Store the aggregated reporting data to a csv file 
    
    """
    logging.info('Start the reporting layer')

    #ingest the primary csv files 
    primary_stop_search = pd.read_csv(PRIMARY_STOP_SEARCH_FILE)
    primary_outcomes = pd.read_csv(PRIMARY_OUTCOMES_FILE)
    primary_street = pd.read_csv(PRIMARY_STREET_FILE)
    staged_population = pd.read_csv(STAGED_POPULATION_FILE)
    primary_postcodes = pd.read_csv(PRIMARY_POSTCODES_FILE , low_memory = False)
    primary_essex_house_price_all = pd.read_csv(PRIMARY_ESSEX_HOUSE_PRICE_ALL_FILE)
    primary_avon_and_somerset_house_price_all = pd.read_csv(PRIMARY_AVON_AND_SOMERSET_HOUSE_PRICE_ALL_FILE)
    primary_essex_house_pricing_by_house_type = pd.read_csv(PRIMARY_ESSEX_HOUSE_PRICING_BY_HOUSE_TYPE_FILE)
    primary_avon_and_somerset_house_pricing_by_house_type = pd.read_csv(PRIMARY_AVON_AND_SOMERSET_HOUSE_PRICING_BY_HOUSE_TYPE_FILE)

    

    try:
        #merging the stop and search data and the population dataframe to make a population stop and search dataframe to be used later 
        population_stop_search = merge_dataframes(primary_stop_search, staged_population, 'Area', 'Area', how = 'left')


        #creating a table grouping by the area and the object of search and counting the object of search
        count_of_object_of_search = group_and_count(primary_stop_search, ['Area', 'Object of search'], 'Object of search')


        #this is creating a table which calculates the crimes per 1000 people in each area, by the count of object of the search and the average population, to be used to make a barplot.  
        stop_and_search_per_1000 = calculate_crimes_per_1000(primary_stop_search, 
                                           group_by_cols = ['Area', 'Object of search'],
                                           count_col = 'Object of search',
                                           pop_df = staged_population, 
                                           df_merge_col = 'Area',
                                           pop_merge_col = 'Area',
                                           pop_col = 'Average population')
        
        
        #this is creating a list of the specific crimes related to property 
        specific_values = ['Violence and sexual offences', 'Burglary' , 'Criminal damage and arson' , 'Robbery' , 'Anti-social behaviour' , 'Vehicle crime']
        #filters the street dateframe to make a new dataframe based on the stated values. 
        specific_crimes = filter_by_column_values(primary_street, 'Crime type' , specific_values)

        #merging the specific crimes and the population dataframes to be used later 
        population_specific_crime = merge_dataframes(specific_crimes, staged_population, 'Falls within', 'Area', how = 'left')
        

        #this is creating a table which calculates the crimes per 1000 people in each area, by the count of the crime type, this is to be used in a barplot 
        specific_crimes_per_1000 = calculate_crimes_per_1000(specific_crimes, 
                                           group_by_cols = ['Falls within'],
                                           count_col = 'Crime type',
                                           pop_df = staged_population, 
                                           df_merge_col = 'Falls within',
                                           pop_merge_col = 'Area',
                                           pop_col = 'Average population')
        
        #this is creating a table which calculates the crimes per 1000 people in each area by each month and by the count of the crime type, this is to be used in a line graph 
        specific_crimes_per_1000_over_time = calculate_crime_statistics(specific_crimes, 
                                           group_by_cols = ['Date' , 'Falls within'],
                                           count_col = 'Crime type',
                                           pop_df = staged_population, 
                                           df_merge_col = 'Falls within',
                                           pop_merge_col = 'Area',
                                           pop_col = 'Average population')

        #this is merging the specific crimes with the outcomes dataframe 
        outcome_specific = merge_specific_columns(df1=specific_crimes, 
                                                  df2=primary_outcomes, 
                                                  merge_on='Crime ID',
                                                  columns_to_keep=['Crime ID', 'Outcome type'])
        
        #this is creating a final outcomes column based on the values in the outcome type and the last outcome category 
        outcome_with_final_outcome = create_final_outcome(df = outcome_specific,
                                                primary_col = 'Outcome type', 
                                                fallback_col = 'Last outcome category', 
                                                new_col_name = 'Final outcome')
        
        #this is categorizing the outcome types into smaller broader categories to be used in a bar plot
        outcome_specific_with_broad_category = categorize_and_group_outcomes(df = outcome_specific, 
                                               area_col = 'Falls within', 
                                               outcome_col = 'Outcome type')
        
        #this is creating a dataframe with just the essex data that has a crime type of burglary to be used to make a heatmap 
        essex_and_burglary = filter_dataframe(df = specific_crimes, conditions = {'Falls within' : 'Essex Police' , 'Crime type' : 'Burglary'})

        #this is creating a dataframe with just the avon and somerset data that has a crime type of burglary to be used to make a heatmap 
        avon_and_somerset_and_burglary = filter_dataframe(df = specific_crimes, conditions = {'Falls within' : 'Avon and Somerset Constabulary' , 'Crime type' : 'Burglary'})

        #this is merging the avon and somerset burglary dataframe with the postcodes as this can be used to show the exact locations of these crimes happening on a map
        merge_avon_somerset_postcodes = pd.merge(avon_and_somerset_and_burglary, primary_postcodes, on = ['Latitude', 'Longitude'])

        #this is merging the essex burglargy dataframe with the postcodes as this can be used to show the exact locations of these crimes happening on a map
        merge_essex_postcode = pd.merge(essex_and_burglary, primary_postcodes, on = ['Latitude', 'Longitude'])

    
        #creates a table that can be used with primary_essex_house_price to create a line graph comparing average house price and the count of burglary
        essex_house_price_line = primary_essex_house_price_all
        essex_burglary_line = group_and_count_reset_index(essex_and_burglary, 'Date', 'Crime type')

        #creates a table that can be used with the staged_avon_and_somerset_house_price to create a line graph comparing average house price and the count of burglary
        avon_and_somerset_price_line = primary_avon_and_somerset_house_price_all
        avon_somerset_burglary_line = group_and_count_reset_index(avon_and_somerset_and_burglary, 'Date', 'Crime type')


        #this creates an average house price of each type of house in essex
        average_essex_house_type = calculate_column_averages(primary_essex_house_pricing_by_house_type, ['Average price Detached houses' , 'Average price Semi-detached houses',
                                                                                    'Average price Terraced houses', 'Average price Flats and maisonettes'],'average_essex_house_type.csv' )
        
        #this creates an average house price of each type of house in avon and somerset 
        average_avon_and_somerset_house_type = calculate_column_averages(primary_avon_and_somerset_house_pricing_by_house_type, ['Average price Detached houses' , 'Average price Semi-detached houses',
                                                                                    'Average price Terraced houses', 'Average price Flats and maisonettes'],'average_avon_and_somerset_house_type.csv')



    
        #save to csv 
        population_stop_search.to_csv(REPORTING_POPULATION_STOP_SEARCH_FILE, index = False)
        count_of_object_of_search.to_csv(REPORTING_COUNT_OF_OBJECT_OF_SEARCH_FILE, index = False)
        stop_and_search_per_1000.to_csv(REPORTING_STOP_AND_SEARCH_PER_1000_FILE, index = False)
        specific_crimes.to_csv(REPORTING_SPECIFIC_CRIMES_FILE, index = False)
        population_specific_crime.to_csv(REPORTING_POPULATION_SPECIFIC_CRIME_FILE, index = False)
        specific_crimes_per_1000.to_csv(REPORTING_SPECIFIC_CRIMES_PER_1000_FILE, index = False)
        specific_crimes_per_1000_over_time.to_csv(REPORTING_SPECIFIC_CRIMES_PER_1000_OVER_TIME_FILE, index = False)
        outcome_with_final_outcome.to_csv(REPORTING_OUTCOME_WITH_FINAL_OUTCOME_FILE, index = False)
        outcome_specific_with_broad_category.to_csv(REPORTING_OUTCOME_SPECIFIC_WITH_BROAD_CATEGORY_FILE, index = False)
        essex_and_burglary.to_csv(REPORTING_ESSEX_AND_BURGLARY_FILE, index = False)
        avon_and_somerset_and_burglary.to_csv(REPORTING_AVON_AND_SOMERSET_AND_BURGLARY_FILE, index = False)
        merge_avon_somerset_postcodes.to_csv(REPORTING_MERGE_AVON_SOMERSET_POSTCODES_FILE, index = False)
        merge_essex_postcode.to_csv(REPORTING_MERGE_ESSEX_POSTCODE_FILE, index = False)
        essex_burglary_line.to_csv(REPORTING_ESSEX_BURGLARY_LINE_FILE, index = False)
        avon_somerset_burglary_line.to_csv(REPORTING_AVON_SOMERSET_BURGLARY_LINE_FILE, index = False)
        essex_house_price_line.to_csv(REPORTING_ESSEX_HOUSE_PRICE_LINE_FILE, index = False)
        avon_and_somerset_price_line.to_csv(REPORTING_AVON_AND_SOMERSET_HOUSE_PRICE_LINE_FILE, index = False)
        average_essex_house_type.to_csv(REPORTING_AVERAGE_ESSEX_HOUSE_TYPE_FILE, index = False)
        average_avon_and_somerset_house_type.to_csv(REPORTING_AVERAGE_AVON_AND_SOMERSET_HOUSE_TYPE_FILE, index = False)



        logging.info('Reporting data completed successfully')

    except Exception as e:
        logging.error(f"Error during reporting data aggregation: {e}")



#Main pipeline 
def main(pipeline='all'):
    logging.info("Pipeline execution started")

    try:
        if pipeline in ['all', 'staging', 'primary', 'reporting']:
            staging()
            logging.info("Staging execution completed successfully")
            if pipeline == 'staging':
                # If only staging is requested, print success and return
                logging.info("Pipeline run complete")
                return
            # Process the staged data
            primary()
            logging.info("Primary execution completed successfully")
            if pipeline == 'primary':
                # If only primary is requested, print success and return 
                logging.info("Pipeline run complete")
                return
            # Generate reports based on processed data
            reporting()
            logging.info("Reporting execution completed successfully")
            if pipeline == 'reporting':
                logging.info("Pipeline run complete")
                return
            logging.info("Full pipeline run complete")
        else:
            # Inform the user about an invalid pipeline stage input
            logging.critical("Invalid pipeline stage specified. Please choose 'staging', 'primary', 'reporting', or 'all'.")
    except Exception as e:
        # Catch and print any exceptions occurred during pipeline execution
        logging.error(f"Pipeline execution failed: {e}")

if __name__ == "__main__":
    main()
    


