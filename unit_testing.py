import pytest
import pandas as pd
import logging
import os
from unittest import mock 
from Crime_project_part_2 import ingest_data, combine_csv_files, add_area_column, concat_dataframes, check_nulls, replace_nulls, remove_nulls_in_columns, calculate_crime_statistics, drop_columns, group_and_count

logging.basicConfig(
    filename='pipeline.log',  # Specifies the log file name
    filemode='a',             # Opens the log file in append mode  
    format='%(asctime)s %(levelname)s:%(message)s',  # Specifies the format of log messages
    datefmt='%Y-%m-%d %H:%M:%S',  # Specifies the date and time format
    level=logging.INFO # Sets the logging level to INFO
)


#sample dataframe for testing 
@pytest.fixture
def sample_dataframe():
    """
    Fixture for a sample dataframe 
    """
    return pd.DataFrame({
        'Crime ID': [1, 2, 3],
        'Crime type': ['Theft' , 'Assault' , 'Assault'],
        'Region': ['Essex', 'Avon and Somerset', 'Essex']})


@pytest.fixture
def crime_data():
    # Simulated crime data
    return pd.DataFrame({
        'Date': ['2020-01', '2020-01', '2020-02', '2020-02'],
        'Region': ['Essex', 'Essex', 'Avon and Somerset', 'Avon and Somerset'],
        'Crime type': ['Theft', 'Assault', 'Theft', 'Assault']
    })

@pytest.fixture
def population_data():
    return pd.DataFrame({
        'Region': ['Essex', 'Avon and Somerset'],
        'Population': [1000, 2000]
    })






#Testing ingest_data function: 3 test cases

# Test case 1: File does not exist
def test_ingest_data_file_not_found():
    # Mock os.path.exists to return False
    with mock.patch('os.path.exists', return_value=False):  #this simulates whether a file exists without needing an actual file system check 
        result = ingest_data("fake_path.csv")

        # Assert that the function returns None if the file does not exist
        assert result is None


# Test case 2: Successfully reading a CSV file
def test_ingest_data_success(sample_dataframe):

    # Mock os.path.exists to return True, and pd.read_csv to return sample data
    with mock.patch('os.path.exists', return_value=True), \
         mock.patch('pandas.read_csv', return_value=sample_dataframe):  #this mocks the behaviour of a csv so we can simulate read data or triggering exceptions without needing actual CSV files. 

        result = ingest_data("dummy_path.csv")

        # Assert that the result matches the sample data
        assert result.equals(sample_dataframe)


# Test case 3: Error while reading the CSV file (ValueError)
def test_ingest_data_read_error():
    # Mock os.path.exists to return True, and pandas.read_csv to raise a ValueError
    with mock.patch('os.path.exists', return_value=True), \
         mock.patch('pandas.read_csv', side_effect=ValueError("Invalid CSV format")):

        # Assert that the function raises a ValueError when read_csv fails
        with pytest.raises(ValueError, match="Invalid CSV format"):
            ingest_data("invalid_csv.csv")


#Testing combine_csv_files function: 3 test cases


# Test case 1: Combining multiple CSV files
def test_combine_csv_files_multiple_files():
    # Mock os.listdir to return a list of CSV files
    with mock.patch('os.listdir', return_value=['file1.csv', 'file2.csv']):
        # Mock pd.read_csv to return specific dataframes for each file
        df1 = pd.DataFrame({'Crime ID': [1, 2], 'Falls within': ['Essex_police', 'West_Midlands']})
        df2 = pd.DataFrame({'Crime ID': [5, 6], 'Falls within': ['Essex_police', 'West_Midlands']})
        
        with mock.patch('pandas.read_csv', side_effect=[df1, df2]):
            result_df = combine_csv_files('fake_folder')

            # Concatenated DataFrame should have 4 rows and 2 columns
            assert result_df.shape == (4, 2)
            
            # Expected DataFrame
            expected_df = pd.DataFrame({
                'Crime ID': [1, 2, 5, 6],
                'Falls within': ['Essex_police', 'West_Midlands', 'Essex_police', 'West_Midlands']
            })
            assert result_df.equals(expected_df)


# Test case 2: No CSV files in the folder
def test_combine_csv_files_empty_folder():
    # Mock os.listdir to return an empty list
    with mock.patch('os.listdir', return_value=[]):
        result_df = combine_csv_files('fake_folder')

        # Assert that an empty DataFrame is returned
        assert result_df.empty


# Test case 3: Handling a file reading error (e.g., file not found or invalid CSV)
def test_combine_csv_files_read_error():
    # Mock os.listdir to return a list of CSV files
    with mock.patch('os.listdir', return_value=['file1.csv']):
        # Mock pd.read_csv to raise an error for the first file
        with mock.patch('pandas.read_csv', side_effect=ValueError('Error reading CSV')):
            with pytest.raises(ValueError, match='Error reading CSV'):
                combine_csv_files('fake_folder')




#Testing add area column: 2 test cases 

#Test case 1: Testing the 'Area' column is added and contains the correct area name 
def test_add_area_column(sample_dataframe):
    
    #example value for Area column
    area_name = 'Test Area'

    result_df = add_area_column(sample_dataframe, area_name)

    #check if the 'Area' column is added
    assert 'Area' in result_df.columns


#Test case 2: Testing the function correctly handles non-string area names 
def test_non_string_area_name(sample_dataframe):

    result_df = add_area_column(sample_dataframe, 123)

    #check if the 'Area' column contains the correct non-string values
    assert all(result_df['Area'] == 123)
    


#Testing concat_dataframes: 2 test cases

#Test case 1: Testing concating along rows 
def test_concat_dataframes_rows():
    # Create some sample dataframes
    df1 = pd.DataFrame({'Crime ID': [1, 2], 'Falls within': ['Essex_police', 'West_Midlands']})
    df2 = pd.DataFrame({'Crime ID': [5, 6], 'Falls within': ['Essex_police', 'West_Midlands']})
    
    # Concatenate along rows (axis=0)
    result_df = concat_dataframes([df1, df2], ignore_index = True)
    expected_df = pd.DataFrame({'Crime ID': [1, 2, 5, 6], 'Falls within': ['Essex_police', 'West_Midlands', 'Essex_police', 'West_Midlands']})
    
    # Assert the concatenated DataFrame matches the expected one
    assert result_df.equals(expected_df)


#Test case 2: Testing concating along columns 
def test_concat_dataframes_cols():
    # Create some sample dataframes
    df1 = pd.DataFrame({'Crime ID': [1, 2], 'Falls within': ['Essex_police', 'West_Midlands']})
    df2 = pd.DataFrame({'Crime ID': [5, 6], 'Falls within': ['Essex_police', 'West_Midlands']})

    # Concatenate along columns (axis=1)
    result_df_col = concat_dataframes([df1, df2], axis=1)
    
    # Assert the concatenated DataFrame along columns
    assert result_df_col.shape == (2, 4)



#Testing check_nulls: 1 test case

def test_check_nulls():
    # DataFrame with some null values
    df = pd.DataFrame({'Crime ID': [1, None, 3], 'Falls within': [None, 'Essex police', 'West_Midlands']})
    
    # Check nulls
    null_count = check_nulls(df)
    expected_null_count = pd.Series({'Crime ID': 1, 'Falls within': 1})
    
    # Assert that the null count matches expected values
    assert null_count.equals(expected_null_count)


#Testing replace_nulls: 1 test case

def test_replace_nulls():
    # DataFrame with some null values
    df = pd.DataFrame({'Crime ID': [1, None, 3], 'Falls within': [None, 'Essex_police', 'West_Midlands']})
    
    # Replace nulls in column 'A'
    df_filled = replace_nulls(df, 'Crime ID', 0)
    expected_df = pd.DataFrame({'Crime ID': [1, 0, 3], 'Falls within': [None, 'Essex_police', 'West_Midlands']})

    # Ensure same data types
    df_filled['Crime ID'] = df_filled['Crime ID'].astype(float)
    expected_df['Crime ID'] = expected_df['Crime ID'].astype(float)
    
    # Assert that the DataFrame after replacing nulls matches expected DataFrame
    assert df_filled.equals(expected_df)


#Testing remove_nulls_in_columns: 2 test cases

#Test case 1: Removing nulls in one column
def test_remove_nulls_in_columns():
    # DataFrame with some null values
    df = pd.DataFrame({'Crime ID': [1, None, 3], 'Falls within': [None, 'Essex_police', 'West_Midlands']})
    
    # Remove rows with null values in column 'A'
    df_cleaned = remove_nulls_in_columns(df, ['Crime ID'])
    expected_df = pd.DataFrame({'Crime ID': [1, 3], 'Falls within': [None, 'West_Midlands']})

    # Create copies of DataFrames for modification
    df_cleaned = df_cleaned.copy()
    expected_df = expected_df.copy()

    # Ensure the same dtypes
    df_cleaned['Crime ID'] = df_cleaned['Crime ID'].astype(float)
    expected_df['Crime ID'] = expected_df['Crime ID'].astype(float)

    # Reset index before comparison
    df_cleaned = df_cleaned.reset_index(drop=True)
    expected_df = expected_df.reset_index(drop=True)
    
    # Assert that the DataFrame after removing nulls matches expected DataFrame
    assert df_cleaned.equals(expected_df)

#Test case 2: Removing nulls in two columns 
def test_remove_nulls_in_both_columns():
    # DataFrame with some null values
    df = pd.DataFrame({'Crime ID': [1, None, 3], 'Falls within': [None, 'Essex_police', 'West_Midlands']})
    
    # Test removing rows with nulls in both columns 'A' and 'B'
    df_cleaned_both = remove_nulls_in_columns(df, ['Crime ID', 'Falls within'])
    expected_df_both = pd.DataFrame({'Crime ID': [3], 'Falls within': ['West_Midlands']})

    # Create copies of DataFrames for modification
    df_cleaned_both = df_cleaned_both.copy()
    expected_df_both = expected_df_both.copy()

    # Ensure the same dtypes
    df_cleaned_both['Crime ID'] = df_cleaned_both['Crime ID'].astype(float)
    expected_df_both['Crime ID'] = expected_df_both['Crime ID'].astype(float)

    # Reset index before comparison
    df_cleaned_both = df_cleaned_both.reset_index(drop=True)
    expected_df_both = expected_df_both.reset_index(drop=True)
    

    # Assert that the DataFrame after removing rows with nulls in multiple columns matches expected DataFrame
    assert df_cleaned_both.equals(expected_df_both)



#Test drop_columns: 1 test case

#Test case 1: Test that specific columns are dropped from the dataframe 
def test_drop_columns(sample_dataframe):
    """Test if specific columns are dropped from the dataframe."""
    df = drop_columns(sample_dataframe, ['Region'])
    assert 'Region' not in df.columns
    


#Test group_and_count: 1 test case

#Test case 1: Testing how it handles an empty dataframe 
def test_group_and_count_empty():
    """Test if the function handles an empty dataframe."""
    empty_df = pd.DataFrame(columns=['Category', 'Value'])
    df = group_and_count(empty_df, 'Category', 'Value')
    assert df.empty





#Testing calculate crime statistics: 3 test cases

# Test cases using parameterization
@pytest.mark.parametrize(
    "group_by_cols, count_col, df_merge_col, pop_merge_col, pop_col, expected_crimes_per_1000",
    [
        (['Date', 'Region'], 'Crime type', 'Region', 'Region', 'Population', [2.0, 1.0]),  # Regular case
    ]
)


#Test case 1: Checking the sample matches the expected 
def test_calculate_crime_statistics(crime_data, population_data, group_by_cols, count_col, df_merge_col, pop_merge_col, pop_col, expected_crimes_per_1000):

    result_df = calculate_crime_statistics(crime_data, group_by_cols, count_col, population_data, df_merge_col, pop_merge_col, pop_col)
    
    # Check if the 'crimes per 1000' column matches the expected values
    assert list(result_df['crimes per 1000']) == expected_crimes_per_1000




#Test case 2: Test for missing columns or incorrect merge
def test_missing_column(crime_data, population_data):
    with pytest.raises(KeyError):
        #Attempting to group by a missing column should raise a KeyError
        calculate_crime_statistics(crime_data, ['MissingColumn'], 'Crime type', population_data, 'Region', 'Region', 'Population')



#Test case 3: Test for mismatch in the merge column
def test_mismatched_merge_column(crime_data, population_data):
    mismatched_pop_df = pd.DataFrame({
        'DifferentRegion': ['North', 'South'],
        #'Population': [1000, 2000]
    })

    with pytest.raises(KeyError):
        # Attempting to merge on non-matching columns should raise a KeyError
        calculate_crime_statistics(crime_data, ['Date', 'Region'], 'Crime type', mismatched_pop_df, 'Region', 'DifferentRegion', 'Population')

