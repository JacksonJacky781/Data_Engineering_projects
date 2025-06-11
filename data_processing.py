import boto3
import pandas as pd
import os

# Local file directories
# These paths can be updated to meet your testing environment needs.
#source_path = "C:\\Users\\f5461960\\OneDrive - FRG\\Documents\\SchoolWork\\Moving Big Data\\data\\Stocks\\"
#save_path = "C:\\Users\\f5461960\\OneDrive - FRG\\Documents\\SchoolWork\\Moving Big Data\\data\\Output\\historical_stock_data.csv"
#index_file_path = 'C:\\Users\\f5461960\\OneDrive - FRG\\Documents\\SchoolWork\\Moving Big Data\\data\\top_companies.txt'

source_path ="/home/ubuntu/s3-bucket/Stocks/"
save_path ="/home/ubuntu/s3-bucket/Output/"
index_file_path ="/home/ubuntu/s3-bucket/CompanyNames/top_companies.txt"
output_file_name ="historical_stock_data.csv" 
#
required_columns = ['Date', 'Open', 'High', "Low","Close","Volume"]

def extract_companies_from_index(index_file_path):
    """Generate a list of company files that need to be processed. 

    Args:
        index_file_path (str): path to index file

    Returns:
        list: Names of company names. 
    """
    company_file = open(index_file_path, "r")
    contents = company_file.read()
    contents = contents.replace("'","")
    contents_list = contents.split(",")
    cleaned_contents_list = [item.strip() for item in contents_list]
    company_file.close()
    return cleaned_contents_list

def get_path_to_company_data(list_of_companies, source_data_path):
    """Creates a list of the paths to the company data
       that will be processed

    Args:
        list_of_companies (list): Extracted `.csv` file names of companies whose data needs to be processed.
        source_data_path (str): Path to where the company `.csv` files are stored. 

    Returns:
        [type]: [description]
    """
    path_to_company_data = []
    for file_name in list_of_companies:
        path_to_company_data.append(source_data_path + file_name)
    return path_to_company_data

def save_table(dataframe, output_path, file_name, header):
    """Saves an input pandas dataframe as a CSV file according to input parameters.

    Args:
        dataframe (pandas.dataframe): Input dataframe.
        output_path (str): Path to which the resulting `.csv` file should be saved. 
        file_name (str): The name of the output `.csv` file. 
        header (boolean): Whether to include column headings in the output file.
    """
    print(f"Path = {output_path}, file = {file_name}")
    dataframe.to_csv(output_path + file_name + ".csv", index=False, header=header)

def data_processing(file_paths, output_path):
    """
    Process and collate company csv file data for use within the data processing component of the formed data pipeline.

    Args:
        file_paths (list[str]): A list of paths to the company csv files that need to be processed. 
        output_path (str): The path to save the resulting csv file to.
    """
    combined_data = []
    
    for file_path in file_paths:
        try:
            # Read the input CSV file
            data = pd.read_csv(file_path)
            
            # Ensure required columns are present
            required_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
            if not all(column in data.columns for column in required_columns):
                print(f"Skipping {file_path}: Missing required columns.")
                continue
            
            # Convert Date to datetime
            data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
            
            # Drop rows with invalid dates
            data = data.dropna(subset=['Date'])
            
            # Calculate daily_percent_change and value_change
            data['daily_percent_change'] = ((data['Close'] - data['Open']) / data['Open']) * 100
            data['value_change'] = data['Close'] - data['Open']
            
            # Add the company_name column based on the file name
            company_name = os.path.basename(file_path).replace('.csv', '')
            data['company_name'] = company_name
            
            # Append processed data
            combined_data.append(data)
        
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            continue

    # Combine all data
    if combined_data:
        final_data = pd.concat(combined_data, ignore_index=True)
        
        # Save to CSV without headers
        final_data.to_csv(output_path, index=False, header=False)
        print(f"Data successfully saved to {output_path}")
    else:
        print("No valid data to process.")

if __name__ == "__main__":

    # Get all file names in source data directory of companies whose data needs to be processed, 
    # This information is specified within the `top_companies.txt` file. 
    file_names = extract_companies_from_index(index_file_path)

    # Update the company file names to include path information. 
    path_to_company_data = get_path_to_company_data(file_names, source_path)
    
    # Process company data and create full data output
    data_processing(path_to_company_data, save_path)