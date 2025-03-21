
import pandas as pd
import sys
import os
from dotenv import load_dotenv
import re
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils import FileUtils

load_dotenv()

def load_excel_dataframe(logger, input_file):
    """Loads an Excel file into a DataFrame."""
    try:
        df = pd.read_excel(input_file, sheet_name='Raw Data', engine='openpyxl')
        logger.info(f"Excel file loaded successfully")
        return df
    except Exception as exc:
        logger.error(f"Unable to read Excel file: {exc}", exc_info=True)
        raise

def extract_college_year(value):
    """Extracts college name and year from passing_year column."""
    match = re.search(r'(.+),\s(\d{4})$', str(value))
    if match:
        return match.group(1), match.group(2)
    return value, None

def extract_mci_details(value):
    """Extracts MCI number and State Medical Council from mci column."""
    match = re.search(r'(\d+)\s(.+),\s(\d{4})$', str(value))
    if match:
        return match.group(1), match.group(2)
    return value, None

def consolidate_data(logger, raw_data):
    """Consolidates raw data and merges multiple URLs into a single row by grouping record_id."""
    try:
        logger.info(f"Consolidating data...")
        raw_data['College name'], raw_data['Year of graduation'] = zip(*raw_data['passing_year'].apply(extract_college_year))
        raw_data['Mci No'], raw_data['State Medical Council'] = zip(*raw_data['mci'].apply(extract_mci_details))

        # to store nuumber format in excel
        raw_data['Pin'] = pd.to_numeric(raw_data['pincode'], errors='coerce')
        raw_data['Year of graduation'] = pd.to_numeric(raw_data['Year of graduation'], errors='coerce')
        raw_data['Mci No'] = pd.to_numeric(raw_data['Mci No'], errors='coerce')
        
        consolidated_data = raw_data.groupby('record_id').agg({
            'name': 'first',
            'url': lambda x: ', '.join(x.dropna().unique()),
            'experience': 'first',
            'speciality': 'first',
            'sheet_speciality': 'first',
            'education': 'first',
            'clinic_name': 'first',
            'address': 'first',
            'city': 'first',
            'state': 'first',
            'pincode': 'first',
            'College name': 'first',
            'Year of graduation': 'first',
            'Mci No': 'first',
            'State Medical Council': 'first'
        }).reset_index()
        
        consolidated_data.rename(columns={
            'name': 'Name',
            'experience': 'Past Experience',
            'speciality': 'Specialty 1',
            'sheet_speciality': 'Specialty 2',
            'education': 'Qualification',
            'clinic_name': 'Clinic name/ Hosp affiliation',
            'address': 'Street name',
            'pincode': 'Pin'
        }, inplace=True)

        consolidated_data.insert(3, 'Gender', '')
        
        logger.info(f"Data consolidation successfully completed.")
        return consolidated_data
    except Exception as exc:
        logger.error(f"Error occurred during data consolidation: {exc}", exc_info=True)
        raise

def main():

    local_path = os.getenv("LOCAL_PATH")
    input_file = f'{local_path}/Raw Data (7).xlsx'
    output_file = os.path.join(local_path, "Processed_data_output.csv")

    # Setup log
    logger = FileUtils.set_logger('ETL_Analysis')
    FileUtils.check_and_create(local_path, logger)

    logger.info(f"Starting ETL process...")
    raw_data = load_excel_dataframe(logger, input_file)
    consolidated_data = consolidate_data(logger, raw_data)

    # Save output
    logger.info(f"Saving processed data...")
    consolidated_data.to_csv(output_file, index=False)
    logger.info(f"Processed file saved as {output_file}")

if __name__ == "__main__":
    main()