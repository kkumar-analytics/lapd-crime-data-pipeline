import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, exc as sa_exc
import keyring
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

def load_config() -> dict:
    """Load configuration from environment variables."""
    load_dotenv()
    config = {
        "host": os.environ.get("MYSQL_HOST", "localhost"),
        "database": os.environ.get("MYSQL_DATABASE", "crime_data"),
        "batch_size": int(os.environ.get("INSERT_BATCH_SIZE", 1000)),
        "input_file": os.environ.get(
            "DATA_FILE_PATH", "../data/lapd_crime_data.csv"
        ),
    }
    return config

def create_connection():
    """Create a connection to MySQL using SQLAlchemy with better error handling."""
    user = keyring.get_password("mysql", "user")
    password = keyring.get_password("mysql", "pwd")
    config = load_config()
    engine = None
    try:
        engine = create_engine(f"mysql+pymysql://{user}:{password}@{config['host']}/{config['database']}")
        engine.connect()
        logging.info("Successfully connected to MySQL.")
    except sa_exc.SQLAlchemyError as e:
        logging.error(f"Error creating or connecting to MySQL: {e}")
    return engine

def validate_data(df):
    """Example function for basic data validation."""
    if 'DR_NO' not in df.columns or df['DR_NO'].isnull().any():
        logging.warning("DR_NO column is missing or contains null values.")
        return False
    return True

def insert_data_to_mysql(engine, data_frame, table_name) -> None:
    """Insert data into MySQL, handling duplicates and NaN values."""
    try:
        # Replace NaN values with None (NULL)
        data_frame = data_frame.replace({np.nan: None})
        # Create a list of column names and placeholders
        cols = ", ".join(data_frame.columns)
        placeholders = ", ".join(["%s"] * len(data_frame.columns))
        update_cols = ", ".join(
            [f"{col} = VALUES({col})" for col in data_frame.columns if col != "DR_NO"]
        )  # Exclude DR_NO from update
        update_cols += ", DL_UPD = CURRENT_TIMESTAMP"
        # Construct the INSERT ... ON DUPLICATE KEY UPDATE statement
        insert_statement = f"""
            INSERT INTO {table_name} ({cols})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_cols}
        """
        # Prepare the data for insertion
        data = [tuple(x) for x in data_frame.to_numpy()]
        # Execute the query using the raw connection
        conn = engine.raw_connection()
        try:
            cursor = conn.cursor()
            cursor.executemany(insert_statement, data)
            conn.commit()
            cursor.close()
        except Exception as e:
            conn.rollback()
            logging.error(f"Error during insertion/update: {e}")
            raise
        finally:
            conn.close()
        logging.info(f"Data inserted/updated successfully in {table_name}.")
    except sa_exc.SQLAlchemyError as e:
        logging.error(f"Error inserting/updating data in {table_name}: {e}")


def main():
    config = load_config()
    engine = create_connection()
    if not engine:
        return
    try:
        # Load data
        input_file = config['input_file']
        logging.info(f"Loading data from: {input_file}")
        df = pd.read_csv(input_file)
        logging.info(f"Successfully loaded {len(df)} rows.")
        # Rename columns
        column_mapping = {
            'Date Rptd': 'DATE_RPTD',
            'DATE OCC': 'DATE_OCC',
            'TIME OCC': 'TIME_OCC',
            'AREA': 'AREA',
            'AREA NAME': 'AREA_NAME',
            'Rpt Dist No': 'RPT_DIST_NO',
            'Part 1-2': 'PART_1_2',
            'Crm Cd': 'CRM_CD',
            'Crm Cd Desc': 'CRM_CD_DESC',
            'Mocodes': 'MOCODES',
            'Vict Age': 'VICT_AGE',
            'Vict Sex': 'VICT_SEX',
            'Vict Descent': 'VICT_DESCENT',
            'Premis Cd': 'PREMIS_CD',
            'Premis Desc': 'PREMIS_DESC',
            'Weapon Used Cd': 'WEAPON_USED_CD',
            'Weapon Desc': 'WEAPON_DESC',
            'Status': 'STATUS',
            'Status Desc': 'STATUS_DESC',
            'Crm Cd 1': 'CRM_CD_1',
            'Crm Cd 2': 'CRM_CD_2',
            'Crm Cd 3': 'CRM_CD_3',
            'Crm Cd 4': 'CRM_CD_4',
            'LOCATION': 'LOCATION',
            'Cross Street': 'CROSS_STREET',
            'LAT': 'LAT',
            'LON': 'LON'
        }
        df = df.rename(columns=column_mapping)
        logging.info("Columns renamed.")
        # Validate data
        if not validate_data(df):
            logging.error("Data validation failed. Aborting insertion.")
            return
        # Convert datetime fields
        try:
            df['DATE_RPTD'] = pd.to_datetime(df['DATE_RPTD'], format='%m/%d/%Y %I:%M:%S %p', errors='coerce')
            df['DATE_OCC'] = pd.to_datetime(df['DATE_OCC'], format='%m/%d/%Y %I:%M:%S %p', errors='coerce')
            logging.info("Datetime fields converted.")
        except ValueError as e:
            logging.error(f"Error converting datetime fields: {e}")
            return
        # Insert into DB
        insert_data_to_mysql(engine, df, table_name='raw_lapd_crime_data')
    except FileNotFoundError:
        logging.error(f"Error: Input file not found at {config['input_file']}")
    except pd.errors.ParserError as e:
        logging.error(f"Error parsing CSV file: {e}")
    except Exception as e:
        logging.critical(f"An unexpected error occurred: {e}")
    finally:
        if engine:
            engine.dispose()

if __name__ == '__main__':
    main()