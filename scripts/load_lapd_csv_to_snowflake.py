import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
import logging
import argparse
from dotenv import load_dotenv
import keyring

# === LOGGING ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

def load_config():
    """Load configuration from environment variables."""
    load_dotenv()
    config = {
        "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
        "SNOWFLAKE_DATABASE": os.getenv("SNOWFLAKE_DATABASE"),
        "SNOWFLAKE_SCHEMA": os.getenv("SNOWFLAKE_SCHEMA"),
        "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "SNOWFLAKE_RAW_TABLE": os.getenv("SNOWFLAKE_RAW_TABLE"),
        "SNOWFLAKE_TEMP_TABLE": os.getenv("SNOWFLAKE_TEMP_TABLE"),
        "SNOWFLAKE_USER": keyring.get_password("snowflake", "user"),
        "SNOWFLAKE_PASSWORD": keyring.get_password("snowflake", "pwd")
    }
    missing_config = [key for key, value in config.items() if value is None]
    if missing_config:
        logger.error(f"Missing configuration: {', '.join(missing_config)}")
        raise ValueError(f"Missing configuration for: {', '.join(missing_config)}")
    logger.info("Configuration successfully loaded.")
    return config

def load_csv(file_path):
    """Loads a CSV file into a Pandas DataFrame."""
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Successfully loaded CSV file: {file_path}")
    except FileNotFoundError:
        logger.error(f"File not found at {file_path}")
        return None
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        return None

    df.columns = df.columns.str.upper().str.replace(" ", "_")
    df.rename(columns=lambda x: x.replace("-", "_"), inplace=True)

    date_cols = ['DATE_RPTD', 'DATE_OCC']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format='%m/%d/%Y %I:%M:%S %p', errors='coerce')
            df[col] = df[col].dt.strftime('%Y-%m-%d')
            logger.info(f"Converted column {col} to datetime")
    return df

def connect_snowflake():
    """Establishes a connection to Snowflake."""
    config = load_config()
    try:
        conn = snowflake.connector.connect(
            account=config['SNOWFLAKE_ACCOUNT'],
            user=config['SNOWFLAKE_USER'],
            password=config['SNOWFLAKE_PASSWORD'],
            warehouse=config['SNOWFLAKE_WAREHOUSE'],
            database=config['SNOWFLAKE_DATABASE'],
            schema=config['SNOWFLAKE_SCHEMA'],
        )
        logger.info("Successfully connected to Snowflake.")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {e}")
        return None

def load_to_snowflake_tmp(df, conn, table_name):
    """Truncates and loads a DataFrame into a Snowflake temp table."""
    if df is None or conn is None:
        logger.error("DataFrame or connection is invalid.")
        return
    try:
        # Truncate the temp table before loading new data
        with conn.cursor() as cur:
            truncate_sql = f"TRUNCATE TABLE {table_name};"
            cur.execute(truncate_sql)
            logger.info(f"Truncated table: {table_name}")
        # Load new data into the table
        success, nchunks, nrows, _ = write_pandas(
            conn, df, table_name, quote_identifiers=False
        )
        logger.info(f"Upload successful: {success}, Rows uploaded: {nrows}")
    except Exception as e:
        logger.error(f"Error loading data to Snowflake: {e}")
        try:
            conn.rollback()
            logger.info("Rolled back transaction.")
        except Exception as rollback_error:
            logger.error(f"Rollback failed: {rollback_error}")
    finally:
        try:
            conn.close()
            logger.info("Closed Snowflake connection.")
        except Exception as close_error:
            logger.error(f"Failed to close connection: {close_error}")

def merge_to_raw_table(conn, config) -> int:
    """Merge data from temp to raw table in Snowflake."""
    temp_table = config["SNOWFLAKE_TEMP_TABLE"]
    raw_table = config["SNOWFLAKE_RAW_TABLE"]
    merge_sql = f"""
        MERGE INTO {raw_table} AS T
USING {temp_table} AS S
ON T.DR_NO = S.DR_NO
WHEN MATCHED AND (
    T.DATE_RPTD IS DISTINCT FROM S.DATE_RPTD OR
    T.DATE_OCC IS DISTINCT FROM S.DATE_OCC OR
    T.TIME_OCC IS DISTINCT FROM S.TIME_OCC OR
    T.AREA IS DISTINCT FROM S.AREA OR
    T.AREA_NAME IS DISTINCT FROM S.AREA_NAME OR
    T.RPT_DIST_NO IS DISTINCT FROM S.RPT_DIST_NO OR
    T.PART_1_2 IS DISTINCT FROM S.PART_1_2 OR
    T.CRM_CD IS DISTINCT FROM S.CRM_CD OR
    T.CRM_CD_DESC IS DISTINCT FROM S.CRM_CD_DESC OR
    T.MOCODES IS DISTINCT FROM S.MOCODES OR
    T.VICT_AGE IS DISTINCT FROM S.VICT_AGE OR
    T.VICT_SEX IS DISTINCT FROM S.VICT_SEX OR
    T.VICT_DESCENT IS DISTINCT FROM S.VICT_DESCENT OR
    T.PREMIS_CD IS DISTINCT FROM S.PREMIS_CD OR
    T.PREMIS_DESC IS DISTINCT FROM S.PREMIS_DESC OR
    T.WEAPON_USED_CD IS DISTINCT FROM S.WEAPON_USED_CD OR
    T.WEAPON_DESC IS DISTINCT FROM S.WEAPON_DESC OR
    T.STATUS IS DISTINCT FROM S.STATUS OR
    T.STATUS_DESC IS DISTINCT FROM S.STATUS_DESC OR
    T.CRM_CD_1 IS DISTINCT FROM S.CRM_CD_1 OR
    T.CRM_CD_2 IS DISTINCT FROM S.CRM_CD_2 OR
    T.CRM_CD_3 IS DISTINCT FROM S.CRM_CD_3 OR
    T.CRM_CD_4 IS DISTINCT FROM S.CRM_CD_4 OR
    T.LOCATION IS DISTINCT FROM S.LOCATION OR
    T.CROSS_STREET IS DISTINCT FROM S.CROSS_STREET OR
    T.LAT IS DISTINCT FROM S.LAT OR
    T.LON IS DISTINCT FROM S.LON OR
    T.DL_UPD IS DISTINCT FROM S.DL_UPD
)
THEN UPDATE SET
    DATE_RPTD = S.DATE_RPTD,
    DATE_OCC = S.DATE_OCC,
    TIME_OCC = S.TIME_OCC,
    AREA = S.AREA,
    AREA_NAME = S.AREA_NAME,
    RPT_DIST_NO = S.RPT_DIST_NO,
    PART_1_2 = S.PART_1_2,
    CRM_CD = S.CRM_CD,
    CRM_CD_DESC = S.CRM_CD_DESC,
    MOCODES = S.MOCODES,
    VICT_AGE = S.VICT_AGE,
    VICT_SEX = S.VICT_SEX,
    VICT_DESCENT = S.VICT_DESCENT,
    PREMIS_CD = S.PREMIS_CD,
    PREMIS_DESC = S.PREMIS_DESC,
    WEAPON_USED_CD = S.WEAPON_USED_CD,
    WEAPON_DESC = S.WEAPON_DESC,
    STATUS = S.STATUS,
    STATUS_DESC = S.STATUS_DESC,
    CRM_CD_1 = S.CRM_CD_1,
    CRM_CD_2 = S.CRM_CD_2,
    CRM_CD_3 = S.CRM_CD_3,
    CRM_CD_4 = S.CRM_CD_4,
    LOCATION = S.LOCATION,
    CROSS_STREET = S.CROSS_STREET,
    LAT = S.LAT,
    LON = S.LON,
    DL_UPD = S.DL_UPD
WHEN NOT MATCHED THEN INSERT (
    DR_NO, DATE_RPTD, DATE_OCC, TIME_OCC, AREA, AREA_NAME, RPT_DIST_NO, PART_1_2,
    CRM_CD, CRM_CD_DESC, MOCODES, VICT_AGE, VICT_SEX, VICT_DESCENT, PREMIS_CD,
    PREMIS_DESC, WEAPON_USED_CD, WEAPON_DESC, STATUS, STATUS_DESC, CRM_CD_1,
    CRM_CD_2, CRM_CD_3, CRM_CD_4, LOCATION, CROSS_STREET, LAT, LON, DL_UPD
) VALUES (
    S.DR_NO, S.DATE_RPTD, S.DATE_OCC, S.TIME_OCC, S.AREA, S.AREA_NAME, S.RPT_DIST_NO, S.PART_1_2,
    S.CRM_CD, S.CRM_CD_DESC, S.MOCODES, S.VICT_AGE, S.VICT_SEX, S.VICT_DESCENT, S.PREMIS_CD,
    S.PREMIS_DESC, S.WEAPON_USED_CD, S.WEAPON_DESC, S.STATUS, S.STATUS_DESC, S.CRM_CD_1,
    S.CRM_CD_2, S.CRM_CD_3, S.CRM_CD_4, S.LOCATION, S.CROSS_STREET, S.LAT, S.LON, S.DL_UPD
);
    """
    try:
        with conn.cursor() as cur:
            cur.execute(merge_sql)
            updated_rows = cur.rowcount
            logger.info(f"MERGE completed into {raw_table}. Rows affected: {updated_rows}")
            return updated_rows
    except Exception as e:
        logger.error(f"Error merging data in Snowflake: {e}")
        raise

def main():
    """Main function to orchestrate the data loading process."""
    parser = argparse.ArgumentParser(description="Load CSV data into Snowflake.")
    parser.add_argument("csv_file_path", type=str, help="Path to the CSV file")
    args = parser.parse_args()

    df = load_csv(args.csv_file_path)
    if df is None:
        logger.error("Exiting due to CSV load error.")
        return

    config = load_config()

    # Load into temp table
    conn_upload = connect_snowflake()
    if conn_upload is None:
        logger.error("Exiting due to Snowflake connection error (upload).")
        return
    load_to_snowflake_tmp(df, conn_upload, config["SNOWFLAKE_TEMP_TABLE"])

    # Merge into raw table
    conn_merge = connect_snowflake()
    if conn_merge is None:
        logger.error("Exiting due to Snowflake connection error (merge).")
        return
    merge_to_raw_table(conn_merge, config)
    conn_merge.close()

    logger.info("Data load and merge process complete.")

if __name__ == "__main__":
    main()
