"""
This script automates the process of loading LAPD crime data from Google Cloud Storage (GCS)
into Snowflake. It performs the following steps:

1. Loads environment and credential configurations.
2. Connects to Snowflake using the Snowflake Python connector.
3. Creates an external stage in Snowflake pointing to the GCS location (if not already created).
4. Loads CSV data from GCS into a temporary Snowflake table using the `COPY INTO` command.
5. Merges the data from the temporary table into a raw table using a MERGE statement with
   logic to handle both inserts and updates based on row-level changes.

This script is intended to be used as part of a data pipeline to keep Snowflake data in sync
with source files stored in GCS. Logging is included for observability and debugging.
"""

import snowflake.connector
import os
import logging
from dotenv import load_dotenv
import keyring
from google.cloud import storage

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
        "SNOWFLAKE_PASSWORD": keyring.get_password("snowflake", "pwd"),
        "GCS_BUCKET_NAME": os.getenv("GCS_BUCKET_NAME"),
        "GCS_FILE_PATH": os.getenv("GCS_FILE_PATH"),
        "SNOWFLAKE_STAGE_NAME": os.getenv("SNOWFLAKE_STAGE_NAME"),
        "SNOWFLAKE_FILE_FORMAT": os.getenv("SNOWFLAKE_FILE_FORMAT"),
        "SNOWFLAKE_STORAGE_INTEGRATION": os.getenv("SNOWFLAKE_STORAGE_INTEGRATION"),
    }
    missing_config = [key for key, value in config.items() if value is None and key not in ["SNOWFLAKE_FILE_FORMAT", "SNOWFLAKE_STAGE_NAME"]]
    if missing_config:
        logger.error(f"Missing configuration: {', '.join(missing_config)}")
        raise ValueError(f"Missing configuration for: {', '.join(missing_config)}")
    logger.info("Configuration successfully loaded.")
    return config

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

def create_snowflake_stage(conn, config):
    """Creates an external stage in Snowflake if it doesn't exist, using a STORAGE INTEGRATION."""
    stage_name = f"{config['SNOWFLAKE_DATABASE']}.{config['SNOWFLAKE_SCHEMA']}.{config['SNOWFLAKE_STAGE_NAME']}"
    gcs_url = f"gcs://{config['GCS_BUCKET_NAME']}/"
    check_stage_sql = f"SHOW STAGES LIKE '{config['SNOWFLAKE_STAGE_NAME']}' IN SCHEMA {config['SNOWFLAKE_DATABASE']}.{config['SNOWFLAKE_SCHEMA']};"
    create_stage_sql = f"""
        CREATE OR REPLACE STAGE {stage_name}
        URL = '{gcs_url}'
        STORAGE_INTEGRATION = {config.get('SNOWFLAKE_STORAGE_INTEGRATION')}
        FILE_FORMAT = (TYPE = CSV 
                       FIELD_DELIMITER = ',' 
                       SKIP_HEADER = 1 
                       NULL_IF = ('NULL', '') 
                       EMPTY_FIELD_AS_NULL = TRUE
                       FIELD_OPTIONALLY_ENCLOSED_BY='"');
    """
    try:
        with conn.cursor() as cur:
            cur.execute(check_stage_sql)
            if not cur.fetchall():
                if not config.get('SNOWFLAKE_STORAGE_INTEGRATION'):
                    logger.error("SNOWFLAKE_STORAGE_INTEGRATION environment variable is not set. It is required for creating GCS stages in this account.")
                    raise ValueError("Missing SNOWFLAKE_STORAGE_INTEGRATION configuration.")
                cur.execute(create_stage_sql)
                logger.info(f"Snowflake stage '{stage_name}' created using storage integration '{config.get('SNOWFLAKE_STORAGE_INTEGRATION')}'.")
            else:
                logger.info(f"Snowflake stage '{stage_name}' already exists.")
    except Exception as e:
        logger.error(f"Error creating or checking Snowflake stage: {e}")
        raise

def load_from_gcs_to_snowflake_tmp(conn, config):
    """Loads data from GCS to a Snowflake temporary table using COPY INTO, checking for file existence first."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(config['GCS_BUCKET_NAME'])
    blob = bucket.blob(config['GCS_FILE_PATH'])
    # check if the file does not exist
    if not blob.exists():
        logger.error(f"GCS file not found: gs://{config['GCS_BUCKET_NAME']}/{config['GCS_FILE_PATH']}")
        return
    #stage_name = f"@{config['SNOWFLAKE_DATABASE']}.{config['SNOWFLAKE_SCHEMA']}.{config['SNOWFLAKE_STAGE_NAME']}"
    file_name = os.path.basename(config['GCS_FILE_PATH'])
    stage_path = f"@{config['SNOWFLAKE_DATABASE']}.{config['SNOWFLAKE_SCHEMA']}.{config['SNOWFLAKE_STAGE_NAME']}/{file_name}"
    temp_table = config["SNOWFLAKE_TEMP_TABLE"]
    truncate_sql = f"TRUNCATE TABLE {temp_table}"
    df_columns_for_copy = [
        "DR_NO", "DATE_RPTD", "DATE_OCC", "TIME_OCC", "AREA", "AREA_NAME",
        "RPT_DIST_NO", "PART_1_2", "CRM_CD", "CRM_CD_DESC", "MOCODES",
        "VICT_AGE", "VICT_SEX", "VICT_DESCENT", "PREMIS_CD", "PREMIS_DESC",
        "WEAPON_USED_CD", "WEAPON_DESC", "STATUS", "STATUS_DESC", "CRM_CD_1",
        "CRM_CD_2", "CRM_CD_3", "CRM_CD_4", "LOCATION", "CROSS_STREET", "LAT", "LON"
    ]
    copy_into_sql = f"""
        COPY INTO {temp_table} ({', '.join(df_columns_for_copy)})
        FROM {stage_path}
        FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='"');
    """
    try:
        with conn.cursor() as cur:
            cur.execute(truncate_sql)
            logger.info(f"Truncated table: {temp_table}")
            cur.execute(copy_into_sql)
            logger.info(f"Data loaded from GCS to Snowflake temporary table '{temp_table}'.")
    except Exception as e:
        logger.error(f"Error loading data from GCS to Snowflake: {e}")
        try:
            conn.rollback()
            logger.info("Rolled back transaction.")
        except Exception as rollback_error:
            logger.error(f"Rollback failed: {rollback_error}")
        raise

def merge_to_raw_table(conn, config) -> int:
    """Merge data from temp to raw table in Snowflake."""
    temp_table = config["SNOWFLAKE_TEMP_TABLE"]
    raw_table = config["SNOWFLAKE_RAW_TABLE"]
    # ... (rest of the merge_to_raw_table function remains the same)
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
    """Main function to orchestrate the data loading process from GCS to Snowflake."""
    config = load_config()
    conn = connect_snowflake()
    if conn is None:
        logger.error("Exiting due to Snowflake connection error.")
        return
    try:
        # Ensure Snowflake stage exists
        create_snowflake_stage(conn, config)
        # Load data from GCS to Snowflake temporary table
        load_from_gcs_to_snowflake_tmp(conn, config)
        # Merge data from temporary table to the raw table
        merge_to_raw_table(conn, config)
        logger.info("Data load and merge process from GCS to Snowflake complete.")
    finally:
        try:
            conn.close()
            logger.info("Closed Snowflake connection.")
        except Exception as close_error:
            logger.error(f"Failed to close connection: {close_error}")

if __name__ == "__main__":
    main()