import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text, exc as sa_exc
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
from dotenv import load_dotenv
import keyring
from datetime import datetime, timedelta, timezone
from typing import Dict

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def load_config() -> Dict[str, str]:
    """Load configuration from environment variables."""
    load_dotenv()
    config = {
        "mysql_host": os.environ.get("MYSQL_HOST"),
        "mysql_database": os.environ.get("MYSQL_DATABASE"),
        "mysql_user": keyring.get_password("mysql", "user"),
        "mysql_password": keyring.get_password("mysql", "pwd"),
        "bigquery_project_id": os.environ.get("BIGQUERY_PROJECT_ID"),
        "bigquery_dataset_id": os.environ.get("BIGQUERY_DATASET_ID"),
        "bigquery_table_name": os.environ.get("BIGQUERY_TABLE_NAME", "stg_lapd_crime_data"),
        "bigquery_staging_table": os.environ.get("BIGQUERY_STAGING_TABLE", "tmp_lapd_crime_data"),
        "last_n_days": int(os.environ.get("LAST_N_DAYS", 7)),
    }
    return config

def create_mysql_engine(config: Dict[str, str]) -> create_engine:
    """Create a SQLAlchemy engine for MySQL."""
    try:
        engine = create_engine(
            f"mysql+pymysql://{config['mysql_user']}:{config['mysql_password']}@{config['mysql_host']}/{config['mysql_database']}"
        )
        engine.connect()
        return engine
    except sa_exc.SQLAlchemyError as e:
        logging.error(f"Error creating MySQL engine: {e}")
        raise

def extract_incremental_data(config: Dict[str, str], engine: create_engine) -> pd.DataFrame:
    """Extract incremental data from MySQL using DL_UPD."""
    since_date = (datetime.now(timezone.utc) - timedelta(days=int(config['last_n_days']))).strftime('%Y-%m-%d %H:%M:%S')
    try:
        query = text(
            """
            SELECT * FROM raw_lapd_crime_data
            WHERE DL_UPD >= :since_date
            """
        )
        df = pd.read_sql_query(query, engine, params={"since_date": since_date})
        df["PART_1_2"] = pd.to_numeric(df["PART_1_2"], errors="coerce")
        df["PREMIS_CD"] = pd.to_numeric(df["PREMIS_CD"], errors="coerce")
        df["LAT"] = pd.to_numeric(df["LAT"], errors="coerce")
        df["LON"] = pd.to_numeric(df["LON"], errors="coerce")
        df["DATE_RPTD"] = pd.to_datetime(df["DATE_RPTD"], errors="coerce")
        df["DATE_OCC"] = pd.to_datetime(df["DATE_OCC"], errors="coerce")
        df["DL_UPD"] = pd.to_datetime(df["DL_UPD"], errors="coerce")
        logging.info(f"Fetched {len(df)} incremental records from MySQL since {since_date}.")
        return df
    except sa_exc.SQLAlchemyError as e:
        logging.error(f"Error extracting data from MySQL: {e}")
        raise

def load_to_staging_bigquery(
    client: bigquery.Client, config: Dict[str, str], df: pd.DataFrame
) -> None:
    """Load data to a staging table in BigQuery."""
    staging_table_id = f"{config['bigquery_project_id']}.{config['bigquery_dataset_id']}.{config['bigquery_staging_table']}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=[bigquery.SchemaField("PREMIS_CD", "INT64"),])
    try:
        job = client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
        job.result()
        logging.info(f"Staging table {staging_table_id} loaded with {len(df)} records.")
    except GoogleCloudError as e:
        logging.error(f"Error loading data to BigQuery staging table: {e}")
        raise

def merge_to_main_table(client: bigquery.Client, config: Dict[str, str]) -> int:
    """Merge data from staging to main table in BigQuery.
    Returns:
        int: Number of rows updated.
    """
    staging_table = f"{config['bigquery_project_id']}.{config['bigquery_dataset_id']}.{config['bigquery_staging_table']}"
    main_table = f"{config['bigquery_project_id']}.{config['bigquery_dataset_id']}.{config['bigquery_table_name']}"

    merge_sql = f"""
        MERGE `{main_table}` T
        USING `{staging_table}` S
        ON T.DR_NO = S.DR_NO
        WHEN MATCHED THEN
          UPDATE SET
            T.DATE_RPTD = S.DATE_RPTD,
            T.DATE_OCC = S.DATE_OCC,
            T.TIME_OCC = S.TIME_OCC,
            T.AREA = S.AREA,
            T.AREA_NAME = S.AREA_NAME,
            T.RPT_DIST_NO = S.RPT_DIST_NO,
            T.PART_1_2 = CAST(S.PART_1_2 AS INT64),
            T.CRM_CD = S.CRM_CD,
            T.CRM_CD_DESC = S.CRM_CD_DESC,
            T.MOCODES = S.MOCODES,
            T.VICT_AGE = S.VICT_AGE,
            T.VICT_SEX = S.VICT_SEX,
            T.VICT_DESCENT = S.VICT_DESCENT,
            T.PREMIS_CD = CAST(S.PREMIS_CD AS INT64),
            T.PREMIS_DESC = S.PREMIS_DESC,
            T.WEAPON_USED_CD = S.WEAPON_USED_CD,
            T.WEAPON_DESC = S.WEAPON_DESC,
            T.STATUS = S.STATUS,
            T.STATUS_DESC = S.STATUS_DESC,
            T.CRM_CD_1 = S.CRM_CD_1,
            T.CRM_CD_2 = S.CRM_CD_2,
            T.CRM_CD_3 = S.CRM_CD_3,
            T.CRM_CD_4 = S.CRM_CD_4,
            T.LOCATION = S.LOCATION,
            T.CROSS_STREET = S.CROSS_STREET,
            T.LAT = S.LAT,
            T.LON = S.LON,
            T.DL_UPD = S.DL_UPD -- Ensure DL_UPD is updated
        WHEN NOT MATCHED THEN
          INSERT (
            DR_NO, DATE_RPTD, DATE_OCC, TIME_OCC, AREA, AREA_NAME, RPT_DIST_NO, PART_1_2,
            CRM_CD, CRM_CD_DESC, MOCODES, VICT_AGE, VICT_SEX, VICT_DESCENT, PREMIS_CD,
            PREMIS_DESC, WEAPON_USED_CD, WEAPON_DESC, STATUS, STATUS_DESC, CRM_CD_1,
            CRM_CD_2, CRM_CD_3, CRM_CD_4, LOCATION, CROSS_STREET, LAT, LON, DL_UPD
          )
          VALUES (
            S.DR_NO, S.DATE_RPTD, S.DATE_OCC, S.TIME_OCC, S.AREA, S.AREA_NAME, S.RPT_DIST_NO,
            S.PART_1_2, S.CRM_CD, S.CRM_CD_DESC, S.MOCODES, S.VICT_AGE, S.VICT_SEX, S.VICT_DESCENT,
            S.PREMIS_CD, S.PREMIS_DESC, S.WEAPON_USED_CD, S.WEAPON_DESC, S.STATUS, S.STATUS_DESC,
            S.CRM_CD_1, S.CRM_CD_2, S.CRM_CD_3, S.CRM_CD_4, S.LOCATION, S.CROSS_STREET, S.LAT, S.LON,
            S.DL_UPD
          )
    """
    try:
        query_job = client.query(merge_sql)
        query_job.result()
        updated_rows = query_job.num_dml_affected_rows
        logging.info(f"MERGE completed into {main_table}. Rows updated: {updated_rows}")
        return updated_rows
    except GoogleCloudError as e:
        logging.error(f"Error merging data in BigQuery: {e}")
        raise

def main():
    """Orchestrate the incremental data load from MySQL to BigQuery."""
    config = load_config()
    client = bigquery.Client(project=config["bigquery_project_id"])
    engine = create_mysql_engine(config)
    try:
        df = extract_incremental_data(config, engine)
        if df.empty:
            logging.info("No new data to load.")
            return
        load_to_staging_bigquery(client, config, df)
        rows_updated = merge_to_main_table(client, config)
        logging.info(f"Incremental and duplicate-safe load completed. Rows updated: {rows_updated}")
    except Exception as e:
        logging.critical(f"Error during MySQL to BigQuery load: {e}")
        raise
    finally:
        engine.dispose()
        logging.info("Connections closed")

if __name__ == "__main__":
    main()