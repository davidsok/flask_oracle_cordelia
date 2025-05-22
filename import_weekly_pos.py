import pyodbc
import pandas as pd
import numpy as np
import cx_Oracle
from datetime import datetime
from db import *
import logging
from config import Config
import os

# ========== CONFIGURATION ==========
LOG_DIR = "C:\\pos_weekly_logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"pos_weekly_log_{datetime.now().strftime('%Y%m%d')}.txt")

# ========== LOGGING SETUP ==========
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# ========== TYPE CLEANUP ==========
def clean_value(val):
    if pd.isna(val):
        return None
    if isinstance(val, (np.generic,)):
        return val.item()  # Converts numpy types to Python native
    if isinstance(val, pd.Timestamp):
        return val.to_pydatetime()
    return val

# ========== ORACLE DUPLICATE CHECK ==========
def get_existing_keys_from_oracle(conn, table_name, keys_df):
    cursor = conn.cursor()
    unique_keys = keys_df[['FORECASTPERIOD', 'PAYMENT_VENDOR_NBR']].drop_duplicates()

    if unique_keys.empty:
        return set()

    conditions = []
    params = {}
    for i, row in unique_keys.iterrows():
        conditions.append(f"(FORECASTPERIOD = :fp{i} AND PAYMENT_VENDOR_NBR = :pv{i})")
        params[f"fp{i}"] = row['FORECASTPERIOD']
        params[f"pv{i}"] = row['PAYMENT_VENDOR_NBR']

    where_clause = " OR ".join(conditions)
    query = f"SELECT FORECASTPERIOD, PAYMENT_VENDOR_NBR FROM {table_name} WHERE {where_clause}"

    cursor.execute(query, params)
    existing = set(cursor.fetchall())
    cursor.close()
    return existing

# ========== INSERT INTO ORACLE ==========
def insert_into_oracle_skip_duplicates():
    conn = None
    cursor = None
    mapping = {
        1: (query_set_ca_last_week, 'XXPOS_POS_RAW', ['processtime', 'forecastperiod', 'payment_vendor_nbr', 'merch_vendor_nbr', 'merch_vendor', 'week', 'category', 'sku', 'upc', 'storenumber', 'model_number', 'store_weeks', 'str_oh_units_wkly', 'sales_units_before_returns', 'return_units', 'sales_$', 'sales_$_before_returns', 'return_$']),
        2: (query_set_us_di_last_week, 'XXPOS_POS_RAW', ['processtime', 'forecastperiod', 'payment_vendor_nbr', 'merch_vendor_nbr', 'merch_vendor', 'week', 'category', 'sku', 'upc', 'storenumber', 'model_number', 'store_weeks', 'str_oh_units_wkly', 'sales_units_before_returns', 'return_units', 'sales_$', 'sales_$_before_returns', 'return_$']),
        3: (query_set_us_dom_last_week, 'XXPOS_POS_RAW', ['processtime', 'forecastperiod', 'payment_vendor_nbr', 'merch_vendor_nbr', 'merch_vendor', 'week', 'category', 'sku', 'upc', 'storenumber', 'model_number', 'store_weeks', 'str_oh_units_wkly', 'sales_units_before_returns', 'return_units', 'sales_$', 'sales_$_before_returns', 'return_$']),
    }

    for i in range(1,4):
        query_func, table, cols = mapping[i]
        try:
            df = query_func()
            df = df.where(pd.notnull(df), None)
            print(df)
            success, error = insert_into_oracle(df, table, cols)
            if success:
                logging.info(f"Query {query_func.__name__} inserted into {table} successfully.")
            else:
                logging.info(f"Failed inserting Query {query_func.__name__}: {error}")
        except Exception as e:
            logging.exception(f"Exception occurred during {query_func.__name__}: {e}")
            logging.info(f"Error: {str(e)}")
                
# ========== MAIN FUNCTION ==========
def main():
    logging.info("WEEKLY POS Import job started.")
    try:
        insert_into_oracle_skip_duplicates()
    except Exception as e:
        logging.error(f"Import failed: {e}")
    logging.info("WEEKLY POS Import job finished.")

# ========== ENTRY POINT ==========
if __name__ == "__main__":
    main()
