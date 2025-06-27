from datetime import timedelta
import cx_Oracle
from config import Config
import pyodbc
import pandas as pd


def row_to_dict(row, cursor_description):
    if row is None:
        return None
    columns = [col[0].lower() for col in cursor_description]
    return dict(zip(columns, row))

# For all rows
def rows_to_dict_list(rows, cursor_description):
    columns = [col[0].lower() for col in cursor_description]
    return [dict(zip(columns, row)) for row in rows]

def get_connection():
    return cx_Oracle.connect(
        user=Config.ORACLE_USER,
        password=Config.ORACLE_PASSWORD,
        dsn=Config.ORACLE_DSN
    )

def get_cloudera_data(dsn, query):
    # Connect to Cloudera via ODBC
    connection = pyodbc.connect(f"DSN={dsn}")
    df = pd.read_sql(query, connection)
    connection.close()
    return df

def get_existing_pos_week(payment_vendor):
    bind_vars = {
        'payment_vendor': payment_vendor
    }
    query = """
        SELECT MAX(FORECASTPERIOD) max_forecastperiod FROM XXJWY.XXPOS_POS_RAW WHERE PAYMENT_VENDOR_NBR = :payment_vendor
        """
    return execute_query(query=query, bind_vars=bind_vars, fetch='one')

def get_all_existing_pos_week():
    query = """
        SELECT FORECASTWEEKLONG, REPORTSTARTINGDATE, REPORTENDINGDATE 
        FROM XXJWY.XXPOS_HD_FISCAL_CALENDAR 
        WHERE FORECASTPERIOD = (SELECT MAX(FORECASTPERIOD) FROM XXPOS_POS_RAW)
        """
    return execute_query(query=query, fetch='one')

def get_none_existing_pos_week(payment_vendor):
    bind_vars = {
        'payment_vendor': payment_vendor
    }
    query = """
        SELECT FORECASTPERIOD, FORECASTWEEKLONG, REPORTSTARTINGDATE, REPORTENDINGDATE FROM XXJWY.XXPOS_HD_FISCAL_CALENDAR WHERE FORECASTPERIOD NOT IN
        (SELECT DISTINCT FORECASTPERIOD FROM XXJWY.XXPOS_POS_RAW WHERE PAYMENT_VENDOR_NBR = :payment_vendor)
        ORDER BY FORECASTPERIOD DESC
        """
    return execute_query(query=query, bind_vars=bind_vars, fetch='all')

def get_forecastperiod_by_pvendor(forecastperiod, payment_vendor):
    bind_vars = {
        'forecastperiod': forecastperiod,
        'payment_vendor' : payment_vendor
    }
    query = """
        SELECT DISTINCT XHFC.FORECASTPERIOD, XHFC.FORECASTWEEKLONG, XHFC.REPORTSTARTINGDATE, XHFC.REPORTENDINGDATE 
        FROM XXJWY.XXPOS_HD_FISCAL_CALENDAR xhfc, XXPOS_POS_RAW xpr 
        WHERE xhfc.forecastperiod = xpr.forecastperiod
        AND xpr.forecastperiod = :forecastperiod
        AND xpr.payment_vendor_nbr = :payment_vendor
        """
    return execute_query(query=query, bind_vars=bind_vars, fetch='one')

def get_report_end_date(forecastperiod):
    bind_vars = {
        'forecastperiod': forecastperiod
    }
    query = """
        SELECT REPORTENDINGDATE 
        FROM XXJWY.XXPOS_HD_FISCAL_CALENDAR 
        where FORECASTPERIOD = :forecastperiod
        """
    return execute_query(query=query, bind_vars=bind_vars, fetch='one')

def execute_query(query, bind_vars=None, fetch='one', size=None):
    """
    Executes a query and fetches results based on the mode.

    Args:
        query (str): SQL query to execute.
        bind_vars (dict): Dictionary of bind variables.
        fetch (str): 'one', 'many', or 'all'.
        size (int): Used only when fetch is 'many'.

    Returns:
        Single row dict, list of dicts, or None on error.
    """
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(query, bind_vars or {})

        if fetch == 'one':
            row = cursor.fetchone()
            return row_to_dict(row, cursor.description) if row else None
        elif fetch == 'many':
            rows = cursor.fetchmany(size or 10)
            return [row_to_dict(r, cursor.description) for r in rows]
        elif fetch == 'all':
            rows = cursor.fetchall()
            return [row_to_dict(r, cursor.description) for r in rows]
        else:
            raise ValueError("Invalid fetch mode: choose 'one', 'many', or 'all'")
    except cx_Oracle.DatabaseError as e:
        print("DB error:", e)
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def insert_into_oracle(df, table_name, columns):
    """
    Insert DataFrame into Oracle using executemany().
    """
    try:
        conn = cx_Oracle.connect(Config.ORACLE_USER, Config.ORACLE_PASSWORD, Config.ORACLE_DSN)
    except Exception as conn_err:
        return False, "Database connection failed"
    cursor = conn.cursor()
    
    try:
        # Build dynamic SQL: INSERT INTO table_name (col1, col2, col3) VALUES (:1, :2, :3)
        value_list = ', '.join([f":{i+1}" for i in range(len(columns))])
        column_names = ', '.join(columns)
        sql = f"INSERT INTO {table_name} ({column_names}) VALUES ({value_list})"

        # Convert DataFrame to list of tuples for the columns
        print(df['payment_vendor_nbr'].head(1).to_list())
        forecastperiod = get_forecastperiod_by_pvendor(df['forecastperiod'].head(1).to_list()[0], df['payment_vendor_nbr'].head(1).to_list()[0])
        print('FORECASTPERIOD', forecastperiod)
        if forecastperiod:
            return False, 'Data Exist!'
        else:
            process_time = get_report_end_date(forecastperiod=df['forecastperiod'].head(1).to_list()[0])
            print('PROCESSTIME', process_time)
            df['processtime'] = pd.to_datetime(process_time['reportendingdate'] + timedelta(days=2), errors='coerce')
            # df['processtime'] = pd.to_datetime(df['processtime'], errors='coerce')
            df = df.astype(object).where(pd.notnull(df), None)
            data = [tuple(row[col] for col in columns) for _, row in df.iterrows()]
            # Execute batch insert
            cursor.executemany(sql, data)
            conn.commit()
            return True, 'Successful!'
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        cursor.close()
        conn.close()

def query_set_ca_last_week():
    conn = pyodbc.connect(f"DSN={Config.CLOUDERA_DSN_CA_14487}", autocommit=True)
    query = """
        SELECT date_format(current_date, 'MM/dd/yyyy') AS PROCESSTIME,
        substr(`CA_VENDORDRILL`.`h_sw_short_week`, -6) AS FORECASTPERIOD,
        `CA_VENDORDRILL`.`h_v_payment_vendor_nbr` AS PAYMENT_VENDOR_NBR,
        `CA_VENDORDRILL`.`h_v_merch_vendor_nbr` AS MERCH_VENDOR_NBR,
        `CA_VENDORDRILL`.`h_v_vendor` AS MERCH_VENDOR,
        `CA_VENDORDRILL`.`h_w_week` AS WEEK,
        `CA_VENDORDRILL`.`d_mph_merch_dept` AS CATEGORY,
        `CA_VENDORDRILL`.`d_v_article_nbr` AS SKU,
        concat('0',`CA_VENDORDRILL`.`d_mph_upc`) AS UPC,
        `CA_VENDORDRILL`.`d_mlh_store_nbr` AS STORENUMBER,
        `CA_VENDORDRILL`.`d_mph_model_number` AS MODEL_NUMBER,
        `CA_VENDORDRILL`.`m_store_weeks` AS STORE_WEEKS,
        `CA_VENDORDRILL`.`m_str_oh_units_wkly` AS STR_OH_UNITS_WKLY,
        `CA_VENDORDRILL`.`m_sales_units_before_returns` AS SALES_UNITS_BEFORE_RETURNS,
        `CA_VENDORDRILL`.`m_return_units` AS RETURN_UNITS,
        `CA_VENDORDRILL`.`m_sales_amount_before_returns` AS SALES_$_BEFORE_RETURNS,
        `CA_VENDORDRILL`.`m_return_amount` AS RETURN_$,
        `CA_VENDORDRILL`.`m_sales_amount_before_returns` + `CA_VENDORDRILL`.`m_return_amount` AS SALES_$
        FROM `CA_VENDORDRILL`.`CA_VENDORDRILL` `CA_VENDORDRILL`
        WHERE (`CA_VENDORDRILL`.`h_tt_time_calculations` IN ('Last WK'))
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def query_set_us_di_last_week():
    conn = pyodbc.connect(f"DSN={Config.CLOUDERA_DSN_US_DI_12746}", autocommit=True)
    query = """
        SELECT 
        date_format(current_date, 'MM/dd/yyyy') AS PROCESSTIME,
        SUBSTR(`VendorDrill`.`Short_Week`, -6) AS FORECASTPERIOD,
        `VendorDrill`.`Payment_Vendor_Nbr` AS PAYMENT_VENDOR_NBR,
        `VendorDrill`.`Merch_Vendor_Nbr` AS MERCH_VENDOR_NBR,
        `VendorDrill`.`Merch_Vendor2` AS MERCH_VENDOR,
        `VendorDrill`.`Week1` AS WEEK,
        `VendorDrill`.`Category_2` AS CATEGORY,
        `VendorDrill`.`SKU_Nbr` AS SKU,
        concat('0', `VendorDrill`.`UPC_CD`) AS UPC,
        `VendorDrill`.`d_Store_Nbr` AS STORENUMBER,
        `VendorDrill`.`Model_Number` AS MODEL_NUMBER,
        `VendorDrill`.`store_weeks` AS STORE_WEEKS,
        `VendorDrill`.`Str_OH_units_wkly` AS STR_OH_UNITS_WKLY,
        `VendorDrill`.`Sales_Units_before_Returns` AS SALES_UNITS_BEFORE_RETURNS,
        `VendorDrill`.`m_ty_return_units_sum` AS RETURN_UNITS,
        `VendorDrill`.`Sales__before_Returns` AS SALES_$_BEFORE_RETURNS,
        `VendorDrill`.`m_ty_Returns_sum` AS RETURN_$,
        `VendorDrill`.`Sales__before_Returns` + `VendorDrill`.`m_ty_Returns_sum` AS SALES_$
        FROM `VendorDrill`.`VendorDrill` `VendorDrill`
        WHERE (`VendorDrill`.`Time_Calculations` IN ('Last WK'))
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def query_set_us_dom_last_week():
    conn = pyodbc.connect(f"DSN={Config.CLOUDERA_DSN_US_DOM_12582}", autocommit=True)
    query = """
        SELECT 
        date_format(current_date, 'MM/dd/yyyy') AS PROCESSTIME,
        SUBSTR(`VendorDrill`.`Short_Week`, -6) AS FORECASTPERIOD,
        `VendorDrill`.`Payment_Vendor_Nbr` AS PAYMENT_VENDOR_NBR,
        `VendorDrill`.`Merch_Vendor_Nbr` AS MERCH_VENDOR_NBR,
        `VendorDrill`.`Merch_Vendor2` AS MERCH_VENDOR,
        `VendorDrill`.`Week1` AS WEEK,
        `VendorDrill`.`Category_2` AS CATEGORY,
        `VendorDrill`.`SKU_Nbr` AS SKU,
        concat('0', `VendorDrill`.`UPC_CD`) AS UPC,
        `VendorDrill`.`d_Store_Nbr` AS STORENUMBER,
        `VendorDrill`.`Model_Number` AS MODEL_NUMBER,
        `VendorDrill`.`store_weeks` AS STORE_WEEKS,
        `VendorDrill`.`Str_OH_units_wkly` AS STR_OH_UNITS_WKLY,
        `VendorDrill`.`Sales_Units_before_Returns` AS SALES_UNITS_BEFORE_RETURNS,
        `VendorDrill`.`m_ty_return_units_sum` AS RETURN_UNITS,
        `VendorDrill`.`Sales__before_Returns` AS SALES_$_BEFORE_RETURNS,
        `VendorDrill`.`m_ty_Returns_sum` AS RETURN_$,
        `VendorDrill`.`Sales__before_Returns` + `VendorDrill`.`m_ty_Returns_sum` AS SALES_$
        FROM `VendorDrill`.`VendorDrill` `VendorDrill`
        WHERE (`VendorDrill`.`Time_Calculations` IN ('Last WK'))
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def query_set_ca_week_number(week_number):
    conn = pyodbc.connect(f"DSN={Config.CLOUDERA_DSN_CA_14487}", autocommit=True)
    if isinstance(week_number, list):
        week_list_str = ', '.join(f"'{w}'" for w in week_number)
    else:
        week_list_str = f"'{week_number}'"
    query = f"""
        SELECT date_format(current_date, 'MM/dd/yyyy') AS PROCESSTIME,
        substr(`CA_VENDORDRILL`.`h_sw_short_week`, -6) AS FORECASTPERIOD,
        `CA_VENDORDRILL`.`h_v_payment_vendor_nbr` AS PAYMENT_VENDOR_NBR,
        `CA_VENDORDRILL`.`h_v_merch_vendor_nbr` AS MERCH_VENDOR_NBR,
        `CA_VENDORDRILL`.`h_v_vendor` AS MERCH_VENDOR,
        `CA_VENDORDRILL`.`h_w_week` AS WEEK,
        `CA_VENDORDRILL`.`d_mph_merch_dept` AS CATEGORY,
        `CA_VENDORDRILL`.`d_v_article_nbr` AS SKU,
        concat('0',`CA_VENDORDRILL`.`d_mph_upc`) AS UPC,
        `CA_VENDORDRILL`.`d_mlh_store_nbr` AS STORENUMBER,
        `CA_VENDORDRILL`.`d_mph_model_number` AS MODEL_NUMBER,
        `CA_VENDORDRILL`.`m_store_weeks` AS STORE_WEEKS,
        `CA_VENDORDRILL`.`m_str_oh_units_wkly` AS STR_OH_UNITS_WKLY,
        `CA_VENDORDRILL`.`m_sales_units_before_returns` AS SALES_UNITS_BEFORE_RETURNS,
        `CA_VENDORDRILL`.`m_return_units` AS RETURN_UNITS,
        `CA_VENDORDRILL`.`m_sales_amount_before_returns` AS SALES_$_BEFORE_RETURNS,
        `CA_VENDORDRILL`.`m_return_amount` AS RETURN_$,
        `CA_VENDORDRILL`.`m_sales_amount_before_returns` + `CA_VENDORDRILL`.`m_return_amount` AS SALES_$
        FROM `CA_VENDORDRILL`.`CA_VENDORDRILL` `CA_VENDORDRILL`
       WHERE (`CA_VENDORDRILL`.`h_w_week` IN ({week_list_str}))
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def query_set_us_di_week_number(week_number):
    conn = pyodbc.connect(f"DSN={Config.CLOUDERA_DSN_US_DI_12746}", autocommit=True)
    if isinstance(week_number, list):
        week_list_str = ', '.join(f"'{w}'" for w in week_number)
    else:
        week_list_str = f"'{week_number}'"
    query = f"""
        SELECT 
        date_format(current_date, 'MM/dd/yyyy') AS PROCESSTIME,
        SUBSTR(`VendorDrill`.`Short_Week`, -6) AS FORECASTPERIOD,
        `VendorDrill`.`Payment_Vendor_Nbr` AS PAYMENT_VENDOR_NBR,
        `VendorDrill`.`Merch_Vendor_Nbr` AS MERCH_VENDOR_NBR,
        `VendorDrill`.`Merch_Vendor2` AS MERCH_VENDOR,
        `VendorDrill`.`Week1` AS WEEK,
        `VendorDrill`.`Category_2` AS CATEGORY,
        `VendorDrill`.`SKU_Nbr` AS SKU,
        concat('0', `VendorDrill`.`UPC_CD`) AS UPC,
        `VendorDrill`.`d_Store_Nbr` AS STORENUMBER,
        `VendorDrill`.`Model_Number` AS MODEL_NUMBER,
        `VendorDrill`.`store_weeks` AS STORE_WEEKS,
        `VendorDrill`.`Str_OH_units_wkly` AS STR_OH_UNITS_WKLY,
        `VendorDrill`.`Sales_Units_before_Returns` AS SALES_UNITS_BEFORE_RETURNS,
        `VendorDrill`.`m_ty_return_units_sum` AS RETURN_UNITS,
        `VendorDrill`.`Sales__before_Returns` AS SALES_$_BEFORE_RETURNS,
        `VendorDrill`.`m_ty_Returns_sum` AS RETURN_$,
        `VendorDrill`.`Sales__before_Returns` + `VendorDrill`.`m_ty_Returns_sum` AS SALES_$
        FROM `VendorDrill`.`VendorDrill` `VendorDrill`
        WHERE VendorDrill.week IN ({week_list_str})
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def query_set_us_dom_week_number(week_number):
    conn = pyodbc.connect(f"DSN={Config.CLOUDERA_DSN_US_DOM_12582}", autocommit=True)
    if isinstance(week_number, list):
        week_list_str = ', '.join(f"'{w}'" for w in week_number)
    else:
        week_list_str = f"'{week_number}'"
    query = f"""
        SELECT 
        date_format(current_date, 'MM/dd/yyyy') AS PROCESSTIME,
        SUBSTR(`VendorDrill`.`Short_Week`, -6) AS FORECASTPERIOD,
        `VendorDrill`.`Payment_Vendor_Nbr` AS PAYMENT_VENDOR_NBR,
        `VendorDrill`.`Merch_Vendor_Nbr` AS MERCH_VENDOR_NBR,
        `VendorDrill`.`Merch_Vendor2` AS MERCH_VENDOR,
        `VendorDrill`.`Week1` AS WEEK,
        `VendorDrill`.`Category_2` AS CATEGORY,
        `VendorDrill`.`SKU_Nbr` AS SKU,
        concat('0', `VendorDrill`.`UPC_CD`) AS UPC,
        `VendorDrill`.`d_Store_Nbr` AS STORENUMBER,
        `VendorDrill`.`Model_Number` AS MODEL_NUMBER,
        `VendorDrill`.`store_weeks` AS STORE_WEEKS,
        `VendorDrill`.`Str_OH_units_wkly` AS STR_OH_UNITS_WKLY,
        `VendorDrill`.`Sales_Units_before_Returns` AS SALES_UNITS_BEFORE_RETURNS,
        `VendorDrill`.`m_ty_return_units_sum` AS RETURN_UNITS,
        `VendorDrill`.`Sales__before_Returns` AS SALES_$_BEFORE_RETURNS,
        `VendorDrill`.`m_ty_Returns_sum` AS RETURN_$,
        `VendorDrill`.`Sales__before_Returns` + `VendorDrill`.`m_ty_Returns_sum` AS SALES_$
        FROM `VendorDrill`.`VendorDrill` `VendorDrill`
        WHERE VendorDrill.week IN ({week_list_str})
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def insert_records(table_name, data, delete_existing=False):
    """
    Inserts one or many records into the given Oracle table using named parameters.

    :param table_name: str - Table name
    :param data: dict or list of dicts A single dict or a list of dicts
    :param delete_existing: bool - Optionally delete existing records from the table
    :return: dict - Insert result summary
    """
    # Normalize input
    if isinstance(data, dict):
        data = [data]
    if not data:
        return {"status": "error", "message": "No data provided."}

    keys = data[0].keys()
    columns = ', '.join(keys)
    placeholders = ', '.join([f":{k}" for k in keys])
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            if delete_existing:
                cursor.execute(f"DELETE FROM {table_name}")
            cursor.executemany(sql, data)
            conn.commit()
            return {
                "status": "success",
                "rows_inserted": cursor.rowcount
            }
    except cx_Oracle.DatabaseError as e:
        return {
            "status": "error",
            "message": str(e)
        }
    
def update_records(table, set_fields, data, where_clause):
    """
    Generic update function that supports single or bulk updates.

    :param table: str Table name
    :param set_fields: list of str Fields to update (e.g., ['led_expense', 'last_updated_by'])
    :param data: dict or list of dicts – Bind data
    :param where_clause: str WHERE clause (e.g., 'inventory_item_id = :inventory_item_id')
    """
    set_clause = ', '.join([f"{field} = :{field}" for field in set_fields])
    sql = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"

    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            if isinstance(data, list):
                cursor.executemany(sql, data)
            else:
                cursor.execute(sql, data)
            conn.commit()
            return {"status": "success", "rows_updated": cursor.rowcount}
    except cx_Oracle.DatabaseError as e:
        return {"status": "error", "message": str(e)}

    
def delete_records(table, where_clause=None, data=None):
    """
    Delete function that supports deleting all or specific records.

    :param table: str Table name
    :param where_clause: str Optional WHERE clause (e.g., 'inventory_item_id = :inventory_item_id')
    :param data: dict or list of dicts Bind data for where clause
    """
    sql = f"DELETE FROM {table}"
    if where_clause:
        sql += f" WHERE {where_clause}"

    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            if data is None:
                cursor.execute(sql)
            elif isinstance(data, list):
                cursor.executemany(sql, data)
            else:
                cursor.execute(sql, data)
            conn.commit()
            return {"status": "success", "rows_deleted": cursor.rowcount}
    except cx_Oracle.DatabaseError as e:
        return {"status": "error", "message": str(e)}
