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


def get_all_users():
    query = """
        SELECT user_id, user_name, email, password, full_name, active_flag, XCS.role_id, XCR.ROLE_NAME, XCR.DESCRIPTION 
        FROM XXJWY.XXCDL_CUSTOM_USERS XCS, XXJWY.XXCDL_CUSTOM_ROLES XCR
        WHERE XCS.ROLE_ID = XCR.ROLE_ID(+) ORDER BY user_name
        """
    return execute_query(query=query, fetch='all')

def get_user_by_email(email):
    bind_vars = {
        'email': email
    }
    query = """
        SELECT user_id, user_name, email, password, full_name, active_flag, XCS.role_id, XCR.ROLE_NAME, XCR.DESCRIPTION 
        FROM XXJWY.XXCDL_CUSTOM_USERS XCS, XXJWY.XXCDL_CUSTOM_ROLES XCR
        WHERE XCS.ROLE_ID = XCR.ROLE_ID(+)
        AND UPPER(email) = UPPER(:email)
        """
    return execute_query(query=query, bind_vars=bind_vars, fetch='one')

def get_user_by_user_name(user_name):
    bind_vars = {
        'user_name': user_name
    }
    query = """
        SELECT user_id, user_name, email, password, full_name, active_flag, XCS.role_id, XCR.ROLE_NAME, XCR.DESCRIPTION 
        FROM XXJWY.XXCDL_CUSTOM_USERS XCS, XXJWY.XXCDL_CUSTOM_ROLES XCR
        WHERE XCS.ROLE_ID = XCR.ROLE_ID(+)
        AND UPPER(user_name) = UPPER(:user_name)
        """
    return execute_query(query=query, bind_vars=bind_vars, fetch='one')

def get_user_by_id(user_id):
    bind_vars = {
        'user_id': user_id
    }
    query =  """
        SELECT user_id, user_name, email, password, full_name, active_flag, XCS.role_id, XCR.ROLE_NAME, XCR.DESCRIPTION 
        FROM XXJWY.XXCDL_CUSTOM_USERS XCS, XXJWY.XXCDL_CUSTOM_ROLES XCR
        WHERE XCS.ROLE_ID = XCR.ROLE_ID(+) AND user_id = :user_id
        """
    return execute_query(query=query, bind_vars=bind_vars, fetch='one')

def get_user_sys_admin():
    query = """
        SELECT user_id, user_name, email, password, full_name, active_flag, XCS.role_id, XCR.ROLE_NAME, XCR.DESCRIPTION 
        FROM XXJWY.XXCDL_CUSTOM_USERS XCS, XXJWY.XXCDL_CUSTOM_ROLES XCR
        WHERE XCS.ROLE_ID = XCR.ROLE_ID(+) AND user_name = 'SYSADMIN'
        """
    return execute_query(query=query, fetch='one')

def get_all_roles():
    query = """
        SELECT ROLE_ID, ROLE_NAME, DESCRIPTION 
        FROM XXJWY.XXCDL_CUSTOM_ROLES ORDER BY ROLE_NAME
        """
    return execute_query(query=query, fetch='all')

def get_role_by_id(role_id):
    bind_vars = { 'role_id': role_id}
    query = """
        SELECT ROLE_ID, ROLE_NAME, DESCRIPTION 
        FROM XXJWY.XXCDL_CUSTOM_ROLES WHERE role_id = :role_id
        """
    return execute_query(query=query, bind_vars=bind_vars, fetch='one')

def get_role_with_users(role_id):
    bind_vars = { 'role_id': role_id}
    query = """
        SELECT XCS.ROLE_ID, XCR.ROLE_NAME, XCR.DESCRIPTION, XCS.USER_ID, XCS.USER_NAME, XCS.EMAIL 
        FROM XXJWY.XXCDL_CUSTOM_USERS XCS, XXJWY.XXCDL_CUSTOM_ROLES XCR 
        WHERE XCS.ROLE_ID = XCR.ROLE_ID AND role_id = :role_id
        """
    return execute_query(query=query, bind_vars=bind_vars, fetch='all')

def get_all_roles_with_users():
    query = """
        SELECT XCS.ROLE_ID, XCR.ROLE_NAME, XCR.DESCRIPTION, XCS.USER_ID, XCS.USER_NAME, XCS.EMAIL 
        FROM XXJWY.XXCDL_CUSTOM_USERS XCS, XXJWY.XXCDL_CUSTOM_ROLES XCR 
        WHERE XCS.ROLE_ID = XCR.ROLE_ID
        """
    return execute_query(query=query, fetch='all')

def get_all_modules():
    query = """
        SELECT MODULE_ID, MODULE_NAME, DESCRIPTION 
        FROM XXJWY.XXCDL_CUSTOM_MODULES ORDER BY MODULE_NAME
        """
    return execute_query(query=query, fetch='all')

def get_module_by_id(module_id):
    bind_vars = { 'module_id': module_id}
    query = """
        SELECT MODULE_ID, MODULE_NAME, DESCRIPTION 
        FROM XXJWY.XXCDL_CUSTOM_MODULES WHERE module_id = :module_id
        """
    return execute_query(query=query, bind_vars=bind_vars, fetch='one')

def get_all_module_access():
    query = """
        SELECT XCMA.ACCESS_ID, XCM.MODULE_ID, XCM.MODULE_NAME, XCM.DESCRIPTION, 
        XCU.USER_ID, XCU.USER_NAME, XCU.FULL_NAME, XCU.EMAIL, XCMA.READ_ACCESS, 
        XCMA.WRITE_ACCESS 
        FROM XXCDL_CUSTOM_MODULE_ACCESS XCMA, XXCDL_CUSTOM_USERS XCU, XXCDL_CUSTOM_MODULES XCM
        WHERE XCMA.USER_ID = XCU.USER_ID AND XCMA.MODULE_ID = XCM.MODULE_ID ORDER BY XCM.MODULE_NAME
        """
    return execute_query(query=query, fetch='all')

def get_module_access_by_id(access_id):
    bind_vars = { 'access_id': access_id}
    query = """
        SELECT XCM.MODULE_ID, XCM.MODULE_NAME, XCM.DESCRIPTION, XCU.USER_ID, XCU.USER_NAME, XCU.FULL_NAME, XCU.EMAIL, XCMA.READ_ACCESS, XCMA.WRITE_ACCESS FROM XXCDL_CUSTOM_MODULE_ACCESS XCMA, XXCDL_CUSTOM_USERS XCU, XXCDL_CUSTOM_MODULES XCM
        WHERE XCMA.USER_ID = XCMA.USER_ID AND XCMA.MODULE_ID = XCM.MODULE_ID WHERE XCMA.ACCESS_ID = :access_id
        """
    return execute_query(query=query, bind_vars=bind_vars, fetch='one')

def get_module_access_by_user(user_id):
    bind_vars = { 'user_id': user_id}
    query = """
        SELECT XCMA.ACCESS_ID, XCM.MODULE_ID, XCM.MODULE_NAME, XCM.DESCRIPTION, 
        XCMA.READ_ACCESS, XCMA.WRITE_ACCESS 
        FROM XXCDL_CUSTOM_MODULES XCM, XXCDL_CUSTOM_MODULE_ACCESS XCMA
        WHERE XCM.MODULE_ID = XCMA.MODULE_ID 
        AND XCMA.ACCESS_ID IN (SELECT ACCESS_ID FROM XXJWY.XXCDL_CUSTOM_MODULE_ACCESS WHERE USER_ID = :user_id)
        """
    return execute_query(query=query, bind_vars=bind_vars, fetch='all')

def get_module_access_by_module_user(module_name, user_id):
    bind_vars = { 'user_id': user_id, 'module_name': module_name}
    query = """
        SELECT XCMA.ACCESS_ID, XCM.MODULE_ID, XCM.MODULE_NAME, XCM.DESCRIPTION, 
        XCMA.READ_ACCESS, XCMA.WRITE_ACCESS 
        FROM XXCDL_CUSTOM_MODULES XCM, XXCDL_CUSTOM_MODULE_ACCESS XCMA
        WHERE XCM.MODULE_ID = XCMA.MODULE_ID 
        AND XCMA.MODULE_ID = (SELECT MODULE_ID FROM XXJWY.XXCDL_CUSTOM_MODULES WHERE MODULE_NAME = :module_name) 
        AND XCMA.ACCESS_ID IN (SELECT ACCESS_ID FROM XXJWY.XXCDL_CUSTOM_MODULE_ACCESS WHERE USER_ID = :user_id)
        """
    return execute_query(query=query, bind_vars=bind_vars, fetch='one')

def get_modules_user_has_no_access(user_id):
    bind_vars = { 'user_id': user_id}
    query = """
        SELECT XCM.MODULE_ID, XCM.MODULE_NAME, XCM.DESCRIPTION FROM XXCDL_CUSTOM_MODULES XCM
        WHERE XCM.MODULE_ID NOT IN (SELECT MODULE_ID FROM XXJWY.XXCDL_CUSTOM_MODULE_ACCESS WHERE USER_ID = :user_id)
        """
    return execute_query(query=query, bind_vars=bind_vars, fetch='all')

def get_item_numbers():
    query = """
        SELECT INVENTORY_ITEM_ID, SEGMENT1 AS ITEM_NUMBER FROM MTL_SYSTEM_ITEMS_B
        WHERE ORGANIZATION_ID = 101 ORDER BY SEGMENT1
        """
    return execute_query(query=query, fetch='all')

def get_item_id_from_number(item_number):
    bind_vars = { 'item_number': item_number}
    query = """
        SELECT inventory_item_id AS INVENTORY_ITEM_ID FROM MTL_SYSTEM_ITEMS_B
        WHERE ORGANIZATION_ID = 101 AND segment1 = :item_number
        """
    return execute_query(query=query, bind_vars=bind_vars, fetch='one')

def get_organizations():
    query = """
        SELECT ood.organization_id, ood.organization_code, hou.name OU
        FROM org_organization_definitions ood, hr_organization_units hou
        WHERE hou.organization_id = ood.operating_unit
        """
    return execute_query(query=query, fetch='all')

def get_item_status():
    query = """
        SELECT MIST.INVENTORY_ITEM_STATUS_CODE, MIST.DESCRIPTION
        FROM MTL_ITEM_STATUS_TL MIST
        WHERE DISABLE_DATE IS NULL
        ORDER BY MIST.INVENTORY_ITEM_STATUS_CODE
        """
    return execute_query(query=query, fetch='all')

def get_item_detail(inventory_item_id):
    bind_vars = {'inventory_item_id': inventory_item_id}
    query = """
        SELECT MSI.INVENTORY_ITEM_ID, MSI.SEGMENT1 AS ITEM_NUMBER, MSI.DESCRIPTION, MSI.ATTRIBUTE1 AS WAREHOUSE, 
        MSI.ATTRIBUTE3 AS HTS_CODE, MSI.ATTRIBUTE5 AS BOX_LABEL, MSI.ATTRIBUTE6 AS MP_UOM, 
        MSI.ATTRIBUTE7 AS MC_UOM, MSI.ATTRIBUTE9 AS ABC_CODE, MSI.INVENTORY_ITEM_STATUS_CODE,
        MSI.ATTRIBUTE17 AS PROD_INTRO_DATE, MSI.PRIMARY_UOM_CODE, MSI.DIMENSION_UOM_CODE,
        MSI.UNIT_LENGTH, MSI.UNIT_WIDTH, MSI.UNIT_WIDTH, MIL.SEGMENT1 LOCATOR, MSI.ITEM_TYPE
        FROM MTL_ITEM_LOCATIONS MIL, MTL_ITEM_LOC_DEFAULTS MILD, MTL_SYSTEM_ITEMS_B MSI
        WHERE MIL.INVENTORY_LOCATION_ID = MILD.LOCATOR_ID AND MSI.INVENTORY_ITEM_ID = MILD.INVENTORY_ITEM_ID AND MIL.ORGANIZATION_ID = 103 
        AND MILD.DEFAULT_TYPE = 1 AND MSI.ORGANIZATION_ID = 103
        AND MSI.INVENTORY_ITEM_ID = :inventory_item_id
        """
    return execute_query(query=query, bind_vars=bind_vars, fetch='one')
    
def get_inv_oh_qty(inventory_item_id, sub_inventory_code):
    bind_vars = {'inventory_item_id': inventory_item_id, 'sub_inventory_code': sub_inventory_code}
    query = """
        SELECT XXJWY_GLOBAL_REPORT_PKG.GET_INV_OH_QTY(P_INVENTORY_ITEM_ID=> :inventory_item_id, P_SUB_INVENTORY_CODE => :sub_inventory_code)
        QTY FROM DUAL
        """
    return execute_query(query=query, bind_vars=bind_vars, fetch='one')

def get_inv_allocated_qty(inventory_item_id, sub_inventory_code):
    bind_vars = {'inventory_item_id': inventory_item_id, 'sub_inventory_code': sub_inventory_code}
    query = """
        SELECT XXJWY_GLOBAL_REPORT_PKG.get_allocation_qty(P_INVENTORY_ITEM_ID=> :inventory_item_id, P_SUB_INVENTORY_CODE => :sub_inventory_code)
        QTY FROM DUAL
        """
    return execute_query(query=query, bind_vars=bind_vars, fetch='one')

def get_inv_available_qty(inventory_item_id, sub_inventory_code):
    bind_vars = {'inventory_item_id': inventory_item_id, 'sub_inventory_code': sub_inventory_code}
    query = """
        SELECT XXJWY_GLOBAL_REPORT_PKG.get_item_available_qty_all(P_INVENTORY_ITEM_ID=> :inventory_item_id, P_SUB_INVENTORY_CODE => :sub_inventory_code)
        QTY FROM DUAL
        """
    return execute_query(query=query, bind_vars=bind_vars, fetch='one')

def get_open_po_qty(inventory_item_id):
    bind_vars = {'inventory_item_id': inventory_item_id}
    query = """
        SELECT XXJWY_GLOBAL_REPORT_PKG.get_open_po_qty(P_INVENTORY_ITEM_ID=> :inventory_item_id)
        QTY FROM DUAL
        """
    return execute_query(query=query, bind_vars=bind_vars, fetch='one')

def get_customer_id_by_number(customer_number):
    bind_vars = {'customer_number': customer_number}
    query = """
        SELECT customer_id FROM AR_CUSTOMERS WHERE customer_number = :customer_number
        """
    return execute_query(query=query, bind_vars=bind_vars, fetch='one')

def get_weekly_ad():
    query = """
        SELECT ac.customer_name, ac.customer_number, msib.segment1 item_number, xwa.weekly_ad_amount, xwa.date_from, xwa.date_to FROM XXCDL_WEEKLY_AD XWA, AR_CUSTOMERS AC, mtl_system_items_b msib WHERE xwa.inventory_item_id= msib.inventory_item_id and AC.customer_id = XWA.cust_account_id and msib.organization_id = 101 order by xwa.date_from DESC, ac.customer_name, msib.segment1
        """
    return execute_query(query=query, fetch='all')

def get_item_customer_weekly_ad(inventory_item_id, customer_id, date_from):
    bind_vars = {
        'inventory_item_id': inventory_item_id,
        'customer_id': customer_id,
        'date_from': date_from
    }
    query = """
        SELECT XWA.INVENTORY_ITEM_ID FROM XXCDL_WEEKLY_AD XWA WHERE XWA.INVENTORY_ITEM_ID = :inventory_item_id AND XWA.DATE_FROM = :date_from AND XWA.CUST_ACCOUNT_ID = :customer_id
        """
    return execute_query(query=query, bind_vars=bind_vars, fetch='one')

def get_customer_allowance():
    query = """
        SELECT ac.customer_id, ac.customer_number, ac.customer_name, NVL(xx.allowance*100,0) allowance, NVL(xx.commission*100,0) commission  from ar_customers ac, XXCDL_CUSTOMER_ALLOWANCE_COMM_RPT xx
        WHERE ac.customer_id = xx.customer_id       
        """
    return execute_query(query=query, fetch='all')

def get_customer_no_allowance():
    query = """
        SELECT customer_id, customer_number, customer_name FROM AR_CUSTOMERS
        WHERE CUSTOMER_ID NOT IN (SELECT ac.customer_id from ar_customers ac, XXCDL_CUSTOMER_ALLOWANCE_COMM_RPT xx
        WHERE ac.customer_id = xx.customer_id)  ORDER BY CUSTOMER_NAME     
        """
    return execute_query(query=query, fetch='all')

def get_customer_allowance_by_customer(customer_id):
    bind_vars = {'customer_id': customer_id}
    query = """
        SELECT ac.customer_id, ac.customer_number, ac.customer_name, NVL(xx.allowance*100,0) allowance, NVL(xx.commission*100,0) commission, xx.creation_date  from ar_customers ac, XXCDL_CUSTOMER_ALLOWANCE_COMM_RPT xx
        WHERE ac.customer_id = xx.customer_id AND xx.customer_id = :customer_id      
        """
    return execute_query(query=query, bind_vars=bind_vars, fetch='one')

def get_item_led_expense():
    query = """
        SELECT msib.inventory_item_id, msib.segment1 item_number, msib.description, NVL(xx.led_expense * 100,0) led_expense from mtl_system_items_b msib, XXCDL_ITEM_LED_EXPENSE xx
        WHERE msib.inventory_item_id = xx.inventory_item_id and msib.organization_id = 103 ORDER BY msib.segment1      
        """
    return execute_query(query=query, fetch='all')

def get_item_led_expense_by_item(item):
    bind_vars = {'inventory_item_id': item}
    query = """
        SELECT msib.inventory_item_id, msib.segment1 item_number, msib.description, NVL(xx.led_expense * 100,0) led_expense from mtl_system_items_b msib, XXCDL_ITEM_LED_EXPENSE xx
        WHERE msib.inventory_item_id = xx.inventory_item_id and msib.organization_id = 103  AND msib.inventory_item_id = :inventory_item_id      
        """
    return execute_query(query=query, bind_vars=bind_vars, fetch='one')

def get_item_not_in_led_expense():
    query = """
        SELECT msib.inventory_item_id, msib.segment1 item_number, msib.description from mtl_system_items_b msib 
        WHERE inventory_item_id NOT IN
            (SELECT inventory_item_id FROM XXCDL_ITEM_LED_EXPENSE)
        AND msib.organization_id = 103       
        """
    return execute_query(query=query, fetch='all')

def get_item_comp_shop():
    query = """
        SELECT msib.inventory_item_id, msib.segment1 item_number, msib.description, our_retail, brand, price from mtl_system_items_b msib, XXCDL_ITEM_COMP_SHOP xx
        WHERE msib.inventory_item_id = xx.inventory_item_id and msib.organization_id = 103 ORDER BY msib.segment1     
        """
    return execute_query(query=query, fetch='all')

def get_item_comp_shop_by_item(item):
    bind_vars = {'inventory_item_id': item}
    query = """
        SELECT msib.inventory_item_id, msib.segment1 item_number, msib.description, our_retail, brand, price from mtl_system_items_b msib, XXCDL_ITEM_COMP_SHOP xx
        WHERE msib.inventory_item_id = xx.inventory_item_id and msib.organization_id = 103 AND xx.inventory_item_id = :inventory_item_id     
        """
    return execute_query(query=query, bind_vars=bind_vars, fetch='one')

def get_customer_item_credit():
    query = """
        SELECT ac.customer_id, ac.customer_number, ac.customer_name, msib.inventory_item_id, msib.segment1 item_number, xx.cm from ar_customers ac, XXCDL_CUSTOMER_ITEM_CM_RPT xx, mtl_system_items_b msib 
        WHERE ac.customer_id = xx.customer_id  AND xx.inventory_item_id = msib.inventory_item_id AND msib.organization_id = 101     
        """
    return execute_query(query=query, fetch='all')


def get_item_not_in_customer_credit(customer):
    bind_vars = {
        'customer_id': customer
    }
    query = """
        SELECT inventory_item_id, segment1 item_number from mtl_system_items_b
        WHERE organization_id = 101 AND inventory_item_id NOT IN 
        (SELECT inventory_item_id from ar_customers ac, XXCDL_CUSTOMER_ITEM_CM_RPT xx 
        WHERE ac.customer_id = xx.customer_id AND ac.customer_id = :customer_id)
        """
    return execute_query(query=query, fetch='all', bind_vars=bind_vars)

def get_customer_item_credit_by_item(customer, item):
    bind_vars = {
        'customer_id': customer,
        'inventory_item_id': item
    }
    query = """
        SELECT ac.customer_id, ac.customer_number, ac.customer_name, msib.inventory_item_id, msib.segment1 item_number, xx.cm from ar_customers ac, XXCDL_CUSTOMER_ITEM_CM_RPT xx, mtl_system_items_b msib 
        WHERE ac.customer_id = xx.customer_id  AND xx.inventory_item_id = msib.inventory_item_id AND msib.organization_id = 101
        AND xx.inventory_item_id = :inventory_item_id
        AND xx.customer_id = :customer_id
        """
    return execute_query(query=query, fetch='one', bind_vars=bind_vars)

def get_all_po():
    query = """
        SELECT PO_HEADER_ID, SEGMENT1 PO_NUMBER FROM PO_HEADERS_ALL WHERE ATTRIBUTE2 NOT IN ('QUOTAION', 'EXP')
        AND AUTHORIZATION_STATUS NOT IN ('INCOMPLETE', 'IN PROCESS')
        AND AUTHORIZATION_STATUS IS NOT NULL
        ORDER BY CREATION_DATE DESC, SEGMENT1 DESC
    """
    return execute_query(query=query, fetch='all')

def get_po_by_number(po):
    bind_vars = {
        'po_header_id': po,
    }
    query = """
        SELECT POH.PO_HEADER_ID,
         POH.SEGMENT1 PO_NUMBER,
         POH.CREATION_DATE,
         HE.LAST_NAME BUYER,
         PV.VENDOR_NAME supplier,
         FLV.DESCRIPTION TYPE,
         PVSA.VENDOR_SITE_CODE,
         HRL1.LOCATION_CODE SHIP_TO_LOCATION,
         HRL2.LOCATION_CODE BILL_TO_LOCATION,
         POH.CURRENCY_CODE,
         PO_HEADERS_SV3.GET_PO_STATUS (POH.PO_HEADER_ID) STATUS,
         SUM (PLA.UNIT_PRICE * PLA.QUANTITY) TOTAL_AMOUNT,
         POH.ORG_ID ORG_ID,
         POH.STYLE_ID STYLE_ID,
         POH.LOCK_OWNER_ROLE LOCK_OWNER_ROLE,
         POH.LOCK_OWNER_USER_ID LOCK_OWNER_USER_ID,
         POH.ENABLE_ALL_SITES ENABLE_ALL_SITES
    FROM PO_HEADERS_ALL POH,
         PO_LINES_ALL PLA,
         HR_EMPLOYEES HE,
         PO_VENDORS PV,
         fnd_lookup_values FLV,
         PO_VENDOR_SITES_ALL PVSA,
         HR_LOCATIONS_ALL_TL HRL1,
         HR_LOCATIONS_ALL_TL HRL2
   WHERE     1 = 1
         AND POH.PO_HEADER_ID = PLA.PO_HEADER_ID
         AND POH.AGENT_ID = HE.EMPLOYEE_ID
         AND POH.VENDOR_ID = PV.VENDOR_ID
         AND POH.TYPE_LOOKUP_CODE = FLV.LOOKUP_CODE
         AND POH.VENDOR_SITE_ID = PVSA.VENDOR_SITE_ID
         AND FLV.LOOKUP_TYPE = 'PO TYPE'
         AND HRL1.LOCATION_ID(+) = POH.SHIP_TO_LOCATION_ID
         AND HRL1.LANGUAGE(+) = USERENV ('LANG')
         AND HRL2.LOCATION_ID(+) = POH.BILL_TO_LOCATION_ID
         AND HRL2.LANGUAGE(+) = USERENV ('LANG')
         AND POH.PO_HEADER_ID = :po_header_id
GROUP BY POH.PO_HEADER_ID,
         POH.SEGMENT1,
         POH.CREATION_DATE,
         HE.LAST_NAME,
         PV.VENDOR_NAME,
         FLV.DESCRIPTION,
         PVSA.VENDOR_SITE_CODE,
         HRL1.LOCATION_CODE,
         HRL2.LOCATION_CODE,
         POH.CURRENCY_CODE,
         POH.ORG_ID,
         POH.STYLE_ID,
         POH.LOCK_OWNER_ROLE,
         POH.LOCK_OWNER_USER_ID,
         POH.ENABLE_ALL_SITES
        """
    return execute_query(query=query, fetch='one', bind_vars=bind_vars)

def get_po_lines_by_po(po_header_id):
    bind_vars = {
        'po_header_id': po_header_id,
    }
    query = """
        SELECT POL.PO_LINE_ID,
        POL.LAST_UPDATE_DATE,
        POL.LAST_UPDATED_BY,
        POL.CREATION_DATE,
        POL.CREATED_BY,
        POL.LAST_UPDATE_LOGIN,
        POL.REQUEST_ID,
        POL.PROGRAM_APPLICATION_ID,
        POL.PROGRAM_ID,
        POL.PROGRAM_UPDATE_DATE,
        POL.PO_HEADER_ID,
        POL.LINE_TYPE_ID,
        POL.LINE_NUM,
        PLTT.LINE_TYPE,
        POL.ITEM_ID,
        MSI.SEGMENT1 ITEM_NUMBER,
        POL.ITEM_REVISION,
        MCB.SEGMENT1 CATEGORY,
        DECODE (
        POL.ITEM_ID,
        NULL, POL.ITEM_DESCRIPTION,
        DECODE (MSI.ALLOW_ITEM_DESC_UPDATE_FLAG,
                'Y', POL.ITEM_DESCRIPTION,
                MSIT.DESCRIPTION))
        ITEM_DESCRIPTION,
        POL.UNIT_MEAS_LOOKUP_CODE,
        POL.QUANTITY_COMMITTED,
        POL.COMMITTED_AMOUNT,
        POL.ALLOW_PRICE_OVERRIDE_FLAG,
        POL.NOT_TO_EXCEED_PRICE,
        POL.LIST_PRICE_PER_UNIT,
        POL.UNIT_PRICE,
        POL.QUANTITY,
        POL.VENDOR_PRODUCT_NUM,
        POL.UN_NUMBER_ID,
        POUN.UN_NUMBER,
        POL.HAZARD_CLASS_ID,
        PHC.HAZARD_CLASS,
        POL.MIN_ORDER_QUANTITY,
        POL.MAX_ORDER_QUANTITY,
        POL.QTY_RCV_TOLERANCE,
        POL.OVER_TOLERANCE_ERROR_FLAG,
        POL.MARKET_PRICE,
        POL.UNORDERED_FLAG,
        POL.CLOSED_FLAG,
        POL.USER_HOLD_FLAG,
        POL.CANCEL_FLAG,
        POL.CANCELLED_BY,
        POL.CANCEL_DATE,
        POL.CANCEL_REASON,
        POL.FIRM_STATUS_LOOKUP_CODE,
        POL.FIRM_DATE,
        POL.CONTRACT_NUM,
        POL.NOTE_TO_VENDOR,
        POL.FROM_HEADER_ID,
        POL.FROM_LINE_ID,
        POL.TAXABLE_FLAG,
        POL.TYPE_1099,
        POL.CAPITAL_EXPENSE_FLAG,
        POL.NEGOTIATED_BY_PREPARER_FLAG,
        POL.MIN_RELEASE_AMOUNT,
        POL.PRICE_TYPE_LOOKUP_CODE,
        POL.PRICE_BREAK_LOOKUP_CODE,
        PLTB.ORDER_TYPE_LOOKUP_CODE,
        NVL (PLTB.OUTSIDE_OPERATION_FLAG, 'N'),
        POL.TRANSACTION_REASON_CODE,
        POL.CLOSED_BY,
        POL.CLOSED_DATE,
        POL.CLOSED_CODE,
        POL.CLOSED_REASON,
        POL.GOVERNMENT_CONTEXT,
        POL.USSGL_TRANSACTION_CODE,
        POL.REFERENCE_NUM,
        POL.ATTRIBUTE_CATEGORY,
        POL.ATTRIBUTE1,
        POL.ATTRIBUTE2,
        POL.ATTRIBUTE3,
        POL.ATTRIBUTE4,
        POL.ATTRIBUTE5,
        POL.ATTRIBUTE6,
        POL.ATTRIBUTE7,
        POL.ATTRIBUTE8,
        POL.ATTRIBUTE9,
        POL.ATTRIBUTE10,
        POL.ATTRIBUTE11,
        POL.ATTRIBUTE12,
        POL.ATTRIBUTE13,
        POL.ATTRIBUTE14,
        POL.ATTRIBUTE15,
        NVL(POL.UNIT_PRICE,0) * NVL(POL.QUANTITY,0) AMOUNT,
        DECODE (POL.item_id, NULL, MUOM1.UOM_CLASS, MUOM2.UOM_CLASS) UOM_CLASS,
        POLC1.DISPLAYED_FIELD,
        POLC2.DISPLAYED_FIELD,
        MSI.ALLOWED_UNITS_LOOKUP_CODE,
        MSI.OUTSIDE_OPERATION_UOM_TYPE,
        POL.GLOBAL_ATTRIBUTE_CATEGORY,
        POL.GLOBAL_ATTRIBUTE1,
        POL.GLOBAL_ATTRIBUTE2,
        POL.GLOBAL_ATTRIBUTE3,
        POL.GLOBAL_ATTRIBUTE4,
        POL.GLOBAL_ATTRIBUTE5,
        POL.GLOBAL_ATTRIBUTE6,
        POL.GLOBAL_ATTRIBUTE7,
        POL.GLOBAL_ATTRIBUTE8,
        POL.GLOBAL_ATTRIBUTE9,
        POL.GLOBAL_ATTRIBUTE10,
        POL.GLOBAL_ATTRIBUTE11,
        POL.GLOBAL_ATTRIBUTE12,
        POL.GLOBAL_ATTRIBUTE13,
        POL.GLOBAL_ATTRIBUTE14,
        POL.GLOBAL_ATTRIBUTE15,
        POL.GLOBAL_ATTRIBUTE16,
        POL.GLOBAL_ATTRIBUTE17,
        POL.GLOBAL_ATTRIBUTE18,
        POL.GLOBAL_ATTRIBUTE19,
        POL.GLOBAL_ATTRIBUTE20,
        POL.EXPIRATION_DATE,
        POL.TAX_CODE_ID,
        POL.QC_GRADE,
        POL.BASE_UOM,
        POL.BASE_QTY,
        POL.SECONDARY_UOM,
        POL.SECONDARY_QTY,
        POL.OKE_CONTRACT_HEADER_ID,
        POL.OKE_CONTRACT_VERSION_ID,
        POL.SECONDARY_UNIT_OF_MEASURE,
        POL.SECONDARY_QUANTITY,
        POL.PREFERRED_GRADE,
        POL.AUCTION_HEADER_ID,
        POL.AUCTION_DISPLAY_NUMBER,
        POL.AUCTION_LINE_NUMBER,
        POL.BID_NUMBER,
        POL.BID_LINE_NUMBER,
        POL.SUPPLIER_REF_NUMBER,
        POL.CONTRACT_ID,
        POL.JOB_ID,
        POL.START_DATE,
        POL.CONTRACTOR_FIRST_NAME,
        POL.CONTRACTOR_LAST_NAME,
        PLTB.PURCHASE_BASIS,
        PLTB.MATCHING_BASIS,
        POL.FROM_LINE_LOCATION_ID,
        POL.BASE_UNIT_PRICE,
        POL.MANUAL_PRICE_CHANGE_FLAG,
        DECODE (MSI.TRACKING_QUANTITY_IND,
            'PS', MSI.SECONDARY_DEFAULT_IND,
            NULL),
        DECODE (MSI.TRACKING_QUANTITY_IND, 'PS', MSI.SECONDARY_UOM_CODE, NULL),
        MSI.GRADE_CONTROL_FLAG,
        POL.ORG_ID
        FROM PO_LINE_TYPES_B PLTB,
        PO_LINE_TYPES_TL PLTT,
        MTL_UNITS_OF_MEASURE MUOM1,
        MTL_UNITS_OF_MEASURE MUOM2,
        PO_UN_NUMBERS_TL POUN,
        PO_HAZARD_CLASSES_TL PHC,
        PO_LOOKUP_CODES POLC1,
        PO_LOOKUP_CODES POLC2,
        MTL_SYSTEM_ITEMS MSI,
        MTL_SYSTEM_ITEMS_TL MSIT,
        PO_LINES_ALL POL,
        MTL_CATEGORIES_B MCB,
        MTL_CATEGORY_SETS_B MCSB,
        MTL_CATEGORY_SETS_TL MCST 
 WHERE  POL.LINE_TYPE_ID = PLTB.LINE_TYPE_ID(+)
        AND POL.LINE_TYPE_ID = PLTT.LINE_TYPE_ID(+)
        AND PLTT.LANGUAGE(+) = USERENV ('LANG')
        AND MSI.INVENTORY_ITEM_ID(+) = POL.ITEM_ID
        AND MSI.ORGANIZATION_ID(+) =
                PO_LINES_SV4.get_inventory_orgid (POL.org_id)
        AND MUOM1.UNIT_OF_MEASURE(+) = POL.UNIT_MEAS_LOOKUP_CODE
        AND MUOM2.UNIT_OF_MEASURE(+) = MSI.PRIMARY_UNIT_OF_MEASURE
        AND POUN.UN_NUMBER_ID(+) = POL.UN_NUMBER_ID
        AND POUN.LANGUAGE(+) = USERENV ('LANG')
        AND PHC.HAZARD_CLASS_ID(+) = POL.HAZARD_CLASS_ID
        AND PHC.LANGUAGE(+) = USERENV ('LANG')
        AND POLC1.LOOKUP_TYPE(+) = 'PRICE TYPE'
        AND POLC1.LOOKUP_CODE(+) = POL.PRICE_TYPE_LOOKUP_CODE
        AND POLC2.LOOKUP_TYPE(+) = 'TRANSACTION REASON'
        AND POLC2.LOOKUP_CODE(+) = POL.TRANSACTION_REASON_CODE
        AND MSI.INVENTORY_ITEM_ID = MSIT.INVENTORY_ITEM_ID(+)
        AND MSI.ORGANIZATION_ID = MSIT.ORGANIZATION_ID(+)
        AND MSIT.LANGUAGE(+) = USERENV ('LANG')
        AND MCB.STRUCTURE_ID = MCSB.STRUCTURE_ID
        AND MCSB.CATEGORY_SET_ID = MCST.CATEGORY_SET_ID
        AND MCST.CATEGORY_SET_NAME = 'Jimway PO Category'
        AND (MCB.DISABLE_DATE IS NULL OR MCB.DISABLE_DATE > SYSDATE)
        AND POL.CATEGORY_ID = MCB.CATEGORY_ID
        AND POL.PO_HEADER_ID = :po_header_id
    """
    return execute_query(query=query, fetch='all', bind_vars=bind_vars)

def get_po_price_change_stage():
    query = """
        SELECT * FROM XXPO_PO_PRICE_CHANGE_STAGE
    """
    return execute_query(query=query, fetch='all')