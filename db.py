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
            return True, None
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