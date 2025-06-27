from db import *

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