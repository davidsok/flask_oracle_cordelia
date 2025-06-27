from db import *

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