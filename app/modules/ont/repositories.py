from db import *

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