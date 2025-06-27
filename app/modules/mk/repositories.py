from db import *

def get_last_wishlist_id():
    query = """
        SELECT MAX(WISHLIST_ID) last_wishlist_id FROM XXCDL_USER_WISHLIST_HEADERS
    """
    return execute_query(query=query, fetch='one')

def get_all_wishlist():
    query = """
        SELECT * FROM XXCDL_USER_WISHLIST_HEADERS
    """
    return execute_query(query=query, fetch='all')

def get_user_wishlist(user_id):
    bind_vars = {
        'user_id': user_id,
    }
    query = """
        SELECT * FROM XXCDL_USER_WISHLIST_HEADERS WHERE user_id = :user_id ORDER BY wishlist_id
    """
    return execute_query(query=query, fetch='all', bind_vars=bind_vars)

def get_wishlist_by_id(wishlist_id):
    bind_vars = {
        'wishlist_id': wishlist_id,
    }
    query = """
        SELECT * FROM XXCDL_USER_WISHLIST_HEADERS WHERE wishlist_id = :wishlist_id
    """
    return execute_query(query=query, fetch='one', bind_vars=bind_vars)

def get_items_not_on_wishlist(wishlist_id):
    bind_vars = {
        'wishlist_id': wishlist_id,
    }
    query = """
        SELECT MSIB.inventory_item_id, MSIB.segment1 item_number FROM MTL_SYSTEM_ITEMS_B MSIB WHERE MSIB.INVENTORY_ITEM_ID NOT IN ( SELECT INVENTORY_ITEM_ID FROM XXCDL_USER_WISHLIST_LINES XX WHERE XX.wishlist_id = :wishlist_id) AND MSIB.ORGANIZATION_ID = 101
    """
    return execute_query(query=query, fetch='all', bind_vars=bind_vars)