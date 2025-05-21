from pickletools import read_uint1
from flask import render_template, redirect, url_for, flash, session, request
from app.modules.inv.forms import ItemInquiryForm, ItemInquiryForm2
from utils.breadcrumbs import register_breadcrumb
from ...modules.po import po_bp
from db import *
import os
import pandas as pd
from flask_cors import CORS
from utils.func import *
from datetime import datetime, date

@po_bp.route('/')
@register_breadcrumb('Purchase Order', url='/po', parent='Home', parent_url='/')
def po_dashboard():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    user_access = get_module_access_by_module_user(module_name='PO', user_id=session['user_id'])
    return render_template('po/po_dashboard.html', user_access = user_access, user=user)

@po_bp.route('/inquiry')
@register_breadcrumb('PO Inquiry', url='/po/inquiry', parent='Purchase Order', parent_url='/po')
def purchase_orders():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    user_access = get_module_access_by_module_user(module_name='PO', user_id=session['user_id'])
    return render_template('po/po_dashboard.html', user_access = user_access, user=user)

@po_bp.route('/weekly_pos')
@register_breadcrumb('Weekly POS Data', url='/po/weekly_pos', parent='PO', parent_url='/po')
def weekly_pos_data():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    user_access = get_module_access_by_module_user(module_name='PO', user_id=session['user_id'])
    latest_week = get_all_existing_pos_week()
    return render_template('po/weekly_pos_data.html', user_access = user_access, user=user, latest_week = latest_week)


@po_bp.route('/pos/update/<int:query_id>', methods=['POST'])
@register_breadcrumb('Update Weekly POS', url='/pos/update', parent='Purchase Order', parent_url='/po')
def update_weekly_pos_data(query_id):
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    mapping = {
        1: (query_set_ca, 'XXPOS_POS_RAW', ['processtime', 'forecastperiod', 'payment_vendor_nbr', 'merch_vendor_nbr', 'merch_vendor', 'week', 'category', 'sku', 'upc', 'storenumber', 'model_number', 'store_weeks', 'str_oh_units_wkly', 'sales_units_before_returns', 'return_units', 'sales_$', 'sales_$_before_returns', 'return_$']),
        2: (query_set_us_di, 'XXPOS_POS_RAW', ['processtime', 'forecastperiod', 'payment_vendor_nbr', 'merch_vendor_nbr', 'merch_vendor', 'week', 'category', 'sku', 'upc', 'storenumber', 'model_number', 'store_weeks', 'str_oh_units_wkly', 'sales_units_before_returns', 'return_units', 'sales_$', 'sales_$_before_returns', 'return_$']),
        3: (query_set_us_dom, 'XXPOS_POS_RAW', ['processtime', 'forecastperiod', 'payment_vendor_nbr', 'merch_vendor_nbr', 'merch_vendor', 'week', 'category', 'sku', 'upc', 'storenumber', 'model_number', 'store_weeks', 'str_oh_units_wkly', 'sales_units_before_returns', 'return_units', 'sales_$', 'sales_$_before_returns', 'return_$']),
    }

    if query_id not in mapping:
        flash("Invalid query selection.")
        return redirect(url_for('po.weekly_post_data'))

    query_func, table, cols = mapping[query_id]
    try:
        df = query_func()
        df = df.where(pd.notnull(df), None)
        print(df)
        success, error = insert_into_oracle(df, table, cols)
        if success:
            flash(f"Query {query_func.__name__} inserted into {table} successfully.")
        else:
            flash(f"Failed inserting Query {query_func.__name__}: {error}")
    except Exception as e:
        flash(f"Error: {str(e)}")
    return redirect(url_for('po.weekly_pos_data'))