from ast import Raise
from pickletools import read_uint1
from flask import render_template, redirect, url_for, flash, session, request
from app.modules.inv.forms import ItemInquiryForm, ItemInquiryForm2
from app.modules.po.forms import PoInquiryForm
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

@po_bp.route('/inquiry', methods=['GET', 'POST'])
@register_breadcrumb('PO Inquiry', url='/po/inquiry', parent='Purchase Order', parent_url='/po')
def po_inquiry():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    user_access = get_module_access_by_module_user(module_name='PO', user_id=session['user_id'])
    all_pos = get_all_po()
    form = PoInquiryForm()
    form.po_number.choices = list((po['po_header_id'], po['po_number']) for po in all_pos)
    if form.validate_on_submit() and request.method == 'POST':
        print(form.po_number.data)
        po = get_po_by_number(po=form.po_number.data)
        po_lines = get_po_lines_by_po(po_header_id=form.po_number.data)
        print(po_lines)
        return render_template('po/po_detail.html', user= user, po=po, po_lines=po_lines)

    return render_template('po/po_inquiry.html', user_access = user_access, user=user, form=form)

@po_bp.route('/weekly_pos')
@register_breadcrumb('Weekly POS Data', url='/po/weekly_pos', parent='PO', parent_url='/po')
def weekly_pos_data():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    user_access = get_module_access_by_module_user(module_name='PO', user_id=session['user_id'])
    latest_week = get_all_existing_pos_week()
    payment_vendors = {
        'CA' : 26014487, 
        'DOM' : 112582,
        'DI' : 12746
    }
    canada_none_pos_week = get_none_existing_pos_week(payment_vendor=payment_vendors['CA'])
    us_di_none_pos_week = get_none_existing_pos_week(payment_vendor=payment_vendors['DI'])
    us_dom_none_pos_week = get_none_existing_pos_week(payment_vendor=payment_vendors['DOM'])

    return render_template('po/weekly_pos_data.html', user_access=user_access, user=user, latest_week = latest_week, canada_none_pos_week=canada_none_pos_week, us_di_none_pos_week=us_di_none_pos_week, us_dom_none_pos_week=us_dom_none_pos_week)


@po_bp.route('/pos/update/<int:query_id>', methods=['POST'])
@register_breadcrumb('Update Weekly POS', url='/pos/update', parent='Purchase Order', parent_url='/po')
def update_weekly_pos_data(query_id, *args, **kwargs):
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    if request.form['week'] == 'last_week':
        mapping = {
            1: (query_set_ca_last_week, 'XXPOS_POS_RAW', ['processtime', 'forecastperiod', 'payment_vendor_nbr', 'merch_vendor_nbr', 'merch_vendor', 'week', 'category', 'sku', 'upc', 'storenumber', 'model_number', 'store_weeks', 'str_oh_units_wkly', 'sales_units_before_returns', 'return_units', 'sales_$', 'sales_$_before_returns', 'return_$']),
            2: (query_set_us_di_last_week, 'XXPOS_POS_RAW', ['processtime', 'forecastperiod', 'payment_vendor_nbr', 'merch_vendor_nbr', 'merch_vendor', 'week', 'category', 'sku', 'upc', 'storenumber', 'model_number', 'store_weeks', 'str_oh_units_wkly', 'sales_units_before_returns', 'return_units', 'sales_$', 'sales_$_before_returns', 'return_$']),
            3: (query_set_us_dom_last_week, 'XXPOS_POS_RAW', ['processtime', 'forecastperiod', 'payment_vendor_nbr', 'merch_vendor_nbr', 'merch_vendor', 'week', 'category', 'sku', 'upc', 'storenumber', 'model_number', 'store_weeks', 'str_oh_units_wkly', 'sales_units_before_returns', 'return_units', 'sales_$', 'sales_$_before_returns', 'return_$']),
        }
        if query_id not in mapping:
            flash("Invalid query selection.")
            return redirect(url_for('po.weekly_pos_data'))
    
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
    else:
        mapping = {
            1: (query_set_ca_week_number, 'XXPOS_POS_RAW', ['processtime', 'forecastperiod', 'payment_vendor_nbr', 'merch_vendor_nbr', 'merch_vendor', 'week', 'category', 'sku', 'upc', 'storenumber', 'model_number', 'store_weeks', 'str_oh_units_wkly', 'sales_units_before_returns', 'return_units', 'sales_$', 'sales_$_before_returns', 'return_$']),
            2: (query_set_us_di_week_number, 'XXPOS_POS_RAW', ['processtime', 'forecastperiod', 'payment_vendor_nbr', 'merch_vendor_nbr', 'merch_vendor', 'week', 'category', 'sku', 'upc', 'storenumber', 'model_number', 'store_weeks', 'str_oh_units_wkly', 'sales_units_before_returns', 'return_units', 'sales_$', 'sales_$_before_returns', 'return_$']),
            3: (query_set_us_dom_week_number, 'XXPOS_POS_RAW', ['processtime', 'forecastperiod', 'payment_vendor_nbr', 'merch_vendor_nbr', 'merch_vendor', 'week', 'category', 'sku', 'upc', 'storenumber', 'model_number', 'store_weeks', 'str_oh_units_wkly', 'sales_units_before_returns', 'return_units', 'sales_$', 'sales_$_before_returns', 'return_$']),
        }
        if query_id not in mapping:
            flash("Invalid query selection.")
            return redirect(url_for('po.weekly_pos_data'))
    
        query_func, table, cols = mapping[query_id]
        try:
            week_number = request.form.get('week')
            print('week_number', week_number)
            if not week_number:
                flash("Missing week number.")
                return redirect(url_for('po.weekly_pos_data'))

            df = query_func(week_number)
            df = df.where(pd.notnull(df), None)
            print(df)
            success, error = insert_into_oracle(df, table, cols)
            if success:
                if query_id == 1:
                    flash(f"HD Canada Weekly POS Updated successfully.")
                elif query_id == 2:
                    flash(f"HD US Direct Import Weekly POS Updated successfully.")
                elif query_id == 3:
                    flash(f"HD US Domestic Weekly POS Updated successfully.")
            else:
                flash(f"Failed inserting Query {query_func.__name__}: {error}")
        except Exception as e:
            flash(f"Error: {str(e)}")
        return redirect(url_for('po.weekly_pos_data'))

@po_bp.route('/price/upload', methods=['GET', 'POST'])
@register_breadcrumb('Upload PO Cost Change', url='/po/price/upload', parent='Purchase Order', parent_url='/po')
def upload_po_price():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    if request.method == 'POST':
        if 'file' not in request.files:
            return 'No file form!'
        file = request.files['file']
        if not file or not hasattr(file, 'filename') or file.filename == '':
            l_message = 'No file selected'
            flash (l_message, 'error')
        elif file and not allowed_file(file.filename):
            l_message = 'Incorrect File Type, please select your CSV file'
            flash (l_message, 'error')
        
        elif file and allowed_file(file.filename):
            print('UPLOAD FOLDER ', Config.UPLOAD_FOLDER)
            print('FILENAME', file.filename)
            save_path = os.path.join(Config.UPLOAD_FOLDER, file.filename)
            file_path = file.save(save_path)
            print('FILEPATH : ', file_path)
            file_name = os.path.splitext(file.filename)[0]
            print('FILENAME : ', file_name)
            try:   
                df = pd.read_csv(save_path, header=0, delimiter=",", names=['po_number', 'vendor_site', 'item_number', 'factory_cost'], skip_blank_lines=True, skipinitialspace=True, engine='python')
                data = df.to_html()
                flash ('Your File has been uploaded successfully!!!', 'success')
                return render_template('po/update_po_price.html', data=data, file_name=file_name, user=user)
            except Exception as e:
                l_message = f"Error reading CSV File : {e}"
                flash (l_message, 'error')
                return render_template('po/upload_po_price.html', data=None, file_name = file_name, user=user)
        else:
            l_message = f"Other error"
            flash (l_message, 'error')
    return render_template('po/upload_po_price.html', user=user)

@po_bp.route('/price/update/<file_name>', methods=['GET', 'POST'])
@register_breadcrumb('Update PO Cost', url='/po/price/update', parent='Purchase Order', parent_url='/po')
def update_po_price(file_name):
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    file_path = get_file_path(file_name)
    print(file_path)
    try:   
        df = pd.read_csv(file_path, header=0, delimiter=",", names=['po_number', 'vendor_site', 'item_number', 'factory_cost'], skip_blank_lines=True, skipinitialspace=True, engine='python')

        print('DF', df)
        bind = []
        for row in df.iloc():
            print('ROW', row)

            bind.append({
                'po_number' : str(row.po_number), 
                'vendor_site' : str(row.vendor_site),
                'item_number' : str(row.item_number),
                'factory_cost' : float(row.factory_cost)
            })
        table_name = "XXJWY.XXPO_PO_PRICE_CHANGE_STAGE"

        # Insert new data to STAGING TABLE
        insert_result = insert_records(table_name=table_name, delete_existing=True, data=bind)
        if insert_result['status'] != 'success':
            flash(f"Insert failed: {insert_result['message']}", 'danger')
        conn = get_connection()
        cursor = conn.cursor()
        errbuff = cursor.var(cx_Oracle.STRING)
        retcode = cursor.var(cx_Oracle.NUMBER)
        cursor.callproc('XXCDL_PO_PRICE_UPDATE.MAIN', [errbuff, retcode])
        conn.close()
        # Use retcode: 0 = success, 2 = warning/error (Oracle standard)
        code = retcode.getvalue()
        message = errbuff.getvalue()
        if code == 0 or code is None:
            flash('Your PO Price has been updated successfully!', 'success')
        else:
            flash(f'Failed to update PO Price. {message}', 'danger')
            
        return render_template('po/update_po_price.html', data={}, file_name=file_name, user=user)
    except pd.errors.ParserError as e:
        flash(f'CSV parsing error: {e}', 'danger')
    except cx_Oracle.DatabaseError as e:
        flash(f'Database error: {e}', 'danger')
    except Exception as e:
        flash(f'Unexpected error: {e}', 'danger')
        return render_template('po/update_po_price.html', data={}, file_name=file_name, user=user)