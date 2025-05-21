from pickletools import read_uint1
from flask import render_template, redirect, url_for, flash, session, request
from app.modules.inv.forms import ItemInquiryForm, ItemInquiryForm2
from utils.breadcrumbs import register_breadcrumb
from ...modules.ont import ont_bp
from db import *
import os
import pandas as pd
from flask_cors import CORS
from utils.func import *
from datetime import datetime, date

@ont_bp.route('/')
@register_breadcrumb('Order Management', url='/ont', parent='Home', parent_url='/')
def ont_dashboard():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    user_access = get_module_access_by_module_user(module_name='OM', user_id=session['user_id'])
    return render_template('ont/ont_dashboard.html', user_access = user_access, user=user)

@ont_bp.route('/orders')
@register_breadcrumb('Order Organizer', url='/ont/orders', parent='Order Management', parent_url='/ont')
def orders():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    user_access = get_module_access_by_module_user(module_name='OM', user_id=session['user_id'])
    return render_template('ont/ont_dashboard.html', user_access = user_access, user=user)

@ont_bp.route('/ad/upload', methods=['GET', 'POST'])
@register_breadcrumb('Upload Weekly Ad', url='/ont/ad/upload', parent='Order Management', parent_url='/ont')
def upload_weekly_ad():
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
                df = pd.read_csv(save_path, header=0, delimiter=",", names=['item_number', 'customer_number', 'customer_name', 'ad_amount', 'date_from', 'date_to'], na_filter=False, skip_blank_lines=True, skipinitialspace=True, engine='python')
                data = df.to_html()
                flash ('Your File has been uploaded successfully!!!', 'success')
                return render_template('ont/update_weekly_ad.html', data=data, file_name=file_name, user=user)
            except Exception as e:
                l_message = f"Error reading CSV File : {e}"
                flash (l_message, 'error')
                return render_template('ont/update_weekly_ad.html', data=None, file_name = file_name, user=user)
        else:
            l_message = f"Other error"
            flash (l_message, 'error')
    return render_template('ont/upload_weekly_ad.html', user=user)

@ont_bp.route('/ont/ad/update/<file_name>', methods=['GET', 'POST'])
@register_breadcrumb('Update Weekly Ad', url='/ont/ad/update', parent='Order Management', parent_url='/ont')
def update_weekly_ad(file_name):
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    file_path = get_file_path(file_name)
    print(file_path)
    try:   
        df = pd.read_csv(file_path, header=0, delimiter=",", names=['item_number', 'customer_number', 'customer_name', 'ad_amount', 'date_from', 'date_to'], na_filter=False, skip_blank_lines=True, skipinitialspace=True, engine='python')

        bind = []
        df['date_from'] = pd.to_datetime(df['date_from'], errors='coerce')
        df['date_to'] = pd.to_datetime(df['date_to'], errors='coerce')
        for row in df.itertuples(index=False):
            bind.append({
                'item_number': str(row.item_number),
                'customer_number': str(row.customer_number),
                'customer_name': str(row.customer_name),
                'ad_amount': float(row.ad_amount),
                'date_from': row.date_from.to_pydatetime() if pd.notnull(row.date_from) else None,
                'date_to': row.date_to.to_pydatetime() if pd.notnull(row.date_to) else None
            })
        conn = get_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("""DELETE FROM XXJWY.XXCDL_WEEKLY_AD_STAGE""")
            conn.commit()
            cursor.executemany("""INSERT INTO XXJWY.XXCDL_WEEKLY_AD_STAGE ( ITEM_NUMBER, CUSTOMER_NUMBER, AD_AMOUNT, DATE_FROM, DATE_TO, CUSTOMER_NAME) VALUES (:item_number, :customer_number, :ad_amount, :date_from, :date_to, :customer_name)""", bind)
            conn.commit()
            errbuff = cursor.var(str)
            retcode = cursor.var(int)
            cursor.callproc('XXCDL_IMPORT_WEEKLY_AD.MAIN', [errbuff, retcode])
            cursor.close()
            flash('Your Weekly Ad Spent has been updated successfully!', 'success')
        return redirect(url_for('ont.ont_dashboard'))
    except Exception as e:
        l_message = f'Error reading CSV file: {e}'
        flash(l_message, 'error')
        return render_template('ont/update_weekly_ad.html', data={}, file_name=file_name, user=user)