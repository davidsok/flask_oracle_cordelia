from pickletools import read_uint1
from flask import render_template, redirect, url_for, flash, session, request
from app.modules.inv.forms import ItemInquiryForm, ItemInquiryForm2
from utils.breadcrumbs import register_breadcrumb
from ...modules.inv import inv_bp
from db import *
import os
import pandas as pd
from flask_cors import CORS
from utils.func import *

@inv_bp.route('/')
@register_breadcrumb('Inventory', url='/inv', parent='Home', parent_url='/')
def inv_dashboard():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    user_access = get_module_access_by_module_user(module_name='INV', user_id=session['user_id'])
    return render_template('inv/inv_dashboard.html', user_access = user_access, user=user)

@inv_bp.route('/items', methods=['GET', 'POST'])
@register_breadcrumb('Item Inquiry', url='/inv/items', parent='Inventory', parent_url='/inv')
def items():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    items = get_item_numbers()
    organizations = get_organizations()
    status = get_item_status()
    if items:
        form = ItemInquiryForm2()
        form.item_number.choices = list((item['inventory_item_id'], item['item_number']) for item in items)
    else:
        form = ItemInquiryForm()
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    if form.validate_on_submit() and request.method == 'POST':
        get_item = get_item_detail(inventory_item_id=form.item_number.data)
        get_fg_oh_qty = get_inv_oh_qty(inventory_item_id=form.item_number.data, sub_inventory_code='FG')
        get_fg_allocated_qty = get_inv_allocated_qty(inventory_item_id=form.item_number.data, sub_inventory_code='FG')
        get_fg_available_qty = get_inv_available_qty(inventory_item_id=form.item_number.data, sub_inventory_code='FG')
        get_inv_open_po_qty = get_open_po_qty(inventory_item_id=form.item_number.data)
        return render_template('inv/item_detail.html', item = get_item, fg_oh_qty=get_fg_oh_qty, fg_allocated_qty=get_fg_allocated_qty, fg_available_qty=get_fg_available_qty, inv_open_po_qty=get_inv_open_po_qty, user= user)
    return render_template('inv/item_inquiry.html', form=form, items = items, user= user)


@inv_bp.route('/items/description/upload', methods=['GET', 'POST'])
@register_breadcrumb('Upload Item Description', url='/inv/items/description/upload', parent='Inventory', parent_url='/inv')
def upload_item_description():
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
                df = pd.read_csv(save_path, header=0, delimiter=",", names=['item_number', 'description'], skip_blank_lines=True, skipinitialspace=True, engine='python')
                data = df.to_html()
                flash ('Your File has been uploaded successfully!!!', 'success')
                return render_template('inv/update_item_description.html', data=data, file_name=file_name, user=user)
            except Exception as e:
                l_message = f"Error reading CSV File : {e}"
                flash (l_message, 'error')
                return render_template('inv/upload_item_description.html', data=None, file_name = file_name, user=user)
        else:
            l_message = f"Other error"
            flash (l_message, 'error')
    return render_template('inv/upload_item_description.html', user=user)

@inv_bp.route('/items/update/description/<file_name>', methods=['GET', 'POST'])
@register_breadcrumb('Update Item Description', url='/inv/items/update/description', parent='Inventory', parent_url='/inv')
def update_item_description(file_name):
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    file_path = get_file_path(file_name)
    print(file_path)
    try:   
        df = pd.read_csv(file_path, header=0, delimiter=",", names=['item_number', 'description'], skip_blank_lines=True, skipinitialspace=True, engine='python')

        print('DF', df)
        bind = []
        for row in df.iloc():
            bind.append({'item_number' : str(row[0]), 'description' : row[1]})
        conn = get_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("""DELETE FROM XXJWY.XXJWY_ITEM_DESCRIPTION_STAGE""")
            conn.commit()
            cursor.executemany("""INSERT INTO XXJWY.XXJWY_ITEM_DESCRIPTION_STAGE VALUES (:item_number, :description)""", bind)
            conn.commit()
            errbuff = cursor.var(str)
            retcode = cursor.var(int)
            cursor.callproc('XXJWY_INV_UPDATE_DESCRIPTION.MAIN', [errbuff, retcode])
            cursor.close()
        flash('Your item description has been updated successfully!!!', 'success')
        data = df.to_html()
        return render_template('inv/update_item_description.html', data=data, file_name=file_name, user=user)
    except Exception as e:
        l_message = f'Error reading CSV file: {e}'
        flash(l_message, 'error')
        return render_template('inv/update_item_description.html', data={}, file_name=file_name, user=user)
    

@inv_bp.route('/items/dim_weight/upload', methods=['GET', 'POST'])
@register_breadcrumb('Upload Item Dim Weight', url='/inv/items/dim_weight/upload', parent='Inventory', parent_url='/inv')
def upload_item_dim_weight():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    if request.method == 'POST':
        if 'file' not in request.files:
            return 'No file form!'
        file = request.files['file']
        if not file or not hasattr(file, 'filename') or file.filename == '':
            l_message = 'No selected file'
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
                df = pd.read_csv(save_path, header=0, delimiter=",", names=['item_number', 'mp_qty', 'unit_weight', 'unit_volume', 'unit_length', 'unit_width', 'unit_height', 'box_label'], skip_blank_lines=True, skipinitialspace=True, engine='python')
                data = df.to_html()
                flash ('Your File has been uploaded successfully!!!', 'success')
                return render_template('inv/update_item_dim_weight.html', data=data, file_name = file_name, user=user)
            except Exception as e:
                l_message = f"Error reading CSV File (1): {e}"
                flash (l_message, 'error')
                return render_template('inv/upload_item_dim_weight.html', data=None, file_name = file_name)
        else:
            l_message = f"Other error"
            flash (l_message, 'error')
    return render_template('inv/upload_item_dim_weight.html')

@inv_bp.route('/items/update/dim_weight/<file_name>', methods=['GET', 'POST'])
@register_breadcrumb('Update Item Dim Weight', url='/inv/items/update/dim_weight', parent='Inventory', parent_url='/inv')
def update_item_dim_weight(file_name):
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    file_path = get_file_path(file_name)
    print(file_path)
    try:   
        df = pd.read_csv(file_path, header=0, delimiter=",", names=['item_number', 'mp_qty', 'unit_weight', 'unit_volume', 'unit_length', 'unit_width', 'unit_height', 'box_label'], skip_blank_lines=True, skipinitialspace=True, engine='python')

        print('DF', df)
        bind = []
        for row in df.iloc():
            bind.append({'item_number' : str(row[0]), 'mp_qty' : row[1], 'unit_weight': row[2], 'unit_volume': row[3], 'unit_length': row[4],  'unit_width': row[5], 'unit_height': row[6], 'box_label': row[7]})
        conn = get_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("""DELETE FROM XXJWY.XXJWY_ITEM_DIM_WEIGHT_STAGE""")
            conn.commit()
            cursor.executemany("""INSERT INTO XXJWY.XXJWY_ITEM_DIM_WEIGHT_STAGE VALUES (:item_number, :mp_qty, :unit_weight, :unit_volume, :unit_length, :unit_width, :unit_height, :box_label )""", bind)
            conn.commit()
            errbuff = cursor.var(str)
            retcode = cursor.var(int)
            cursor.callproc('XXJWY_INV_UPDATE_DIMENSION.MAIN', [errbuff, retcode])
            cursor.close()
        flash('Your items dimensions and weight have been updated successfully!!!', 'success')
        data = df.to_html()
        return render_template('inv/update_item_dim_weight.html', data=data, file_name=file_name)
    except Exception as e:
        l_message = f'Error reading CSV file (2): {e}'
        flash(l_message, 'error')
        return render_template('inv/update_item_dim_weight.html', data=None, file_name=file_name)