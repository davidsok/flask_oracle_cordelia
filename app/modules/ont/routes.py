from pickletools import read_uint1
from flask import render_template, redirect, url_for, flash, session, request
from app.modules.inv.forms import ItemInquiryForm, ItemInquiryForm2
from app.modules.ont.forms import AddAllowanceForm, AddItemLEDExpenseForm, EditAllowanceForm, EditItemLEDExpenseForm
from utils.breadcrumbs import register_breadcrumb
from ...modules.ont import ont_bp
from db import *
import os
import pandas as pd
from flask_cors import CORS
from utils.func import *
from datetime import datetime, date
from werkzeug.utils import secure_filename

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

@ont_bp.route('/weekly_advertisement')
@register_breadcrumb('Weekly Advertisement', url='/ont/advertisement', parent='Order Management', parent_url='/ont')
def weekly_advertisement():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    user_access = get_module_access_by_module_user(module_name='OM', user_id=session['user_id'])
    weekly_ads = get_weekly_ad()
    return render_template('ont/weekly_advertisement.html', user_access = user_access, user=user, weekly_ads=weekly_ads)

@ont_bp.route('/ad/upload', methods=['GET', 'POST'])
@register_breadcrumb('Upload Weekly Ad', url='/ont/ad/upload', parent='Order Management', parent_url='/ont')
def upload_weekly_ad():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    
    user = get_user_by_id(session['user_id'])
    
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file part in the request', 'error')
            return render_template('ont/upload_weekly_ad.html', user=user)
        
        file = request.files['file']
        
        if not file or file.filename == '':
            flash('No file selected', 'error')
            return render_template('ont/upload_weekly_ad.html', user=user)
        
        if not allowed_file(file.filename):
            flash('Incorrect file type. Please upload a CSV file', 'error')
            return render_template('ont/upload_weekly_ad.html', user=user)
        
        filename = secure_filename(file.filename)
        save_path = os.path.join(Config.UPLOAD_FOLDER, filename)
        
        try:
            # Ensure upload directory exists
            os.makedirs(Config.UPLOAD_FOLDER, exist_ok=True)
            
            # Save the uploaded file
            file.save(save_path)
            
            try:
                # Process the CSV file
                df = pd.read_csv(
                    save_path,
                    header=0,
                    delimiter=",",
                    names=['item_number', 'customer_number', 'customer_name',
                          'ad_amount', 'date_from', 'date_to'],
                    na_filter=False,
                    skip_blank_lines=True,
                    skipinitialspace=True,
                    engine='python'
                )
                
                # Convert date columns if they exist
                for date_col in ['date_from', 'date_to']:
                    if date_col in df.columns:
                        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                
                data = df.to_html()
                file_name = os.path.splitext(filename)[0]
                flash('File uploaded and processed successfully!', 'success')
                
                # Clean up - remove temporary file
                try:
                    os.remove(save_path)
                except Exception as e:
                    print(f"Warning: Could not remove temporary file: {e}")
                
                return render_template('ont/update_weekly_ad.html',
                                    data=data,
                                    file_name=file_name,
                                    user=user)
            
            except PermissionError:
                flash('Error: File is locked by another program. Please close it and try again.', 'error')
            except pd.errors.EmptyDataError:
                flash('Error: The CSV file is empty or not properly formatted', 'error')
            except Exception as e:
                flash(f'Error processing CSV file: {str(e)}', 'error')
            
            # Clean up if CSV processing failed
            try:
                if os.path.exists(save_path):
                    os.remove(save_path)
            except Exception as e:
                print(f"Warning: Could not clean up temporary file: {e}")
            
            return render_template('ont/upload_weekly_ad.html', user=user)
        
        except PermissionError:
            flash('Error: Cannot save file. It may be open in another program or you do not have permission to open the file.', 'error')
        except Exception as e:
            flash(f'Error saving file: {str(e)}', 'error')
        
        return render_template('ont/upload_weekly_ad.html', user=user)
    
    return render_template('ont/upload_weekly_ad.html', user=user)

@ont_bp.route('/ad/update/<file_name>', methods=['GET', 'POST'])
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

@ont_bp.route('/allowance')
@register_breadcrumb('Customer Allowance', url='/ont/allowance', parent='Order Management', parent_url='/ont')
def customer_allowance():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    user_access = get_module_access_by_module_user(module_name='OM', user_id=session['user_id'])
    allowances = get_customer_allowance()
    return render_template('ont/customer_allowance.html', user_access = user_access, user=user, allowances=allowances)

@ont_bp.route('/allowance/upload', methods=['GET', 'POST'])
@register_breadcrumb('Upload Customer Allowance', url='/ont/allowance/upload', parent='Order Management', parent_url='/ont')
def upload_customer_allowance():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    
    user = get_user_by_id(session['user_id'])
    
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file form!', 'error')
            return render_template('ont/upload_customer_allowance.html', user=user)
            
        file = request.files['file']
        
        if not file or file.filename == '':
            flash('No file selected', 'error')
            return render_template('ont/upload_customer_allowance.html', user=user)
        
        if not allowed_file(file.filename):
            flash('Incorrect File Type, please select your CSV file', 'error')
            return render_template('ont/upload_customer_allowance.html', user=user)
        
        # Create upload folder if it doesn't exist
        os.makedirs(Config.UPLOAD_FOLDER, exist_ok=True)
        
        # Generate a unique filename to avoid conflicts
        filename = secure_filename(file.filename)
        save_path = os.path.join(Config.UPLOAD_FOLDER, filename)
        
        try:
            # First attempt to save the file
            file.save(save_path)
            
            # Then attempt to read it
            try:
                df = pd.read_csv(
                    save_path, 
                    header=0, 
                    delimiter=",", 
                    names=['Account Name', 'Account Number', 'Allowance', 'Commission'],
                    na_filter=False,
                    skip_blank_lines=True,
                    skipinitialspace=True,
                    engine='python'
                )
                
                data = df.to_html()
                flash('Your File has been uploaded successfully!', 'success')
                
                # Clean up - remove the file after processing
                try:
                    os.remove(save_path)
                except:
                    pass  # Don't fail if we can't delete
                
                return render_template(
                    'ont/update_customer_allowance.html', 
                    data=data, 
                    file_name=os.path.splitext(filename)[0], 
                    user=user
                )
                
            except PermissionError as e:
                flash(f"Error: The file is open in another program. Please close it and try again. Details: {str(e)}", 'error')
            except pd.errors.EmptyDataError:
                flash("Error: The file is empty or not properly formatted", 'error')
            except Exception as e:
                flash(f"Error processing CSV file: {str(e)}", 'error')
                
            # If we got here, there was an error - try to clean up
            try:
                if os.path.exists(save_path):
                    os.remove(save_path)
            except:
                pass
                
        except PermissionError as e:
            flash(f"Error: Cannot save file - it may be open in another program. Details: {str(e)}", 'error')
        except Exception as e:
            flash(f"Error saving file: {str(e)}", 'error')
            
        return render_template('ont/upload_customer_allowance.html', user=user)
    
    return render_template('ont/upload_customer_allowance.html', user=user)

@ont_bp.route('/allowance/update/<file_name>', methods=['GET', 'POST'])
@register_breadcrumb('Update Customer Allowance', url='/ont/allowance/update', parent='Order Management', parent_url='/ont')
def update_customer_allowance(file_name):
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    file_path = get_file_path(file_name)
    print(file_path)
    try:   
        df = pd.read_csv(file_path, header=0, delimiter=",", names=['account_name', 'account_number', 'allowance', 'commission'], na_filter=False, skip_blank_lines=True, skipinitialspace=True, engine='python')
        bind = []
        for row in df.itertuples(index=False):
            customer = get_customer_id_by_number(row.account_number)
            if not customer:
                l_message = f"Customer not found {row.account_number}"
                flash (l_message, 'error')
            print(customer)
            bind.append({
                'customer_id': customer['customer_id'],
                'allowance': row.allowance,
                'commission': row.commission,
                'created_by': session['user_id'],
                'last_updated_by': session['user_id']
            })
        print('bind', bind)
        conn = get_connection()
        if conn:
            cursor = conn.cursor()
            cursor.executemany("""INSERT INTO XXJWY.XXCDL_CUSTOMER_ALLOWANCE_COMM_RPT ( CUSTOMER_ID, ALLOWANCE, COMMISSION, CREATED_BY, LAST_UPDATED_BY) VALUES (:customer_id, :allowance, :commission, :created_by, :last_updated_by)""", bind)
            conn.commit()
            cursor.close()
            flash('Your Customer Allowances and Commission have been successfully added!', 'success')
        return redirect(url_for('ont.customer_allowance'))
    except Exception as e:
        l_message = f'Error reading CSV file: {e}'
        flash(l_message, 'error')
        return render_template('ont/update_customer_allowance.html', data={}, file_name=file_name, user=user)
    
@ont_bp.route('/allowance/new', methods=['GET', 'POST'])
@register_breadcrumb('New Allowance', url='/ont/allowance/new', parent='Customer Allowance', parent_url='/ont/allowance')
def new_allowance():
    form = AddAllowanceForm()
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    customers = get_customer_no_allowance()
    if customers:
        form.customer.choices = list((customer['customer_id'], f" {customer['customer_number']} - {customer['customer_name']}") for customer in customers)
    else:
        form.customer.choices = []
    if form.validate_on_submit():
        conn = get_connection()
        print(conn)
        if conn:
            bind = {
                'customer': int(form.customer.data),
                'allowance': float(form.allowance.data)/100,
                'commission': float(form.commission.data)/100,
                'created_by': session['user_id'],
                'last_updated_by': session['user_id']
            }
            print('BIND :', bind)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO XXJWY.XXCDL_CUSTOMER_ALLOWANCE_COMM_RPT
                (CUSTOMER_ID, ALLOWANCE, COMMISSION, CREATED_BY, LAST_UPDATED_BY)           
                VALUES(:customer, :allowance, :commission, :created_by, :last_updated_by)
            """, bind)
            conn.commit()
            cursor.close()
            conn.close()
            flash(f'New customer allowance has been added!', 'success')
            return redirect(url_for('ont.customer_allowance'))
    else:
        print("Form errors:", form.errors)
    return render_template('ont/add_customer_allowance.html', form=form, user=user)

@ont_bp.route('/allowance/edit/<int:customer_id>', methods=['GET', 'POST'])
@register_breadcrumb('Edit Allowance', url=lambda customer_id: f'/ont/allowance/edit/{customer_id}', parent='Customer Allowance', parent_url='/ont/allowance')
def edit_allowance(customer_id):
    form = AddAllowanceForm()
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    customer_allowance = get_customer_allowance_by_customer(customer_id=customer_id)
    if not customer_allowance:
        flash('Cannot find allowance for this customer', 'danger')
        return redirect(url_for('ont.allowance'))
    if request.method == 'POST':
        form = EditAllowanceForm()
    else:
        form = EditAllowanceForm(data=customer_allowance)
    if form.validate_on_submit():
        conn = get_connection()
        print(conn)
        if conn:
            bind = {
                'customer_id': customer_id,
                'allowance': float(form.allowance.data)/100,
                'commission': float(form.commission.data)/100,
                'last_updated_by': session['user_id']
            }
            print('BIND :', bind)
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE XXJWY.XXCDL_CUSTOMER_ALLOWANCE_COMM_RPT
                SET ALLOWANCE = :allowance, COMMISSION = :commission, LAST_UPDATED_BY = :last_updated_by         
                WHERE CUSTOMER_ID = :customer_id
            """, bind)
            conn.commit()
            cursor.close()
            conn.close()
            flash(f'Customer allowance has been updated!', 'success')
            return redirect(url_for('ont.customer_allowance'))
    else:
        print("Form errors:", form.errors)
    return render_template('ont/edit_customer_allowance.html', form=form, user=user, allowance=customer_allowance)

@ont_bp.route('/allowance/delete/<int:customer_id>', methods=['GET', 'POST'])
@register_breadcrumb('Delete Allowance', url=lambda customer_id: f'/ont/allowance/delete/{customer_id}', parent='Customer Allowance', parent_url='/ont/allowance')
def delete_allowance(customer_id):
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    customer_allowance = get_customer_allowance_by_customer(customer_id=customer_id)
    if not customer_allowance:
        flash('Cannot find allowance for this customer', 'danger')
        return redirect(url_for('ont.allowance'))
    conn = get_connection()
    print(conn)
    if conn:
        bind = {
            'customer_id': customer_id
        }
        print('BIND :', bind)
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM XXJWY.XXCDL_CUSTOMER_ALLOWANCE_COMM_RPT WHERE CUSTOMER_ID = :customer_id
        """, bind)
        conn.commit()
        cursor.close()
        conn.close()
        flash(f'Customer allowance has been deleted!', 'success')
    return redirect(url_for('ont.customer_allowance'))

@ont_bp.route('/item_led_expense')
@register_breadcrumb('Item LED Expense', url='/ont/item_led_expense', parent='Order Management', parent_url='/ont')
def item_led_expense():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    user_access = get_module_access_by_module_user(module_name='OM', user_id=session['user_id'])
    led_expenses = get_item_led_expense()
    return render_template('ont/item_led_expense.html', user_access = user_access, user=user, led_expenses=led_expenses)

@ont_bp.route('/item_led_expense/new', methods=['GET', 'POST'])
@register_breadcrumb('New Allowance', url='/ont/item_led_expense/new', parent='Item LED Expense', parent_url='/ont/item_led_expense')
def new_item_led_expense():
    form = AddItemLEDExpenseForm()
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    items = get_item_not_in_led_expense()
    if items:
        form.item.choices = list((item['inventory_item_id'], item['item_number']) for item in items)
    else:
        form.item.choices = []
    if request.method == 'POST':
        if form.validate_on_submit():
            conn = get_connection()
            print(conn)
            if conn:
                bind = {
                    'inventory_item_id': int(form.item.data),
                    'led_expense': float(form.led_expense.data)/100,
                    'created_by': session['user_id'],
                    'last_updated_by': session['user_id']
                }
                print('BIND :', bind)
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO XXJWY.XXCDL_ITEM_LED_EXPENSE
                    (INVENTORY_ITEM_ID, LED_EXPENSE, CREATED_BY, LAST_UPDATED_BY)           
                    VALUES(:inventory_item_id, :led_expense, :created_by, :last_updated_by)
                """, bind)
                conn.commit()
                cursor.close()
                conn.close()
                flash(f'New Item LED Expense has been added!', 'success')
                return redirect(url_for('ont.item_led_expense'))
        else:
            print("Form errors:", form.errors)
    return render_template('ont/add_item_led_expense.html', form=form, user=user)

@ont_bp.route('/item_led_expense/edit/<int:inventory_item_id>', methods=['GET', 'POST'])
@register_breadcrumb('Edit Item LED Expense', url=lambda inventory_item_id: f'/ont/item_led_expense/edit/{inventory_item_id}', parent='Item LED Expense', parent_url='/ont/item_led_expense')
def edit_item_led_expense(inventory_item_id):
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    item_led_expense = get_item_led_expense_by_item(item=inventory_item_id)
    if not item_led_expense:
        flash('Cannot find LED Expense for this Item', 'danger')
        return redirect(url_for('ont.item_led_expense'))
    if request.method == 'POST':
        form = EditItemLEDExpenseForm()
    else:
        form = EditItemLEDExpenseForm(data=item_led_expense)
    if request.method == 'POST':
        if form.validate_on_submit():
            conn = get_connection()
            print(conn)
            if conn:
                bind = {
                    'inventory_item_id': inventory_item_id,
                    'led_expense': float(form.led_expense.data)/100,
                    'last_updated_by': session['user_id']
                }
                print('BIND :', bind)
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE XXJWY.XXCDL_ITEM_LED_EXPENSE
                    SET led_expense = :led_expense, LAST_UPDATED_BY = :last_updated_by         
                    WHERE INVENTORY_ITEM_ID = :inventory_item_id
                """, bind)
                conn.commit()
                cursor.close()
                conn.close()
                flash(f'Item LED Expense has been updated!', 'success')
                return redirect(url_for('ont.item_led_expense'))
        else:
            print("Form errors:", form.errors)
    return render_template('ont/edit_item_led_expense.html', form=form, user=user, item_led_expense=item_led_expense)

@ont_bp.route('/item_led_expense/delete/<int:inventory_item_id>', methods=['GET', 'POST'])
@register_breadcrumb('Delete Allowance', url=lambda inventory_item_id: f'/ont/item_led_expense/delete/{inventory_item_id}', parent='Item LED Expense', parent_url='/ont/item_led_expense')
def delete_item_led_expense(inventory_item_id):
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    item_led_expense = get_item_led_expense_by_item(item=inventory_item_id)
    if not item_led_expense:
        flash('Cannot find LED Expense for this item!', 'danger')
        return redirect(url_for('ont.item_led_expense'))
    conn = get_connection()
    print(conn)
    if conn:
        bind = {
            'inventory_item_id': inventory_item_id
        }
        print('BIND :', bind)
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM XXJWY.XXCDL_ITEM_LED_EXPENSE WHERE INVENTORY_ITEM_ID = :inventory_item_id
        """, bind)
        conn.commit()
        cursor.close()
        conn.close()
        flash(f'Item LED Expense has been deleted!', 'success')
    return redirect(url_for('ont.item_led_expense'))

@ont_bp.route('/item_comp_shop')
@register_breadcrumb('Item Comp Shop', url='/ont/item_comp_shop', parent='Order Management', parent_url='/ont')
def item_comp_shop():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    user_access = get_module_access_by_module_user(module_name='OM', user_id=session['user_id'])
    item_comp_shops = get_item_comp_shop()
    return render_template('ont/item_comp_shop.html', user_access = user_access, user=user, item_comp_shops=item_comp_shops)

@ont_bp.route('/item_comp_shop/upload', methods=['GET', 'POST'])
@register_breadcrumb('Upload Item Comp Shop', url='/ont/item_comp_shop/upload', parent='Order Management', parent_url='/ont')
def upload_item_comp_shop():
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
                df = pd.read_csv(save_path, header=0, delimiter=",", names=['item_number', 'our_retail', 'brand', 'price'], na_filter=False, skip_blank_lines=True, skipinitialspace=True, engine='python')
                data = df.to_html()
                flash ('Your File has been uploaded successfully!', 'success')
                return render_template('ont/update_item_comp_shop.html', data=data, file_name=file_name, user=user)
            except Exception as e:
                l_message = f"Error reading CSV File : {e}"
                flash (l_message, 'error')
                return render_template('ont/update_item_comp_shop.html', data=None, file_name=file_name, user=user)
        else:
            l_message = f"Other error"
            flash (l_message, 'error')
    return render_template('ont/upload_item_comp_shop.html', user=user)

@ont_bp.route('/item_comp_shop/update/<file_name>', methods=['GET', 'POST'])
@register_breadcrumb('Update Item Comp Shop', url='/ont/item_comp_shop/update', parent='Order Management', parent_url='/ont')
def update_item_comp_shop(file_name):
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    file_path = get_file_path(file_name)
    print(file_path)
    try:   
        df = pd.read_csv(file_path, header=0, delimiter=",",  names=['item_number', 'our_retail', 'brand', 'price'], na_filter=False, skip_blank_lines=True, skipinitialspace=True, engine='python')

        bind = []
        for row in df.itertuples(index=False):
            item_id = get_item_id_from_number(item_number=row.item_number)
            bind.append({
                'inventory_item_id': item_id['item_number'],
                'our_retail': str(row.our_retail),
                'brand': str(row.brand),
                'price': float(row.price),
                'created_by': session['user_id'],
                'last_updated_by': session['user_id']
            })
            print('BIND', bind)
            
        conn = get_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("""DELETE FROM XXJWY.XXCDL_ITEM_COMP_SHOP""")
            conn.commit()
            cursor.executemany("""INSERT INTO XXJWY.XXCDL_ITEM_COMP_SHOP ( INVENTORY_ITEM_ID, OUR_RETAIL, BRAND, PRICE, CREATED_BY, LAST_UPDATED_BY) VALUES (:inventory_item_id, :our_retail, :brand, :price, :created_by, :last_updated_by)""", bind)
            conn.commit()
            cursor.close()
            flash('Your Item Comp Shop has been updated successfully!', 'success')
        return redirect(url_for('ont.item_comp_shop'))
    except Exception as e:
        l_message = f'Error reading CSV file: {e}'
        flash(l_message, 'error')
        return render_template('ont/update_item_comp_shop.html', data={}, file_name=file_name, user=user)