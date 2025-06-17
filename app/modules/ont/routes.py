from pickletools import read_uint1
from re import I
from flask import render_template, redirect, url_for, flash, session, request
from app.modules.inv.forms import ItemInquiryForm, ItemInquiryForm2
from app.modules.ont.forms import AddAllowanceForm, AddCustomerItemCreditForm, AddItemCompShopForm, AddItemLEDExpenseForm, EditAllowanceForm, EditCustomerItemCreditForm, EditItemCompShopForm, EditItemLEDExpenseForm
from utils.breadcrumbs import register_breadcrumb
from ...modules.ont import ont_bp
from db import *
import os
import pandas as pd
from flask_cors import CORS
from utils.func import *
from datetime import datetime, date
from werkzeug.utils import secure_filename
from utils.func import *

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
@register_breadcrumb('Weekly Advertisement', url='/ont/weekly_advertisement', parent='Order Management', parent_url='/ont')
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
                df = pd.read_csv(save_path, header=0, delimiter=",",
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
            item = get_item_id_from_number(item_number=row.item_number)
            if not item:
                flash(f'Item {row.item_number} not found!', 'warning')
                return redirect(url_for('ont.weekly_advertisement'))
            customer = get_customer_id_by_number(customer_number=row.customer_number)
            if not customer:
                flash(f'Item {row.item_number} not found!', 'warning')
                return redirect(url_for('ont.weekly_advertisement'))
            existing_item = get_item_customer_weekly_ad(inventory_item_id=item['inventory_item_id'], customer_id=customer['customer_id'], date_from=row.date_from.to_pydatetime() if pd.notnull(row.date_from) else None)
            if existing_item:
                flash(f'Item {row.item_number} exists in weekly ad table for this customer {row.customer_name}!', 'warning')
                return redirect(url_for('ont.weekly_advertisement'))
            bind.append({
                'inventory_item_id': item['inventory_item_id'],
                'cust_account_id': customer['customer_id'],
                'weekly_ad_amount': float(row.ad_amount),
                'date_from': row.date_from.to_pydatetime() if pd.notnull(row.date_from) else None,
                'date_to': row.date_to.to_pydatetime() if pd.notnull(row.date_to) else None,
                'created_by': session['user_id'],
                'last_updated_by': session['user_id']
            })

        table_name = "XXJWY.XXCDL_WEEKLY_AD"

        insert_result = insert_records(table_name=table_name, data=bind, delete_existing=False)

        if insert_result['status'] == 'success':
            flash('Your Item Weekly Ad Spent has been successfully uploaded!', 'success')
        else:
            flash(f"Insert failed: {insert_result['message']}", 'danger')
        return redirect(url_for('ont.weekly_advertisement'))
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
                return render_template('ont/update_customer_allowance.html', data=data, file_name=os.path.splitext(filename)[0], user=user)
                
            except PermissionError as e:
                flash(f"Error: The file is open in another program. Please close it and try again. Details: {str(e)}", 'error')
            except pd.errors.EmptyDataError:
                flash("Error: The file is empty or not properly formatted", 'error')
            except Exception as e:
                flash(f"Error processing CSV file: {str(e)}", 'error')
                
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
        # df['allowance'] = df['allowance'].apply(lambda x: float(x.replace('%','').strip())/100 if '%' in str(x) else x)
        # df['commission'] = df['commission'].apply(lambda x: float(x.replace('%','').strip())/100 if '%' in str(x) else x)
        bind = []
        for row in df.itertuples(index=False):
            customer = get_customer_id_by_number(row.account_number)
            if not customer:
                l_message = f"Customer not found {row.account_number}"
                flash (l_message, 'error')
            # allowance = percentage_converter(row.allowance)
            # commission = percentage_converter(row.commission)
            bind.append({
                'customer_id': customer['customer_id'],
                'allowance': row.allowance,
                'commission': row.commission,
                'created_by': session['user_id'],
                'last_updated_by': session['user_id']
            })
        

        table_name = "XXJWY.XXCDL_CUSTOMER_ALLOWANCE_COMM_RPT"

        insert_result = insert_records(table_name=table_name, data=bind, delete_existing=True)

        if insert_result['status'] == 'success':
            flash('Customer Allowances and Commission have been successfully!', 'success')
        else:
            flash(f"Insert failed: {insert_result['message']}", 'danger')
        return redirect(url_for('ont.customer_allowance'))
    except Exception as e:
        l_message = f'Error reading CSV file: {e}'
        flash(l_message, 'error')
        return render_template('ont/update_customer_allowance.html', data={}, file_name=file_name, user=user)
    
@ont_bp.route('/allowance/new', methods=['GET', 'POST'])
@register_breadcrumb('New Customer Allowance', url='/ont/allowance/new', parent='Customer Allowance', parent_url='/ont/allowance')
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
        bind = {
                'customer_id': int(form.customer.data),
                'allowance': float(form.allowance.data)/100,
                'commission': float(form.commission.data)/100,
                'created_by': session['user_id'],
                'last_updated_by': session['user_id']
        }
        table_name="XXJWY.XXCDL_CUSTOMER_ALLOWANCE_COMM_RPT"

        result = insert_records(table_name=table_name, data=bind, delete_existing=False)

        if result['status'] == 'success':
            flash('New Customer Allowance has been added!', 'success')
            return redirect(url_for('ont.customer_allowance'))
        else:
            flash(f"Insert failed: {result['message']}", 'error')
            return redirect(url_for('ont.customer_allowance'))
    else:
        print("Form errors:", form.errors)
    return render_template('ont/add_customer_allowance.html', form=form, user=user)

@ont_bp.route('/allowance/edit/<int:customer_id>', methods=['GET', 'POST'])
@register_breadcrumb('Edit Customer Allowance', url=lambda customer_id: f'/ont/allowance/edit/{customer_id}', parent='Customer Allowance', parent_url='/ont/allowance')
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
        bind = {
            'customer_id': customer_id,
            'allowance': float(form.allowance.data)/100,
            'commission': float(form.commission.data)/100,
            'last_updated_by': session['user_id'],
            'last_update_date': datetime.now()
        }
        table_name="XXJWY.XXCDL_CUSTOMER_ALLOWANCE_COMM_RPT"
        set_fields = ['customer_id', 'allowance', 'commission', 'last_updated_by', 'last_update_date']
        where_cluse = 'customer_id = :customer_id'
        result = update_records(table=table_name, set_fields=set_fields, where_clause=where_cluse, data=bind)

        if result['status'] == 'success':
            flash(f"Customer Allowance for {customer_allowance['customer_name']} has been updated!", 'success')
            return redirect(url_for('ont.customer_allowance'))
        else:
            flash(f"Insert failed: {result['message']}", 'error')
            return redirect(url_for('ont.customer_allowance'))    
    else:
        print("Form errors:", form.errors)
    return render_template('ont/edit_customer_allowance.html', form=form, user=user, allowance=customer_allowance)

@ont_bp.route('/allowance/delete/<int:customer_id>', methods=['GET', 'POST'])
@register_breadcrumb('Delete Customer Allowance', url=lambda customer_id: f'/ont/allowance/delete/{customer_id}', parent='Customer Allowance', parent_url='/ont/allowance')
def delete_allowance(customer_id):
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    customer_allowance = get_customer_allowance_by_customer(customer_id=customer_id)
    if not customer_allowance:
        flash('Cannot find allowance for this customer', 'danger')
        return redirect(url_for('ont.allowance'))
    bind = {
        'customer_id': customer_id
    }
    result = delete_records(table='XXJWY.XXCDL_CUSTOMER_ALLOWANCE_COMM_RPT', where_clause='customer_id = :customer_id', data=bind)

    if result['status'] == 'success':
        flash('Customer Allowance has been deleted!', 'success')
    else:
        flash(f"Delete failed: {result['message']}", 'danger')

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
@register_breadcrumb('New Iem LED Expense', url='/ont/item_led_expense/new', parent='Item LED Expense', parent_url='/ont/item_led_expense')
def new_item_led_expense():
    form = AddItemLEDExpenseForm()
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))

    user = get_user_by_id(session['user_id'])
    items = get_item_not_in_led_expense()
    form.item.choices = [(item['inventory_item_id'], item['item_number']) for item in items] if items else []

    if request.method == 'POST' and form.validate_on_submit():
        bind_data = {
            'inventory_item_id': int(form.item.data),
            'led_expense': float(form.led_expense.data) / 100,
            'created_by': session['user_id'],
            'last_updated_by': session['user_id']
        }

        result = insert_records(
            table_name="XXJWY.XXCDL_ITEM_LED_EXPENSE",
            data=bind_data
        )

        if result['status'] == 'success':
            flash('New Item LED Expense has been added!', 'success')
            return redirect(url_for('ont.item_led_expense'))
        else:
            flash(f"Insert failed: {result['message']}", 'error')
    elif request.method == 'POST':
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

    form = EditItemLEDExpenseForm(data=item_led_expense)

    if request.method == 'POST' and form.validate_on_submit():
        bind_data = {
            'inventory_item_id': inventory_item_id,
            'led_expense': float(form.led_expense.data) / 100,
            'last_updated_by': session['user_id']
        }

        result = update_records(
            table='XXJWY.XXCDL_ITEM_LED_EXPENSE',
            set_fields=['led_expense', 'last_updated_by'],
            where_clause='inventory_item_id = :inventory_item_id', 
            data=bind_data
            )

        if result['status'] == 'success':
            flash('Item LED Expense has been updated!', 'success')
            return redirect(url_for('ont.item_led_expense'))
        else:
            flash(f"Update failed: {result['message']}", 'danger')

    elif request.method == 'POST':
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

    result = delete_records(
        table='XXJWY.XXCDL_ITEM_LED_EXPENSE',
        where_clause='inventory_item_id = :inventory_item_id',
        data={'inventory_item_id': inventory_item_id}
    )

    if result['status'] == 'success':
        flash('Item LED Expense has been deleted!', 'success')
    else:
        flash(f"Delete failed: {result['message']}", 'danger')

    return redirect(url_for('ont.item_led_expense'))

@ont_bp.route('/item_led_expense/upload', methods=['GET', 'POST'])
@register_breadcrumb('Upload Item LED Expense', url='/ont/item_led_expense/upload', parent='Order Management', parent_url='/ont')
def upload_item_led_expense():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    
    user = get_user_by_id(session['user_id'])
    
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file form!', 'error')
            return render_template('ont/upload_item_led_expense.html', user=user)
            
        file = request.files['file']
        
        if not file or file.filename == '':
            flash('No file selected', 'error')
            return render_template('ont/upload_item_led_expense.html', user=user)
        
        if not allowed_file(file.filename):
            flash('Incorrect File Type, please select your CSV file', 'error')
            return render_template('ont/upload_item_led_expense.html', user=user)
        
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
                    names=['item_number', 'led_expense'],
                    na_filter=False,
                    skip_blank_lines=True,
                    skipinitialspace=True,
                    engine='python'
                )
                
                data = df.to_html()
                flash('Your File has been uploaded successfully!', 'success')
                
                return render_template('ont/update_item_led_expense.html', data=data, file_name=os.path.splitext(filename)[0], user=user)
                
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
            
        return render_template('ont/upload_item_led_expense.html', user=user)
    
    return render_template('ont/upload_item_led_expense.html', user=user)

@ont_bp.route('/item_led_expense/update/<file_name>', methods=['GET', 'POST'])
@register_breadcrumb('Update Item LED Expense', url='/ont/item_led_expense/update', parent='Order Management', parent_url='/ont')
def update_item_led_expense(file_name):
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))

    user = get_user_by_id(session['user_id'])
    file_path = get_file_path(file_name)
    print(file_path)

    try:   
        df = pd.read_csv(
            file_path,
            header=0,
            delimiter=",",
            names=['item_number', 'led_expense'],
            na_filter=False,
            skip_blank_lines=True,
            skipinitialspace=True,
            engine='python'
        )

        bind = []
        for row in df.itertuples(index=False):
            item = get_item_id_from_number(item_number=row.item_number)
            if not item:
                flash(f"Item not found: {row.item_number}", 'error')
                continue  # Skip if item not found

            bind.append({
                'inventory_item_id': item['inventory_item_id'],
                'led_expense': row.led_expense,
                'created_by': session['user_id'],
                'last_updated_by': session['user_id']
            })

        table_name = "XXJWY.XXCDL_ITEM_LED_EXPENSE"

        insert_result = insert_records(table_name=table_name,data=bind,delete_existing=True)
        
        if insert_result['status'] == 'success':
            flash('Your Item LED Expense has been successfully uploaded!', 'success')
        else:
            flash(f"Insert failed: {insert_result['message']}", 'danger')

        return redirect(url_for('ont.item_led_expense'))

    except Exception as e:
        flash(f'Error reading CSV file: {e}', 'error')
        return render_template('ont/update_item_led_expense.html', data={}, file_name=file_name, user=user)

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
        # Read CSV
        df = pd.read_csv(
            file_path,
            header=0,
            delimiter=",",
            names=['item_number', 'our_retail', 'brand', 'price'],
            na_filter=False,
            skip_blank_lines=True,
            skipinitialspace=True,
            engine='python'
        )

        bind = []
        for row in df.itertuples(index=False):
            item = get_item_id_from_number(item_number=row.item_number)
            if not item:
                flash(f"Item not found: {row.item_number}", 'error')
                continue
            print('ROW.PRICE', row.price, row.item_number)
            if row.price == 0 or row.price == '':
                row_price = 0
            else:
                row_price = float(row.price)        
            if row.our_retail == 0 or row.our_retail == '':
                row_our_retail = 0
            else:
                row_our_retail = float(row.our_retail)
            bind.append({
                'inventory_item_id': item['inventory_item_id'],
                'our_retail': row_our_retail,
                'brand': str(row.brand),
                'price': row_price,
                'created_by': session['user_id'],
                'last_updated_by': session['user_id']
            })

        table_name = "XXJWY.XXCDL_ITEM_COMP_SHOP"

        # Insert new data
        insert_result = insert_records(table_name=table_name, delete_existing=True, data=bind)

        if insert_result['status'] == 'success':
            flash('Your Item Comp Shop has been updated successfully!', 'success')
        else:
            flash(f"Insert failed: {insert_result['message']}", 'danger')

        return redirect(url_for('ont.item_comp_shop'))

    except Exception as e:
        flash(f'Error reading CSV file: {e}', 'error')
        return render_template('ont/update_item_comp_shop.html', data={}, file_name=file_name, user=user)


@ont_bp.route('/item_comp_shop/new', methods=['GET', 'POST'])
@register_breadcrumb('New Item Comp Shop', url='/ont/item_comp_shop/new', parent='Item Comp Shop', parent_url='/ont/item_comp_shop')
def new_item_comp_shop():
    form = AddItemCompShopForm()
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    items = get_item_numbers()
    if items:
        form.item.choices = list((item['inventory_item_id'], item['item_number']) for item in items)
    else:
        form.item.choices = []
    if request.method == 'POST':
        if form.validate_on_submit():
            bind = {
                'inventory_item_id': int(form.item.data),
                'our_retail': float(form.our_retail.data),
                'brand': str(form.brand.data),
                'price': float(form.price.data),
                'created_by': session['user_id'],
                'last_updated_by': session['user_id']
            }
            table_name = "XXJWY.XXCDL_ITEM_COMP_SHOP"

            # Insert new data
            insert_result = insert_records(table_name=table_name, data=bind)
            if insert_result['status'] == 'success':
                flash('Your Item Comp Shop has been updated successfully!', 'success')
            else:
                flash(f"Insert failed: {insert_result['message']}", 'danger')
            return redirect(url_for('ont.item_comp_shop'))
        else:
            print("Form errors:", form.errors)
    return render_template('ont/add_item_comp_shop.html', form=form, user=user)

@ont_bp.route('/item_comp_shop/edit/<int:inventory_item_id>', methods=['GET', 'POST'])
@register_breadcrumb('Edit Item Comp Shop', url=lambda inventory_item_id: f'/ont/item_comp_shop/edit/{inventory_item_id}', parent='Item Comp Shop', parent_url='/ont/item_comp_shop')
def edit_item_comp_shop(inventory_item_id):
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    item_comp_shop = get_item_comp_shop_by_item(item=inventory_item_id)
    if not item_comp_shop:
        flash('Cannot find Item Comp Shop for this Item', 'danger')
        return redirect(url_for('ont.item_comp_shop'))
    form = EditItemCompShopForm(data=item_comp_shop)

    if request.method == 'POST' and form.validate_on_submit():
        bind_data = {
            'inventory_item_id': inventory_item_id,
            'our_retail': float(form.our_retail.data),
            'brand': str(form.brand.data),
            'price': float(form.price.data),
            'last_updated_by': session['user_id']
        }

        result = update_records(
            table='XXJWY.XXCDL_ITEM_COMP_SHOP',
            set_fields=['our_retail', 'brand', 'price','last_updated_by'],
            where_clause='inventory_item_id = :inventory_item_id',
            data=bind_data
        )

        if result['status'] == 'success':
            flash('Item Comp Shop has been updated!', 'success')
            return redirect(url_for('ont.item_comp_shop'))
        else:
            flash(f"Update failed: {result['message']}", 'error')
    return render_template('ont/edit_item_comp_shop.html', form=form, user=user, item_comp_shop=item_comp_shop)

@ont_bp.route('/item_led_expense/delete/<int:inventory_item_id>', methods=['GET', 'POST'])
@register_breadcrumb('Delete Allowance', url=lambda inventory_item_id: f'/ont/item_led_expense/delete/{inventory_item_id}', parent='Item LED Expense', parent_url='/ont/item_led_expense')
def delete_item_comp_shop(inventory_item_id):
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

@ont_bp.route('/customer_item_credit')
@register_breadcrumb('Customer Item Credit', url='/ont/customer_item_credit', parent='Order Management', parent_url='/ont')
def customer_item_credit():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    user_access = get_module_access_by_module_user(module_name='OM', user_id=session['user_id'])
    credits = get_customer_item_credit()
    return render_template('ont/customer_item_credit.html', user_access = user_access, user=user, credits=credits)

@ont_bp.route('/customer_item_credit/upload', methods=['GET', 'POST'])
@register_breadcrumb('Upload Customer Item Credit', url='/ont/customer_item_credit/upload', parent='Customer Item Credit', parent_url='/ont/customer_item_credit')
def upload_customer_item_credit():
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
                    names=['customer_number', 'item_number', 'cm_amount'],
                    na_filter=False,
                    skip_blank_lines=True,
                    skipinitialspace=True,
                    engine='python'
                )
                data = df.to_html()
                flash('Your File has been uploaded successfully!', 'success')
                return render_template('ont/update_customer_item_credit.html', data=data, file_name=os.path.splitext(filename)[0], user=user)
                
            except PermissionError as e:
                flash(f"Error: The file is open in another program. Please close it and try again. Details: {str(e)}", 'error')
            except pd.errors.EmptyDataError:
                flash("Error: The file is empty or not properly formatted", 'error')
            except Exception as e:
                flash(f"Error processing CSV file: {str(e)}", 'error')
                
        except PermissionError as e:
            flash(f"Error: Cannot save file - it may be open in another program. Details: {str(e)}", 'error')
        except Exception as e:
            flash(f"Error saving file: {str(e)}", 'error')
            
        return render_template('ont/upload_customer_item_credit.html', user=user)
    
    return render_template('ont/upload_customer_item_credit.html', user=user)

@ont_bp.route('/customer_item_credit/update/<file_name>', methods=['GET', 'POST'])
@register_breadcrumb('Update Customer Item Credit', url='/ont/customer_item_credit/update', parent='Order Management', parent_url='/ont')
def update_customer_item_credit(file_name):
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    file_path = get_file_path(file_name)
    print(file_path)
    try:   
        df = pd.read_csv(file_path, header=0, delimiter=",", names=['customer_number', 'item_number', 'cm_amount'], na_filter=False, skip_blank_lines=True, skipinitialspace=True, engine='python')
        bind = []
        for row in df.itertuples(index=False):
            customer = get_customer_id_by_number(row.customer_number)
            item = get_item_id_from_number(row.item_number)
            if not item:
                l_message = f"Item not found {row.customer_number}"
                flash (l_message, 'error')
                return redirect(url_for('ont.customer_item_credit'))
            if not customer:
                l_message = f"Customer not found {row.customer_number}"
                flash (l_message, 'error')
                return redirect(url_for('ont.customer_item_credit'))
            bind.append({
                'customer_id': customer['customer_id'],
                'inventory_item_id': item['inventory_item_id'],
                'cm': float(row.cm_amount),
                'created_by': session['user_id'],
                'last_updated_by': session['user_id']
            })
        

        table_name = "XXJWY.XXCDL_CUSTOMER_ITEM_CM_RPT"

        insert_result = insert_records(table_name=table_name, data=bind, delete_existing=True)

        if insert_result['status'] == 'success':
            flash('Customer Item Credits have been updated successfully!', 'success')
        else:
            flash(f"Insert failed: {insert_result['message']}", 'danger')
        return redirect(url_for('ont.customer_item_credit'))
    except Exception as e:
        l_message = f'Error reading CSV file: {e}'
        flash(l_message, 'error')
    return render_template('ont/update_customer_item_credit.html', data={}, file_name=file_name, user=user)

@ont_bp.route('/customer_item_credit/edit/<int:customer_id>/<int:inventory_item_id>', methods=['GET', 'POST'])
@register_breadcrumb('Edit Customer Item Credit', url=lambda customer_id, inventory_item_id: f'/ont/customer_item_credit/edit/{customer_id}/{inventory_item_id}', parent='Customer Item Credit', parent_url='/ont/customer_item_credit')
def edit_customer_item_credit(customer_id, inventory_item_id):
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))

    user = get_user_by_id(session['user_id'])
    item_credit = get_customer_item_credit_by_item(customer=customer_id,item=inventory_item_id)
    if not item_credit:
        flash('Cannot find Customer Credit for this Item', 'danger')
        return redirect(url_for('ont.customer_item_credit'))

    form = EditCustomerItemCreditForm(data=item_credit)

    if request.method == 'POST' and form.validate_on_submit():
        bind_data = {
            'inventory_item_id': inventory_item_id,
            'customer_id': customer_id,
            'cm': float(form.cm.data),
            'last_updated_by': session['user_id'],
            'last_update_date': datetime.now()
        }

        result = update_records(
            table='XXJWY.XXCDL_CUSTOMER_ITEM_CM_RPT',
            set_fields=['cm', 'last_updated_by', 'last_update_date'],
            where_clause='inventory_item_id = :inventory_item_id AND customer_id = :customer_id', 
            data=bind_data
            )

        if result['status'] == 'success':
            flash('Customer Item Credit (CM) has been updated!', 'success')
            return redirect(url_for('ont.customer_item_credit'))
        else:
            flash(f"Update failed: {result['message']}", 'danger')

    elif request.method == 'POST':
        print("Form errors:", form.errors)

    return render_template('ont/edit_customer_item_credit.html', form=form, user=user, item_credit=item_credit)