from pickletools import read_uint1
from flask import render_template, redirect, url_for, flash, session, request
from app.modules.inv.forms import ItemInquiryForm, ItemInquiryForm2
from app.modules.mk.forms import AddWishlist, EditWishlist
from utils.breadcrumbs import register_breadcrumb
from app.modules.mk import mk_bp
from app.modules.mk.repositories import *
from app.modules.auth.repositories import *
from app.modules.inv.repositories import *
from db import *
import os
import pandas as pd
from flask_cors import CORS
from utils.func import *
import math
from datetime import datetime

@mk_bp.route('/')
@register_breadcrumb('Marketing', url='/mk', parent='Home', parent_url='/')
def mk_dashboard():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    user_access = get_module_access_by_module_user(module_name='MK', user_id=session['user_id'])
    return render_template('mk/mk_dashboard.html', user_access = user_access, user=user)

@mk_bp.route('/items', methods=['GET', 'POST'])
@register_breadcrumb('Item Gallery', url='/mk/items', parent='Marketing', parent_url='/mk')
def items():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    # Pagination setup
    page = request.args.get('page', 1, type=int)
    per_page = 20

    all_items = get_item_numbers() or []  # Assume this returns a list of dicts
    total_items = len(all_items)
    total_pages = math.ceil(total_items / per_page)

    paged_items = all_items[(page - 1) * per_page : page * per_page]

    # Form logic (based on all items)


    return render_template('mk/item_list.html',items=paged_items, page=page, total_pages=total_pages, user=user)

@mk_bp.route('/wishlist', methods=['GET', 'POST'])
@register_breadcrumb('My Wishlist', url='/mk/wishlist', parent='Marketing', parent_url='/mk')
def wishlist():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    user_access = get_module_access_by_module_user(module_name='MK', user_id=session['user_id'])

    wishlists = get_user_wishlist(user_id=session['user_id'])
    return render_template('mk/wishlist.html', wishlists=wishlists, user=user, user_access=user_access)

@mk_bp.route('/wishlist/edit/<int:wishlist_id>', methods=['GET', 'POST'])
@register_breadcrumb('Edit Customer Allowance', url=lambda wishlist_id: f'/mk/wishlist/edit/{wishlist_id}', parent='My Wish List', parent_url='/mk/wishlist')
def edit_wishlist(wishlist_id):
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    wishlist = get_wishlist_by_id(wishlist_id=wishlist_id)
    print(wishlist)
    if wishlist['wishlist_id'] is None:
        flash('Cannot find allowance for this customer', 'danger')
        return redirect(url_for('mk.wishlist'))
    if request.method == 'POST':
        form = EditWishlist()
    else:
        form = EditWishlist(data=wishlist)
    if form.validate_on_submit():
        bind = {
            'wishlist_id': wishlist_id,
            'name': str(form.name.data),
            'last_updated_by': session['user_id'],
            'last_update_date': datetime.now()
        }
        table_name="XXJWY.XXCDL_USER_WISHLIST_HEADERS"
        set_fields = ['wishlist_id', 'name', 'last_updated_by', 'last_update_date']
        where_cluse = 'wishlist_id = :wishlist_id'
        result = update_records(table=table_name, set_fields=set_fields, where_clause=where_cluse, data=bind)

        if result['status'] == 'success':
            flash(f"Wishlist {wishlist['name']} has been updated!", 'success')
            return redirect(url_for('mk.wishlist'))
        else:
            flash(f"Update failed: {result['message']}", 'error')
            return redirect(url_for('mk.wishlist'))    
    else:
        print("Form errors:", form.errors)
    return render_template('mk/edit_wishlist.html', form=form, user=user, wishlist=wishlist)

@mk_bp.route('/wishlist/new', methods=['GET', 'POST'])
@register_breadcrumb('New Wish List', url='/mk/wishlist/new', parent='Wish List', parent_url='/mk')
def new_wishlist():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    form = AddWishlist()
    if request.method == 'POST':
        if form.validate_on_submit():
            wishlist = get_last_wishlist_id()
            print(wishlist)
            if wishlist['last_wishlist_id'] is not None:
                wishlist_id = int(wishlist['last_wishlist_id']) + 1
            else: 
                wishlist_id = 1
            bind = {
                'wishlist_id': wishlist_id,
                'name': str(form.name.data),
                'user_id' : session['user_id'],
                'created_by': session['user_id'],
                'last_updated_by': session['user_id']
            }
            table_name = "XXJWY.XXCDL_USER_WISHLIST_HEADERS"

            # Insert new data
            insert_result = insert_records(table_name=table_name, data=bind)
            if insert_result['status'] == 'success':
                flash('New wishlist has been added successfully!', 'success')
            else:
                flash(f"Insert failed: {insert_result['message']}", 'danger')
            return redirect(url_for('mk.wishlist'))
        else:
            print("Form errors:", form.errors)
    return render_template('mk/new_wishlist.html', user=user, form=form)

@mk_bp.route('/wishlist/<int:wishlist_id>/add_item', methods=['GET', 'POST'])
@register_breadcrumb('Add Item to Wish List', url=lambda wishlist_id: f'/mk/wishlist/{wishlist_id}/add_item', parent='Wish List', parent_url='/mk')
def add_item_to_wishlist(wishlist_id):
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(session['user_id'])
    items_not_on_wishlist = get_items_not_on_wishlist(wishlist_id=wishlist_id)
    wishlist = get_wishlist_by_id(wishlist_id=wishlist_id)
    if request.method == 'POST':
        print(request.form.getlist('item'))
        for item in request.form.getlist('item'):
            print(item)
            bind = {
                'wishlist_id': wishlist_id,
                'inventory_item_id': int(item),
                'created_by': session['user_id'],
                'last_updated_by': session['user_id']
            }
            table_name = "XXJWY.XXCDL_USER_WISHLIST_LINES"

            # Insert new data
            insert_result = insert_records(table_name=table_name, data=bind)
            if insert_result['status'] == 'success':
                flash('New Item has been added to wishlist!', 'success')
            else:
                flash(f"Add Item failed: {insert_result['message']}", 'danger')
        return redirect(url_for('mk.add_item_to_wishlist', wishlist_id=wishlist_id))
    return render_template('mk/add_item_to_wishlist.html', wishlist=wishlist, user=user, items=items_not_on_wishlist)