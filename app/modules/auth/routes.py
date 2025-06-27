import re
from flask import render_template, redirect, url_for, flash, session, request
from flask_bcrypt import Bcrypt
from utils.breadcrumbs import register_breadcrumb
from app.modules.auth import auth_bp
from app.modules.auth.forms import *
from app.modules.auth.repositories import *

from app.modules import auth

bcrypt = Bcrypt()

@auth_bp.route('/')
@register_breadcrumb('Home', url='/')
def auth_dashboard():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    user = get_user_by_id(user_id=session['user_id'])
    user_access = get_module_access_by_user(user_id=session['user_id'])
    return render_template('auth/index.html' , user = user, user_access=user_access)

@auth_bp.route('/admin', methods=['GET', 'POST'])
@register_breadcrumb('Admin', url='/admin', parent='Home', parent_url='/')
def admin():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    if session['user_id'] == 0:
        return render_template('auth/admin.html')
    user = get_user_by_id(user_id=session['user_id'])
    if user:
        role = get_role_by_id(role_id=user["role_id"])
        if role and role['role_name'] != 'ADMIN':
            return redirect(url_for('auth.auth_dashboard'))  
    return render_template('auth/admin.html', user=user)



@auth_bp.route('/admin/users')
@register_breadcrumb('Users', url='/admin/users', parent='Admin', parent_url='/admin')
def users():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    users = get_all_users()
    return render_template('auth/users.html', users=users)

@auth_bp.route('/admin/users/profile')
@register_breadcrumb('User Profile', url='/admin/users/profile', parent='Users', parent_url='/admin/users')
def user_profile():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    edit_user = get_user_by_id(session['user_id'])
    if not edit_user:
        flash('Cannot find user', 'danger')
        return redirect(url_for('auth.users'))
    form = PasswordUpdateForm()
    if form.validate_on_submit():
        if form.password.data:
            new_hashed_password = bcrypt.generate_password_hash(form.password.data).decode('utf-8')
            conn = get_connection()
            print(conn)

            if conn:
                bind = {
                    'password': new_hashed_password,
                    'user_id': session['user_id']
                }
                cursor = conn.cursor()
                cursor.execute("""
                UPDATE XXJWY.XXCDL_CUSTOM_USERS SET password = :password WHERE user_id = :user_id
                """, bind)
                conn.commit()
                cursor.close()
                conn.close()
                flash('Your password has been updated!', 'success')
    else:
        print("Form errors:", form.errors)
    return render_template('auth/user_profile.html', form=form, user=edit_user)

@auth_bp.route('/register', methods=['GET', 'POST'])
@register_breadcrumb('Users', url='/register', parent='Home', parent_url='/')
def register():
    form = RegistrationForm()
    if form.validate_on_submit():
        hashed_password = bcrypt.generate_password_hash(form.password.data).decode('utf-8')
        conn = get_connection()
        print(conn)
        if conn:
            cursor = conn.cursor()
            bind = {
                'user_name': form.user_name.data
            }
            cursor.execute("""
                SELECT FU.USER_ID, FU.USER_NAME, HE.EMAIL_ADDRESS, HE.LAST_NAME FROM FND_USER FU, HR_EMPLOYEES  HE
                WHERE FU.USER_NAME = :user_name
                AND FU.EMPLOYEE_ID = HE.EMPLOYEE_ID
                AND END_DATE IS NULL""", bind)
            user_row = cursor.fetchone()
            print(user_row)
            if not user_row:
                cursor.close()
                conn.close()
                flash('You do not have Oracle Account. Please contact IT Dept!', 'danger')
                return redirect(url_for('auth.login'))
            else:
                if user_row[0] == 0:
                    cursor.execute("""
                    INSERT INTO XXJWY.XXCDL_CUSTOM_USERS (user_id, user_name, email, password, full_name, created_by, last_updated_by, active_flag) VALUES (:1, :2, :3, :4, :5, :6, :7, :8)
                    """, (user_row[0], user_row[1], user_row[2], hashed_password, user_row[3], 0, 0, 'Y'))
                    conn.commit()
                    cursor.close()
                    conn.close()
                    flash('Welcome SYSADMIN user!', 'success')
                    return redirect(url_for('auth.login'))
                else:
                    cursor.execute("""
                    INSERT INTO XXJWY.XXCDL_CUSTOM_USERS (user_id, user_name, email, password, full_name, created_by, last_updated_by, active_flag) VALUES (:1, :2, :3, :4, :5, :6, :7, :8)
                    """, (user_row[0], user_row[1], user_row[2], hashed_password, user_row[3], 0, 0, 'N'))
                    conn.commit()
                    cursor.close()
                    conn.close()
                    flash('Account created successfully! Please contact IT Dept to activate your account!', 'success')
                    return redirect(url_for('auth.login'))
    elif form.is_submitted():
        if form.password.data != form.confirm_password.data:
            flash('Confirm Password is incorrect!', 'danger')
    return render_template('auth/register.html', form=form)

@auth_bp.route('/admin/users/edit/<int:user_id>', methods=['GET', 'POST'])
@register_breadcrumb('Edit User', url=lambda user_id: f'/admin/users/edit/{user_id}', parent='Users', parent_url='/admin/users' )
def edit_user(user_id):
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    edit_user = get_user_by_id(user_id)
    if not edit_user:
        flash('Cannot find user', 'danger')
        return redirect(url_for('auth.users'))
    roles = get_all_roles()
    if request.method == 'POST':
        form = UserUpdateForm()
        if roles:
            form.role_id.choices = list((role['role_id'], role['role_name']) for role in roles)
        else:
            form.role_id.choices = []
    else:
        form = UserUpdateForm(data=edit_user)
        if roles:
            form.role_id.choices = list((role['role_id'], role['role_name']) for role in roles)
        else:
            form.role_id.choices = []

    if form.validate_on_submit():
        if form.password.data:
            new_hashed_password = bcrypt.generate_password_hash(form.password.data).decode('utf-8')
            conn = get_connection()
            print(conn)

            if conn:
                bind = {
                    'active_flag': form.active_flag.data,
                    'password': new_hashed_password,
                    'user_id': user_id,
                    'role_id': form.role_id.data
                }
                cursor = conn.cursor()
                cursor.execute("""
                UPDATE XXJWY.XXCDL_CUSTOM_USERS SET active_flag = :active_flag, password = :password, role_id = :role_id WHERE user_id = :user_id
                """, bind)
                conn.commit()
                cursor.close()
                conn.close()
                flash('Account update successfully!', 'success')
                return redirect(url_for('auth.users'))
        else:
            conn = get_connection()
            print(conn)

            if conn:
                bind = {
                    'active_flag': form.active_flag.data,
                    'user_id': user_id,
                    'role_id' : form.role_id.data
                }
                cursor = conn.cursor()
                cursor.execute("""
                UPDATE XXJWY.XXCDL_CUSTOM_USERS SET active_flag = :active_flag, role_id = :role_id WHERE user_id = :user_id
                """, bind)
                conn.commit()
                cursor.close()
                conn.close()
                flash('Account update successfully!', 'success')
                return redirect(url_for('auth.users'))
    else:
        print("Form errors:", form.errors)
    return render_template('auth/edit_user.html', form=form)

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    form = LoginForm()
    if form.validate_on_submit():
        user = get_user_by_user_name(form.user_name.data)
        print(form.user_name.data)
        if user and bcrypt.check_password_hash(user['password'], form.password.data):
            if user['active_flag'] != 'Y':
                flash('Your account is not activated, please contact IT Team!', 'danger')
            else:    
                session['user_id'] = user['user_id']
                session['user_name'] = user['user_name']
                role = get_role_by_id(role_id=user["role_id"])
                print('ROLE', role)
                if session['user_id'] == 0:
                    return redirect(url_for('auth.admin'))
                else:
                    return redirect(url_for('auth.auth_dashboard'))   
        else:
            flash('Invalid user_name or password!', 'danger')
    elif form.is_submitted():
        flash('Invalid user_name or password. Please check your input!', 'danger')
    return render_template('auth/login.html', form=form)

@auth_bp.route('/logout')
def logout():
    session.pop('user_id', None)
    flash('You are logged out!', 'info')
    return redirect(url_for('auth.login'))

@auth_bp.route('/admin/roles')
@register_breadcrumb('Roles', url='/admin/roles', parent='Admin', parent_url='/admin')
def roles():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    roles = get_all_roles()
    return render_template('auth/roles.html', roles=roles)

@auth_bp.route('/admin/roles/add', methods=['GET', 'POST'])
@register_breadcrumb('Add Role', url='/admin/roles/add', parent='Roles', parent_url='/admin/roles')
def add_role():
    form = AddRoleForm()
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    if form.validate_on_submit():
        conn = get_connection()
        print(conn)
        if conn:
            cursor = conn.cursor()
            bind = {
                'role_name': form.role_name.data,
                'description': form.description.data
            }
            cursor.execute("""
            INSERT INTO XXJWY.XXCDL_CUSTOM_ROLES (role_name, description, created_by, last_updated_by) VALUES (:role_name, :description, 0, 0)
            """, bind)
            conn.commit()
            cursor.close()
            conn.close()
            flash(f'New Role {bind["role_name"]}!', 'success')
            return redirect(url_for('auth.roles'))
    else:
        print("Form errors:", form.errors)
    return render_template('auth/add_role.html', form=form)

@auth_bp.route('/admin/roles/edit/<int:role_id>', methods=['GET', 'POST'])
@register_breadcrumb('Edit Role', url=lambda role_id: f'/admin/users/edit/{role_id}', parent='Roles', parent_url='/admin/roles')
def edit_role(role_id):
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    role = get_role_by_id(role_id)
    if not role:
        flash('Cannot find role', 'danger')
        return redirect(url_for('auth.roles'))
    if request.method == 'POST':
        form = UpdateRoleForm()
    else:
        form = UpdateRoleForm(data=role)
    if form.validate_on_submit():
        conn = get_connection()
        print(conn)
        if conn:
            bind = {
                'role_id': role_id,
                'role_name': form.role_name.data,
                'description': form.description.data
            }
            cursor = conn.cursor()
            cursor.execute("""
            UPDATE XXJWY.XXCDL_CUSTOM_ROLES SET role_name = :role_name, description = :description WHERE role_id = :role_id
            """, bind)
            conn.commit()
            cursor.close()
            conn.close()
            flash(f'Role updates successfully {role["role_name"]}!', 'success')
            return redirect(url_for('auth.roles'))
    else:
        print("Form errors:", form.errors)
    return render_template('auth/edit_role.html', form=form)

@auth_bp.route('/admin/modules')
@register_breadcrumb('Modules', url='/admin/modules', parent='Admin', parent_url='/admin')
def modules():
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    modules = get_all_modules()
    return render_template('auth/modules.html', modules=modules)

@auth_bp.route('/admin/modules/add', methods=['GET', 'POST'])
@register_breadcrumb('Add Module', url='/admin/modules/add', parent='Modules', parent_url='/admin/modules')
def add_module():
    form = AddModuleForm()
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    if form.validate_on_submit():
        conn = get_connection()
        print(conn)
        if conn:
            cursor = conn.cursor()
            bind = {
                'module_name': form.module_name.data,
                'description': form.description.data
            }
            cursor.execute("""
            INSERT INTO XXJWY.XXCDL_CUSTOM_MODULES (module_name, description, created_by, last_updated_by) VALUES (:module_name, :description, 0, 0)
            """, bind)
            conn.commit()
            cursor.close()
            conn.close()
            flash(f'New Module {bind["module_name"]} Added!', 'success')
            return redirect(url_for('auth.modules'))
    else:
        print("Form errors:", form.errors)
    return render_template('auth/add_module.html', form=form)

@auth_bp.route('/admin/modules/edit/<int:module_id>', methods=['GET', 'POST'])
@register_breadcrumb('Edit Module', url=lambda module_id: f'/admin/users/edit/{module_id}', parent='Modules', parent_url='/admin/modules')
def edit_module(module_id):
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    module = get_module_by_id(module_id)
    print(module)
    if not module:
        flash('Cannot find Module', 'danger')
        return redirect(url_for('auth.modules'))
    if request.method == 'POST':
        form = UpdateModuleForm()
    else:
        form = UpdateModuleForm(data=module)
    if form.validate_on_submit():
        conn = get_connection()
        print(conn)
        if conn:
            bind = {
                'module_id': module_id,
                'module_name': form.module_name.data,
                'description': form.description.data
            }
            cursor = conn.cursor()
            cursor.execute("""
            UPDATE XXJWY.XXCDL_CUSTOM_MODULES SET module_name = :module_name, description = :description WHERE module_id = :module_id
            """, bind)
            conn.commit()
            cursor.close()
            conn.close()
            flash('Module updated successfully!', 'success')
            return redirect(url_for('auth.modules'))
    else:
        print("Form errors:", form.errors)
    return render_template('auth/edit_module.html', form=form)

@auth_bp.route('/admin/users/module_access/<int:user_id>')
@register_breadcrumb('Module Access', url=lambda user_id: f'/admin/users/module_access/{user_id}', parent='Users', parent_url='/admin/users')
def user_module_access(user_id):
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    module_access = get_module_access_by_user(user_id=user_id)
    print(module_access)
    user = get_user_by_id(user_id)
    return render_template('auth/user_module_access.html', module_access=module_access, user=user)

@auth_bp.route('/admin/users/module_access/grant/<int:grantee>', methods=['GET', 'POST'])
@register_breadcrumb('Grant Module Access', url=lambda grantee: f'/admin/users/module_access/grant/{grantee}', parent='Module Access', parent_url=lambda grantee: f'/admin/users/module_access/{grantee}' )
def grant_module_access(grantee):
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    form = AddModuleAccessForm()
    modules = get_modules_user_has_no_access(user_id=grantee)
    grantee_user = get_user_by_id(grantee)
    if modules:
        form.module_id.choices = list((module['module_id'], module['module_name']) for module in modules)
    else:
        form.module_id.choices = []
    if form.validate_on_submit():
        conn = get_connection()
        print(conn)
        if conn:
            module = get_module_by_id(module_id=form.module_id.data)
            bind = {
                'user_id': grantee,
                'module_id': form.module_id.data,
                'read_access': form.read_access.data,
                'write_access': form.write_access.data,
                'creator_id': session['user_id']
            }
            print('BIND :', bind)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO XXJWY.XXCDL_CUSTOM_MODULE_ACCESS
                (USER_ID, MODULE_ID, READ_ACCESS, WRITE_ACCESS, CREATED_BY, LAST_UPDATED_BY)           
                VALUES(:user_id, :module_id, :read_access, :write_access, :creator_id, :creator_id)
            """, bind)
            conn.commit()
            cursor.close()
            conn.close()
            flash(f'You have granted {grantee_user["user_name"]} the access to module {module["module_name"]}!', 'success')
            return redirect(url_for('auth.user_module_access', user_id=grantee))
    else:
        print("Form errors:", form.errors)
    return render_template('auth/grant_module_access.html', form=form, grantee_user=grantee_user)


@auth_bp.route('/admin/users/module_access/edit/<int:access_id>', methods=['GET', 'POST'])
@register_breadcrumb('Edit Module Access', url=lambda access_id: f'/admin/users/module_access/edit/{access_id}', parent='Module Access', parent_url='/admin/module_access' )
def edit_module_access(access_id):
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
    module_access = get_module_access_by_id(access_id)
    form = UpdateModuleAccessForm(access_id)
    if not edit_user:
        flash('Cannot find user', 'danger')
        return redirect(url_for('auth.users'))
    if form.validate_on_submit():
        conn = get_connection()
        print(conn)

        if conn:
            bind = {
                'active_flag': form.active_flag.data,
                'password': new_hashed_password,
                'user_id': user_id,
                'role_id': form.role_id.data
            }
            cursor = conn.cursor()
            cursor.execute("""
            UPDATE XXJWY.XXCDL_CUSTOM_USERS SET active_flag = :active_flag, password = :password, role_id = :role_id WHERE user_id = :user_id
            """, bind)
            conn.commit()
            cursor.close()
            conn.close()
            flash('Account update successfully!', 'success')
            return redirect(url_for('auth.user_module'))
    else:
        print("Form errors:", form.errors)
    return render_template('auth/edit_user.html', form=form)