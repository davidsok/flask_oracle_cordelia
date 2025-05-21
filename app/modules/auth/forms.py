from flask_wtf import FlaskForm
from wtforms import SelectField, StringField, PasswordField, SubmitField
from wtforms.validators import DataRequired, Email, EqualTo, Length

class RegistrationForm(FlaskForm):
    user_name = StringField('Username', validators=[DataRequired(), Length(min=3, max=50)])
    password = PasswordField('Password', validators=[DataRequired(), Length(min=6)])
    confirm_password = PasswordField('Confirm Password', validators=[DataRequired(), EqualTo('password')])
    submit = SubmitField('Register')

class LoginForm(FlaskForm):
    user_name = StringField('Username', validators=[DataRequired(), Length(min=3, max=50)])
    password = PasswordField('Password', validators=[DataRequired()])
    submit = SubmitField('Login')

class UserUpdateForm(FlaskForm):
    user_name = StringField('Username', validators=[DataRequired(), Length(min=3, max=50)])
    password = PasswordField('Password')
    role_id = SelectField('Role', coerce=int, validators=[DataRequired()])
    active_flag = SelectField('Is Active', choices=[('Y', 'Yes'), ('N', 'No')], validators=[Length(min=1, max=1)] )
    submit = SubmitField('Update')

class PasswordUpdateForm(FlaskForm):
    password = PasswordField('Password')
    submit = SubmitField('Update Password')

class AddRoleForm(FlaskForm):
    role_name = StringField('Role Name', validators=[DataRequired(), Length(min=2, max=50)])
    description = StringField('Description')
    submit = SubmitField('Register')

class UpdateRoleForm(FlaskForm):
    role_name = StringField('Role Name', validators=[DataRequired(), Length(min=2, max=50)])
    description = StringField('Description')
    submit = SubmitField('Update')

class AddModuleForm(FlaskForm):
    module_name = StringField('Module Name', validators=[DataRequired(), Length(min=2, max=50)])
    description = StringField('Description')
    submit = SubmitField('Register')

class UpdateModuleForm(FlaskForm):
    module_name = StringField('Module Name', validators=[DataRequired(), Length(min=2, max=50)])
    description = StringField('Description')
    submit = SubmitField('Update')

class AddModuleAccessForm(FlaskForm):
    module_id = SelectField('Module', coerce=int, validators=[DataRequired()])
    read_access = SelectField('Read Access', choices=[('Y', 'Yes'), ('N', 'No')], validators=[Length(min=1, max=1)] )
    write_access = SelectField('Write Access', choices=[('Y', 'Yes'), ('N', 'No')], validators=[Length(min=1, max=1)] )
    submit = SubmitField('Add')

class UpdateModuleAccessForm(FlaskForm):
    read_access = SelectField('Read Access', choices=[('Y', 'Yes'), ('N', 'No')], validators=[Length(min=1, max=1)] )
    write_access = SelectField('Write Access', choices=[('Y', 'Yes'), ('N', 'No')], validators=[Length(min=1, max=1)] )
    submit = SubmitField('Add')