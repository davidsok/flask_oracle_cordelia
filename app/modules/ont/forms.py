from flask_wtf import FlaskForm
from wtforms import SelectField, StringField, PasswordField, SubmitField
from wtforms.validators import DataRequired, Email, EqualTo, Length

class ItemInquiryForm(FlaskForm):
    item_number = StringField('Item Number', validators=[DataRequired(), Length(min=1, max=50)])
    organization = SelectField('Organization', coerce=int, validators=[DataRequired()], default=(101,'MAS'))
    item_status = SelectField('Item Status', coerce=str, validators=[DataRequired()])
    item_type = SelectField('Item Types', coerce=str, validators=[DataRequired()])
    submit = SubmitField('Find')

class ItemInquiryForm2(FlaskForm):
    item_number = SelectField('Item Number', coerce=str, validators=[DataRequired()])
    submit = SubmitField('Find')

class AddAllowanceForm(FlaskForm):
    customer = SelectField('Customer', coerce=int, validators=[DataRequired()])
    allowance = StringField('Allowance % (1% Enter 1)')
    commission = StringField('Commission % (1% Enter 1)')
    submit = SubmitField('Add')

class EditAllowanceForm(FlaskForm):
    allowance = StringField('Allowance % (1% Enter 1)')
    commission = StringField('Commission % (1% Enter 1)')
    submit = SubmitField('Update')

class AddItemLEDExpenseForm(FlaskForm):
    item = SelectField('Item', coerce=int, validators=[DataRequired()])
    led_expense = StringField('LED Expense % (1% Enter 1)')
    submit = SubmitField('Add')

class EditItemLEDExpenseForm(FlaskForm):
    led_expense = StringField('LED Expense % (1% Enter 1)')
    submit = SubmitField('Update')