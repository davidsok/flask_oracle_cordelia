from tokenize import String
from flask_wtf import FlaskForm
from wtforms import DecimalField, SelectField, StringField, PasswordField, SubmitField, IntegerField
from wtforms.validators import DataRequired, Email, EqualTo, Length, NumberRange

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
    allowance = DecimalField('Allowance % (1% Enter 1)', validators=[NumberRange(min=0.00, message='Allowance must not be negative')])
    commission = DecimalField('Commission % (1% Enter 1)', validators=[NumberRange(min=0.00, message='Commission must not be negative')])
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
    led_expense = DecimalField('LED Expense % (1% Enter 1)')
    submit = SubmitField('Update')

class AddItemCompShopForm(FlaskForm):
    item = SelectField('Customer', coerce=int, validators=[DataRequired()])
    our_retail = DecimalField('Our Retail', validators=[NumberRange(min=0.01, message='Retail price must be positive')])
    brand = StringField('Brand')
    price = DecimalField('Price', validators=[NumberRange(min=0.01, message='Price must be positive')])
    submit = SubmitField('Add')

class EditItemCompShopForm(FlaskForm):
    our_retail = DecimalField('Our Retail', validators=[NumberRange(min=0.01, message='Retail price must be positive')])
    brand = StringField('Brand')
    price = DecimalField('Price', validators=[NumberRange(min=0.01, message='Price must be positive')])
    submit = SubmitField('Update')

class AddCustomerItemCreditForm(FlaskForm):
    customer = SelectField('Customer', coerce=int, validators=[DataRequired()])
    item = SelectField('Item', coerce=int, validators=[DataRequired()])
    cm = DecimalField('Credit Amount', validators=[NumberRange(min=0.01, message='Credit Amount per item must be positive')])
    submit = SubmitField('Add')

class EditCustomerItemCreditForm(FlaskForm):
    cm = DecimalField('Credit Amount', validators=[NumberRange(min=0.01, message='Credit Amount per item must be positive')])
    submit = SubmitField('Update')