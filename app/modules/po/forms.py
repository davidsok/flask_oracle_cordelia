from flask_wtf import FlaskForm
from wtforms import SelectField, StringField, PasswordField, SubmitField
from wtforms.validators import DataRequired, Email, EqualTo, Length

class PoInquiryForm(FlaskForm):
    po_number = SelectField('PO Number', coerce=str, validators=[DataRequired()])
    submit = SubmitField('Find')