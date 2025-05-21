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