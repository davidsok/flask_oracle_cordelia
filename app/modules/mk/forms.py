from flask_wtf import FlaskForm
from wtforms import SelectField, StringField, PasswordField, SubmitField
from wtforms.validators import DataRequired, Email, EqualTo, Length

class AddWishlist(FlaskForm):
    name = StringField('Name', validators=[DataRequired(), Length(min=1, max=150)])
    submit = SubmitField('Add')

class EditWishlist(FlaskForm):
    name = StringField('Name', validators=[DataRequired(), Length(min=1, max=150)])
    submit = SubmitField('Update')