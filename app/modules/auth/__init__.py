from flask import Blueprint

auth_bp = Blueprint('auth', __name__, url_prefix='/')

from . import routes  # Import routes after defining Blueprint