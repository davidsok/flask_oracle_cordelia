from flask import Blueprint

ont_bp = Blueprint('ont', __name__, template_folder='templates', static_folder='static', url_prefix='/ont')

from . import routes  # Import routes after defining Blueprint