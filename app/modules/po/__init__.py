from flask import Blueprint

po_bp = Blueprint('po', __name__, template_folder='templates', static_folder='static', url_prefix='/po')

from . import routes  # Import routes after defining Blueprint