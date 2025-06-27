from flask import Blueprint

mk_bp = Blueprint('mk', __name__, template_folder='templates', static_folder='static', url_prefix='/mk')

from . import routes  # Import routes after defining Blueprint