from flask import Blueprint

inv_bp = Blueprint('inv', __name__, template_folder='templates', static_folder='static', url_prefix='/inv')

from . import routes  # Import routes after defining Blueprint