from flask import Flask
from flask_bcrypt import Bcrypt
from app.modules import mk
from config import Config


def create_app():
    app = Flask(__name__)
    bcrypt = Bcrypt(app)

    app.config.from_object(Config)

    # Import and register blueprints
    from app.modules.inv import inv_bp
    from app.modules.ont import ont_bp
    from app.modules.auth import auth_bp
    from app.modules.po import po_bp
    from app.modules.mk import mk_bp

    app.register_blueprint(inv_bp)
    app.register_blueprint(ont_bp)
    app.register_blueprint(auth_bp)
    app.register_blueprint(po_bp)
    app.register_blueprint(mk_bp)

    return app

