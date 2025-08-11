import os
from flask import Flask
from flask_cors import CORS

from .config import Settings
from .services.db import engine
from .models.models import Base

def create_app() -> Flask:
    app = Flask(__name__)
    app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret")

    # CORS: allow your front-end origin (set CLIENT_ORIGIN in .env later)
    CORS(app, resources={r"/*": {"origins": os.getenv("CLIENT_ORIGIN", "*")}})

    # Register blueprints
    from .routes.health import health_bp
    from .routes.search import search_bp
    app.register_blueprint(health_bp)
    app.register_blueprint(search_bp)

    # Create tables (okay for MVP; migrate with Alembic later)
    with app.app_context():
        Base.metadata.create_all(bind=engine)

    return app
