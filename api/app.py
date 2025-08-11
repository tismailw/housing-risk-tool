import os
from flask import Flask, jsonify
from flask_cors import CORS
from routes.search import search_bp
from services.db import engine
from models.models import Base

def create_app():
    app = Flask(__name__)
    app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret")
    CORS(app, resources={r"/*": {"origins": os.getenv("CLIENT_ORIGIN", "*")}})

    @app.get("/healthz")
    def healthz():
        return jsonify(status="ok")
    
    app.register_blueprint(search_bp)
    
    with app.app_context():
        Base.metadata.create_all(bind=engine)
    return app

app = create_app()

if __name__ == "__main__":
    app.run(host=os.getenv("API_HOST", "0.0.0.0"), port=int(os.getenv("API_PORT", "8000")))
