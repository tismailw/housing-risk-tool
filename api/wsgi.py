from app import create_app
app = create_app()

if __name__ == "__main__":
    # Dev server; later swap to gunicorn/uwsgi in prod
    app.run(host="0.0.0.0", port=8000)
