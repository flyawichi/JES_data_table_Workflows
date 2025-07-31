from flask import Flask
from ar_db_sync.routes import ar_api

app = Flask(__name__)
app.register_blueprint(ar_api)

if __name__ == "__main__":
    app.run(debug=True)
