from flask import Flask
from ar_db_sync.routes import ar_api
from ap_db_sync.routes import ap_api
from job_records_sync.routes import job_records_api


app = Flask(__name__)
app.register_blueprint(ar_api)
app.register_blueprint(ap_api)
app.register_blueprint(job_records_api)


print(app.url_map)

if __name__ == "__main__":
    app.run(debug=True)
