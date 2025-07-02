from flask import Flask

app = Flask(__name__)

@app.route('/')
def home():
    return 'CoinGecko ETL Server is Running. Hit /run to execute the pipeline.'

@app.route('/run')
def run_etl():
    # Run your ETL logic here
    return 'ETL executed!'
