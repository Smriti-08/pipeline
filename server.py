from flask import Flask, redirect, send_from_directory, render_template_string
from coingecko import coingecko_pipeline_flow

app = Flask(__name__)

@app.route('/')
def home():
    return render_template_string("""
        <h1>CoinGecko ETL Server is Running</h1>
        <form action="/run" method="post">
            <button type="submit">Run ETL Pipeline</button>
        </form>
    """)

@app.route('/run', methods=["POST"])
def run_etl():
    # Run ETL without deploying to Vercel
    coingecko_pipeline_flow(deploy=False)
    return redirect('/visualization')

@app.route('/visualization')
def serve_chart():
    return send_from_directory('public', 'index.html')
