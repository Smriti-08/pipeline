from flask import Flask, redirect, send_from_directory, render_template_string
from coingecko import coingecko_pipeline_flow
import threading  # Import threading for background tasks
import os

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
    # Run ETL in a background thread to avoid blocking the worker
    threading.Thread(target=coingecko_pipeline_flow, kwargs={'deploy': False}, daemon=True).start()
    return redirect('/visualization')

@app.route('/visualization')
def serve_chart():
    return send_from_directory('public', 'index.html')

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))  # 10000 is a safe default
    app.run(host="0.0.0.0", port=port)
