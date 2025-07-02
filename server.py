from flask import Flask, jsonify, send_from_directory, redirect, url_for
from prefect import flow
from coingecko import coingecko_pipeline_flow  # Make sure this is correct
import os

app = Flask(__name__)
OUTPUT_DIR = os.path.abspath("public")  # Directory for chart HTML

@app.route("/")
def home():
    return """
    <html>
    <head><title>CoinGecko ETL Server</title></head>
    <body style='font-family:sans-serif; padding: 30px;'>
        <h2>🟢 CoinGecko ETL Server is Running</h2>
        <p>This will fetch fresh token data and show the latest chart.</p>
        <form action="/run" method="get">
            <button type="submit" style="padding: 10px 20px; font-size: 16px; cursor: pointer;">🚀 Run Pipeline & View Chart</button>
        </form>
    </body>
    </html>
    """

@app.route("/run", methods=["GET"])
def run_pipeline():
    try:
        coingecko_pipeline_flow(deploy=False)  # Run pipeline & save to `public/index.html`
        return redirect(url_for('view_chart'))  # Go to chart page
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route("/chart")
def view_chart():
    # Serve the freshly generated chart
    return send_from_directory(OUTPUT_DIR, "index.html")

if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    app.run(debug=True, port=8000)
