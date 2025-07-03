from flask import Flask, redirect, send_from_directory, render_template_string
from coingecko import coingecko_pipeline_flow
import threading
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
    # Run pipeline in background
    threading.Thread(target=coingecko_pipeline_flow, daemon=True).start()
    return redirect('/processing')

@app.route('/processing')
def processing():
    return render_template_string("""
        <h2>Pipeline is running, please wait...</h2>
        <div id="status">Checking for results...</div>
        <script>
        function checkReady() {
            fetch('/visualization').then(resp => {
                if (resp.status === 200) {
                    window.location.href = '/visualization';
                } else {
                    setTimeout(checkReady, 3000);
                }
            });
        }
        checkReady();
        </script>
    """)

@app.route('/visualization')
def serve_chart():
    chart_path = os.path.join('public', 'index.html')
    if os.path.exists(chart_path):
        return send_from_directory('public', 'index.html')
    else:
        return "Visualization not ready. Please wait...", 404

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
