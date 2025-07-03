import subprocess
import logging
from datetime import datetime, timezone, timedelta
import os
import webbrowser

import pandas as pd
from prefect import task, flow
from supabase import create_client, Client
import requests
import plotly.graph_objects as go

# Supabase setup
SUPABASE_URL = "https://pimjfpjuuxdphucisnno.supabase.co"
SUPABASE_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBpbWpmcGp1dXhkcGh1Y2lzbm5vIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTA2NzQ4OTksImV4cCI6MjA2NjI1MDg5OX0.UiIOEJEKxd0AA6fxAxWXfcVivsPHpK4_yztgll3pSEM"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_API_KEY)

# Logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)

# CoinGecko Stream (no Airbyte dependency)
class CoinGeckoTopTokensStream:
    def __init__(self, api_key):
        self.api_key = api_key

    def name(self):
        return "coingecko_top_tokens"

    def primary_key(self):
        return "symbol"

    def read_records(self, **kwargs):
        logger.info("📡 Fetching data from CoinGecko API...")
        url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
        headers = {
            "accept": "application/json",
            "x-cg-api-key": self.api_key
        }

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"❌ Error fetching data: {response.status_code} {response.text}")

        data = response.json()
        sorted_data = sorted(data, key=lambda x: x.get("market_cap", 0), reverse=True)
        top_100 = sorted_data[:100]
        now = datetime.now(timezone.utc).isoformat()

        for coin in top_100:
            market_cap = coin.get("market_cap") or 0
            volume = coin.get("total_volume") or 0
            current_price = coin.get("current_price") or 1
            high = coin.get("high_24h") or 0
            low = coin.get("low_24h") or 0

            yield {
                "symbol": coin.get("symbol"),
                "name": coin.get("name"),
                "current_price": current_price,
                "market_cap": market_cap,
                "total_volume": volume,
                "high_24h": high,
                "low_24h": low,
                "price_change_percentage_24h": coin.get("price_change_percentage_24h"),
                "total_supply": coin.get("total_supply"),
                "volume_marketcap_ratio": volume / market_cap if market_cap else None,
                "volatility": ((high - low) * 100 / current_price) if current_price else None,
                "fetched_at": now
            }

# Plotly Chart
@task
def plot_token_prices():
    logger.info("📊 Generating chart...")
    since = (datetime.utcnow() - timedelta(hours=24)).isoformat()
    result = supabase.table("coingecko").select("*").gt("fetched_at", since).execute()
    records = result.data

    if not records:
        logger.warning("⚠️ No records to plot.")
        return

    df = pd.DataFrame(records)
    df = df.sort_values(by="current_price", ascending=False)
    tokens = df["name"]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=tokens,
        y=df["current_price"],
        mode='lines+markers',
        name='Current Price',
        line=dict(color='blue', width=3),
        marker=dict(size=8),
        customdata=df[["high_24h", "low_24h"]],
        hovertemplate="<b>%{x}</b><br>Current Price: $%{y:.2f}<br>High 24h: $%{customdata[0]:.2f}<br>Low 24h: $%{customdata[1]:.2f}<extra></extra>"
    ))
    fig.add_trace(go.Scatter(
        x=tokens,
        y=df["volatility"],
        mode='lines+markers',
        name='Volatility (24h)',
        line=dict(color='purple', width=2, dash='dash'),
        yaxis="y2",
        marker=dict(size=8),
        hovertemplate="<b>%{x}</b><br>Volatility%: %{y:.4f}<extra></extra>"
    ))

    fig.update_layout(
        title="Top 100 Tokens: Price and Volatility",
        xaxis=dict(title="Token"),
        yaxis=dict(title="Price (USD)", type="log", side='left', showgrid=True),
        yaxis2=dict(title="Volatility%", overlaying="y", side="right", showgrid=False),
        legend=dict(x=1, y=1, xanchor='right', yanchor='top'),
        template="plotly_white"
    )

    filepath = "token_price_chart.html"
    fig.write_html(filepath, auto_open=True)
    logger.info(f"✅ Chart saved to {os.path.abspath(filepath)}")

@task
def run_etl():
    logger.info("🚀 Running CoinGecko ETL")
    stream = CoinGeckoTopTokensStream(api_key="CG-ynabWh7ssewjefk1sDs8hf7g")
    records = list(stream.read_records())

    if not records:
        logger.warning("⚠️ No records returned from API.")
        return

    logger.info("🗑️ Deleting existing records...")
    supabase.table("coingecko").delete().neq("id", 0).execute()

    logger.info("📤 Inserting new records to Supabase...")
    supabase.table("coingecko").insert(records).execute()
    logger.info(f"✅ Inserted {len(records)} records.")

@flow(name="CoinGecko Pipeline")
def coingecko_pipeline_flow(deploy=True):
    run_etl()
    plot_token_prices()

    import shutil
    os.makedirs("public", exist_ok=True)
    shutil.copy("token_price_chart.html", os.path.join("public", "index.html"))

    if deploy:
        try:
            subprocess.run([r"C:\\Users\\Smriti\\AppData\\Roaming\\npm\\vercel.cmd", "--prod", "--yes"], check=True)
            logger.info("✅ Deployed to Vercel.")
        except Exception as e:
            logger.error(f"❌ Deployment failed: {e}")
    else:
        logger.info("✅ Chart ready to serve via Flask.")

if __name__ == "__main__":
    coingecko_pipeline_flow()
