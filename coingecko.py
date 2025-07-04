from dotenv import load_dotenv
load_dotenv()

import logging
from datetime import datetime, timezone, timedelta
import os
import shutil

import pandas as pd
import requests
from prefect import task, flow
from supabase import create_client, Client
import plotly.graph_objects as go

# Supabase setup
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_API_KEY)

# Logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)

@task
def fetch_and_store_data():
    logger.info("📡 Fetching data from CoinGecko API...")

    url = os.getenv("COINGECKO_API_URL")
    api_key = os.getenv("COINGECKO_API_KEY")
    headers = {
        "accept": "application/json",
        "x-cg-api-key": api_key
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"❌ Error fetching data: {response.status_code} {response.text}")

    data = response.json()
    sorted_data = sorted(data, key=lambda x: x.get("market_cap", 0), reverse=True)
    top_100 = sorted_data[:100]
    now = datetime.now(timezone.utc).isoformat()

    records = []
    for coin in top_100:
        market_cap = coin.get("market_cap") or 0
        volume = coin.get("total_volume") or 0
        current_price = coin.get("current_price") or 1
        high = coin.get("high_24h") or 0
        low = coin.get("low_24h") or 0

        records.append({
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
        })

    logger.info("🗑️ Deleting existing records...")
    supabase.table("coingecko").delete().neq("id", 0).execute()

    logger.info("📤 Inserting new records to Supabase...")
    supabase.table("coingecko").insert(records).execute()
    logger.info(f"✅ Inserted {len(records)} records.")

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

    # Get the latest fetch timestamp
    latest_timestamp = max(pd.to_datetime(df["fetched_at"]))
    timestamp_str = latest_timestamp.strftime("%d %B %Y, %H:%M UTC")

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
        title={
            "text": "Top 100 Tokens: Price and Volatility",
            "x": 0.5,
            "xanchor": "center"
        },
        xaxis=dict(title="Token"),
        yaxis=dict(title="Price (USD)", type="log", side='left', showgrid=True),
        yaxis2=dict(title="Volatility%", overlaying="y", side="right", showgrid=False),
        legend=dict(x=1, y=1, xanchor='right', yanchor='top'),
        template="plotly_white",
        margin=dict(b=100),
        annotations=[
            dict(
                text=f"Last updated: {timestamp_str}",
                showarrow=False,
                xref="paper", yref="paper",
                x=0.5, y=-0.25,
                xanchor='center',
                font=dict(size=12, color="gray")
            )
        ]
    )

    filepath = "token_price_chart.html"
    fig.write_html(filepath, auto_open=False)
    logger.info(f"✅ Chart saved to {os.path.abspath(filepath)}")

@flow(name="CoinGecko Simple Pipeline")
def coingecko_pipeline_flow():
    fetch_and_store_data()
    plot_token_prices()

    try:
        os.makedirs("public", exist_ok=True)
        shutil.copy("token_price_chart.html", os.path.join("public", "index.html"))
        logger.info("✅ Chart copied to public/index.html for Render static hosting.")
    except Exception as e:
        logger.error(f"❌ Copy to public/ failed: {e}")

if __name__ == "__main__":
    coingecko_pipeline_flow()
