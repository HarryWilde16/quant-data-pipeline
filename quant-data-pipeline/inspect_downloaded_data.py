"""
Inspect Downloaded Data (Before It Goes to Database)

This shows you the actual data that was downloaded from:
- Yahoo Finance (cryptocurrency prices)
- Google Trends (search volume)

Run with: python inspect_downloaded_data.py
"""

from src.data_ingestion import DataIngestionPipeline
import pandas as pd


def inspect_data():
    """Download and inspect cryptocurrency and trends data"""
    
    print("=" * 80)
    print("DATA INSPECTION: Real Downloads from Yahoo Finance & Google Trends")
    print("=" * 80)
    
    pipeline = DataIngestionPipeline()
    
    # ===== BITCOIN PRICES =====
    print("\n[1] BITCOIN PRICE DATA from Yahoo Finance")
    print("-" * 80)
    
    btc_data = pipeline.download_cryptocurrency_data(
        ticker="BTC-USD",
        start_date="2024-01-01",
        end_date="2024-01-31"
    )
    
    if btc_data is not None and len(btc_data) > 0:
        print(f"✓ Downloaded {len(btc_data)} days of Bitcoin prices")
        print(f"\nData Structure:")
        print(f"  Columns: {list(btc_data.columns)}")
        print(f"  Date Range: {btc_data.index[0].date()} to {btc_data.index[-1].date()}")
        print(f"  Data Type: {type(btc_data)}")
        
        print(f"\nFirst 5 rows:")
        print(btc_data.head(5).to_string())
        
        print(f"\n\nStatistics:")
        print(btc_data['Close'].describe().to_string())
    
    # ===== ETH PRICES =====
    print("\n\n" + "=" * 80)
    print("[2] ETHEREUM PRICE DATA from Yahoo Finance")
    print("-" * 80)
    
    eth_data = pipeline.download_cryptocurrency_data(
        ticker="ETH-USD",
        start_date="2024-01-01",
        end_date="2024-01-31"
    )
    
    if eth_data is not None and len(eth_data) > 0:
        print(f"✓ Downloaded {len(eth_data)} days of Ethereum prices")
        print(f"\nFirst 5 rows:")
        print(eth_data.head(5).to_string())
    
    # ===== BITCOIN GOOGLE TRENDS =====
    print("\n\n" + "=" * 80)
    print("[3] BITCOIN SEARCH TRENDS from Google Trends")
    print("-" * 80)
    
    btc_trends = pipeline.download_google_trends_data(
        keyword="Bitcoin",
        start_date="2024-01-01",
        end_date="2024-01-31"
    )
    
    if btc_trends is not None and len(btc_trends) > 0:
        print(f"✓ Downloaded {len(btc_trends)} days of Bitcoin search volume")
        print(f"\nData Structure:")
        print(f"  Columns: {list(btc_trends.columns)}")
        print(f"  Date Range: {btc_trends.index[0].date()} to {btc_trends.index[-1].date()}")
        
        print(f"\nFirst 10 rows:")
        print(btc_trends.head(10).to_string())
        
        print(f"\n\nStatistics:")
        print(btc_trends['value'].describe().to_string())
    
    # ===== ETHEREUM GOOGLE TRENDS =====
    print("\n\n" + "=" * 80)
    print("[4] ETHEREUM SEARCH TRENDS from Google Trends")
    print("-" * 80)
    
    eth_trends = pipeline.download_google_trends_data(
        keyword="Ethereum",
        start_date="2024-01-01",
        end_date="2024-01-31"
    )
    
    if eth_trends is not None and len(eth_trends) > 0:
        print(f"✓ Downloaded {len(eth_trends)} days of Ethereum search volume")
        print(f"\nFirst 10 rows:")
        print(eth_trends.head(10).to_string())
    
    # ===== COMPARISON =====
    print("\n\n" + "=" * 80)
    print("[5] DATA ALIGNMENT COMPARISON")
    print("-" * 80)
    
    if btc_data is not None and btc_trends is not None:
        print(f"\nBitcoin Price Data:")
        print(f"  • Rows: {len(btc_data)}")
        print(f"  • Date starts: {btc_data.index[0].date()}")
        print(f"  • Date ends: {btc_data.index[-1].date()}")
        
        print(f"\nBitcoin Trends Data:")
        print(f"  • Rows: {len(btc_trends)}")
        print(f"  • Date starts: {btc_trends.index[0].date()}")
        print(f"  • Date ends: {btc_trends.index[-1].date()}")
        
        # Find overlapping dates
        price_dates = set(btc_data.index.date)
        trends_dates = set(btc_trends.index.date)
        overlap = price_dates.intersection(trends_dates)
        
        print(f"\nDate Alignment:")
        print(f"  • Price dates: {len(price_dates)} unique days")
        print(f"  • Trends dates: {len(trends_dates)} unique days")
        print(f"  • Overlapping: {len(overlap)} days")
        print(f"  • Ready to merge? {len(overlap) > 0}")
    
    # ===== SUMMARY =====
    print("\n\n" + "=" * 80)
    print("SUMMARY: What You're Looking At")
    print("=" * 80)
    print("""
Phase 2 Downloaded Real Data:
  ✓ Bitcoin prices (daily OHLCV) - 30 days
  ✓ Ethereum prices (daily OHLCV) - 30 days  
  ✓ Bitcoin search volume - 31 days
  ✓ Ethereum search volume - 31 days

This data is currently in MEMORY (as Pandas DataFrames).

Phase 3 Will:
  ✓ Align the dates
  ✓ Merge price + trends data
  ✓ Clean & validate
  ✓ INSERT into SQLite database
  ✓ Calculate features (spikes, returns, volume changes)

After Phase 3:
  ✓ view_data.py will show populated database tables
  ✓ Ready for Phase 4 (Feature Engineering)
  ✓ Ready for Phase 5 (Hypothesis Testing)
    """)
    print("=" * 80)


if __name__ == "__main__":
    inspect_data()
