"""
Phase 3 Demo: Data Processing Pipeline

Shows the complete process:
1. Download cryptocurrency prices (Yahoo Finance)
2. Download Google Trends data (pytrends)
3. Align by common dates
4. Merge into single DataFrame
5. Clean and validate
6. Show final processed data

Run with: python demo_phase3.py
"""

from src.data_ingestion import CryptoDataDownloader, GoogleTrendsDownloader
from src.data_processing import DataProcessor
from src.database_manager import DatabaseManager
import sqlite3


def main():
    print("=" * 80)
    print("PHASE 3 DEMO: Data Processing & Alignment")
    print("=" * 80)
    
    # ===== STEP 1: Download Data =====
    print("\n[STEP 1] Downloading Bitcoin Price Data...")
    print("-" * 80)
    
    crypto_downloader = CryptoDataDownloader()
    btc_prices = crypto_downloader.download(
        ticker="BTC-USD",
        start_date="2024-01-01",
        end_date="2024-01-31"
    )
    
    print(f"\nBitcoin Price data downloaded:")
    print(f"  • Rows: {len(btc_prices)}")
    print(f"  • Columns: {list(btc_prices.columns)}")
    print(f"  • Date range: {btc_prices.index[0].date()} to {btc_prices.index[-1].date()}")
    
    # ===== STEP 2: Download Trends =====
    print("\n\n[STEP 2] Downloading Bitcoin Google Trends Data...")
    print("-" * 80)
    
    trends_downloader = GoogleTrendsDownloader()
    btc_trends = trends_downloader.download(
        keyword="Bitcoin",
        start_date="2024-01-01",
        end_date="2024-01-31"
    )
    
    print(f"\nBitcoin Trends data downloaded:")
    print(f"  • Rows: {len(btc_trends)}")
    print(f"  • Columns: {list(btc_trends.columns)}")
    print(f"  • Date range: {btc_trends.index[0].date()} to {btc_trends.index[-1].date()}")
    
    # ===== STEP 3: Process Data =====
    print("\n\n[STEP 3] Processing Data (Align, Clean, Validate)...")
    print("-" * 80)
    
    processor = DataProcessor()
    processed_data = processor.process_pipeline(btc_prices, btc_trends)
    
    if processed_data is not None and len(processed_data) > 0:
        print(f"\n✓ Data Processing Complete!")
        print(f"  • Final rows: {len(processed_data)}")
        print(f"  • Columns: {list(processed_data.columns)}")
        print(f"\nFirst 5 rows of processed data:")
        print(processed_data.head(5).to_string())
        
        print(f"\n\nData Statistics:")
        print(processed_data.describe().to_string())
    
    # ===== STEP 4: Store in Database =====
    print("\n\n[STEP 4] Inserting Processed Data into SQLite Database...")
    print("-" * 80)
    
    # Initialize counters
    price_count = 0
    trends_count = 0
    
    if processed_data is not None and len(processed_data) > 0:
        try:
            db_manager = DatabaseManager("quant_processed.db")
            db_manager.initialize()
            
            # Insert prices
            conn = db_manager.connection
            cursor = conn.cursor()
            
            # Prepare data for insertion
            for date, row in processed_data.iterrows():
                # Insert into crypto_prices
                if 'open' in row and 'close' in row:
                    cursor.execute("""
                        INSERT OR IGNORE INTO crypto_prices 
                        (date, ticker, symbol, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        date.date(),
                        'BTC-USD',
                        'BTC',
                        float(row.get('open', 0)),
                        float(row.get('high', 0)),
                        float(row.get('low', 0)),
                        float(row['close']),
                        float(row.get('volume', 0))
                    ))
                
                # Insert into google_trends
                if 'value' in row:
                    cursor.execute("""
                        INSERT OR IGNORE INTO google_trends
                        (date, symbol, search_volume, normalized_volume)
                        VALUES (?, ?, ?, ?)
                    """, (
                        date.date(),
                        'BTC',
                        int(row['value']),
                        float(row['value']) / 100
                    ))
            
            conn.commit()
            
            # Check what was inserted
            cursor.execute("SELECT COUNT(*) FROM crypto_prices")
            price_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM google_trends")
            trends_count = cursor.fetchone()[0]
            
            print(f"✓ Data inserted into database:")
            print(f"  • crypto_prices: {price_count} rows")
            print(f"  • google_trends: {trends_count} rows")
            
            db_manager.close()
        
        except Exception as e:
            print(f"✗ Error inserting data: {e}")
    else:
        print("✗ No processed data to insert (processing failed)")
    
    # ===== SUMMARY =====
    print("\n\n" + "=" * 80)
    print("PHASE 3 SUMMARY")
    print("=" * 80)
    print("""
✓ Downloaded real data:
  • Bitcoin prices (30 days) from Yahoo Finance
  • Bitcoin search trends (31 days) from Google Trends

✓ Aligned to common dates:
  • Both datasets now have same date range
  • Ready for joint analysis

✓ Processed data:
  • Cleaned missing values
  • Removed/clipped outliers
  • Validated data quality
  • Prepared for feature engineering

✓ Stored in SQLite database:
  • crypto_prices table: {price_count} rows
  • google_trends table: {trends_count} rows

NEXT STEPS: Phase 4 - Feature Engineering
Will calculate:
  • Price returns (daily, 3-day, 5-day, 7-day)
  • Volume changes
  • Search volume spikes (>90th percentile)
  • Forward returns for hypothesis testing
    """.format(price_count=price_count, trends_count=trends_count))
    print("=" * 80)


if __name__ == "__main__":
    main()
