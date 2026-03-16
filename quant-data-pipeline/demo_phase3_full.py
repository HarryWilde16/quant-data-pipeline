"""
Phase 3 Full Demo: Data Processing for All 10 Cryptocurrencies

Downloads, processes, and stores data for:
BTC, ETH, XRP, SOL, ADA, TAO, DOGE, LINK, HBAR, SUI

Run with: python demo_phase3_full.py
"""

from src.data_ingestion import CryptoDataDownloader, GoogleTrendsDownloader
from src.data_processing import DataProcessor
from src.database_manager import DatabaseManager
import sqlite3
from datetime import datetime, timedelta

# Your 10 coins from hypothesis.yaml
COINS = {
    'BTC': 'BTC-USD',
    'ETH': 'ETH-USD',
    'XRP': 'XRP-USD',
    'SOL': 'SOL-USD',
    'ADA': 'ADA-USD',
    'TAO': 'TAO-USD',
    'DOGE': 'DOGE-USD',
    'LINK': 'LINK-USD',
    'HBAR': 'HBAR-USD',
    'SUI': 'SUI-USD'
}


def main():
    print("=" * 100)
    print("PHASE 3 FULL DEMO: Processing All 10 Cryptocurrencies")
    print("=" * 100)
    
    # Determine date range (last 90 days from today)
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    
    print(f"\n📅 Date Range: {start_date} to {end_date} (90 days)")
    
    # Initialize database
    db_manager = DatabaseManager("quant_processed.db")
    db_manager.initialize()
    conn = db_manager.connection
    cursor = conn.cursor()
    
    # Track results
    results = {
        'success': [],
        'failed': [],
        'totals': {'prices': 0, 'trends': 0}
    }
    
    # Download and process each coin
    crypto_downloader = CryptoDataDownloader()
    trends_downloader = GoogleTrendsDownloader()
    processor = DataProcessor()
    
    print("\n" + "=" * 100)
    print("DOWNLOADING & PROCESSING COINS")
    print("=" * 100)
    
    for symbol, ticker in COINS.items():
        print(f"\n{'─' * 100}")
        print(f"📊 Processing {symbol:6} ({ticker})")
        print(f"{'─' * 100}")
        
        try:
            # Step 1: Download price data
            print(f"  [1/4] Downloading price data...", end=" ", flush=True)
            prices = crypto_downloader.download(ticker, start_date, end_date)
            
            if len(prices) == 0:
                print(f"❌ No price data found")
                results['failed'].append((symbol, "No price data"))
                continue
            
            print(f"✓ {len(prices)} rows")
            
            # Step 2: Download trends data
            print(f"  [2/4] Downloading trends data...", end=" ", flush=True)
            trends = trends_downloader.download(symbol, start_date, end_date)
            
            if len(trends) == 0:
                print(f"❌ No trends data found")
                results['failed'].append((symbol, "No trends data"))
                continue
            
            print(f"✓ {len(trends)} rows")
            
            # Step 3: Process data
            print(f"  [3/4] Processing (align, clean, validate)...", end=" ", flush=True)
            processed = processor.process_pipeline(prices, trends)
            
            if processed is None or len(processed) == 0:
                print(f"❌ Processing failed")
                results['failed'].append((symbol, "Processing failed"))
                continue
            
            print(f"✓ {len(processed)} aligned rows")
            
            # Step 4: Insert into database
            print(f"  [4/4] Inserting into database...", end=" ", flush=True)
            
            for date, row in processed.iterrows():
                # Insert price
                if 'open' in row and 'close' in row:
                    cursor.execute("""
                        INSERT OR IGNORE INTO crypto_prices 
                        (date, ticker, symbol, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        date.date(),
                        ticker,
                        symbol,
                        float(row.get('open', 0)),
                        float(row.get('high', 0)),
                        float(row.get('low', 0)),
                        float(row['close']),
                        float(row.get('volume', 0))
                    ))
                
                # Insert trends
                if 'value' in row:
                    cursor.execute("""
                        INSERT OR IGNORE INTO google_trends
                        (date, symbol, search_volume, normalized_volume)
                        VALUES (?, ?, ?, ?)
                    """, (
                        date.date(),
                        symbol,
                        int(row['value']),
                        float(row['value']) / 100
                    ))
            
            conn.commit()
            
            # Get row counts for this coin
            cursor.execute("SELECT COUNT(*) FROM crypto_prices WHERE symbol = ?", (symbol,))
            price_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM google_trends WHERE symbol = ?", (symbol,))
            trends_count = cursor.fetchone()[0]
            
            print(f"✓ {price_count} prices, {trends_count} trends")
            
            results['success'].append((symbol, price_count, trends_count))
            results['totals']['prices'] += price_count
            results['totals']['trends'] += trends_count
            
        except Exception as e:
            print(f"❌ Error: {str(e)[:50]}")
            results['failed'].append((symbol, str(e)[:50]))
            continue
    
    db_manager.close()
    
    # Print summary
    print("\n" + "=" * 100)
    print("SUMMARY")
    print("=" * 100)
    
    if results['success']:
        print(f"\n✓ Successfully processed {len(results['success'])} coins:")
        for symbol, prices, trends in results['success']:
            print(f"  • {symbol:6} : {prices:4} prices, {trends:4} trends")
    
    if results['failed']:
        print(f"\n❌ Failed to process {len(results['failed'])} coins:")
        for symbol, reason in results['failed']:
            print(f"  • {symbol:6} : {reason}")
    
    print(f"\n📊 TOTALS:")
    print(f"  • Total Crypto Price Records: {results['totals']['prices']}")
    print(f"  • Total Trends Records:      {results['totals']['trends']}")
    print(f"  • Database File:             quant_processed.db")
    
    print("\n" + "=" * 100)
    print("NEXT STEPS:")
    print("=" * 100)
    print("""
Phase 4 - Feature Engineering:
  1. Calculate price returns (daily, 3-day, 5-day, 7-day)
  2. Detect search volume spikes (>90th percentile)
  3. Create forward-returns for lag analysis
  4. Populate 'features' table

Phase 5 - Hypothesis Testing:
  1. Compare returns on spike days vs normal days
  2. Calculate t-statistics and p-values
  3. Test at 95% confidence level
  4. Populate 'results' table

View all data:
  python view_database.py
    """)
    print("=" * 100)


if __name__ == "__main__":
    main()
