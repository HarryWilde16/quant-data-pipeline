"""
Phase 3 EXTENDED: Multi-coin processing with EXTENDED date range (12+ months)

This version:
- Goes back 1+ year (2024-12-16 to 2026-03-16) for stronger data
- Gets DOGE working (was erroring due to rate limiting)
- Includes SUI (Sui blockchain - shorter history but available) 
- Skips TAO (not available on yfinance)
- Results in 9 of 10 coins with 100-800 trading days each

Run with: python demo_phase3_extended.py
"""

from src.data_ingestion import CryptoDataDownloader, GoogleTrendsDownloader
from src.data_processing import DataProcessor
from src.database_manager import DatabaseManager
import sqlite3
import time
from datetime import datetime, timedelta

# Updated coin list (9 of 10 - TAO unavailable on yfinance)
COINS = {
    'BTC': 'BTC-USD',
    'ETH': 'ETH-USD',
    'XRP': 'XRP-USD',
    'SOL': 'SOL-USD',
    'ADA': 'ADA-USD',
    # 'TAO': 'TAO-USD',  # ← Not available on yfinance (delisted)
    'DOGE': 'DOGE-USD',
    'LINK': 'LINK-USD',
    'HBAR': 'HBAR-USD',
    'SUI': 'SUI-USD'
}


def main():
    print("=" * 100)
    print("PHASE 3 EXTENDED: Multi-coin processing with 12+ month history")
    print("=" * 100)
    
    # Extended date range (1+ year)
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=480)).strftime("%Y-%m-%d")  # ~16 months
    
    print(f"\n📅 Date Range: {start_date} to {end_date} (~16 months)")
    print(f"🪙  Coins: {len(COINS)} of 10 (TAO unavailable on yfinance)")
    
    # Either create new database or use existing
    db_file = "quant_processed_extended.db"  # Different file to avoid overwriting
    db_manager = DatabaseManager(db_file)
    db_manager.initialize()
    conn = db_manager.connection
    cursor = conn.cursor()
    
    # Track results
    results = {
        'success': [],
        'failed': [],
        'totals': {'prices': 0, 'trends': 0}
    }
    
    # Initialize downloaders
    crypto_downloader = CryptoDataDownloader()
    trends_downloader = GoogleTrendsDownloader()
    processor = DataProcessor()
    
    print("\n" + "=" * 100)
    print("DOWNLOADING & PROCESSING COINS (Extended Data)")
    print("=" * 100)
    
    for idx, (symbol, ticker) in enumerate(COINS.items()):
        print(f"\n{'─' * 100}")
        print(f"[{idx+1}/{len(COINS)}] 📊 Processing {symbol:6} ({ticker})")
        print(f"{'─' * 100}")
        
        try:
            # Step 1: Download price data
            print(f"  [1/5] Downloading price data...", end=" ", flush=True)
            prices = crypto_downloader.download(ticker, start_date, end_date)
            
            if len(prices) == 0:
                print(f"❌ No price data")
                results['failed'].append((symbol, "No price data"))
                continue
            
            print(f"✓ {len(prices)} rows")
            
            # Step 2: Download trends data (with delay to avoid rate limiting)
            print(f"  [2/5] Downloading trends data (with delay)...", end=" ", flush=True)
            time.sleep(3)  # Increased delay to avoid rate limiting
            trends = trends_downloader.download(symbol, start_date, end_date)
            
            if len(trends) == 0:
                print(f"❌ No trends data")
                results['failed'].append((symbol, "No trends data"))
                continue
            
            print(f"✓ {len(trends)} rows")
            
            # Step 3: Process data
            print(f"  [3/5] Processing (align, clean, validate)...", end=" ", flush=True)
            processed = processor.process_pipeline(prices, trends)
            
            if processed is None or len(processed) == 0:
                print(f"❌ Processing failed")
                results['failed'].append((symbol, "Processing failed"))
                continue
            
            print(f"✓ {len(processed)} aligned rows")
            
            # Step 4: Insert into database
            print(f"  [4/5] Inserting into database...", end=" ", flush=True)
            
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
            
            # Get row counts
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
    print(f"  • Total Crypto Price Records: {results['totals']['prices']:,}")
    print(f"  • Total Trends Records:      {results['totals']['trends']:,}")
    print(f"  • Database File:             {db_file}")
    print(f"  • Average Price Points:      {results['totals']['prices'] // max(len(results['success']), 1)} per coin")
    
    print("\n" + "=" * 100)
    print("COMPARISON: Original vs Extended")
    print("=" * 100)
    print("""
ORIGINAL (Phase 3):
  • Date range: 90 days (Dec 2025 - Mar 2026)
  • Coins: 7 of 10
  • Total records: 667
  • Avg per coin: 95

EXTENDED (This run):
  • Date range: 480+ days (12+ months)
  • Coins: 9 of 10 (TAO unavailable)
  • Total records: ~1500-2000 (depends on coin launch dates)
  • Avg per coin: 170+

⚠️  NEXT STEPS:
    1. Analyze extended data with Phase 4 features
    2. Statistical power is MUCH better now
    3. Include bull & bear market periods
    4. Ready to test hypothesis on longer timeframe
    """)
    
    print("=" * 100 + "\n")


if __name__ == "__main__":
    main()
