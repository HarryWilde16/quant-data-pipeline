"""
View all cryptocurrency data currently in the database
Shows statistics for each coin
"""
import sqlite3
import pandas as pd
from datetime import datetime

def show_crypto_stats():
    """Show statistics for each cryptocurrency in database"""
    
    conn = sqlite3.connect('quant_processed.db')
    
    print("\n" + "=" * 120)
    print("🗄️  CRYPTOCURRENCY DATA IN DATABASE")
    print("=" * 120)
    
    # Get unique coins
    query = "SELECT DISTINCT symbol FROM crypto_prices ORDER BY symbol"
    coins = pd.read_sql_query(query, conn)['symbol'].tolist()
    
    print(f"\n✓ Coins in database: {len(coins)}")
    print(f"  Symbols: {', '.join(coins)}\n")
    
    # Show stats for each coin
    for symbol in coins:
        print(f"\n{'─' * 120}")
        print(f"📊 {symbol}")
        print(f"{'─' * 120}")
        
        # Price stats
        price_query = f"""
        SELECT 
            COUNT(*) as rows,
            MIN(date) as first_date,
            MAX(date) as last_date,
            ROUND(AVG(close), 2) as avg_close,
            ROUND(MIN(close), 2) as min_close,
            ROUND(MAX(close), 2) as max_close,
            ROUND(AVG(volume), 0) as avg_volume
        FROM crypto_prices 
        WHERE symbol = '{symbol}'
        """
        price_stats = pd.read_sql_query(price_query, conn)
        
        if price_stats['rows'].values[0] > 0:
            row = price_stats.iloc[0]
            print(f"  Price Data:")
            print(f"    • Rows:       {int(row['rows'])}")
            print(f"    • Date Range: {row['first_date']} to {row['last_date']}")
            print(f"    • Close Price: avg ${row['avg_close']}, range ${row['min_close']}-${row['max_close']}")
            print(f"    • Avg Volume: ${row['avg_volume']:,.0f}")
        
        # Trends stats
        trends_query = f"""
        SELECT 
            COUNT(*) as rows,
            MIN(date) as first_date,
            MAX(date) as last_date,
            ROUND(AVG(search_volume), 1) as avg_search,
            MIN(search_volume) as min_search,
            MAX(search_volume) as max_search
        FROM google_trends 
        WHERE symbol = '{symbol}'
        """
        trends_stats = pd.read_sql_query(trends_query, conn)
        
        if trends_stats['rows'].values[0] > 0:
            row = trends_stats.iloc[0]
            print(f"  Trends Data:")
            print(f"    • Rows:       {int(row['rows'])}")
            print(f"    • Date Range: {row['first_date']} to {row['last_date']}")
            print(f"    • Search Vol: avg {row['avg_search']}, range {int(row['min_search'])}-{int(row['max_search'])} (0-100 scale)")
    
    # Overall summary
    print(f"\n{'─' * 120}")
    print("📈 OVERALL SUMMARY")
    print(f"{'─' * 120}")
    
    total_prices = pd.read_sql_query("SELECT COUNT(*) as count FROM crypto_prices", conn)['count'].values[0]
    total_trends = pd.read_sql_query("SELECT COUNT(*) as count FROM google_trends", conn)['count'].values[0]
    total_features = pd.read_sql_query("SELECT COUNT(*) as count FROM features", conn)['count'].values[0]
    total_results = pd.read_sql_query("SELECT COUNT(*) as count FROM results", conn)['count'].values[0]
    
    print(f"  • Total Price Records:  {total_prices:,}")
    print(f"  • Total Trends Records: {total_trends:,}")
    print(f"  • Features Calculated:  {total_features:,} (Phase 4 - not yet)")
    print(f"  • Results with Tests:   {total_results:,} (Phase 5 - not yet)")
    
    print(f"\n{'─' * 120}")
    print("🔍 SAMPLE DATA (5 most recent rows)")
    print(f"{'─' * 120}")
    
    # Show most recent data
    sample_query = """
    SELECT symbol, date, ROUND(close, 2) as price, volume, 'prices' as type
    FROM crypto_prices
    ORDER BY date DESC, symbol
    LIMIT 10
    """
    sample_data = pd.read_sql_query(sample_query, conn)
    print("\n📌 Latest Prices:")
    print(sample_data.head(8).to_string(index=False))
    
    conn.close()
    print("\n" + "=" * 120 + "\n")

if __name__ == "__main__":
    show_crypto_stats()
