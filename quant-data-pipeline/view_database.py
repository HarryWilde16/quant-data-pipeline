"""
Simple database viewer - See what's actually in your SQLite database
"""
import sqlite3
import pandas as pd

def view_database(db_file):
    """Open and display database contents"""
    
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cursor.fetchall()]
    
    print("\n" + "=" * 100)
    print(f"📊 DATABASE: {db_file}")
    print("=" * 100)
    
    for table_name in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        if row_count > 0:
            print(f"\n📊 TABLE: {table_name.upper()}")
            print(f"   Rows: {row_count}")
            print("-" * 100)
            
            # Read and display data
            df = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 10", conn)
            print(df.to_string(index=False))
        else:
            print(f"\n📊 TABLE: {table_name.upper()} (empty)")
    
    conn.close()
    print("\n" + "=" * 100)

if __name__ == "__main__":
    print("\n🗄️  VIEW YOUR DATA\n")
    
    print("\n1️⃣  PHASE 3 DATABASE (Most Recent - with cleaned data):")
    view_database("quant_processed.db")
    
    print("\n\n2️⃣  PHASE 2 DATABASE (Raw downloaded data):")
    view_database("demo_quant.db")
