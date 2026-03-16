"""
View Data from SQLite Database

Shows you what data is actually stored in the database.

Run with: python view_data.py
"""

import sqlite3
import pandas as pd


def view_database(db_path="demo_quant.db"):
    """View all data in the database"""
    
    try:
        conn = sqlite3.connect(db_path)
        
        print("=" * 70)
        print(f"VIEWING DATA: {db_path}")
        print("=" * 70)
        
        # List all tables
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        print(f"\n📊 Tables in database ({len(tables)} total):\n")
        for table_name, in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            print(f"  • {table_name:20} ({row_count:3} rows)")
        
        # Show data from each table
        print("\n" + "=" * 70)
        print("TABLE CONTENTS")
        print("=" * 70)
        
        for table_name, in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            if row_count == 0:
                print(f"\n📋 {table_name.upper()} (empty)")
            else:
                print(f"\n📋 {table_name.upper()} ({row_count} rows)")
                print("-" * 70)
                
                # Read data using pandas for nice formatting
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
                
                if len(df) > 0:
                    print(df.to_string(index=False))
                    print("-" * 70)
        
        conn.close()
        
        print("\n" + "=" * 70)
        print("✓ Data viewing complete!")
        print("=" * 70)
        
    except FileNotFoundError:
        print(f"❌ Database file not found: {db_path}")
        print("   Run demo_phase2.py first to create the database")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    view_database()
