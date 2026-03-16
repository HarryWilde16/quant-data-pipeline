"""
Database initialization and connection management

This module handles:
- Creating SQLite database
- Initializing schema tables
- Providing database connections
"""

import sqlite3
import os
from pathlib import Path
from database.schema import ALL_SCHEMAS


class DatabaseManager:
    """Manages SQLite database operations"""
    
    def __init__(self, db_path: str = "quant.db"):
        """
        Initialize database manager
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.connection = None
    
    def connect(self) -> sqlite3.Connection:
        """
        Create connection to database
        
        Returns:
            sqlite3 connection object
        """
        try:
            self.connection = sqlite3.connect(self.db_path)
            self.connection.row_factory = sqlite3.Row  # Return rows as dict-like objects
            return self.connection
        except sqlite3.Error as e:
            raise Exception(f"Database connection error: {e}")
    
    def initialize(self) -> bool:
        """
        Create all tables in database
        
        Returns:
            True if successful, False otherwise
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            # Execute each schema definition separately
            # (SQLite requires one statement at a time)
            for schema in ALL_SCHEMAS:
                # Split by semicolon and execute each statement
                statements = [stmt.strip() for stmt in schema.split(';') if stmt.strip()]
                for statement in statements:
                    cursor.execute(statement)
            
            conn.commit()
            print(f"✓ Database initialized: {self.db_path}")
            return True
        
        except sqlite3.Error as e:
            print(f"✗ Database initialization error: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
    
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists in database"""
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        )
        return cursor.fetchone() is not None
    
    def get_row_count(self, table_name: str) -> int:
        """Get number of rows in table"""
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        return cursor.fetchone()[0]


def get_database_connection(db_path: str = "quant.db") -> sqlite3.Connection:
    """
    Convenience function to get database connection
    
    Args:
        db_path: Path to SQLite database
        
    Returns:
        Database connection
    """
    manager = DatabaseManager(db_path)
    return manager.connect()
