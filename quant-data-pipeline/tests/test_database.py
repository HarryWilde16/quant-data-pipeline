"""
Unit tests for database initialization and management

Run with: pytest tests/test_database.py -v
"""

import unittest
import sqlite3
import os
from pathlib import Path
from src.database_manager import DatabaseManager


class TestDatabaseManager(unittest.TestCase):
    """Test database manager functionality"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test database before all tests"""
        cls.test_db = "test_quant.db"
        # Remove test db if it exists
        if os.path.exists(cls.test_db):
            os.remove(cls.test_db)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests"""
        if os.path.exists(cls.test_db):
            os.remove(cls.test_db)
    
    def setUp(self):
        """Set up before each test"""
        self.manager = DatabaseManager(self.test_db)
    
    def tearDown(self):
        """Clean up after each test"""
        if self.manager.connection:
            self.manager.close()
    
    def test_database_connection(self):
        """Test that we can connect to database"""
        connection = self.manager.connect()
        self.assertIsNotNone(connection)
        self.assertIsInstance(connection, sqlite3.Connection)
    
    def test_database_initialization(self):
        """Test that database is initialized with all tables"""
        result = self.manager.initialize()
        self.assertTrue(result)
        
        # Check that key tables exist
        expected_tables = [
            'cryptocurrencies',
            'crypto_prices',
            'google_trends',
            'features',
            'results',
            'metadata'
        ]
        
        for table in expected_tables:
            self.assertTrue(
                self.manager.table_exists(table),
                f"Table {table} was not created"
            )
    
    def test_table_exists_method(self):
        """Test the table_exists method"""
        self.manager.initialize()
        
        # Should find existing table
        self.assertTrue(self.manager.table_exists('cryptocurrencies'))
        
        # Should not find non-existing table
        self.assertFalse(self.manager.table_exists('nonexistent_table'))
    
    def test_get_row_count(self):
        """Test row count retrieval"""
        self.manager.initialize()
        
        # Empty table should have 0 rows
        count = self.manager.get_row_count('cryptocurrencies')
        self.assertEqual(count, 0)
    
    def test_crypto_prices_table_structure(self):
        """Test that crypto_prices table has required columns"""
        self.manager.initialize()
        conn = self.manager.connection
        cursor = conn.cursor()
        
        # Get table info
        cursor.execute("PRAGMA table_info(crypto_prices)")
        columns = {col[1] for col in cursor.fetchall()}
        
        required_columns = {'date', 'ticker', 'symbol', 'open', 'high', 'low', 'close', 'volume'}
        self.assertTrue(required_columns.issubset(columns))
    
    def test_google_trends_table_structure(self):
        """Test that google_trends table has required columns"""
        self.manager.initialize()
        conn = self.manager.connection
        cursor = conn.cursor()
        
        cursor.execute("PRAGMA table_info(google_trends)")
        columns = {col[1] for col in cursor.fetchall()}
        
        required_columns = {'date', 'symbol', 'search_volume', 'normalized_volume'}
        self.assertTrue(required_columns.issubset(columns))
    
    def test_features_table_structure(self):
        """Test that features table has all required columns"""
        self.manager.initialize()
        conn = self.manager.connection
        cursor = conn.cursor()
        
        cursor.execute("PRAGMA table_info(features)")
        columns = {col[1] for col in cursor.fetchall()}
        
        required_columns = {
            'date', 'ticker', 'symbol', 'is_spike',
            'return_1d', 'return_3d', 'return_5d', 'return_7d'
        }
        self.assertTrue(required_columns.issubset(columns))


if __name__ == '__main__':
    unittest.main()
