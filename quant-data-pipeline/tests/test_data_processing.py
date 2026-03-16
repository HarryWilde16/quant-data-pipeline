"""
Unit tests for data processing module

Tests data alignment, cleaning, and validation

Run with: python -m unittest tests.test_data_processing -v
"""

import unittest
import pandas as pd
from datetime import datetime
from src.data_processing import DataProcessor


class TestDataAlignment(unittest.TestCase):
    """Test data alignment between prices and trends"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.processor = DataProcessor()
        
        # Create sample price data (30 days)
        self.prices = pd.DataFrame({
            'ticker': ['BTC-USD'] * 30,
            'symbol': ['BTC'] * 30,
            'close': [40000 + i * 100 for i in range(30)],
            'volume': [10000000 + i * 100000 for i in range(30)]
        }, index=pd.date_range('2024-01-01', periods=30))
        self.prices.index.name = 'date'
        
        # Create sample trends data (31 days)
        self.trends = pd.DataFrame({
            'search_volume': [30 + i for i in range(31)]
        }, index=pd.date_range('2024-01-01', periods=31))
        self.trends.index.name = 'date'
    
    def test_align_to_common_dates(self):
        """Test that data is aligned to common dates"""
        aligned_prices, aligned_trends = self.processor.align_data(
            self.prices, self.trends
        )
        
        # Both should have same length
        self.assertEqual(len(aligned_prices), len(aligned_trends))
        
        # Both should have same date range
        self.assertTrue((aligned_prices.index == aligned_trends.index).all())
    
    def test_aligned_data_not_empty(self):
        """Test that aligned data is not empty"""
        aligned_prices, aligned_trends = self.processor.align_data(
            self.prices, self.trends
        )
        
        self.assertGreater(len(aligned_prices), 0)
        self.assertGreater(len(aligned_trends), 0)
    
    def test_no_data_loss_in_alignment(self):
        """Test that we keep max possible overlapping dates"""
        aligned_prices, aligned_trends = self.processor.align_data(
            self.prices, self.trends
        )
        
        # Should keep the 30 overlapping days (min of 30 and 31)
        self.assertEqual(len(aligned_prices), 30)


class TestDataCleaning(unittest.TestCase):
    """Test data cleaning operations"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.processor = DataProcessor()
    
    def test_handle_missing_values(self):
        """Test handling of missing values"""
        df = pd.DataFrame({
            'close': [100, None, 102, None, 104],
            'volume': [1000, 2000, None, 4000, 5000]
        })
        
        cleaned = self.processor.clean_data(df)
        
        # Should have no NaN values
        self.assertEqual(cleaned.isnull().sum().sum(), 0)
    
    def test_remove_outliers(self):
        """Test outlier detection and clipping"""
        df = pd.DataFrame({
            'close': [100, 101, 102, 1000, 103, 104, 105],  # 1000 is outlier
        })
        
        cleaned = self.processor.clean_data(df, handle_outliers=True)
        
        # Outlier should be clipped to reasonable range
        # (IQR method clips rather than removes)
        self.assertLess(cleaned['close'].max(), 1000)
    
    def test_normalized_data_structure(self):
        """Test that cleaned data has expected structure"""
        df = pd.DataFrame({
            'close': [100, 101, 102],
            'volume': [1000, 2000, 3000],
            'search_volume': [50, 55, 60]
        })
        
        cleaned = self.processor.clean_data(df)
        
        # Should still have all columns
        self.assertIn('close', cleaned.columns)
        self.assertIn('volume', cleaned.columns)
        self.assertIn('search_volume', cleaned.columns)


class TestDataValidation(unittest.TestCase):
    """Test data validation checks"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.processor = DataProcessor()
    
    def test_validate_price_column(self):
        """Test that prices are positive"""
        valid_df = pd.DataFrame({
            'close': [100, 101, 102]
        })
        
        result = self.processor.validate_data(valid_df, 'close')
        self.assertTrue(result)
    
    def test_validate_negative_prices_fail(self):
        """Test that negative prices fail validation"""
        invalid_df = pd.DataFrame({
            'close': [100, -50, 102]  # Negative price
        })
        
        result = self.processor.validate_data(invalid_df, 'close')
        self.assertFalse(result)
    
    def test_validate_volume_non_negative(self):
        """Test that volume is non-negative"""
        valid_df = pd.DataFrame({
            'volume': [1000, 2000, 0]  # 0 is valid
        })
        
        result = self.processor.validate_data(valid_df, 'volume', allow_zero=True)
        self.assertTrue(result)
    
    def test_validate_search_volume_in_range(self):
        """Test that search volume is 0-100"""
        valid_df = pd.DataFrame({
            'search_volume': [0, 50, 100]
        })
        
        result = self.processor.validate_data_range(valid_df, 'search_volume', 0, 100)
        self.assertTrue(result)
    
    def test_validate_search_volume_out_of_range_fails(self):
        """Test that out-of-range search volume fails"""
        invalid_df = pd.DataFrame({
            'search_volume': [50, 150, 100]  # 150 is out of range
        })
        
        result = self.processor.validate_data_range(invalid_df, 'search_volume', 0, 100)
        self.assertFalse(result)


class TestDataMerging(unittest.TestCase):
    """Test merging of price and trends data"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.processor = DataProcessor()
        
        # Price data
        self.prices = pd.DataFrame({
            'close': [100, 101, 102],
            'volume': [1000, 2000, 3000]
        }, index=pd.date_range('2024-01-01', periods=3))
        self.prices.index.name = 'date'
        
        # Trends data
        self.trends = pd.DataFrame({
            'search_volume': [50, 55, 60]
        }, index=pd.date_range('2024-01-01', periods=3))
        self.trends.index.name = 'date'
    
    def test_merge_price_and_trends(self):
        """Test merging price and trends dataframes"""
        merged = self.processor.merge_data(self.prices, self.trends)
        
        # Should have columns from both
        self.assertIn('close', merged.columns)
        self.assertIn('search_volume', merged.columns)
        
        # Should have same length
        self.assertEqual(len(merged), 3)
    
    def test_merged_data_has_no_gaps(self):
        """Test that merged data has no missing values"""
        merged = self.processor.merge_data(self.prices, self.trends)
        
        self.assertEqual(merged.isnull().sum().sum(), 0)


if __name__ == '__main__':
    unittest.main()
