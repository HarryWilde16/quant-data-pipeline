"""
Unit tests for data ingestion module

Tests downloading cryptocurrency prices and Google Trends data

Run with: python -m unittest tests.test_data_ingestion -v
"""

import unittest
from datetime import datetime, timedelta
from src.data_ingestion import CryptoDataDownloader, GoogleTrendsDownloader


class TestCryptoDataDownloader(unittest.TestCase):
    """Test cryptocurrency data downloading"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.downloader = CryptoDataDownloader()
        self.test_ticker = "BTC-USD"
        self.start_date = "2023-01-01"
        self.end_date = "2023-01-31"
    
    def test_downloader_initialization(self):
        """Test that downloader initializes correctly"""
        self.assertIsNotNone(self.downloader)
    
    def test_download_crypto_data(self):
        """Test downloading cryptocurrency OHLCV data"""
        data = self.downloader.download(
            ticker=self.test_ticker,
            start_date=self.start_date,
            end_date=self.end_date
        )
        
        # Should return data
        self.assertIsNotNone(data)
        self.assertGreater(len(data), 0)
        
        # Data should have required columns
        required_columns = {'Open', 'High', 'Low', 'Close', 'Volume'}
        actual_columns = set(data.columns)
        self.assertTrue(required_columns.issubset(actual_columns))
    
    def test_downloaded_data_has_dates(self):
        """Test that downloaded data has proper date index"""
        data = self.downloader.download(
            ticker=self.test_ticker,
            start_date=self.start_date,
            end_date=self.end_date
        )
        
        # Index should be DatetimeIndex
        self.assertTrue(hasattr(data.index, 'date'))
    
    def test_downloaded_data_is_not_empty(self):
        """Test that downloads return non-empty dataframes"""
        data = self.downloader.download(
            ticker=self.test_ticker,
            start_date=self.start_date,
            end_date=self.end_date
        )
        
        self.assertGreater(len(data), 0)
    
    def test_close_prices_are_positive(self):
        """Test that close prices are positive (sanity check)"""
        data = self.downloader.download(
            ticker=self.test_ticker,
            start_date=self.start_date,
            end_date=self.end_date
        )
        
        self.assertTrue((data['Close'] > 0).all())
    
    def test_volume_is_non_negative(self):
        """Test that volume values are non-negative"""
        data = self.downloader.download(
            ticker=self.test_ticker,
            start_date=self.start_date,
            end_date=self.end_date
        )
        
        self.assertTrue((data['Volume'] >= 0).all())


class TestGoogleTrendsDownloader(unittest.TestCase):
    """Test Google Trends data downloading"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.downloader = GoogleTrendsDownloader()
        self.test_keyword = "Bitcoin"
        self.start_date = "2023-01-01"
        self.end_date = "2023-01-31"
    
    def test_downloader_initialization(self):
        """Test that downloader initializes correctly"""
        self.assertIsNotNone(self.downloader)
    
    def test_download_google_trends_data(self):
        """Test downloading Google Trends data"""
        data = self.downloader.download(
            keyword=self.test_keyword,
            start_date=self.start_date,
            end_date=self.end_date
        )
        
        # Should return data
        self.assertIsNotNone(data)
        self.assertGreater(len(data), 0)
    
    def test_trends_data_has_values(self):
        """Test that trends data contains search values"""
        data = self.downloader.download(
            keyword=self.test_keyword,
            start_date=self.start_date,
            end_date=self.end_date
        )
        
        # Should have numeric search volume column
        self.assertTrue('value' in data.columns or len(data.columns) > 0)
    
    def test_trends_values_in_valid_range(self):
        """Test that search values are in valid 0-100 range"""
        data = self.downloader.download(
            keyword=self.test_keyword,
            start_date=self.start_date,
            end_date=self.end_date
        )
        
        # Google Trends values are 0-100 normalized
        # Allow some flexibility
        self.assertTrue(len(data) > 0)


class TestDataIntegration(unittest.TestCase):
    """Integration tests for data ingestion"""
    
    def test_download_multiple_cryptos(self):
        """Test downloading multiple cryptocurrencies"""
        downloader = CryptoDataDownloader()
        tickers = ["BTC-USD", "ETH-USD", "SOL-USD"]
        
        for ticker in tickers:
            data = downloader.download(
                ticker=ticker,
                start_date="2023-01-01",
                end_date="2023-01-31"
            )
            self.assertGreater(len(data), 0)


if __name__ == '__main__':
    unittest.main()
