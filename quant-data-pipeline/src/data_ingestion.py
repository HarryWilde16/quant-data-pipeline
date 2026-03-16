"""
Data ingestion module for cryptocurrency prices and Google Trends

Handles downloading data from:
- Yahoo Finance (yfinance) for cryptocurrency OHLCV data
- Google Trends (pytrends) for search volume data
"""

import yfinance as yf
import pandas as pd
from pytrends.request import TrendReq
from typing import Optional
import time


class CryptoDataDownloader:
    """Download cryptocurrency OHLCV data from Yahoo Finance"""
    
    def __init__(self):
        """Initialize cryptocurrency data downloader"""
        pass
    
    def download(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
        progress: bool = False
    ) -> pd.DataFrame:
        """
        Download cryptocurrency OHLCV data
        
        Args:
            ticker: Cryptocurrency ticker (e.g., "BTC-USD")
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            progress: Show progress bar
            
        Returns:
            DataFrame with OHLCV data
        """
        try:
            data = yf.download(
                ticker,
                start=start_date,
                end=end_date,
                progress=progress
            )
            
            if len(data) == 0:
                print(f"⚠ No data found for {ticker}")
                return pd.DataFrame()
            
            print(f"✓ Downloaded {len(data)} rows for {ticker}")
            return data
        
        except Exception as e:
            print(f"✗ Error downloading {ticker}: {e}")
            return pd.DataFrame()
    
    def validate_data(self, data: pd.DataFrame) -> bool:
        """
        Validate downloaded data
        
        Args:
            data: DataFrame to validate
            
        Returns:
            True if valid, False otherwise
        """
        if data.empty:
            return False
        
        required_columns = {'Open', 'High', 'Low', 'Close', 'Volume'}
        if not required_columns.issubset(set(data.columns)):
            return False
        
        # Check for negative prices
        if (data['Close'] <= 0).any():
            return False
        
        return True


class GoogleTrendsDownloader:
    """Download Google Trends search volume data"""
    
    def __init__(self):
        """Initialize Google Trends downloader"""
        self.pytrends = TrendReq(hl='en-US', tz=360)
        self.max_retries = 3
        self.retry_delay = 2  # seconds
    
    def download(
        self,
        keyword: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        Download Google Trends data
        
        Args:
            keyword: Search term (e.g., "Bitcoin")
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            
        Returns:
            DataFrame with search volume data
        """
        for attempt in range(self.max_retries):
            try:
                # Build timeframe string for pytrends
                timeframe = f"{start_date} {end_date}"
                
                # Download trends
                self.pytrends.build_payload(
                    [keyword],
                    timeframe=timeframe,
                    geo=''
                )
                
                # Get interest over time
                data = self.pytrends.interest_over_time()
                data = data.drop('isPartial', axis=1)  # Remove partial column
                data = data.rename(columns={keyword: 'value'})
                
                print(f"✓ Downloaded Google Trends for '{keyword}'")
                return data
            
            except Exception as e:
                print(f"⚠ Attempt {attempt + 1} failed for '{keyword}': {e}")
                
                if attempt < self.max_retries - 1:
                    print(f"  Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    print(f"✗ Failed to download Google Trends for '{keyword}'")
                    return pd.DataFrame()
        
        return pd.DataFrame()
    
    def validate_data(self, data: pd.DataFrame) -> bool:
        """
        Validate Google Trends data
        
        Args:
            data: DataFrame to validate
            
        Returns:
            True if valid, False otherwise
        """
        if data.empty:
            return False
        
        # Check for required column
        if 'value' not in data.columns:
            return False
        
        # Google Trends values should be 0-100
        if (data['value'] < 0).any() or (data['value'] > 100).any():
            return False
        
        return True


class DataIngestionPipeline:
    """Orchestrate data ingestion for crypto and Google Trends"""
    
    def __init__(self):
        """Initialize pipeline"""
        self.crypto_downloader = CryptoDataDownloader()
        self.trends_downloader = GoogleTrendsDownloader()
    
    def download_cryptocurrency_data(
        self,
        ticker: str,
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        Download and validate cryptocurrency data
        
        Args:
            ticker: Cryptocurrency ticker
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            Valid DataFrame or None
        """
        data = self.crypto_downloader.download(ticker, start_date, end_date)
        
        if self.crypto_downloader.validate_data(data):
            return data
        
        return None
    
    def download_google_trends_data(
        self,
        keyword: str,
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        Download and validate Google Trends data
        
        Args:
            keyword: Search keyword
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            Valid DataFrame or None
        """
        data = self.trends_downloader.download(keyword, start_date, end_date)
        
        if self.trends_downloader.validate_data(data):
            return data
        
        return None
