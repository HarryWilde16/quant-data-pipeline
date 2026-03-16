"""
Data Processing Module

Handles:
- Aligning cryptocurrency prices with Google Trends data
- Cleaning missing values and outliers
- Validating data quality
- Merging and storing processed data
"""

import pandas as pd
import numpy as np
from typing import Tuple, Optional
import logging


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataProcessor:
    """Process cryptocurrency and trends data for analysis"""
    
    def __init__(self):
        """Initialize data processor"""
        self.aligned_data = None
    
    @staticmethod
    def align_data(
        prices_df: pd.DataFrame,
        trends_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Align price and trends data to common dates
        
        Args:
            prices_df: Cryptocurrency price DataFrame (index = date)
            trends_df: Google Trends DataFrame (index = date)
            
        Returns:
            Tuple of (aligned_prices, aligned_trends) with matching dates
        """
        try:
            # Get common dates
            common_dates = prices_df.index.intersection(trends_df.index)
            
            if len(common_dates) == 0:
                logger.warning("No overlapping dates between prices and trends")
                return pd.DataFrame(), pd.DataFrame()
            
            # Align to common dates
            prices_aligned = prices_df.loc[common_dates].copy()
            trends_aligned = trends_df.loc[common_dates].copy()
            
            logger.info(f"✓ Aligned {len(common_dates)} common dates")
            return prices_aligned, trends_aligned
        
        except Exception as e:
            logger.error(f"Error aligning data: {e}")
            return pd.DataFrame(), pd.DataFrame()
    
    @staticmethod
    def clean_data(
        df: pd.DataFrame,
        handle_outliers: bool = True,
        fill_method: str = 'forward'
    ) -> pd.DataFrame:
        """
        Clean data by handling missing values and outliers
        
        Args:
            df: DataFrame to clean
            handle_outliers: Whether to remove/flag outliers
            fill_method: Method to fill missing values ('forward', 'backward', 'drop')
            
        Returns:
            Cleaned DataFrame
        """
        try:
            df_clean = df.copy()
            
            # Handle missing values
            if fill_method == 'forward':
                df_clean = df_clean.ffill().bfill()
            elif fill_method == 'backward':
                df_clean = df_clean.bfill().ffill()
            elif fill_method == 'drop':
                df_clean = df_clean.dropna()
            else:
                logger.warning(f"Unknown fill method: {fill_method}")
            
            # Handle outliers using IQR method
            if handle_outliers:
                numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
                
                for col in numeric_cols:
                    Q1 = df_clean[col].quantile(0.25)
                    Q3 = df_clean[col].quantile(0.75)
                    IQR = Q3 - Q1
                    
                    lower_bound = Q1 - 3 * IQR  # 3 * IQR for more tolerance
                    upper_bound = Q3 + 3 * IQR
                    
                    # Replace outliers with median
                    median = df_clean[col].median()
                    df_clean[col] = df_clean[col].clip(lower_bound, upper_bound)
                    
                    outlier_count = ((df_clean[col] < lower_bound) | 
                                   (df_clean[col] > upper_bound)).sum()
                    
                    if outlier_count > 0:
                        logger.info(f"  Cleaned {outlier_count} outliers in {col}")
            
            logger.info(f"✓ Data cleaned ({len(df_clean)} rows)")
            return df_clean
        
        except Exception as e:
            logger.error(f"Error cleaning data: {e}")
            return df
    
    @staticmethod
    def validate_data(
        df: pd.DataFrame,
        column: str,
        allow_zero: bool = False,
        allow_negative: bool = False
    ) -> bool:
        """
        Validate that a column meets criteria
        
        Args:
            df: DataFrame to validate
            column: Column name to validate
            allow_zero: Whether zero values are allowed
            allow_negative: Whether negative values are allowed
            
        Returns:
            True if valid, False otherwise
        """
        if column not in df.columns:
            logger.error(f"Column {column} not found in DataFrame")
            return False
        
        values = df[column]
        
        # Check for NaN
        if values.isnull().any():
            logger.warning(f"Column {column} contains NaN values")
            return False
        
        # Check for zeros
        if not allow_zero and (values == 0).any():
            logger.warning(f"Column {column} contains zero values")
            return False
        
        # Check for negative
        if not allow_negative and (values < 0).any():
            logger.error(f"Column {column} contains negative values")
            return False
        
        logger.info(f"✓ Validation passed for {column}")
        return True
    
    @staticmethod
    def validate_data_range(
        df: pd.DataFrame,
        column: str,
        min_val: float,
        max_val: float
    ) -> bool:
        """
        Validate that column values are within range
        
        Args:
            df: DataFrame to validate
            column: Column name
            min_val: Minimum allowed value
            max_val: Maximum allowed value
            
        Returns:
            True if all values in range, False otherwise
        """
        if column not in df.columns:
            logger.error(f"Column {column} not found")
            return False
        
        values = df[column]
        out_of_range = ((values < min_val) | (values > max_val)).sum()
        
        if out_of_range > 0:
            logger.error(f"Column {column} has {out_of_range} values out of range [{min_val}, {max_val}]")
            return False
        
        logger.info(f"✓ Range validation passed for {column} [{min_val}, {max_val}]")
        return True
    
    @staticmethod
    def merge_data(
        prices_df: pd.DataFrame,
        trends_df: pd.DataFrame,
        how: str = 'inner'
    ) -> pd.DataFrame:
        """
        Merge price and trends data
        
        Args:
            prices_df: Price DataFrame
            trends_df: Trends DataFrame
            how: Join method ('inner', 'outer', 'left', 'right')
            
        Returns:
            Merged DataFrame
        """
        try:
            # Rename trends column to avoid conflicts
            trends_renamed = trends_df.copy()
            
            # Merge on date index
            merged = pd.merge(
                prices_df,
                trends_renamed,
                left_index=True,
                right_index=True,
                how=how
            )
            
            logger.info(f"✓ Data merged ({len(merged)} rows)")
            return merged
        
        except Exception as e:
            logger.error(f"Error merging data: {e}")
            return pd.DataFrame()
    
    @staticmethod
    def flatten_multiindex_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Flatten MultiIndex columns from yfinance data
        
        Args:
            df: DataFrame with potentially MultiIndex columns
            
        Returns:
            DataFrame with flattened columns
        """
        if isinstance(df.columns, pd.MultiIndex):
            # Flatten: use first level if exists
            df.columns = [col[0] for col in df.columns]
        
        # Convert to lowercase for consistency
        df.columns = [col.lower() for col in df.columns]
        return df
    
    def process_pipeline(
        self,
        prices_df: pd.DataFrame,
        trends_df: pd.DataFrame
    ) -> Optional[pd.DataFrame]:
        """
        Execute complete data processing pipeline
        
        Steps:
        1. Flatten MultiIndex columns
        2. Align data to common dates
        3. Merge price and trends
        4. Clean data
        5. Validate quality
        
        Args:
            prices_df: Price data from Yahoo Finance
            trends_df: Trends data from Google Trends
            
        Returns:
            Processed DataFrame ready for feature engineering
        """
        logger.info("=" * 60)
        logger.info("DATA PROCESSING PIPELINE")
        logger.info("=" * 60)
        
        # Step 0: Flatten MultiIndex columns
        logger.info("\n[Step 0] Preparing data (flattening MultiIndex columns)...")
        prices_df = self.flatten_multiindex_columns(prices_df)
        trends_df = self.flatten_multiindex_columns(trends_df)
        
        # Step 1: Align
        logger.info("\n[Step 1] Aligning data to common dates...")
        prices_aligned, trends_aligned = self.align_data(prices_df, trends_df)
        
        if len(prices_aligned) == 0:
            logger.error("Failed to align data")
            return None
        
        # Step 3: Merge
        logger.info("\n[Step 3] Merging price and trends data...")
        merged = self.merge_data(prices_aligned, trends_aligned)
        
        if merged.empty:
            logger.error("Failed to merge data")
            return None
        
        # Step 4: Clean
        logger.info("\n[Step 4] Cleaning data...")
        cleaned = self.clean_data(merged)
        
        # Step 5: Validate
        logger.info("\n[Step 5] Validating data quality...")
        
        # Validate key columns
        valid = True
        
        # Check prices
        if 'close' in cleaned.columns:
            valid = valid and self.validate_data(cleaned, 'close')
        
        if 'volume' in cleaned.columns:
            valid = valid and self.validate_data(cleaned, 'volume', allow_zero=True)
        
        # Check trends
        if 'search_volume' in cleaned.columns or 'value' in cleaned.columns:
            trends_col = 'search_volume' if 'search_volume' in cleaned.columns else 'value'
            valid = valid and self.validate_data_range(cleaned, trends_col, 0, 100)
        
        logger.info("\n" + "=" * 60)
        if valid:
            logger.info("✓ PIPELINE COMPLETE - Data is ready for feature engineering")
        else:
            logger.warning("⚠ Pipeline completed with validation warnings")
        logger.info("=" * 60)
        
        self.aligned_data = cleaned
        return cleaned


def process_crypto_trends(
    prices: pd.DataFrame,
    trends: pd.DataFrame
) -> Optional[pd.DataFrame]:
    """
    Convenience function to process cryptocurrency and trends data
    
    Args:
        prices: Price DataFrame from yfinance
        trends: Trends DataFrame from pytrends
        
    Returns:
        Processed DataFrame
    """
    processor = DataProcessor()
    return processor.process_pipeline(prices, trends)
