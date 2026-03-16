"""
SQLite Database Schema for Crypto Google Trends Analysis

This file defines the database structure for storing:
1. Cryptocurrency OHLCV data from Yahoo Finance
2. Google search trends data
3. Calculated features
4. Analysis results
"""

# Table 1: crypto_prices
# Stores OHLCV (Open, High, Low, Close, Volume) data
crypto_prices_schema = """
CREATE TABLE IF NOT EXISTS crypto_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    ticker TEXT NOT NULL,
    symbol TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL,
    UNIQUE(date, ticker),
    FOREIGN KEY(ticker) REFERENCES cryptocurrencies(ticker)
);
CREATE INDEX IF NOT EXISTS idx_crypto_prices_date_ticker ON crypto_prices(date, ticker);
"""

# Table 2: google_trends
# Stores normalized Google search volume for each cryptocurrency
google_trends_schema = """
CREATE TABLE IF NOT EXISTS google_trends (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    search_volume INTEGER NOT NULL,
    normalized_volume REAL,
    UNIQUE(date, symbol),
    FOREIGN KEY(symbol) REFERENCES cryptocurrencies(symbol)
);
CREATE INDEX IF NOT EXISTS idx_google_trends_date_symbol ON google_trends(date, symbol);
"""

# Table 3: features
# Stores calculated features for analysis
features_schema = """
CREATE TABLE IF NOT EXISTS features (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    ticker TEXT NOT NULL,
    symbol TEXT NOT NULL,
    
    -- Google Trends features
    search_volume INTEGER,
    normalized_search REAL,
    is_spike INTEGER,  -- 1 if above 90th percentile, 0 otherwise
    
    -- Price/Volume metrics
    daily_return REAL,
    volume_change REAL,
    high_low_range REAL,
    
    -- Forward returns (for lag analysis)
    return_1d REAL,  -- Next day return
    return_3d REAL,  -- 3-day return
    return_5d REAL,  -- 5-day return
    return_7d REAL,  -- 7-day return
    
    volume_1d REAL,  -- Next day volume
    volume_3d REAL,
    
    UNIQUE(date, ticker),
    FOREIGN KEY(ticker) REFERENCES cryptocurrencies(ticker)
);
CREATE INDEX IF NOT EXISTS idx_features_date_ticker ON features(date, ticker);
CREATE INDEX IF NOT EXISTS idx_features_spike ON features(is_spike);
"""

# Table 4: cryptocurrencies
# Reference table for cryptocurrency metadata
cryptocurrencies_schema = """
CREATE TABLE IF NOT EXISTS cryptocurrencies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT UNIQUE NOT NULL,
    ticker TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# Table 5: results
# Stores final analysis results
results_schema = """
CREATE TABLE IF NOT EXISTS results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    lag_days INTEGER NOT NULL,
    
    -- Spike day statistics
    spike_day_mean_return REAL,
    spike_day_std_return REAL,
    spike_day_count INTEGER,
    
    -- Normal day statistics
    normal_day_mean_return REAL,
    normal_day_std_return REAL,
    normal_day_count INTEGER,
    
    -- Statistical test results
    return_difference REAL,
    t_statistic REAL,
    p_value REAL,
    is_significant INTEGER,  -- 1 if p_value < 0.05
    
    -- Volume metrics
    spike_day_mean_volume REAL,
    normal_day_mean_volume REAL,
    volume_difference REAL,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, lag_days)
);
"""

# Metadata table
metadata_schema = """
CREATE TABLE IF NOT EXISTS metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT UNIQUE NOT NULL,
    value TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# All schemas combined for easy execution
ALL_SCHEMAS = [
    cryptocurrencies_schema,
    crypto_prices_schema,
    google_trends_schema,
    features_schema,
    results_schema,
    metadata_schema
]
