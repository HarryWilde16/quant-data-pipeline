# API Documentation

## Core Modules

### data_ingestion.py
Functions for downloading and caching market data

### data_processing.py
Functions for cleaning, validating, and transforming data

### analysis.py
Functions for factor research and backtesting

### utils.py
Utility functions and helpers

## Configuration

Configuration is managed through environment variables and config files in the `config/` directory.

Required environment variables:
- `QUANT_DATA_DIR` - Path to data directory
- `DATABASE_URL` - PostgreSQL connection string
