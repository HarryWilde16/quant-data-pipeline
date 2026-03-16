# Quant Data Pipeline

## Project Goal & Research Hypothesis

**Hypothesis:** Cryptocurrency prices and trading volumes spike 1-7 days after Google search trends show increased interest in the coin.

**Coins Analyzed:** BTC, ETH, XRP, SOL, ADA, TAO, DOGE, LINK, HBAR, SUI

**Intuition:** Retail investors use Google to research cryptocurrencies before buying. A spike in Google searches signals incoming retail demand, causing price/volume increases within days.

This pipeline demonstrates:
- **Software Engineering Excellence** - Modular, production-ready code
- **Data Engineering Fundamentals** - ETL workflows, data validation, scalability
- **Quantitative Expertise** - Hypothesis testing, factor research, signal analysis
- **Problem Solving** - Multi-source data integration (Google Trends + market data)

## Architecture Overview

The pipeline follows a modular, production-ready architecture:

```
Data Sources → Ingestion → Processing → Analysis → Output
```

Each stage is separated into distinct modules for maintainability and scalability.

## Project Structure

```
quant-data-pipeline/
├── src/              # Core pipeline modules
│   ├── data_ingestion.py
│   ├── data_processing.py
│   ├── analysis.py
│   └── utils.py
├── data/             # Data storage (local cache)
│   ├── raw/          # Raw downloaded data
│   ├── processed/    # Cleaned and processed data
│   └── backtest/     # Backtest results
├── notebooks/        # Jupyter notebooks for exploration and documentation
├── config/           # Configuration files (API keys, parameters)
├── database/         # Database schemas and migration scripts
├── tests/            # Unit and integration tests
├── docs/             # Project documentation
├── requirements.txt  # Python dependencies
└── README.md         # This file
```

## Data Sources

- **Yahoo Finance (yfinance):** OHLCV data + volume for cryptocurrencies (BTC-USD, ETH-USD, etc.)
- **Google Trends (pytrends):** Search volume for cryptocurrency terms
- **Storage:** SQLite database (lightweight, zero-setup, perfect for research)

## Pipeline Stages

### Stage 1: Data Ingestion
- Download cryptocurrency OHLCV data from Yahoo Finance
- Download Google search trends for each coin
- Handle API rate limiting and errors
- Store raw data in SQLite

### Stage 2: Data Processing
- Align cryptocurrency prices with Google Trends data (daily frequency)
- Clean missing values and outliers
- Handle timezone alignment

### Stage 3: Feature Engineering
- Calculate price/volume changes
- Detect search volume spikes (> 90th percentile)
- Create 1-7 day forward returns
- Normalize features

### Stage 4: Analysis & Hypothesis Testing
- Compare returns on spike days vs normal days
- Calculate significance (t-tests, p-values)
- Analyze lag effects (1 day, 3 days, 5 days, 7 days)
- Generate research report with findings

## Setup & Installation

### Prerequisites
- Python 3.11+
- Conda (recommended)
- SQLite (built-in with Python)

### Installation

1. Create conda environment
```bash
conda create -n quant python=3.11
conda activate quant
```

2. Install dependencies
```bash
pip install -r requirements.txt
```

3. Verify setup
```bash
python src/test_setup.py
```

## Development Status

### Phase 1: Foundation ✓
- Environment setup and package management
- Professional project structure
- Git repository initialized
- Comprehensive documentation

### Phase 2: Data Ingestion (In Progress)
- Cryptocurrency data download (yfinance)
- Google Trends data collection (pytrends)
- SQLite schema design
- Rate limiting and error handling
- Project repository structure
- Basic data ingestion test

### In Progress
- Core pipeline modules
- Data processing functions
- Alpha factor research

### Planned
- PostgreSQL integration
- Backtesting engine
- Performance analytics
- Documentation notebooks

## Contributing

Follow these conventions:
- Use snake_case for functions and variables
- Document functions with docstrings
- Add unit tests for new features
- Keep configuration in `config/` folder

## References & Resources

- Yahoo Finance API: https://finance.yahoo.com/
- Quantitative Finance Textbooks: "Advances in Financial Machine Learning" (López de Prado)
- Research Papers: SSRN, arXiv
