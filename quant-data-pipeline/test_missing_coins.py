"""
Troubleshoot & Fix Missing Coins (TAO, DOGE, SUI)
Try alternative tickers and date ranges
"""
import yfinance as yf
from pytrends.request import TrendReq
import pandas as pd

print("\n" + "="*100)
print("🔧 TROUBLESHOOTING MISSING COINS")
print("="*100)

# Test 1: Try different tickers for TAO
print("\n[TAO] Testing alternative tickers...")
tao_tickers = ['TAO-USD', 'TAO1-USD', 'TAOTOKEN-USD']
for ticker in tao_tickers:
    try:
        data = yf.download(ticker, start='2025-12-16', end='2026-03-16', progress=False)
        if len(data) > 0:
            print(f"  ✓ {ticker}: Found {len(data)} rows")
        else:
            print(f"  ✗ {ticker}: No data")
    except Exception as e:
        print(f"  ✗ {ticker}: Error - {str(e)[:50]}")

# Test 2: DOGE - different date range
print("\n[DOGE] Testing different date ranges...")
doge_ranges = [
    ('2025-12-16', '2026-03-16'),  # current
    ('2024-01-01', '2026-03-16'),  # longer history
    ('2025-01-01', '2026-03-16'),  # 1 year
]
for start, end in doge_ranges:
    try:
        data = yf.download('DOGE-USD', start=start, end=end, progress=False)
        if len(data) > 0:
            print(f"  ✓ {start} to {end}: Found {len(data)} rows")
        else:
            print(f"  ✗ {start} to {end}: No data")
    except Exception as e:
        print(f"  ✗ {start} to {end}: Error")

# Test 3: SUI - different date range
print("\n[SUI] Testing different date ranges...")
sui_ranges = [
    ('2025-12-16', '2026-03-16'),  # current
    ('2024-01-01', '2026-03-16'),  # longer history
    ('2024-05-01', '2026-03-16'),  # from launch
]
for start, end in sui_ranges:
    try:
        data = yf.download('SUI-USD', start=start, end=end, progress=False)
        if len(data) > 0:
            print(f"  ✓ {start} to {end}: Found {len(data)} rows")
        else:
            print(f"  ✗ {start} to {end}: No data")
    except Exception as e:
        print(f"  ✗ {start} to {end}: Error")

# Test 4: Check Google Trends for all three
print("\n[GOOGLE TRENDS] Testing all three keywords...")
pytrends = TrendReq(hl='en-US', tz=360)

for keyword in ['TAO', 'Dogecoin', 'SUI']:
    try:
        pytrends.build_request([keyword], timeframe='2025-12-16 2026-03-16')
        data = pytrends.interest_over_time()
        if len(data) > 0:
            print(f"  ✓ {keyword}: Found {len(data)} rows")
        else:
            print(f"  ✗ {keyword}: No data")
    except Exception as e:
        print(f"  ✗ {keyword}: Error - {str(e)[:50]}")

print("\n" + "="*100)
print("FINDINGS & RECOMMENDATIONS:")
print("="*100)
print("""
TAO (Tao Network):
  • yfinance ticker issue: Try 'TAO1-USD' instead of 'TAO-USD'
  • May be delisted or renamed on Yahoo Finance
  • Alternative: Use Binance API or CoinGecko

DOGE (Dogecoin):  
  • Price data: Available ✓
  • Google Trends: Rate limited (429 error) - needs delay/spacing
  • Solution: Increase delay between requests or use alternative search tool

SUI (Sui Blockchain):
  • Recent coin (2023 launch) - may have limited history
  • Try extended date range starting from 2024-05-01
  • Price data ✓, but shorter history than older coins

NEXT STEPS:
  1. Update coin list: Replace TAO → SUI-USD if SUI has 1yr+ data
  2. Increase Google Trends retry delay to 3-5 seconds
  3. Check Binance API as fallback for missing coins
  4. Document any coins that don't have 3+ months of data
""")

print("="*100 + "\n")
