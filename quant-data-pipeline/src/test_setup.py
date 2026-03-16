import yfinance as yf

print("Environment working")

data = yf.download("AAPL", start="2020-01-01")

print(data.head())