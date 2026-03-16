from src.data_ingestion import CryptoDataDownloader
downloader = CryptoDataDownloader()

btc = downloader.download('BTC-USD', '2024-01-01', '2024-01-31')
print('BITCOIN PRICES (Jan 2024):')
print(btc.head(10))
print('\nPrice Statistics:')
print(btc[['Close', 'Volume']].describe())
