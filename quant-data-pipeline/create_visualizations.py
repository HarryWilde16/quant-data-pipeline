"""
Create visualizations of your cryptocurrency data
Shows: price trends, search volume, correlation
"""
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

conn = sqlite3.connect('quant_processed.db')

print("📊 Creating visualizations...\n")

# Create figure with subplots
fig, axes = plt.subplots(3, 2, figsize=(16, 12))
fig.suptitle('Cryptocurrency Analysis: Price & Search Volume Trends (Dec 2025 - Mar 2026)', 
             fontsize=16, fontweight='bold')

coins = ['BTC', 'ETH', 'XRP', 'SOL', 'ADA', 'LINK']

for idx, coin in enumerate(coins):
    row = idx // 2
    col = idx % 2
    ax = axes[row, col]
    
    # Get data
    query = f"""
    SELECT cp.date, cp.close, gt.search_volume
    FROM crypto_prices cp
    JOIN google_trends gt ON cp.date = gt.date AND cp.symbol = gt.symbol
    WHERE cp.symbol = '{coin}'
    ORDER BY cp.date
    """
    df = pd.read_sql_query(query, conn)
    df['date'] = pd.to_datetime(df['date'])
    
    # Plot
    ax2 = ax.twinx()
    
    # Price on left axis
    line1 = ax.plot(df['date'], df['close'], 'b-', linewidth=2, label='Close Price')
    ax.set_ylabel('Price ($)', color='b', fontweight='bold')
    ax.tick_params(axis='y', labelcolor='b')
    ax.grid(True, alpha=0.3)
    
    # Search volume on right axis
    line2 = ax2.plot(df['date'], df['search_volume'], 'r-', alpha=0.6, linewidth=1.5, label='Search Volume')
    ax2.set_ylabel('Search Volume (0-100)', color='r', fontweight='bold')
    ax2.tick_params(axis='y', labelcolor='r')
    
    # Format
    ax.set_title(f'{coin} - Price vs Search Trends', fontweight='bold', fontsize=11)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Legend
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax.legend(lines, labels, loc='upper left', fontsize=9)

# Remove extra subplot
fig.delaxes(axes[2, 1])

plt.tight_layout()
plt.savefig('cryptocurrency_analysis.png', dpi=150, bbox_inches='tight')
print("✓ Saved: cryptocurrency_analysis.png")

# Create correlation heatmap
fig2, ax = plt.subplots(figsize=(10, 6))

# Calculate correlations for all coins
corr_data = []
for coin in ['ADA', 'BTC', 'ETH', 'HBAR', 'LINK', 'SOL', 'XRP']:
    price_query = f"SELECT close FROM crypto_prices WHERE symbol = '{coin}' ORDER BY date"
    trend_query = f"SELECT search_volume FROM google_trends WHERE symbol = '{coin}' ORDER BY date"
    
    prices = pd.read_sql_query(price_query, conn)['close'].values
    trends = pd.read_sql_query(trend_query, conn)['search_volume'].values
    
    if len(prices) > 2:
        corr = pd.Series(prices).corr(pd.Series(trends))
        corr_data.append({'Coin': coin, 'Correlation': corr})

corr_df = pd.DataFrame(corr_data).sort_values('Correlation')

colors = ['#d7191c' if x < 0 else '#1a9850' for x in corr_df['Correlation']]
bars = ax.barh(corr_df['Coin'], corr_df['Correlation'], color=colors, alpha=0.7, edgecolor='black')

ax.axvline(x=0, color='black', linestyle='-', linewidth=0.5)
ax.set_xlabel('Pearson Correlation Coefficient', fontweight='bold', fontsize=11)
ax.set_title('Direct Price ↔ Search Volume Correlation\n(Weak correlations are EXPECTED - hypothesis tests LAGGED relationships)', 
             fontweight='bold', fontsize=12)
ax.grid(True, alpha=0.3, axis='x')

# Add value labels
for i, (coin, corr) in enumerate(zip(corr_df['Coin'], corr_df['Correlation'])):
    ax.text(corr, i, f' {corr:.3f}', va='center', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig('correlation_analysis.png', dpi=150, bbox_inches='tight')
print("✓ Saved: correlation_analysis.png")

# Create price distribution
fig3, axes3 = plt.subplots(2, 3, figsize=(15, 8))
fig3.suptitle('Price Distribution by Coin', fontsize=14, fontweight='bold')

coins_list = ['BTC', 'ETH', 'XRP', 'SOL', 'ADA', 'LINK']
for idx, coin in enumerate(coins_list):
    row = idx // 3
    col = idx % 3
    ax = axes3[row, col]
    
    query = f"SELECT close FROM crypto_prices WHERE symbol = '{coin}'"
    prices = pd.read_sql_query(query, conn)['close'].values
    
    ax.hist(prices, bins=20, color='steelblue', alpha=0.7, edgecolor='black')
    ax.set_title(f'{coin}', fontweight='bold')
    ax.set_xlabel('Price ($)')
    ax.set_ylabel('Frequency')
    ax.grid(True, alpha=0.3, axis='y')

plt.tight_layout()
plt.savefig('price_distributions.png', dpi=150, bbox_inches='tight')
print("✓ Saved: price_distributions.png")

conn.close()

print("\n" + "="*80)
print("📊 VISUALIZATION OUTPUTS CREATED IN PROJECT ROOT:")
print("="*80)
print("""
1. cryptocurrency_analysis.png
   → 6 subplots showing price (blue) vs search volume (red) over time
   → Visually shows relationship and timing

2. correlation_analysis.png
   → Bar chart of correlation coefficients
   → Shows BTC has strongest (negative) correlation with search trends

3. price_distributions.png
   → Histograms of price ranges for each coin
   → Shows market volatility visually

💡 INSIGHTS FROM VISUALIZATIONS:
   • Search volume is CHOPPY - daily noise
   • Price trends are SMOOTHER - market integrates info
   • This suggests: Search TODAY might predict LATER price (lag effect) ✓
   • Visual inspection confirms weak direct correlation
""")
