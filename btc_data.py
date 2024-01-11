import pandas as pd
import sqlite3

#### BTC Data

import pandas as pd

# Read the historical data CSV file
historical_data = pd.read_csv(r'C:\Users\alecj\python\Crypto\historical_data_2.csv')

# Drop the 'Symbol' column
historical_data = historical_data.drop(columns=['Symbol'])

# Save the modified DataFrame back to CSV, overwriting the original file
historical_data.to_csv(r'C:\Users\alecj\python\Crypto\historical_data_2.csv', index=False)

# Read the historical data CSV file
historical_data = pd.read_csv(r'C:\Users\alecj\python\Crypto\historical_data_2.csv')

# Filter data for symbol 'BTC' and select required columns
btc_hourly_data = historical_data[historical_data['symbol'] == 'BTC'][['time', 'close', 'Volume USDT']]

# Save BTC_hourly_data to CSV
btc_hourly_data.to_csv(r'C:\Users\alecj\python\Crypto\BTC_hourly_data.csv', index=False)

# Use SQLite to join BTC_hourly_data with historical_data
conn = sqlite3.connect(':memory:')  # Create an in-memory SQLite database
btc_hourly_data.to_sql('btc_hourly_data', conn, index=False)
historical_data.to_sql('historical_data', conn, index=False)  # Use if_exists='replace' to replace the existing table

# SQL query to join on the 'time' column
query = '''
    SELECT a.*, b.close AS BTC_close, b."Volume USDT" AS BTC_volume
    FROM historical_data a
    LEFT JOIN btc_hourly_data b ON a.time = b.time
'''

# Execute the query and get the result as a DataFrame
joined_data = pd.read_sql_query(query, conn)

# Save the joined DataFrame to a new CSV file
joined_data.to_csv(r'C:\Users\alecj\python\Crypto\historical_data_2.csv', index=False)

# Close the SQLite connection
conn.close()

#### ETH Data

# Read the historical data CSV file
historical_data = pd.read_csv(r'C:\Users\alecj\python\Crypto\historical_data_2.csv')

# Filter data for symbol 'ETH' and select required columns
eth_hourly_data = historical_data[historical_data['symbol'] == 'ETH'][['time', 'close', 'Volume USDT']]

# Save ETH_hourly_data to CSV
eth_hourly_data.to_csv(r'C:\Users\alecj\python\Crypto\ETH_hourly_data.csv', index=False)

# Use SQLite to join ETH_hourly_data with historical_data
conn = sqlite3.connect(':memory:')  # Create an in-memory SQLite database
eth_hourly_data.to_sql('eth_hourly_data', conn, index=False)
historical_data.to_sql('historical_data', conn, index=False)  # Use if_exists='replace' to replace the existing table

# SQL query to join on the 'time' column
query = '''
    SELECT a.*, b.close AS ETH_close, b."Volume USDT" AS ETH_volume
    FROM historical_data a
    LEFT JOIN eth_hourly_data b ON a.time = b.time
'''

# Execute the query and get the result as a DataFrame
joined_data = pd.read_sql_query(query, conn)

# Save the joined DataFrame to a new CSV file
joined_data.to_csv(r'C:\Users\alecj\python\Crypto\historical_data_2.csv', index=False)

# Close the SQLite connection
conn.close()