import pandas as pd
import sqlite3

# Define the input and output file paths
input_csv = r'C:\Users\alecj\python\Crypto\historical_data.csv'
output_csv = r'C:\Users\alecj\python\Crypto\ml_data_clean.csv'

# Load the input data into a DataFrame
df = pd.read_csv(input_csv)

# Create an empty 'action' column
df['action'] = ''

# Create an SQLite database in memory
conn = sqlite3.connect(':memory:')

# Insert the DataFrame into the database
df.to_sql('crypto_data', conn, index=False, if_exists='replace')

# Define the logic
m = 1.1

# Apply the logic using SQL
sql_query = """
UPDATE crypto_data
SET action = CASE
    WHEN (SELECT close FROM crypto_data AS t2 WHERE t2.rowid = crypto_data.rowid + 1 AND t2.symbol = crypto_data.symbol) >= ? * close
        AND (SELECT action FROM crypto_data AS t3 WHERE t3.rowid = crypto_data.rowid - 1 AND t3.symbol = crypto_data.symbol) != 'buy'
    THEN 'buy'
    WHEN (SELECT action FROM crypto_data AS t4 WHERE t4.rowid = crypto_data.rowid - 1 AND t4.symbol = crypto_data.symbol) = 'buy'
    THEN 'sell'
    ELSE 'wait'
END
"""
conn.execute(sql_query, (m,))

# Fetch the updated data from the database
updated_df = pd.read_sql('SELECT * FROM crypto_data', conn)

# Close the database connection
conn.close()

# Save the updated DataFrame to the output CSV file
updated_df.to_csv(output_csv, index=False)
