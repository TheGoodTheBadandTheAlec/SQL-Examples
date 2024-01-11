#### Create Monetary Data

import pandas as pd
import ast

# Load the data from the CSV file
data = pd.read_csv(r'C:\Users\alecj\python\Crypto\ml_data_analysis.csv')

close_tomorrow_results = []

# Group by symbol and perform calculations for each group
for symbol, symbol_data in data.groupby('symbol'):
    # Sort the symbol_data by time
    symbol_data = symbol_data.sort_values(by='time')

    # Handle potential issues with literal_eval using try-except
    def safe_eval(x):
        try:
            return ast.literal_eval(x)[0] if not pd.isna(x) else x
        except (SyntaxError, ValueError):
            return pd.NaT

    # Apply safe_eval to the 'predicted_close' column
    symbol_data['predicted_close'] = symbol_data['predicted_close'].apply(safe_eval)
    symbol_data['predicted_close'] = pd.to_numeric(symbol_data['predicted_close'], errors='coerce')
    symbol_data['close_ratio'] = symbol_data['predicted_close'] / symbol_data['predicted_close'].shift(24)  # Adjust for 24 rows prior

    # Filter out the most recent row
    most_recent_row = symbol_data.iloc[-1]

    # Calculate the estimated close for tomorrow
    estimated_close_tomorrow = most_recent_row['close'] * most_recent_row['close_ratio']
    predicted_close_tomorrow = most_recent_row['predicted_close']

    # Create a DataFrame with the results
    tomorrow_data = pd.DataFrame({
        'Symbol': [symbol],
        'Close Today': [most_recent_row['close']],
        'Estimated Close Tomorrow': [estimated_close_tomorrow],
        'Close Ratio': [most_recent_row['close_ratio']],
        'Most Recent Data Time (Unix)': [most_recent_row['time']],
        'Predicted Close Tomorrow': [predicted_close_tomorrow]
    })

    close_tomorrow_results.append(tomorrow_data)

# Combine the results into a DataFrame
close_tomorrow_df = pd.concat(close_tomorrow_results)

# Sort the DataFrame by close ratio in descending order
close_tomorrow_df = close_tomorrow_df.sort_values(by='Close Ratio', ascending=False)

# Reset the index after sorting
close_tomorrow_df = close_tomorrow_df.reset_index(drop=True)

# Save the results to a CSV file
close_tomorrow_csv_path = r'C:\Users\alecj\python\Crypto\ml_close_tomorrow.csv'
close_tomorrow_df.to_csv(close_tomorrow_csv_path, index=False)
print(f'Results saved to {close_tomorrow_csv_path}')

#### Create Accuracy Data

from sklearn.metrics import mean_squared_error

# Load the data from the CSV file
data = pd.read_csv(r'C:\Users\alecj\python\Crypto\ml_data_analysis.csv')

# Convert 'close' and 'predicted_close' columns to numeric data types and handle non-numeric values
data['close'] = pd.to_numeric(data['close'], errors='coerce')

# Handle non-numeric values in 'predicted_close' using try-except
def safe_eval(x):
    try:
        return ast.literal_eval(x)[0] if pd.notna(x) else x
    except (SyntaxError, ValueError):
        return pd.NaT

data['predicted_close'] = data['predicted_close'].apply(safe_eval)
data['predicted_close'] = pd.to_numeric(data['predicted_close'], errors='coerce')

# Drop rows with NaN values in 'close' or 'predicted_close'
data = data.dropna(subset=['close', 'predicted_close'])

# Create a DataFrame to store the results
results = []

# Calculate metrics for each symbol
symbols = data['symbol'].unique()

for symbol in symbols:
    symbol_data = data[data['symbol'] == symbol]
    actual = symbol_data['close']
    predicted = symbol_data['predicted_close']

    # Adjust for 24 hours prior in 'predicted'
    predicted_shifted = predicted.shift(24)

    # Drop NaN values
    valid_indices = ~actual.isna() & ~predicted_shifted.isna()
    actual = actual[valid_indices]
    predicted_shifted = predicted_shifted[valid_indices]

    # Calculate MAPE
    mape = (abs(actual - predicted_shifted) / actual).mean()
    
    # Calculate DA
    direction_accuracy = (actual.shift(-1) - actual) * (predicted_shifted.shift(-1) - predicted_shifted) > 0
    da = direction_accuracy.mean()

    # Additional metrics
    mse = mean_squared_error(actual, predicted_shifted)
    rmse = mean_squared_error(actual, predicted_shifted, squared=False)
    r2 = 1 - (mse / actual.var())
    
    # Append results to the list
    results.append({
        'Symbol': symbol,
        'MAPE': mape,
        'DA': da,
        'MSE': mse,
        'RMSE': rmse,
        'R2': r2
    })

# Create a DataFrame from the results
results_df = pd.DataFrame(results)

# Define the bounds for each metric
metric_bounds = {
    'MAPE': (0, 1),
    'DA': (0, 1),
    'MSE': (0, 1),
    'RMSE': (0, 1),
    'R2': (0, 1)
}

# Calculate percentiles for each metric based on predefined bounds
percentiles = {}
for metric in metric_bounds:
    lower, upper = metric_bounds[metric]
    percentiles[metric] = (results_df[metric] - lower) / (upper - lower)
    percentiles[metric] = percentiles[metric].clip(0, 1)  # Clip values to the [0, 1] range

# Invert percentiles for specific metrics
percentiles['MAPE'] = 1 - percentiles['MAPE']
percentiles['MSE'] = 1 - percentiles['MSE']
percentiles['RMSE'] = 1 - percentiles['RMSE']

# Average percentiles across metrics
results_df['MAPE Percentile'] = percentiles['MAPE']
results_df['DA Percentile'] = percentiles['DA']
results_df['MSE Percentile'] = percentiles['MSE']
results_df['RMSE Percentile'] = percentiles['RMSE']
results_df['R2 Percentile'] = percentiles['R2']

# Rank symbols based on average percentiles
results_df['Average DA Percentile'] = results_df[['MAPE Percentile', 'DA Percentile']].mean(axis=1)

# Rank symbols based on average percentiles
results_df['Average MSE Percentile'] = results_df[['MSE Percentile', 'RMSE Percentile']].mean(axis=1)

# Sort the DataFrame based on average percentiles
results_df = results_df.sort_values(by='Average DA Percentile', ascending=False)

# Save the results to a CSV file
output_csv_path = r'C:\Users\alecj\python\Crypto\evaluation_results.csv'
results_df.to_csv(output_csv_path, index=False)
print(f'Results saved to {output_csv_path}')

#### Join Data

import sqlite3
import pandas as pd

# Load CSV data into DataFrames
evaluation_results_path = r'C:\Users\alecj\python\Crypto\evaluation_results.csv'
ml_close_tomorrow_path = r'C:\Users\alecj\python\Crypto\ml_close_tomorrow.csv'

evaluation_results_df = pd.read_csv(evaluation_results_path)
ml_close_tomorrow_df = pd.read_csv(ml_close_tomorrow_path)

# Create SQLite database and connect
conn = sqlite3.connect(':memory:')

# Write DataFrames to SQLite tables
evaluation_results_df.to_sql('evaluation_results', conn, index=False)
ml_close_tomorrow_df.to_sql('ml_close_tomorrow', conn, index=False)

# Perform JOIN operation on 'Symbol'
query = '''
    SELECT *
    FROM evaluation_results
    JOIN ml_close_tomorrow
    ON evaluation_results.Symbol = ml_close_tomorrow.Symbol
'''

# Execute the query and fetch the results into a DataFrame
result_df = pd.read_sql(query, conn)

# Close the connection
conn.close()
result_df['Predicted Close Ratio'] = result_df['Predicted Close Tomorrow'] / result_df['Close Today']
desired_columns_order = [
    'Symbol', 'Close Today', 'MAPE', 'DA', 'Estimated Close Tomorrow', 'Close Ratio',
    'MSE', 'RMSE', 'Predicted Close Ratio', 'Predicted Close Tomorrow', 'R2', 'MAPE Percentile', 'DA Percentile',
    'MSE Percentile', 'RMSE Percentile', 'R2 Percentile', 'Average DA Percentile',
    'Average MSE Percentile', 'Most Recent Data Time (Unix)'
]

# Reorder the columns
result_df = result_df[desired_columns_order]

# Sort the DataFrame based on average percentiles
result_df = result_df.sort_values(by='MSE', ascending=False)

# Save the joined result to a CSV file
joined_csv_path = r'C:\Users\alecj\python\Crypto\ml_analytics.csv'
result_df.to_csv(joined_csv_path, index=False)
print(f'Joined results saved to {joined_csv_path}')