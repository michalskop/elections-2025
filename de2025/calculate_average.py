import pandas as pd
import numpy as np
from datetime import datetime, timedelta

path = "de2025/"

def calculate_weight(days_diff):
  """Calculate weight based on days difference from today"""
  return 0.5 ** (days_diff / 15)

def process_german_polls(input_file, output_file):
  # Read the CSV file
  df = pd.read_csv(path + input_file)
  
  # Convert dates to datetime
  df['start_date'] = pd.to_datetime(df['start_date'])
  df['end_date'] = pd.to_datetime(df['end_date'])
  
  # Calculate middle date for each poll
  df['middle_date'] = df['start_date'] + (df['end_date'] - df['start_date']) / 2
  
  # Define columns to process
  columns_to_check = ['cdu_csu', 'spd', 'gruene', 'fdp', 'linke', 'afd', 'fw', 'bsw', 'others']
  
  # Store first appearance dates for each party
  first_appearances = {}
  
  # Process each column
  for col in columns_to_check:
    # Find first valid measurement for each party (excluding ** values)
    mask = (df[col] != '**') & (df[col].notna())
    valid_dates = df[mask]['middle_date']
    if not valid_dates.empty:
      first_appearances[col] = valid_dates.min()
    
    # Convert to numeric, replacing ** with NaN
    df[col] = pd.to_numeric(df[col].replace('**', np.nan), errors='coerce')
  
  # Add election results as a row
  election_date = pd.Timestamp('2021-09-26')
  election_results = pd.DataFrame([{
    'middle_date': election_date,
    'cdu_csu': 24.1,
    'spd': 25.7,
    'gruene': 14.8,
    'fdp': 11.5,
    'linke': 4.9,
    'afd': 10.3,
    'fw': 2.4,
    'bsw': np.nan,  # BSW didn't participate
    'others': 4.3  # Remaining percentage
  }])
  
  # Combine polls with election results
  df = pd.concat([df, election_results], ignore_index=True)
  
  # Calculate weights using current date
  today = pd.Timestamp.now().normalize()
  df['days_diff'] = (today - df['middle_date']).dt.days
  df['weight'] = df['days_diff'].apply(calculate_weight)
  
  # Give election results initial weight of 5
  df.loc[df['middle_date'] == election_date, 'weight'] *= 5
  
  # Generate dates from election to today using current weekday
  current_weekday = today.weekday()
  date_range = pd.date_range(
    start=election_date, 
    end=today, 
    freq=f'W-{today.strftime("%a").upper()}'
  )
  
  # Calculate weighted averages for each date
  results = []
  for date in date_range:
    # Calculate weights for this date
    days_diff = (date - df['middle_date']).dt.days
    weights = np.where(days_diff >= 0, calculate_weight(days_diff), 0)
    
    # Give election results 5x weight
    weights = np.where(df['middle_date'] == election_date, weights * 5, weights)
    
    # Skip dates where all weights are 0
    if sum(weights) == 0:
      continue
      
    # Calculate weighted averages for each party
    result = {
      'middle_date': date.strftime('%Y-%m-%d')
    }
    
    # Calculate weighted average for each party
    for party in columns_to_check:
      # Check if we have any measurements for this party yet
      if party in first_appearances:
        first_date = first_appearances[party]
        if date < first_date:
          result[party] = ''
          continue
      
      # Create mask for valid values (not NaN) and valid weights
      valid_mask = ~df[party].isna()
      
      # Only include values from dates up to the current date point
      date_mask = df['middle_date'] <= date
      
      # Combine masks
      final_mask = valid_mask & date_mask
      masked_weights = np.where(final_mask, weights, 0)
      
      if sum(masked_weights) > 0:
        result[party] = np.average(df[party][final_mask], weights=masked_weights[final_mask])
      else:
        result[party] = ''
    
    results.append(result)
  
  # Create output DataFrame and save to CSV
  output_df = pd.DataFrame(results)
  
  # Ensure columns are in the correct order
  column_order = ['middle_date'] + columns_to_check
  output_df = output_df[column_order]
  
  # Save to CSV with proper formatting
  output_df.to_csv(path + output_file, index=False, float_format='%.3f', na_rep='')

if __name__ == "__main__":
  process_german_polls('german_polls.csv', 'german_polls_weighted.csv')
  print("Data processed and saved to 'german_polls_weighted.csv'.")