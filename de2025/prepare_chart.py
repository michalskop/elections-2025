import pandas as pd
import numpy as np
from datetime import datetime

path = "de2025/"

def process_german_polls(polls_file, weighted_file, election_date='2021-09-26'):
  # Read the files
  polls_df = pd.read_csv(polls_file)
  weighted_df = pd.read_csv(weighted_file)
  
  # Create election results DataFrame
  election_results = pd.DataFrame([{
      'middle_date': election_date,
      'cdu_csu': 24.1,
      'spd': 25.7,
      'gruene': 14.8,
      'fdp': 11.5,
      'linke': 4.9,
      'afd': 10.3,
      'fw': 2.4,
      'bsw': np.nan  # BSW didn't participate
  }])

  # Party name mapping
  party_mapping = {
      'cdu_csu': 'CDU/CSU',
      'spd': 'SPD',
      'gruene': 'Grüne',
      'fdp': 'FDP',
      'linke': 'Linke',
      'afd': 'AfD',
      'bsw': 'BSW'
  }
  
  # Set up date filtering
  election_date = pd.to_datetime(election_date)
  current_date = pd.to_datetime(datetime.now())
  
  # Helper function to check if a date is valid (not too far in the future)
  def is_valid_date(date):
    # Allow dates up to 3 months in the future from current date
    # This helps catch obvious data entry errors (like wrong year) while allowing near-future dates
    max_future_date = current_date + pd.DateOffset(months=3)
    return election_date <= date <= max_future_date
  
  # Convert and filter dates in polls_df
  polls_df['start_date'] = pd.to_datetime(polls_df['start_date'])
  polls_df['end_date'] = pd.to_datetime(polls_df['end_date'])
  
  # Filter out data before election and invalid future dates
  polls_df = polls_df[
      polls_df.apply(lambda row: 
          is_valid_date(row['start_date']) and 
          is_valid_date(row['end_date']), axis=1)
  ]
  
  # Convert and filter dates in weighted_df
  weighted_df['middle_date'] = pd.to_datetime(weighted_df['middle_date'])
  weighted_df = weighted_df[
      weighted_df['middle_date'].apply(is_valid_date)
  ]
  
  # Process regular polls
  polls_transformed = []
  for _, row in polls_df.iterrows():
    # Calculate middle date
    middle_date = row['start_date'] + (row['end_date'] - row['start_date']) / 2
    
    # Transform pollster name
    pollster = row['pollster'].capitalize()
    
    # Process each party
    for party_col, party_name in party_mapping.items():
      if party_col in row and pd.notna(row[party_col]):
        polls_transformed.append({
            'Agentura': pollster,
            'Datum': middle_date.strftime('%Y-%m-%d'),
            'Hodnota': round(row[party_col], 1),
            'Průměr': '',
            'Strana': party_name,
            'size': 6
        })

  # Process weighted polls
  weighted_transformed = []
  # Get unique pollsters from polls_df and capitalize them
  pollsters = sorted(polls_df['pollster'].unique())
  pollsters = [p.capitalize() for p in pollsters]
  
  for _, row in weighted_df.iterrows():
    for party_col, party_name in party_mapping.items():
      if party_col in row and pd.notna(row[party_col]):
        for pollster in pollsters:
          weighted_transformed.append({
              'Agentura': pollster,
              'Datum': row['middle_date'].strftime('%Y-%m-%d'),
              'Hodnota': round(row[party_col], 1),
              'Průměr': party_name,
              'Strana': party_name,
              'size': 1
          })

  # Process election results
  election_transformed = []
  for party_col, party_name in party_mapping.items():
    if party_col in election_results.columns:
      value = election_results.iloc[0][party_col]
      if pd.notna(value):
        election_transformed.append({
            'Agentura': 'volby',
            'Datum': election_results.iloc[0]['middle_date'],
            'Hodnota': round(value, 1),
            'Průměr': '',
            'Strana': party_name,
            'size': 12
        })

  # Combine all transformations
  result_df = pd.concat([
      pd.DataFrame(polls_transformed),
      pd.DataFrame(weighted_transformed),
      pd.DataFrame(election_transformed)
  ]).reset_index(drop=True)

  return result_df


# Usage example:
result = process_german_polls(path + 'german_polls.csv', path + 'german_polls_weighted.csv')
result.to_csv(path + 'combined_polls.csv', index=False)

print("Data processed and saved to 'combined_polls.csv'.")