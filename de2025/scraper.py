import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re

POLLSTERS = {
  'allensbach': 'https://www.wahlrecht.de/umfragen/allensbach.htm',
  'emnid': 'https://www.wahlrecht.de/umfragen/emnid.htm',
  'forsa': 'https://www.wahlrecht.de/umfragen/forsa.htm',
  'politbarometer': 'https://www.wahlrecht.de/umfragen/politbarometer.htm',
  'gms': 'https://www.wahlrecht.de/umfragen/gms.htm',
  'dimap': 'https://www.wahlrecht.de/umfragen/dimap.htm',
  'insa': 'https://www.wahlrecht.de/umfragen/insa.htm',
  'yougov': 'https://www.wahlrecht.de/umfragen/yougov.htm'
}

path = "de2025/"

def parse_german_date_range(date_range, year):
  """Convert German date range format to ISO format start and end dates."""
  try:
    if '–' not in date_range:
      return None, None
      
    # Split into start and end parts
    parts = date_range.strip().split('–')
    start = parts[0].strip()
    end = parts[1].strip()
    
    # Parse start date
    if '.' not in start:
      return None, None
      
    start_parts = start.split('.')
    start_day = int(start_parts[0])
    start_month = int(start_parts[1])
    start_date = f"{year}-{start_month:02d}-{start_day:02d}"
    
    # Parse end date
    if '.' in end:
      end_parts = end.split('.')
      end_day = int(end_parts[0])
      end_month = int(end_parts[1])
    else:
      end_day = int(end)
      end_month = start_month
    
    end_date = f"{year}-{end_month:02d}-{end_day:02d}"
    
    return start_date, end_date
    
  except Exception as e:
    print(f"Error while parsing date range '{date_range}': {str(e)}")
    return None, None

def clean_percentage(value):
  """Convert German percentage format to float."""
  try:
    if value == '–' or value == '':
      return ''
    
    # Check if it's a clean percentage value
    if '%' in value:
      return float(value.replace(',', '.').replace(' %', ''))
      
    # If it's not a clean percentage, return empty
    return ''
      
  except Exception:
    # If any error occurs during conversion, return empty
    return ''

def get_party_columns(table):
  """Extract party names and their column indices from table headers."""
  party_columns = {}
  headers = table.find_all('tr')[0].find_all('th')  # Get header row
  
  # Map of possible party names in headers to standardized names
  party_map = {
    'CDU': 'cdu_csu',
    'CSU': 'cdu_csu',
    'SPD': 'spd',
    'GRÜNE': 'gruene',
    'FDP': 'fdp',
    'LINKE': 'linke',
    'AfD': 'afd',
    'FW': 'fw',
    'BSW': 'bsw',
    'Sonstige': 'others'
  }
  
  for i, header in enumerate(headers):
    # Special handling for CDU/CSU
    if header.find_all('a'):
      links = header.find_all('a')
      header_text = ''.join(link.text for link in links)
    else:
      header_text = header.text.strip()
    
    # Map the header text to standardized party name
    for party_name, column_name in party_map.items():
      if party_name in header_text:
        # For CDU/CSU specifically
        if party_name in ['CDU', 'CSU']:
          if 'CSU' in header_text and 'CDU' in header_text:
            party_columns['cdu_csu'] = i
        else:
          party_columns[column_name] = i
        break
  
  print(f"Debug - Found column mapping: {party_columns}")
  return party_columns

def process_poll_data(url, pollster):
  """Download and process poll data from a given URL."""
  response = requests.get(url)
  soup = BeautifulSoup(response.content, 'html.parser')
  
  # Find the main data table
  table = soup.find('table', class_='wilko')
  if not table:
    print(f"No data table found for {pollster}")
    return []
  
  # Get party column mapping
  party_columns = get_party_columns(table)
  print(f"Found party columns: {party_columns}")
  
  rows = table.find_all('tr')
  data = []
  
  # Skip header rows
  for row in rows[4:]:  # Skip header rows
    cells = row.find_all(['td'])
    if len(cells) < 10:  # Skip rows without full data
      continue
        
    # Extract date and year
    date_cell = cells[0].text.strip()
    if not date_cell or not re.match(r'\d{2}\.\d{2}\.\d{4}', date_cell):
      continue
        
    # Get year from the first column
    year = date_cell.split('.')[-1]
    
    # Extract the date range from the last column
    date_range = cells[-1].text.strip()
    start_date, end_date = parse_german_date_range(date_range, year)
    
    if not start_date or not end_date:
      continue
    
    # Extract poll values
    try:
      # Start with basic data
      row_data = {
        'pollster': pollster,
        'start_date': start_date,
        'end_date': end_date
      }
      
      # Add party data using the column mapping
      for party, col_idx in party_columns.items():
        if col_idx < len(cells):
          row_data[party] = clean_percentage(cells[col_idx].text.strip())
      
      data.append(row_data)
    except Exception as e:
      print(f"Error processing row: {str(e)}")
      continue
  
  return data

def main():
  all_data = []
  
  # Process each pollster
  for pollster, url in POLLSTERS.items():
    try:
      print(f"Processing {pollster}...")
      poll_data = process_poll_data(url, pollster)
      if poll_data:
        print(f"Found {len(poll_data)} entries for {pollster}")
      all_data.extend(poll_data)
    except Exception as e:
      print(f"Error processing {pollster}: {str(e)}")
  
  if not all_data:
    print("No data collected!")
    return
    
  # Convert to DataFrame and save
  df = pd.DataFrame(all_data)
  
  # Sort by date
  if 'end_date' in df.columns:
    df = df.sort_values(['end_date', 'pollster'], ascending=[False, True])
  
  # Save to CSV
  df.to_csv(path + 'german_polls.csv', index=False)
  print(f"Data saved to german_polls.csv")
  
  # Print first few rows to verify
  print("\nFirst few rows of collected data:")
  print(df.head())

if __name__ == "__main__":
  main()
  print("Data collection complete and saved to 'german_polls.csv'.")