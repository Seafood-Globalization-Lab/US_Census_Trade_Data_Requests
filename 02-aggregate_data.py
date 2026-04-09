"""
02-aggregate_data.py

Purpose:
- Aggregates the raw data files into consolidated datasets.

Key Functions:
- Reading Files: Reads multiple CSV files from directories recursively.
- Combining Data: Merges data into a single DataFrame.
- Saving Aggregated Data: Writes processed data to new CSV files.

Execution:
- Run this script **after** `01-request_data.py` to process and combine raw data files.
"""

import os
import glob
import pandas as pd

def aggregate_files_recursive(base_path):
    """
    Recursively finds all CSV files in the given base path and combines them.
    
    Args:
        base_path: Base directory path to search for CSV files
        
    Returns:
        Combined DataFrame with all CSV data
    """
    # Find all CSV files recursively
    csv_pattern = os.path.join(base_path, '**', '*.csv')
    csv_files = glob.glob(csv_pattern, recursive=True)
    
    # Filter out request_log.csv files
    csv_files = [f for f in csv_files if 'request_log.csv' not in f]
    
    print(f'Found {len(csv_files)} CSV files in {base_path}')
    
    dfs = []
    
    for i, csv_file in enumerate(csv_files, 1):
        try:
            file_size = os.path.getsize(csv_file)
            if file_size > 0:
                curr_df = pd.read_csv(csv_file)
                if not curr_df.empty:
                    dfs.append(curr_df)
                    if i % 100 == 0:
                        print(f'  Processed {i}/{len(csv_files)} files...')
        except Exception as e:
            print(f'  Warning: Could not read {csv_file}: {e}')
            continue
    
    if not dfs:
        print(f'  No valid data found in {base_path}')
        return pd.DataFrame()
    
    print(f'  Combining {len(dfs)} dataframes...')
    aggregate_df = pd.concat(dfs, ignore_index=True)
    return aggregate_df

# Define paths
output_base = 'output'
import_pattern = os.path.join(output_base, '*', 'imports')
export_pattern = os.path.join(output_base, '*', 'exports')

print('=' * 60)
print('Aggregating IMPORT files')
print('=' * 60)
import_df = aggregate_files_recursive(import_pattern)

print('\n' + '=' * 60)
print('Aggregating EXPORT files')
print('=' * 60)
export_df = aggregate_files_recursive(export_pattern)

# Create output directory if it doesn't exist
os.makedirs('output', exist_ok=True)

out_import_path = 'output/imports_combined.csv'
out_export_path = 'output/exports_combined.csv'

print('\n' + '=' * 60)
print('Writing IMPORT records')
print('=' * 60)
if not import_df.empty:
    import_df.to_csv(out_import_path, index=False)
    print(f'  Saved {len(import_df):,} import records to {out_import_path}')
else:
    print('  No import data to save')

print('\n' + '=' * 60)
print('Writing EXPORT records')
print('=' * 60)
if not export_df.empty:
    export_df.to_csv(out_export_path, index=False)
    print(f'  Saved {len(export_df):,} export records to {out_export_path}')
else:
    print('  No export data to save')

print('\n' + '=' * 60)
print('Aggregation complete!')
print('=' * 60)