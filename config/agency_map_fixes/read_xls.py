import pandas as pd
import os

def read_csv_file(file_path):
    """Read a CSV file and return a DataFrame"""
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        print(f"Error reading file: {e}")
        return None

if __name__ == "__main__":
    # Try multiple possible paths
    possible_paths = [
        os.path.join("results", "dca_data_v2.csv"),
        "dca_data_v2.csv",
        os.path.join("..", "results", "dca_data_v2.csv")
    ]
    
    df = None
    for file_path in possible_paths:
        print(f"Trying: {file_path}")
        if os.path.exists(file_path):
            print(f"Found file at: {file_path}")
            df = read_csv_file(file_path)
            break
        else:
            print(f"File not found at: {file_path}")
    
    if df is not None:
        print(f"\nShape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")
        
        # Get unique agency names
        if 'Agency Name' in df.columns:
            agency_names = df['Agency Name'].dropna().unique()
            print(f"\nUnique agency names ({len(agency_names)}):")
            for agency in sorted(agency_names):
                print(f"  - {agency}")
        else:
            print("\nNo 'Agency Name' column found. Available columns:")
            print(df.columns.tolist())
    else:
        print("\nCould not find dca_data_v2.csv in any of the expected locations.")
        print("Current directory contents:")
        for item in os.listdir("."):
            print(f"  {item}")