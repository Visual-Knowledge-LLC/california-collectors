import csv
import os

def check_agency_mappings():
    """Check current agency mappings to understand the pattern"""
    
    agency_file = '../inputs/agency_matches_all.csv'
    if os.path.exists(agency_file):
        print("Current agency mappings:")
        print("=" * 60)
        with open(agency_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            print(f"Header: {header}")
            print("=" * 60)
            
            for line in reader:
                if len(line) >= 2:
                    aid, name = line[0], line[1]
                    print(f"{aid:4} | {name}")
    else:
        print(f"Agency file not found at {agency_file}")

if __name__ == "__main__":
    check_agency_mappings()