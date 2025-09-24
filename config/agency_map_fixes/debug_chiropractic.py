import pandas as pd
import csv
import os

def debug_chiropractic_filtering():
    """Debug why Board of Chiropractic Examiners records are not making it to the database"""
    
    print("=== DEBUGGING BOARD OF CHIROPRACTIC EXAMINERS ===\n")
    
    # 1) Load the main CSV and filter for chiropractic records
    print("1. Loading main CSV and filtering for chiropractic records...")
    df = pd.read_csv('../results/dca_data_v2.csv', dtype=str)
    chiro_df = df[df['Agency Name'].str.contains('Chiropractic', case=False, na=False)]
    print(f"   Found {len(chiro_df)} chiropractic records in source file")
    
    if len(chiro_df) > 0:
        print(f"   Exact agency names found: {chiro_df['Agency Name'].unique()}")
        print(f"   Sample zip codes: {chiro_df['Zip'].dropna().unique()[:10]}")
        print(f"   Sample counties: {chiro_df['County'].dropna().unique()[:10]}")
    else:
        print("   No chiropractic records found! Exiting.")
        return
    
    # 2) Check zips_dict
    print("\n2. Checking zip code mapping...")
    zips_dict = {}
    zip_file = '../inputs/zips/all_zips.csv'
    if os.path.exists(zip_file):
        with open(zip_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for line in reader:
                if len(line) >= 2:
                    zip_code, bbb_id = line[0], line[1]
                    zips_dict[zip_code] = bbb_id
        print(f"   Loaded {len(zips_dict)} zip codes from mapping file")
        
        # Check how many chiropractic zips are in the mapping
        chiro_zips = chiro_df['Zip'].dropna().unique()
        missing_zips = [z for z in chiro_zips if z not in zips_dict]
        valid_zips = [z for z in chiro_zips if z in zips_dict]
        
        print(f"   Chiropractic records with valid zips: {len(valid_zips)}")
        print(f"   Chiropractic records with missing zips: {len(missing_zips)}")
        if missing_zips:
            print(f"   Missing zips sample: {missing_zips[:10]}")
    else:
        print(f"   ERROR: Zip file not found at {zip_file}")
        return
    
    # 3) Check agency mapping
    print("\n3. Checking agency name mapping...")
    agency_map = {}
    agency_file = '../inputs/agency_matches_all.csv'
    if os.path.exists(agency_file):
        with open(agency_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            for line in reader:
                if len(line) >= 2:
                    aid, name = line[0], line[1]
                    agency_map[name.upper()] = aid
        print(f"   Loaded {len(agency_map)} agency mappings")
        
        # Check if chiropractic agency is mapped
        chiro_agencies = chiro_df['Agency Name'].dropna().unique()
        for agency in chiro_agencies:
            if agency.upper() in agency_map:
                print(f"   ✓ FOUND mapping for: '{agency}' -> {agency_map[agency.upper()]}")
            else:
                print(f"   ✗ MISSING mapping for: '{agency}'")
    else:
        print(f"   ERROR: Agency file not found at {agency_file}")
        return
    
    # 4) Apply the same filtering logic as post_dca_data.py
    print("\n4. Applying filtering logic...")
    
    # Rename zip column
    df.rename(columns={'Zip':'zip'}, inplace=True)
    df['zip'] = df['zip'].fillna('NA')
    
    # Filter for chiropractic again after rename
    chiro_df = df[df['Agency Name'].str.contains('Chiropractic', case=False, na=False)]
    print(f"   Starting with {len(chiro_df)} chiropractic records")
    
    # Step 1: Filter by zip codes
    chiro_df_zip_filtered = chiro_df[chiro_df['zip'].isin(zips_dict)]
    print(f"   After zip filtering: {len(chiro_df_zip_filtered)} records")
    
    # Step 2: Add agency_id
    chiro_df_zip_filtered['agency_id'] = chiro_df_zip_filtered['Agency Name'].str.upper().map(agency_map)
    
    # Step 3: Filter by agency_id
    chiro_df_agency_filtered = chiro_df_zip_filtered[chiro_df_zip_filtered['agency_id'].notna()]
    print(f"   After agency_id filtering: {len(chiro_df_agency_filtered)} records")
    
    # Step 4: Filter by zip OR county
    chiro_df_final = chiro_df_agency_filtered[
        (chiro_df_agency_filtered['zip'] != '') | 
        (chiro_df_agency_filtered['County'].fillna('') != '')
    ]
    print(f"   After zip/county filtering: {len(chiro_df_final)} records")
    
    # Final summary
    print(f"\n=== SUMMARY ===")
    print(f"Records surviving all filters: {len(chiro_df_final)}")
    if len(chiro_df_final) > 0:
        print("Sample surviving records:")
        print(chiro_df_final[['Agency Name', 'License Number', 'zip', 'County', 'agency_id']].head())
    else:
        print("No records survived filtering!")
        
        # Identify the main bottleneck
        if len(chiro_df_zip_filtered) == 0:
            print("BOTTLENECK: Zip code filtering - no valid BBB service area zips")
        elif len(chiro_df_agency_filtered) == 0:
            print("BOTTLENECK: Agency mapping - no agency_id found")
        else:
            print("BOTTLENECK: Zip/county requirement")

if __name__ == "__main__":
    debug_chiropractic_filtering()