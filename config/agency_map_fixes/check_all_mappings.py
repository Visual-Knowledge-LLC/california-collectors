import pandas as pd
import csv
import os

def check_all_mappings():
    """Check for all potential agency mapping issues"""
    
    print("=== COMPREHENSIVE AGENCY MAPPING CHECK ===\n")
    
    # 1) Load all agency names from CSV
    print("1. Loading all agency names from source data...")
    df = pd.read_csv('../results/dca_data_v2.csv', dtype=str)
    source_agencies = df['Agency Name'].dropna().unique()
    print(f"   Found {len(source_agencies)} unique agencies in source data")
    
    # 2) Load agency mappings
    print("\n2. Loading agency mappings...")
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
    else:
        print(f"   ERROR: Agency file not found at {agency_file}")
        return
    
    # 3) Check for missing mappings
    print("\n3. Checking for missing mappings...")
    missing_agencies = []
    mapped_agencies = []
    
    for agency in source_agencies:
        if agency.upper() in agency_map:
            mapped_agencies.append(agency)
        else:
            missing_agencies.append(agency)
    
    print(f"   ✓ Mapped agencies: {len(mapped_agencies)}")
    print(f"   ✗ Missing agencies: {len(missing_agencies)}")
    
    if missing_agencies:
        print("\n   MISSING AGENCY MAPPINGS:")
        for agency in sorted(missing_agencies):
            record_count = len(df[df['Agency Name'] == agency])
            print(f"     - '{agency}' ({record_count:,} records)")
    
    # 4) Check for case sensitivity issues
    print("\n4. Checking for case sensitivity issues...")
    case_issues = []
    
    for source_agency in source_agencies:
        # Check if there's a mapping with different case
        for mapped_agency in agency_map.keys():
            if (source_agency.upper() == mapped_agency.upper() and 
                source_agency != mapped_agency.replace(mapped_agency, mapped_agency.title())):
                case_issues.append((source_agency, mapped_agency))
    
    if case_issues:
        print(f"   Found {len(case_issues)} potential case sensitivity issues:")
        for source, mapped in case_issues:
            print(f"     Source: '{source}' vs Mapped: '{mapped}'")
    else:
        print("   No case sensitivity issues found")
    
    # 5) Record impact analysis
    print("\n5. Record impact analysis...")
    total_records = len(df)
    mapped_records = 0
    missing_records = 0
    
    for agency in source_agencies:
        count = len(df[df['Agency Name'] == agency])
        if agency.upper() in agency_map:
            mapped_records += count
        else:
            missing_records += count
    
    print(f"   Total records: {total_records:,}")
    print(f"   Records with mappings: {mapped_records:,} ({mapped_records/total_records*100:.1f}%)")
    print(f"   Records without mappings: {missing_records:,} ({missing_records/total_records*100:.1f}%)")
    
    # 6) Summary recommendations
    print("\n6. RECOMMENDATIONS:")
    if missing_agencies:
        print(f"   • Add {len(missing_agencies)} missing agency mappings")
        print(f"   • This would recover {missing_records:,} records ({missing_records/total_records*100:.1f}% of total)")
    
    if case_issues:
        print(f"   • Fix {len(case_issues)} case sensitivity issues")
    
    if not missing_agencies and not case_issues:
        print("   • All agencies are properly mapped!")
    
    # 7) Generate suggested mappings
    if missing_agencies:
        print(f"\n7. SUGGESTED MAPPINGS (next available IDs):")
        used_ids = set(agency_map.values())
        next_id = 36  # Start from 36 since we saw up to 35
        
        for agency in sorted(missing_agencies):
            while str(next_id) in used_ids:
                next_id += 1
            record_count = len(df[df['Agency Name'] == agency])
            print(f"   {next_id:3} | {agency} ({record_count:,} records)")
            next_id += 1

if __name__ == "__main__":
    check_all_mappings()