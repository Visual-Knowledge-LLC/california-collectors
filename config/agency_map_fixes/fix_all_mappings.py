import pandas as pd
import csv
import os
import shutil
from datetime import datetime

def fix_all_mappings():
    """Fix all agency mapping issues by updating the mapping file to match source data"""
    
    print("=== FIXING ALL AGENCY MAPPING ISSUES ===\n")
    
    # 1) Load source data to get correct agency names
    print("1. Loading source data...")
    df = pd.read_csv('../results/dca_data_v2.csv', dtype=str)
    source_agencies = df['Agency Name'].dropna().unique()
    print(f"   Found {len(source_agencies)} unique agencies in source data")
    
    # 2) Load current mappings
    agency_file = '../inputs/agency_matches_all.csv'
    backup_file = f'../inputs/agency_matches_all_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    
    if not os.path.exists(agency_file):
        print(f"   ERROR: Agency file not found at {agency_file}")
        return
    
    # Create timestamped backup
    shutil.copy2(agency_file, backup_file)
    print(f"   Created backup at {backup_file}")
    
    # Read current mappings
    current_mappings = {}
    all_rows = []
    
    with open(agency_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        all_rows.append(header)
        
        for line in reader:
            if len(line) >= 2:
                aid, name = line[0], line[1]
                current_mappings[name.upper()] = {'id': aid, 'name': name, 'row': line}
                all_rows.append(line)
    
    print(f"   Loaded {len(current_mappings)} current mappings")
    
    # 3) Identify changes needed
    print("\n2. Identifying changes needed...")
    
    # Map source agencies to existing mappings (case-insensitive)
    updates_needed = []
    missing_agencies = []
    
    for source_agency in source_agencies:
        source_upper = source_agency.upper()
        
        if source_upper in current_mappings:
            # Check if case needs updating
            current_name = current_mappings[source_upper]['name']
            if current_name != source_agency:
                updates_needed.append({
                    'id': current_mappings[source_upper]['id'],
                    'old_name': current_name,
                    'new_name': source_agency,
                    'action': 'case_update'
                })
        else:
            # Missing mapping
            record_count = len(df[df['Agency Name'] == source_agency])
            missing_agencies.append({
                'name': source_agency,
                'count': record_count
            })
    
    print(f"   Case updates needed: {len(updates_needed)}")
    print(f"   Missing mappings: {len(missing_agencies)}")
    
    # 4) Apply case updates
    print("\n3. Applying case updates...")
    updated_rows = []
    
    for row in all_rows:
        if row == header:
            updated_rows.append(row)
            continue
            
        if len(row) >= 2:
            aid, name = row[0], row[1]
            
            # Check if this row needs a case update
            for update in updates_needed:
                if update['id'] == aid and update['old_name'] == name:
                    row[1] = update['new_name']
                    print(f"   Updated ID {aid}: '{update['old_name']}' -> '{update['new_name']}'")
                    break
        
        updated_rows.append(row)
    
    # 5) Add missing mappings
    print("\n4. Adding missing mappings...")
    
    # Find next available ID
    used_ids = set()
    for row in updated_rows[1:]:  # Skip header
        if len(row) >= 1:
            used_ids.add(int(row[0]))
    
    next_id = 36
    while next_id in used_ids:
        next_id += 1
    
    # Add missing agencies
    for missing in sorted(missing_agencies, key=lambda x: -x['count']):  # Sort by record count desc
        new_row = [str(next_id), missing['name']] + [''] * (len(header) - 2)  # Fill remaining columns
        updated_rows.append(new_row)
        print(f"   Added ID {next_id}: '{missing['name']}' ({missing['count']:,} records)")
        next_id += 1
    
    # 6) Write updated mappings
    print("\n5. Writing updated mappings...")
    with open(agency_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(updated_rows)
    
    print(f"   Updated {agency_file}")
    
    # 7) Validation
    print("\n6. VALIDATION:")
    
    # Re-read the updated file
    new_mappings = {}
    with open(agency_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for line in reader:
            if len(line) >= 2:
                aid, name = line[0], line[1]
                new_mappings[name.upper()] = aid
    
    # Check coverage
    mapped_count = 0
    unmapped_count = 0
    
    for source_agency in source_agencies:
        if source_agency.upper() in new_mappings:
            mapped_count += 1
        else:
            unmapped_count += 1
            print(f"   ✗ Still missing: '{source_agency}'")
    
    print(f"\n   RESULTS:")
    print(f"   ✓ Mapped agencies: {mapped_count}/{len(source_agencies)}")
    print(f"   ✗ Unmapped agencies: {unmapped_count}/{len(source_agencies)}")
    
    if unmapped_count == 0:
        print("   🎉 ALL AGENCIES NOW MAPPED!")
    
    # Record impact
    total_records = len(df)
    mapped_records = 0
    
    for agency in source_agencies:
        if agency.upper() in new_mappings:
            mapped_records += len(df[df['Agency Name'] == agency])
    
    print(f"\n   RECORD IMPACT:")
    print(f"   Total records: {total_records:,}")
    print(f"   Mapped records: {mapped_records:,} ({mapped_records/total_records*100:.1f}%)")
    print(f"   Recovered records: {mapped_records - 2628556:,}")
    
    print(f"\n   SUMMARY:")
    print(f"   • Applied {len(updates_needed)} case updates")
    print(f"   • Added {len(missing_agencies)} new mappings")
    print(f"   • Backup saved to: {backup_file}")

if __name__ == "__main__":
    fix_all_mappings()