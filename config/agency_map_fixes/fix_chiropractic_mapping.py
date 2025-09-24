import csv
import os
import shutil

def fix_chiropractic_mapping():
    """Fix the chiropractic agency mapping by updating the CSV file"""
    
    agency_file = '../inputs/agency_matches_all.csv'
    backup_file = '../inputs/agency_matches_all_backup.csv'
    
    if not os.path.exists(agency_file):
        print(f"Agency file not found at {agency_file}")
        return
    
    # Create backup
    shutil.copy2(agency_file, backup_file)
    print(f"Created backup at {backup_file}")
    
    # Read current mappings
    rows = []
    with open(agency_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        rows = list(reader)
    
    # Find and update the chiropractic row
    updated = False
    for i, row in enumerate(rows):
        if len(row) >= 2 and row[0] == '35':
            old_name = row[1]
            row[1] = 'Board Of Chiropractic Examiners'  # Match the CSV data exactly
            print(f"Updated row {i}: '{old_name}' -> '{row[1]}'")
            updated = True
            break
    
    if updated:
        # Write updated mappings
        with open(agency_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(rows)
        print(f"Updated {agency_file}")
        
        # Verify the change
        print("\nVerifying update...")
        with open(agency_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            for line in reader:
                if len(line) >= 2 and line[0] == '35':
                    print(f"✓ Verified: ID 35 -> '{line[1]}'")
                    break
    else:
        print("Could not find ID 35 in the mapping file")

if __name__ == "__main__":
    fix_chiropractic_mapping()