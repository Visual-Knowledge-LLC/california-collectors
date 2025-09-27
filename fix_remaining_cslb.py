#!/usr/bin/env python3
"""
Fix remaining CSLB records by directly constructing URLs from license numbers
No need for source data - just build the URL:
https://www2.cslb.ca.gov/OnlineServices/CheckLicenseII/LicenseDetail.aspx?LicNum={license_number}
"""

import psycopg2
import json
from pathlib import Path
import sys

def get_db_config():
    config_file = Path.home() / '.vk' / 'db_config.json'
    with open(config_file, 'r') as f:
        config = json.load(f)
        config['port'] = int(config['port'])
        return config

def fix_remaining():
    config = get_db_config()
    conn = psycopg2.connect(**config)
    cursor = conn.cursor()

    print("="*60)
    print("FIXING REMAINING CSLB RECORDS")
    print("Building URLs directly from license numbers")
    print("="*60)

    bbb_ids = ('1116', '1126', '1216', '1236')
    agency_ids = ('117', '3888', '13', '2150')

    tables = [
        ('match_results', 'agency_license_url'),
        ('business_licenses_updates', 'url'),
        ('stage_business_licenses_updates', 'url')
    ]

    total_fixed = 0

    try:
        for table_name, url_column in tables:
            print(f"\nUpdating {table_name}...")

            # Update all records with license numbers to have correct URLs
            update_sql = f"""
            UPDATE {table_name}
            SET {url_column} = 'https://www2.cslb.ca.gov/OnlineServices/CheckLicenseII/LicenseDetail.aspx?LicNum=' || license_number
            WHERE bbb_id IN %s
              AND agency_id IN %s
              AND license_number IS NOT NULL
              AND license_number != ''
              AND ({url_column} IS NULL
                   OR {url_column} NOT LIKE '%%cslb.ca.gov%%LicNum=%%')
            """

            cursor.execute(update_sql, (bbb_ids, agency_ids))
            updated = cursor.rowcount
            total_fixed += updated
            print(f"  ✅ Fixed {updated:,} records")

            # Commit after each table
            conn.commit()

        print(f"\n{'='*60}")
        print(f"TOTAL FIXED: {total_fixed:,} records")
        print(f"{'='*60}")

        # Verify results
        print("\nVERIFYING RESULTS...")
        for table_name, url_column in tables:
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM {table_name}
                WHERE bbb_id IN %s
                  AND agency_id IN %s
                  AND ({url_column} IS NULL
                       OR {url_column} NOT LIKE '%%cslb.ca.gov%%LicNum=%%')
            """, (bbb_ids, agency_ids))

            remaining = cursor.fetchone()[0]
            if remaining > 0:
                print(f"  ⚠️  {table_name}: {remaining:,} records still need fixing")
            else:
                print(f"  ✅ {table_name}: All CSLB records have correct URLs")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    response = input("This will update ~960K production records. Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("Aborted.")
        sys.exit(0)

    fix_remaining()