#!/usr/bin/env python3
"""
Test CSLB fix on small dataset to verify logic
"""

import psycopg2
import json
from pathlib import Path

def get_db_config():
    config_file = Path.home() / '.vk' / 'db_config.json'
    with open(config_file, 'r') as f:
        config = json.load(f)
        config['port'] = int(config['port'])
        return config

def test_fix():
    config = get_db_config()
    conn = psycopg2.connect(**config)
    cursor = conn.cursor()

    print("="*60)
    print("TESTING CSLB FIX ON 5 RECORDS")
    print("="*60)

    try:
        # 1. Show current state
        cursor.execute("""
            SELECT license_number,
                   CASE
                     WHEN url IS NULL THEN 'NULL'
                     WHEN url LIKE '%cslb.ca.gov%LicNum=%' THEN 'CORRECT'
                     ELSE 'WRONG'
                   END as status,
                   LEFT(url, 40) as url_preview
            FROM test_cslb_fix
            ORDER BY license_number
        """)

        print("\nBEFORE UPDATE:")
        for row in cursor.fetchall():
            print(f"  License: {row[0]:20} Status: {row[1]:10} URL: {row[2]}")

        # 2. Create mapping from source data
        cursor.execute("DROP TABLE IF EXISTS test_url_map")
        cursor.execute("""
            CREATE TEMP TABLE test_url_map AS
            SELECT DISTINCT
                bbb_id,
                license_nbr as license_number,
                MAX(agency_url) as correct_url
            FROM bbb_uploaded_data
            WHERE bbb_id = '1116'
              AND agency_name IN ('Contractors State Licensing Board',
                                  'Contactors State Licensing Board')
              AND agency_url LIKE '%cslb.ca.gov%LicNum=%'
              AND license_nbr IN (SELECT license_number FROM test_cslb_fix)
            GROUP BY bbb_id, license_nbr
        """)

        cursor.execute("SELECT COUNT(*) FROM test_url_map")
        map_count = cursor.fetchone()[0]
        print(f"\nCreated mapping with {map_count} URLs")

        # 3. Show what will be updated
        cursor.execute("""
            SELECT t.license_number, t.url as old_url, m.correct_url as new_url
            FROM test_cslb_fix t
            JOIN test_url_map m ON t.bbb_id = m.bbb_id
              AND t.license_number = m.license_number
            WHERE t.url IS NULL
               OR t.url NOT LIKE '%cslb.ca.gov%'
               OR (t.url LIKE '%cslb%' AND t.url NOT LIKE '%LicNum=%')
        """)

        updates = cursor.fetchall()
        print(f"\nWill update {len(updates)} records:")
        for lic, old, new in updates:
            old_display = 'NULL' if old is None else old[:40]
            print(f"  {lic}: {old_display} -> {new[:40]}...")

        # 4. Perform update
        response = input("\nPerform update? (yes/no): ")
        if response.lower() == 'yes':
            cursor.execute("""
                UPDATE test_cslb_fix t
                SET url = m.correct_url
                FROM test_url_map m
                WHERE t.bbb_id = m.bbb_id
                  AND t.license_number = m.license_number
                  AND (t.url IS NULL
                       OR t.url NOT LIKE '%cslb.ca.gov%'
                       OR (t.url LIKE '%cslb%' AND t.url NOT LIKE '%LicNum=%'))
            """)

            updated = cursor.rowcount
            conn.commit()
            print(f"\n✅ Updated {updated} records")

            # 5. Show results
            cursor.execute("""
                SELECT license_number,
                       CASE
                         WHEN url IS NULL THEN 'NULL'
                         WHEN url LIKE '%cslb.ca.gov%LicNum=%' THEN 'CORRECT'
                         ELSE 'WRONG'
                       END as status,
                       LEFT(url, 40) as url_preview
                FROM test_cslb_fix
                ORDER BY license_number
            """)

            print("\nAFTER UPDATE:")
            for row in cursor.fetchall():
                print(f"  License: {row[0]:20} Status: {row[1]:10} URL: {row[2]}")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    test_fix()