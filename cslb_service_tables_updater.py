#!/usr/bin/env python3
"""
CSLB Service Tables URL Updater
================================

PURPOSE:
Fix incorrect URLs in service tables for California Contractors State License Board (CSLB) records.
Business website URLs are incorrectly populating CSLB license records instead of the proper
CSLB license lookup URLs.

PROBLEM SUMMARY:
- Business website URLs are being used instead of CSLB license lookup URLs
- Affects all California BBBs: 1116, 1126, 1216, 1236
- Total ~2.7 million records need fixing across multiple tables

DATA VERIFICATION:
- Source of truth: bbb_uploaded_data table (189,607 unique CSLB licenses)
- BBB 1116: 44,471 unique licenses
- BBB 1126: 54,965 unique licenses
- BBB 1216: 74,791 unique licenses
- BBB 1236: 15,380 unique licenses

TABLES TO UPDATE:
1. match_results - 2.5M records (massive duplication 10-30x per license)
2. business_licenses_updates - 105K records
3. stage_business_licenses_updates - 105K records

AGENCY MAPPINGS:
- BBB 1116 → Agency 117
- BBB 1126 → Agency 3888
- BBB 1216 → Agency 13
- BBB 1236 → Agency 2150

URL FORMAT:
- Incorrect: Business websites or https://www2.cslb.ca.gov/.../checklicense.aspx (no license)
- Correct: https://www2.cslb.ca.gov/OnlineServices/CheckLicenseII/LicenseDetail.aspx?LicNum=123456

NOTES:
- Handles both "Contractors" and "Contactors" (common misspelling)
- This is a temporary fix until VK URL priority is reversed in ETL/Service1
- After fix, run approval service to push to Blue API via queue automation

Author: Claude
Date: September 2024
"""

import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values
import argparse
import sys
import signal
from datetime import datetime
from io import StringIO
from tqdm import tqdm
import logging
import os
import json
from pathlib import Path
from typing import Dict, List, Tuple, Optional


def get_db_config():
    """Get database configuration from ~/.vk/db_config.json"""
    config_file = Path.home() / '.vk' / 'db_config.json'

    if config_file.exists():
        with open(config_file, 'r') as f:
            config = json.load(f)
            # Ensure port is an integer
            config['port'] = int(config['port']) if isinstance(config['port'], str) else config['port']
            return config
    else:
        # Fallback to environment variables
        return {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', '5433')),
            'database': os.getenv('DB_NAME', 'data_uploader'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'postgres')
        }


# Get database configuration
db_config = get_db_config()


class CSLBServiceTablesUpdater:
    """
    Updates CSLB URLs in service tables to correct format.

    Fixes ~2.7 million records across match_results, business_licenses_updates,
    and stage_business_licenses_updates tables.
    """

    def __init__(self, batch_size: int = 5000):
        """
        Initialize the CSLB URL updater.

        Args:
            batch_size: Number of records to process per batch
        """
        self.batch_size = batch_size
        self.conn = None
        self.cursor = None
        self.interrupted = False

        # CSLB agency name variants (including common misspelling)
        self.cslb_agency_names = [
            'Contractors State Licensing Board',
            'Contactors State Licensing Board'  # Common misspelling
        ]

        # BBB to Agency ID mapping
        self.bbb_agency_map = {
            '1116': '117',   # 44,471 CSLB licenses
            '1126': '3888',  # 54,965 CSLB licenses
            '1216': '13',    # 74,791 CSLB licenses
            '1236': '2150'   # 15,380 CSLB licenses
        }

        # All BBBs and agency IDs
        self.bbb_ids = list(self.bbb_agency_map.keys())
        self.agency_ids = list(self.bbb_agency_map.values())

        # Statistics tracking
        self.stats = {
            'total_records': 0,
            'records_updated': 0,
            'records_skipped': 0,
            'errors': 0,
            'null_urls_fixed': 0,
            'wrong_urls_fixed': 0,
            'cslb_format_fixed': 0
        }

        # Set up signal handler
        signal.signal(signal.SIGINT, self._signal_handler)

        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _signal_handler(self, signum, frame):
        """Handle interrupt signal gracefully."""
        print("\n\n⚠️  Interrupt received. Finishing current batch...")
        self.interrupted = True

    def connect(self):
        """Establish database connection."""
        try:
            # Handle both dict and object configs
            if isinstance(db_config, dict):
                self.conn = psycopg2.connect(
                    host=db_config['host'],
                    port=db_config['port'],
                    database=db_config['database'],
                    user=db_config['user'],
                    password=db_config['password']
                )
            else:
                self.conn = psycopg2.connect(db_config.connection_string)

            self.cursor = self.conn.cursor()
            self.logger.info("✅ Database connection established")

            # Test connection
            self.cursor.execute("SELECT version()")
            version = self.cursor.fetchone()
            self.logger.info(f"PostgreSQL version: {version[0][:50]}...")

        except Exception as e:
            self.logger.error(f"❌ Failed to connect to database: {e}")
            raise

    def disconnect(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.logger.info("Database connection closed")

    def create_url_mapping(self, dry_run: bool = False) -> int:
        """
        Create temporary table with correct CSLB URLs from bbb_uploaded_data.

        Returns:
            Number of URL mappings created
        """
        self.logger.info("Creating CSLB URL mapping from source data...")

        try:
            # Drop temp table if exists
            self.cursor.execute("DROP TABLE IF EXISTS cslb_url_map")

            # Create mapping table with correct URLs
            create_sql = """
            CREATE TEMP TABLE cslb_url_map AS
            SELECT DISTINCT
                bbb_id,
                license_nbr as license_number,
                MAX(agency_url) as correct_url
            FROM bbb_uploaded_data
            WHERE bbb_id IN %s
              AND agency_name IN %s
              AND agency_url LIKE '%%cslb.ca.gov%%LicNum=%%'
            GROUP BY bbb_id, license_nbr
            """

            self.cursor.execute(create_sql, (
                tuple(self.bbb_ids),
                tuple(self.cslb_agency_names)
            ))

            # Create index for performance
            self.cursor.execute(
                "CREATE INDEX idx_cslb_map ON cslb_url_map(bbb_id, license_number)"
            )

            # Get count
            self.cursor.execute("SELECT COUNT(*) FROM cslb_url_map")
            count = self.cursor.fetchone()[0]

            self.logger.info(f"✅ Created URL mapping with {count:,} correct URLs")

            if not dry_run:
                self.conn.commit()

            return count

        except Exception as e:
            self.logger.error(f"Error creating URL mapping: {e}")
            self.conn.rollback()
            raise

    def analyze_table(self, table_name: str, url_column: str = 'url') -> Dict:
        """
        Analyze a table to see how many records need updating.

        Args:
            table_name: Name of table to analyze
            url_column: Name of URL column in the table

        Returns:
            Dictionary with analysis results
        """
        self.logger.info(f"Analyzing {table_name}...")

        # Adjust column name for match_results
        if table_name == 'match_results':
            url_column = 'agency_license_url'

        # Build WHERE clause based on table
        if table_name == 'match_results':
            where_clause = f"""
                bbb_id IN %s
                AND agency_id IN %s
            """
            where_params = (tuple(self.bbb_ids), tuple(self.agency_ids))
        else:
            where_clause = f"""
                bbb_id IN %s
                AND agency_id IN %s
            """
            where_params = (tuple(self.bbb_ids), tuple(self.agency_ids))

        # Count different URL types
        analysis_sql = f"""
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN {url_column} IS NULL THEN 1 END) as null_urls,
            COUNT(CASE WHEN {url_column} LIKE '%%cslb%%' AND {url_column} LIKE '%%LicNum=%%' THEN 1 END) as correct_urls,
            COUNT(CASE WHEN {url_column} LIKE '%%cslb%%' AND {url_column} NOT LIKE '%%LicNum=%%' THEN 1 END) as wrong_cslb_format,
            COUNT(CASE WHEN {url_column} LIKE 'http%%' AND {url_column} NOT LIKE '%%cslb%%' THEN 1 END) as wrong_urls
        FROM {table_name}
        WHERE {where_clause}
        """

        self.cursor.execute(analysis_sql, where_params)
        result = self.cursor.fetchone()

        analysis = {
            'table': table_name,
            'total': result[0] or 0,
            'null_urls': result[1] or 0,
            'correct_urls': result[2] or 0,
            'wrong_cslb_format': result[3] or 0,
            'wrong_urls': result[4] or 0,
            'needs_update': (result[1] or 0) + (result[3] or 0) + (result[4] or 0)
        }

        self.logger.info(f"  Total records: {analysis['total']:,}")
        self.logger.info(f"  Correct URLs: {analysis['correct_urls']:,}")
        self.logger.info(f"  Needs update: {analysis['needs_update']:,}")
        self.logger.info(f"    - Null URLs: {analysis['null_urls']:,}")
        self.logger.info(f"    - Wrong CSLB format: {analysis['wrong_cslb_format']:,}")
        self.logger.info(f"    - Wrong URLs (business sites): {analysis['wrong_urls']:,}")

        return analysis

    def update_table(self, table_name: str, dry_run: bool = False) -> int:
        """
        Update URLs in a specific table.

        Args:
            table_name: Name of table to update
            dry_run: If True, only simulate the update

        Returns:
            Number of records updated
        """
        # Determine URL column name
        url_column = 'agency_license_url' if table_name == 'match_results' else 'url'

        self.logger.info(f"\n{'[DRY RUN] ' if dry_run else ''}Updating {table_name}...")

        try:
            # Build update query
            if table_name == 'match_results':
                update_sql = f"""
                UPDATE {table_name} m
                SET {url_column} = c.correct_url
                FROM cslb_url_map c
                WHERE m.bbb_id = c.bbb_id
                  AND m.license_number = c.license_number
                  AND m.agency_id IN %s
                  AND (
                    m.{url_column} IS NULL
                    OR m.{url_column} NOT LIKE '%%cslb.ca.gov%%'
                    OR (m.{url_column} LIKE '%%cslb%%' AND m.{url_column} NOT LIKE '%%LicNum=%%')
                  )
                RETURNING m.bbb_id, m.license_number
                """
                params = (tuple(self.agency_ids),)
            else:
                update_sql = f"""
                UPDATE {table_name} b
                SET {url_column} = c.correct_url
                FROM cslb_url_map c
                WHERE b.bbb_id = c.bbb_id
                  AND b.license_number = c.license_number
                  AND b.agency_id IN %s
                  AND (
                    b.{url_column} IS NULL
                    OR b.{url_column} NOT LIKE '%%cslb.ca.gov%%'
                    OR (b.{url_column} LIKE '%%cslb%%' AND b.{url_column} NOT LIKE '%%LicNum=%%')
                  )
                RETURNING b.bbb_id, b.license_number
                """
                params = (tuple(self.agency_ids),)

            if dry_run:
                # Just count how many would be updated
                if table_name == 'match_results':
                    count_sql = f"""
                    SELECT COUNT(*)
                    FROM {table_name} m
                    JOIN cslb_url_map c ON m.bbb_id = c.bbb_id
                      AND m.license_number = c.license_number
                    WHERE m.agency_id IN %s
                      AND (
                        m.{url_column} IS NULL
                        OR m.{url_column} NOT LIKE '%%cslb.ca.gov%%'
                        OR (m.{url_column} LIKE '%%cslb%%' AND m.{url_column} NOT LIKE '%%LicNum=%%')
                      )
                    """
                else:
                    count_sql = f"""
                    SELECT COUNT(*)
                    FROM {table_name} b
                    JOIN cslb_url_map c ON b.bbb_id = c.bbb_id
                      AND b.license_number = c.license_number
                    WHERE b.agency_id IN %s
                      AND (
                        b.{url_column} IS NULL
                        OR b.{url_column} NOT LIKE '%%cslb.ca.gov%%'
                        OR (b.{url_column} LIKE '%%cslb%%' AND b.{url_column} NOT LIKE '%%LicNum=%%')
                      )
                    """
                self.cursor.execute(count_sql, params)
                count = self.cursor.fetchone()[0]
                self.logger.info(f"  [DRY RUN] Would update {count:,} records")
                return count

            # Execute actual update
            self.cursor.execute(update_sql, params)
            updated_records = self.cursor.fetchall()
            count = len(updated_records)

            self.stats['records_updated'] += count
            self.logger.info(f"  ✅ Updated {count:,} records")

            return count

        except Exception as e:
            self.logger.error(f"Error updating {table_name}: {e}")
            if not dry_run:
                self.conn.rollback()
            raise

    def run(self, dry_run: bool = False, tables: Optional[List[str]] = None):
        """
        Run the complete CSLB URL update process.

        Args:
            dry_run: If True, only simulate updates
            tables: List of specific tables to update (default: all)
        """
        start_time = datetime.now()

        if not tables:
            tables = [
                'match_results',
                'business_licenses_updates',
                'stage_business_licenses_updates'
            ]

        try:
            # Connect to database
            self.connect()

            # Analyze current state
            self.logger.info("\n" + "="*60)
            self.logger.info("ANALYZING CURRENT STATE")
            self.logger.info("="*60)

            analyses = {}
            total_needs_update = 0
            for table in tables:
                analysis = self.analyze_table(table)
                analyses[table] = analysis
                total_needs_update += analysis['needs_update']

            if total_needs_update == 0:
                self.logger.info("\n✅ All URLs are already correct! No updates needed.")
                return

            self.logger.info(f"\nTotal records needing updates: {total_needs_update:,}")

            # Create URL mapping
            self.logger.info("\n" + "="*60)
            self.logger.info("CREATING URL MAPPING")
            self.logger.info("="*60)

            mapping_count = self.create_url_mapping(dry_run)

            if mapping_count == 0:
                self.logger.error("❌ No URL mappings found. Cannot proceed.")
                return

            # Update each table
            self.logger.info("\n" + "="*60)
            self.logger.info(f"{'[DRY RUN] ' if dry_run else ''}UPDATING TABLES")
            self.logger.info("="*60)

            for table in tables:
                if self.interrupted:
                    self.logger.warning("Process interrupted by user")
                    break

                if analyses[table]['needs_update'] > 0:
                    self.update_table(table, dry_run)
                else:
                    self.logger.info(f"\n✓ {table} - No updates needed")

            # Commit if not dry run
            if not dry_run and not self.interrupted:
                self.conn.commit()
                self.logger.info("\n✅ All changes committed to database")
            elif dry_run:
                self.logger.info("\n[DRY RUN] No changes made to database")

            # Verify results
            if not dry_run and not self.interrupted:
                self.logger.info("\n" + "="*60)
                self.logger.info("VERIFYING RESULTS")
                self.logger.info("="*60)

                for table in tables:
                    analysis = self.analyze_table(table)
                    if analysis['needs_update'] > 0:
                        self.logger.warning(f"⚠️  {table} still has {analysis['needs_update']:,} records needing updates")

            # Print summary
            elapsed = datetime.now() - start_time
            self.logger.info("\n" + "="*60)
            self.logger.info("SUMMARY")
            self.logger.info("="*60)
            self.logger.info(f"Total records updated: {self.stats['records_updated']:,}")
            self.logger.info(f"Time elapsed: {elapsed}")

            if not dry_run and self.stats['records_updated'] > 0:
                self.logger.info("\n✅ SUCCESS! CSLB URLs have been fixed.")
                self.logger.info("Next steps:")
                self.logger.info("1. Run approval service to stage the changes")
                self.logger.info("2. Queue automation will push to Blue API")

        except Exception as e:
            self.logger.error(f"Fatal error: {e}")
            if self.conn:
                self.conn.rollback()
            raise
        finally:
            self.disconnect()


def main():
    """Main entry point for the CSLB URL updater."""
    parser = argparse.ArgumentParser(
        description='Fix CSLB URLs in VK service tables',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run to see what would be updated
  python cslb_service_tables_updater.py --dry-run

  # Update all tables
  python cslb_service_tables_updater.py

  # Update specific table only
  python cslb_service_tables_updater.py --tables business_licenses_updates

  # Update multiple specific tables
  python cslb_service_tables_updater.py --tables match_results business_licenses_updates
        """
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Simulate the update without making changes'
    )

    parser.add_argument(
        '--tables',
        nargs='+',
        choices=['match_results', 'business_licenses_updates', 'stage_business_licenses_updates'],
        help='Specific tables to update (default: all)'
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        default=5000,
        help='Number of records to process per batch (default: 5000)'
    )

    args = parser.parse_args()

    print("""
╔══════════════════════════════════════════════════════════╗
║           CSLB Service Tables URL Updater               ║
║                                                          ║
║  Fixes ~2.7M records across service tables where        ║
║  business URLs incorrectly replaced CSLB license URLs   ║
╚══════════════════════════════════════════════════════════╝
    """)

    if args.dry_run:
        print("🔍 DRY RUN MODE - No changes will be made\n")
    else:
        print("⚠️  PRODUCTION MODE - Database will be updated")
        response = input("Continue? (yes/no): ")
        if response.lower() != 'yes':
            print("Aborted by user")
            sys.exit(0)

    # Create and run updater
    updater = CSLBServiceTablesUpdater(batch_size=args.batch_size)

    try:
        updater.run(dry_run=args.dry_run, tables=args.tables)
    except KeyboardInterrupt:
        print("\n\n❌ Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()