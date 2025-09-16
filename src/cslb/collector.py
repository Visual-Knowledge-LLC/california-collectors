"""
CSLB (Contractors State License Board) data collector.
Modernized version with progress tracking and improved error handling.
"""

import sys
import base64
import csv
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional

import requests
import pandas as pd

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))

from common.database import get_db_manager
from common.config import get_config
from common.progress import ScraperProgress

logger = logging.getLogger(__name__)


class CSLBCollector:
    """Collector for CSLB contractor license data."""

    def __init__(self, config=None, progress=None):
        """
        Initialize CSLB collector.

        Args:
            config: CollectorConfig instance
            progress: ScraperProgress instance
        """
        self.config = config or get_config()
        self.progress = progress or ScraperProgress("CA CSLB Collector")
        self.db = get_db_manager()

        # Load mappings
        self.zip_to_bbb = {}
        self.bbb_to_agency = {}

    def load_mappings(self) -> bool:
        """
        Load ZIP to BBB ID and BBB to Agency ID mappings.

        Returns:
            True if successful, False otherwise
        """
        try:
            self.progress.log("Loading mapping files")

            # Load ZIP to BBB ID mapping
            zip_file = self.config.get_input_path("zips/all_zips.csv")
            if zip_file.exists():
                with open(zip_file, "r", encoding="utf-8") as f:
                    reader = csv.reader(f)
                    for row in reader:
                        if len(row) >= 2:
                            zip_code, bbb_id = row[0], row[1]
                            self.zip_to_bbb[zip_code] = bbb_id
                self.progress.log(f"Loaded {len(self.zip_to_bbb)} ZIP code mappings")
            else:
                self.progress.log(f"ZIP mapping file not found: {zip_file}", level="warning")
                return False

            # Load BBB to Agency ID mapping
            agency_file = self.config.get_input_path("licensing_agencies/cslb_agency_ids.csv")
            if agency_file.exists():
                with open(agency_file, "r", encoding="utf-8") as f:
                    reader = csv.reader(f)
                    for row in reader:
                        if len(row) >= 2:
                            bbb_id = row[0].replace('\ufeff', '')
                            agency_id = row[1]
                            self.bbb_to_agency[bbb_id] = agency_id
                self.progress.log(f"Loaded {len(self.bbb_to_agency)} agency ID mappings")
            else:
                self.progress.log(f"Agency mapping file not found: {agency_file}", level="warning")
                return False

            return True

        except Exception as e:
            self.progress.log_error(e, "Loading mappings")
            return False

    def fetch_data(self) -> Optional[str]:
        """
        Fetch data from CSLB API.

        Returns:
            Decoded CSV data or None if failed
        """
        try:
            self.progress.set_phase('COLLECT', 'Downloading CSLB master file')

            # Prepare SOAP request
            payload = f"""<?xml version="1.0" encoding="utf-8"?>
            <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                          xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                          xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
              <soap:Body>
                <GetMasterFile xmlns="http://CSLB.Ca.gov/">
                  <fileType>CSV</fileType>
                  <Token>{self.config.cslb.api_token}</Token>
                </GetMasterFile>
              </soap:Body>
            </soap:Envelope>"""

            headers = {
                'Host': 'www.cslb.ca.gov',
                'Content-Type': 'text/xml; charset=utf-8',
                'SoapAction': 'http://CSLB.Ca.gov/GetMasterFile'
            }

            # Make API request
            self.progress.log(f"Making API request to {self.config.cslb.api_url}")
            response = requests.post(
                self.config.cslb.api_url,
                headers=headers,
                data=payload.encode('utf-8'),
                timeout=300  # 5 minute timeout for large file
            )
            response.raise_for_status()

            self.progress.log(f"Response received, status: {response.status_code}")

            # Extract base64 content from SOAP response
            content = str(response.content)

            # Remove SOAP envelope
            content = content.replace('<?xml version="1.0" encoding="utf-8"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema"><soap:Body><GetMasterFileResponse xmlns="http://CSLB.Ca.gov/"><GetMasterFileResult>', '')
            content = content.replace('</GetMasterFileResult></GetMasterFileResponse></soap:Body></soap:Envelope>', '')

            # Decode base64
            data = content.encode().decode('utf-8').splitlines()
            file_string = ''.join(data)[2:-1]  # Remove b' and ' from string representation
            decoded = base64.b64decode(file_string).decode('utf-8', errors='replace')

            # Save to temp file
            temp_file = self.config.get_temp_path("cslb_data.csv")
            with open(temp_file, "w", encoding="utf-8") as f:
                f.write(decoded)

            self.progress.log(f"Data saved to {temp_file}")
            return str(temp_file)

        except requests.RequestException as e:
            self.progress.log_error(e, "API request failed")
            return None
        except Exception as e:
            self.progress.log_error(e, "Fetching data")
            return None

    def process_records(self, csv_file: str) -> List[Dict[str, Any]]:
        """
        Process CSLB records from CSV file.

        Args:
            csv_file: Path to CSV file

        Returns:
            List of processed records ready for insertion
        """
        self.progress.set_phase('PROCESS', 'Processing CSLB records')

        try:
            # Load CSV
            df = pd.read_csv(csv_file)
            total_records = len(df)
            self.progress.set_total(total_records)
            self.progress.log(f"Processing {total_records:,} records")

            records = []
            skipped_stats = {
                'no_bbb_id': 0,
                'no_agency_id': 0,
                'invalid_license': 0,
                'unknown_zips': set()
            }

            with self.progress.progress_bar(total_records, "Processing records") as pbar:
                for idx, row in df.iterrows():
                    try:
                        # Process ZIP code
                        zip_code = str(row['ZIPCode'])[:5] if pd.notna(row['ZIPCode']) else ''
                        bbb_id = self.zip_to_bbb.get(zip_code)

                        if not bbb_id:
                            skipped_stats['no_bbb_id'] += 1
                            if zip_code and zip_code not in skipped_stats['unknown_zips']:
                                skipped_stats['unknown_zips'].add(zip_code)
                            pbar.update(1)
                            continue

                        # Get agency ID
                        agency_id = self.bbb_to_agency.get(bbb_id)
                        if not agency_id:
                            skipped_stats['no_agency_id'] += 1
                            pbar.update(1)
                            continue

                        # Validate license number
                        if pd.isna(row['LicenseNo']) or not str(row['LicenseNo']).strip():
                            skipped_stats['invalid_license'] += 1
                            pbar.update(1)
                            continue

                        # Create UUID
                        uuid = f"{agency_id}{row['LicenseNo']}"
                        if 'None' in uuid or 'nan' in uuid.lower():
                            skipped_stats['invalid_license'] += 1
                            pbar.update(1)
                            continue

                        # Determine business name
                        business_name = row.get('FullBusinessName', '')
                        if not business_name or pd.isna(business_name):
                            business_name = row.get('BusinessName', '')

                        # Create record
                        record = {
                            'uuid': uuid,
                            'bbb_id': bbb_id,
                            'agency_id': agency_id,
                            'business_name': business_name,
                            'street': row.get('MailingAddress', ''),
                            'city': row.get('City', ''),
                            'zip': row.get('ZIPCode', ''),
                            'state_established': row.get('State', ''),
                            'date_established': row.get('IssueDate', ''),
                            'license_nbr': str(row['LicenseNo']),
                            'agency_url': f"https://www2.cslb.ca.gov/OnlineServices/CheckLicenseII/LicenseDetail.aspx?LicNum={row['LicenseNo']}",
                            'phone_number': row.get('BusinessPhone', ''),
                            'license_expiration': row.get('ExpirationDate', ''),
                            'license_status': row.get('PrimaryStatus', ''),
                            'reportable_data': 'false',
                            'agency_name': self.config.cslb.agency_name,
                            'category': row.get('Classifications(s)', '')
                        }

                        records.append(record)

                    except Exception as e:
                        self.progress.log(f"Error processing row {idx}: {e}", level="debug")
                        skipped_stats['invalid_license'] += 1

                    pbar.update(1)
                    self.progress.update(1)

            # Log statistics
            self.progress.log(f"Valid records: {len(records):,}")
            self.progress.log(f"Skipped - No BBB ID: {skipped_stats['no_bbb_id']:,}")
            self.progress.log(f"Skipped - No Agency ID: {skipped_stats['no_agency_id']:,}")
            self.progress.log(f"Skipped - Invalid license: {skipped_stats['invalid_license']:,}")

            if skipped_stats['unknown_zips']:
                self.progress.log(f"Unknown ZIP codes: {len(skipped_stats['unknown_zips'])}")
                # Save unknown ZIPs for review
                unknown_zips_file = self.config.get_output_path("unknown_zips.txt")
                with open(unknown_zips_file, "w") as f:
                    f.write("\n".join(sorted(skipped_stats['unknown_zips'])))
                self.progress.log(f"Unknown ZIPs saved to {unknown_zips_file}")

            return records

        except Exception as e:
            self.progress.log_error(e, "Processing records")
            return []

    def upload_records(self, records: List[Dict[str, Any]]) -> bool:
        """
        Upload records to database.

        Args:
            records: List of records to upload

        Returns:
            True if successful, False otherwise
        """
        if not records:
            self.progress.log("No records to upload", level="warning")
            return True

        try:
            self.progress.set_phase('UPLOAD', 'Uploading to database')

            # Clear delta table if using it
            if self.config.use_delta_table:
                self.db.clear_delta_table()
                table_name = "delta_bbb_uploaded_data"
            else:
                table_name = "bbb_uploaded_data"

            # Upload in batches
            with self.progress.progress_bar(len(records), f"Uploading to {table_name}") as pbar:
                batch_size = self.config.db_batch_size

                for i in range(0, len(records), batch_size):
                    batch = records[i:i + batch_size]

                    try:
                        self.db.bulk_insert(table_name, batch, batch_size=1000)
                        self.progress.increment_uploaded(len(batch))
                        pbar.update(len(batch))
                    except Exception as e:
                        self.progress.log_error(e, f"Batch upload at index {i}")
                        self.progress.increment_failed(len(batch))
                        pbar.update(len(batch))

            # Merge delta to main if using delta table
            if self.config.use_delta_table:
                self.progress.set_phase('UPLOAD', 'Merging to main table')
                merged_count = self.db.merge_delta_to_main()
                self.progress.log(f"Merged {merged_count:,} records to main table")

            return True

        except Exception as e:
            self.progress.log_error(e, "Uploading records")
            return False

    def run(self) -> bool:
        """
        Run the complete CSLB collection process.

        Returns:
            True if successful, False otherwise
        """
        try:
            # Configuration phase
            self.progress.set_phase('CONFIG', 'Setting up collector')

            # Test database connection
            if not self.db.test_connection():
                raise Exception("Database connection failed")

            # Load mappings
            if not self.load_mappings():
                raise Exception("Failed to load mapping files")

            # Fetch data
            csv_file = self.fetch_data()
            if not csv_file:
                raise Exception("Failed to fetch CSLB data")

            # Process records
            records = self.process_records(csv_file)
            if not records:
                self.progress.log("No valid records to process", level="warning")
                return True

            # Upload records
            if not self.upload_records(records):
                raise Exception("Failed to upload records")

            # Cleanup
            self.progress.set_phase('CLEANUP', 'Cleaning up')
            self.db.close()

            # Complete
            self.progress.complete()
            return True

        except Exception as e:
            self.progress.set_phase('ERROR')
            self.progress.log_error(e, "Main execution")
            return False


def main():
    """Main entry point for CSLB collector."""
    collector = CSLBCollector()
    success = collector.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()