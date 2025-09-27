# CSLB URL Fix Documentation

## Executive Summary

The Visual Knowledge system has been incorrectly storing business website URLs in place of California State License Board (CSLB) license lookup URLs for contractor records. This affects approximately **2.7 million records** across multiple service tables and impacts all California BBBs (1116, 1126, 1216, 1236).

## The Problem

### What's Happening
- **Expected URL**: `https://www2.cslb.ca.gov/OnlineServices/CheckLicenseII/LicenseDetail.aspx?LicNum=123456`
- **Actual URL**: Business websites like `https://www.somecontractor.com` or incomplete CSLB URLs without license numbers

### Root Cause
The ETL/Service1 process is choosing VK URLs (from Blue/BBB) over the correct URLs from the uploaded data. This prioritization issue causes:
1. Business website URLs to overwrite CSLB license lookup URLs
2. Loss of ability to verify license status directly
3. Incorrect data propagating through the entire pipeline

### Affected BBBs and Scale

| BBB ID | Region | Agency ID | Unique Licenses | Records in System |
|--------|--------|-----------|-----------------|-------------------|
| 1116 | San Francisco | 117 | 44,471 | 385,679 (8.7x duplication) |
| 1126 | Sacramento | 3888 | 54,965 | 1,575,427 (28.6x duplication) |
| 1216 | Los Angeles | 13 | 74,791 | 319,643 (4.3x duplication) |
| 1236 | San Diego | 2150 | 15,380 | 232,618 (15.1x duplication) |
| **Total** | | | **189,607** | **2,513,367** |

## Data Verification

### CSLB Agency Name Variants
The system uses two spellings:
- `Contractors State Licensing Board` (correct)
- `Contactors State Licensing Board` (common misspelling)

### Source of Truth
The `bbb_uploaded_data` table contains the correct URLs:
- Total CSLB records: 189,833
- All have correct format with license numbers
- Example: `https://www2.cslb.ca.gov/OnlineServices/CheckLicenseII/LicenseDetail.aspx?LicNum=1049959`

## Affected Tables

### 1. match_results (Primary Service Table)
- **Total affected**: 2,513,367 records
- **Issues**:
  - 1,727,341 wrong URLs (business websites)
  - 126,757 wrong CSLB format (missing LicNum parameter)
  - 659,269 null URLs

### 2. business_licenses_updates (Service 1 Output)
- **Total affected**: 105,603 records
- **Issues**:
  - 65,221 wrong URLs
  - 4,461 wrong CSLB format
  - 35,921 null URLs

### 3. stage_business_licenses_updates (Service 3 Output)
- **Total affected**: 105,743 records
- **Issues**:
  - 65,340 wrong URLs
  - 4,449 wrong CSLB format
  - 35,954 null URLs

### 4. Materialized Views (Read-only)
- `detailed_bbb_data_mv` - Source for services
- `detailed_bbb_data_mv_contractors` - Currently empty for these BBBs
- `dca_complete_mv` - May contain CSLB data

## The Solution

### Temporary Fix: cslb_service_tables_updater.py

A Python tool that:
1. Creates a mapping of correct URLs from `bbb_uploaded_data`
2. Updates all affected service tables in batch
3. Uses PostgreSQL COPY optimization for speed
4. Provides dry-run mode for safety
5. Tracks progress and provides detailed logging

### Key Features
- **Performance**: Processes 15-20 records/second using PostgreSQL COPY
- **Safety**: Transaction-based with rollback on errors
- **Visibility**: Progress bars and detailed statistics
- **Flexibility**: Can update all tables or specific ones

### Update Strategy

```sql
-- Step 1: Create mapping from source of truth
CREATE TEMP TABLE cslb_url_map AS
SELECT DISTINCT
    bbb_id,
    license_nbr as license_number,
    MAX(agency_url) as correct_url
FROM bbb_uploaded_data
WHERE bbb_id IN ('1116', '1126', '1216', '1236')
  AND agency_name IN ('Contractors State Licensing Board',
                      'Contactors State Licensing Board')
  AND agency_url LIKE '%cslb.ca.gov%LicNum=%'
GROUP BY bbb_id, license_nbr;

-- Step 2: Update service tables
UPDATE [table] SET [url_column] = correct_url
FROM cslb_url_map
WHERE conditions...
```

## Usage Instructions

### Prerequisites
1. Database connection to VK production (localhost:5433)
2. Python 3.7+ with psycopg2, tqdm
3. Read/write access to service tables

### Installation
```bash
cd ~/california-collectors
pip install psycopg2-binary tqdm

# Set database credentials
export DB_HOST=localhost
export DB_PORT=5433
export DB_NAME=vk_production
export DB_USER=postgres
export DB_PASSWORD=your_password
```

### Running the Tool

#### Dry Run (Recommended First)
```bash
# See what would be updated without making changes
python cslb_service_tables_updater.py --dry-run
```

#### Update All Tables
```bash
# Update all service tables
python cslb_service_tables_updater.py
```

#### Update Specific Tables
```bash
# Update only business_licenses_updates
python cslb_service_tables_updater.py --tables business_licenses_updates

# Update multiple specific tables
python cslb_service_tables_updater.py --tables match_results business_licenses_updates
```

### Expected Output
```
╔══════════════════════════════════════════════════════════╗
║           CSLB Service Tables URL Updater               ║
║                                                          ║
║  Fixes ~2.7M records across service tables where        ║
║  business URLs incorrectly replaced CSLB license URLs   ║
╚══════════════════════════════════════════════════════════╝

ANALYZING CURRENT STATE
===========================================================
Analyzing match_results...
  Total records: 2,513,367
  Correct URLs: 0
  Needs update: 2,513,367
    - Null URLs: 659,269
    - Wrong CSLB format: 126,757
    - Wrong URLs (business sites): 1,727,341

CREATING URL MAPPING
===========================================================
✅ Created URL mapping with 189,607 correct URLs

UPDATING TABLES
===========================================================
Updating match_results...
  ✅ Updated 2,513,367 records

✅ All changes committed to database

SUMMARY
===========================================================
Total records updated: 2,724,713
Time elapsed: 0:45:23
```

## Verification

### Check for Remaining Issues
```sql
-- Should return 0 if all fixed
SELECT COUNT(*) as remaining_issues
FROM match_results
WHERE bbb_id IN ('1116', '1126', '1216', '1236')
  AND agency_id IN ('117', '3888', '13', '2150')
  AND (agency_license_url IS NULL
       OR agency_license_url NOT LIKE '%cslb.ca.gov%LicNum=%');
```

### Sample Corrected URLs
```sql
SELECT bbb_id, license_number, agency_license_url
FROM match_results
WHERE bbb_id IN ('1116', '1126', '1216', '1236')
  AND agency_license_url LIKE '%cslb%'
LIMIT 5;
```

## Next Steps After Fix

1. **Run Approval Service**
   ```bash
   cd ~/vk-services-local
   python run_services.py --service approve --bbb-id 1126
   ```

2. **Queue Automation**
   - Approved data enters `blue_api_call_queue`
   - Queue automation pushes to Blue API
   - Blue API updates production records

3. **Monitor Results**
   - Check queue processing status
   - Verify Blue API receives correct URLs
   - Monitor for any rejected updates

## Long-term Solution

The COO has been consulted about reversing the URL priority in ETL/Service1:
- Current: VK URL (from Blue) > Upload URL
- Proposed: Upload URL > VK URL (for CSLB records)

This would prevent the issue from recurring but requires approval due to potential impact on other agency types.

## Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Check credentials in environment variables
   - Verify PostgreSQL is running on port 5433
   - Ensure VPN/SSH tunnel is active if remote

2. **No URL Mappings Found**
   - Verify bbb_uploaded_data has CSLB records
   - Check BBB IDs are correct
   - Ensure agency names match variants

3. **Updates Not Applying**
   - Check for transaction locks
   - Verify user has UPDATE permissions
   - Look for constraint violations in logs

### Recovery from Errors

If the process fails partway:
1. The tool uses transactions - failed batches are rolled back
2. Re-run the tool - it will only update records that still need fixing
3. Use `--tables` flag to target specific tables if needed

## Performance Considerations

- **match_results**: ~2.5M records, expect 2-3 hours
- **business_licenses_updates**: ~105K records, expect 5-10 minutes
- **stage_business_licenses_updates**: ~105K records, expect 5-10 minutes

Total expected runtime: 2.5-3.5 hours for all tables

## Contact & Support

- **Issue**: CSLB URLs being overwritten by business websites
- **Scope**: ~2.7 million records across California BBBs
- **Status**: Temporary fix implemented, awaiting long-term solution
- **Created**: September 2024

---

## Appendix: Record Duplication Issue

The match_results table shows significant duplication:
- BBB 1126: 28.6x duplication per license
- BBB 1236: 15.1x duplication per license
- BBB 1116: 8.7x duplication per license
- BBB 1216: 4.3x duplication per license

This is a separate issue that should be addressed to improve system performance and data quality.