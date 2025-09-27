#!/usr/bin/env python3
"""
Comprehensive audit of CSLB detection and URL priority logic
"""

def is_cslb_record(agency_name, bbb_id=None, agency_id=None):
    """
    Thoroughly check if this is a CSLB record.
    Same logic as in service1_license_updater.py
    """
    # Check 1: Agency name contains CSLB indicators
    if agency_name:
        agency_lower = str(agency_name).lower()
        if 'contractors state' in agency_lower or 'contactors state' in agency_lower:
            return True

    # Check 2: Known CSLB BBB and Agency ID combinations
    cslb_mappings = {
        '1116': '117',   # San Francisco Bay Area
        '1126': '3888',  # Sacramento
        '1216': '13',    # Los Angeles
        '1236': '2150'   # San Diego
    }

    # Convert to strings and strip for comparison
    bbb_id_str = str(bbb_id).strip() if bbb_id else ''
    agency_id_str = str(agency_id).strip() if agency_id else ''

    # Check if this BBB/Agency combination matches CSLB
    # IMPORTANT: Only check when we have BOTH BBB and Agency IDs
    if bbb_id_str and agency_id_str:
        if bbb_id_str in cslb_mappings:
            expected_agency = cslb_mappings[bbb_id_str]
            if agency_id_str == expected_agency:
                return True

    return False


def determine_agency_url(bbb_url, db_agency_url, db_raw_url, agency_name, bbb_id=None, agency_id=None):
    """
    Determine the correct agency URL with special handling for CSLB records.
    Same logic as in service1_license_updater.py
    """
    # Use comprehensive CSLB check
    if is_cslb_record(agency_name, bbb_id, agency_id):
        # For CSLB: Prefer agency database URLs over BBB URLs
        if db_agency_url and len(str(db_agency_url)) > 3 and db_agency_url != 'None':
            return db_agency_url
        elif db_raw_url and len(str(db_raw_url)) > 3 and db_raw_url != 'None':
            return db_raw_url
        elif bbb_url and len(str(bbb_url)) > 3 and bbb_url != 'None':
            return bbb_url
        else:
            return ''
    else:
        # For non-CSLB: Keep original priority (BBB first)
        if bbb_url and len(str(bbb_url)) > 3 and bbb_url != 'None':
            return bbb_url
        elif db_raw_url and len(str(db_raw_url)) > 3 and db_raw_url != 'None':
            return db_raw_url
        elif db_agency_url and len(str(db_agency_url)) > 3 and db_agency_url != 'None':
            return db_agency_url
        else:
            return ''


print("="*70)
print("COMPREHENSIVE AUDIT: CSLB Detection and URL Priority Logic")
print("="*70)

# Test 1: CSLB Detection
print("\n1. TESTING is_cslb_record() FUNCTION")
print("-"*50)

detection_tests = [
    # Test cases: (agency_name, bbb_id, agency_id, expected_result, description)

    # Should be TRUE - Agency name matches
    ("Contractors State Licensing Board", None, None, True, "Name match - proper spelling"),
    ("CONTRACTORS STATE LICENSING BOARD", None, None, True, "Name match - uppercase"),
    ("Contactors State Licensing Board", None, None, True, "Name match - misspelling"),
    ("contractors state license", None, None, True, "Name match - partial"),

    # Should be TRUE - Valid BBB/Agency combinations
    ("", "1116", "117", True, "SF BBB + CSLB Agency"),
    ("", "1126", "3888", True, "Sacramento BBB + CSLB Agency"),
    ("", "1216", "13", True, "LA BBB + CSLB Agency"),
    ("", "1236", "2150", True, "SD BBB + CSLB Agency"),

    # Should be TRUE - Both name AND IDs match
    ("Contractors State Licensing Board", "1116", "117", True, "Name + valid IDs"),

    # Should be FALSE - Agency ID alone (removed this check)
    ("", None, "117", False, "Agency ID alone - NOT unique"),
    ("", None, "3888", False, "Agency ID alone - NOT unique"),

    # Should be FALSE - Wrong combinations
    ("", "1116", "999", False, "BBB 1116 + wrong agency"),
    ("", "9999", "117", False, "Wrong BBB + CSLB agency"),
    ("Department of Consumer Affairs", None, None, False, "Different agency name"),
    ("", None, None, False, "No data provided"),
    ("", "1116", None, False, "BBB without agency ID"),
]

passed = 0
failed = 0

for agency_name, bbb_id, agency_id, expected, description in detection_tests:
    result = is_cslb_record(agency_name, bbb_id, agency_id)
    status = "✅ PASS" if result == expected else "❌ FAIL"

    if result == expected:
        passed += 1
    else:
        failed += 1

    print(f"{status}: {description}")
    print(f"       Input: name='{agency_name[:30]}...', bbb={bbb_id}, agency={agency_id}")
    print(f"       Result: {result}, Expected: {expected}")

print(f"\nDetection Results: {passed} passed, {failed} failed")

# Test 2: URL Priority Logic
print("\n2. TESTING determine_agency_url() PRIORITY LOGIC")
print("-"*50)

url_tests = [
    # (bbb_url, db_agency_url, db_raw_url, agency_name, bbb_id, agency_id, expected_choice, description)

    # CSLB Records - Should prefer db_agency_url
    ("https://business.com", "https://cslb.ca.gov/lic?123", "https://raw.com",
     "Contractors State Licensing Board", "1116", "117", "https://cslb.ca.gov/lic?123",
     "CSLB: db_agency_url available"),

    # CSLB - Fallback to db_raw_url
    ("https://business.com", None, "https://cslb.ca.gov/raw?456",
     "Contractors State Licensing Board", "1216", "13", "https://cslb.ca.gov/raw?456",
     "CSLB: fallback to db_raw_url"),

    # CSLB - Last resort BBB URL
    ("https://business.com", None, None,
     "Contractors State Licensing Board", "1126", "3888", "https://business.com",
     "CSLB: only BBB URL available"),

    # Non-CSLB - Should prefer BBB URL
    ("https://bbb-site.com", "https://agency.gov", "https://raw.gov",
     "Department of Consumer Affairs", "0995", "123", "https://bbb-site.com",
     "Non-CSLB: BBB URL preferred"),

    # Non-CSLB - Fallback to db_raw_url
    (None, "https://agency.gov", "https://raw.gov",
     "Some Other Agency", "0995", "456", "https://raw.gov",
     "Non-CSLB: fallback to db_raw_url"),

    # Edge cases
    ("abc", "https://valid.com", None,
     "Contractors State Licensing Board", "1116", "117", "https://valid.com",
     "CSLB: Short BBB URL ignored"),

    ("None", "https://valid.com", None,
     "Contractors State Licensing Board", "1236", "2150", "https://valid.com",
     "CSLB: 'None' string ignored"),
]

passed_url = 0
failed_url = 0

print("\nURL Priority Tests:")
for bbb_url, db_agency_url, db_raw_url, agency_name, bbb_id, agency_id, expected, description in url_tests:
    result = determine_agency_url(bbb_url, db_agency_url, db_raw_url, agency_name, bbb_id, agency_id)
    status = "✅ PASS" if result == expected else "❌ FAIL"

    if result == expected:
        passed_url += 1
    else:
        failed_url += 1

    print(f"\n{status}: {description}")
    print(f"  Inputs:")
    print(f"    BBB URL: {bbb_url}")
    print(f"    DB Agency URL: {db_agency_url}")
    print(f"    DB Raw URL: {db_raw_url}")
    print(f"    Agency: {agency_name}, BBB: {bbb_id}, Agency ID: {agency_id}")
    print(f"  Result: {result}")
    print(f"  Expected: {expected}")

print(f"\nURL Priority Results: {passed_url} passed, {failed_url} failed")

# Final Summary
print("\n" + "="*70)
print("AUDIT SUMMARY")
print("="*70)
total_passed = passed + passed_url
total_failed = failed + failed_url

if total_failed == 0:
    print(f"✅ ALL TESTS PASSED! ({total_passed}/{total_passed + total_failed})")
    print("\nKey validations confirmed:")
    print("  1. CSLB detected by agency name (including misspellings)")
    print("  2. CSLB detected by BBB+Agency ID combinations")
    print("  3. Agency ID alone does NOT trigger CSLB (correct!)")
    print("  4. CSLB records prioritize: db_agency → db_raw → bbb")
    print("  5. Non-CSLB records prioritize: bbb → db_raw → db_agency")
else:
    print(f"❌ FAILURES DETECTED: {total_failed} tests failed")
    print(f"   Passed: {total_passed}")
    print(f"   Failed: {total_failed}")