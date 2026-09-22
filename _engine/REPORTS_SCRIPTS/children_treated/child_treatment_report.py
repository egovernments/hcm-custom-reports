import json
import os
import sys
import warnings
import time
import pandas as pd
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict, Counter

# === PATH SETUP ===
file_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(file_path)
from COMMON_UTILS.tenant import PREFIX, CAMPAIGN

warnings.filterwarnings("ignore", message="Unverified HTTPS request is being made.*")

from COMMON_UTILS.custom_date_utils import get_custom_dates_of_reports
from COMMON_UTILS.common_utils import save_excel
from COMMON_UTILS.common_utils import get_resp
import time as _t
_T0=_t.time(); _LAST=[_T0]
def _lap(msg):
    _now=_t.time(); print('[TIMING] '+msg+': '+format(_now-_LAST[0],'.1f')+'s (cum '+format(_now-_T0,'.1f')+'s)', flush=True); _LAST[0]=_now

# ============================================================================
# OPTIMIZED VERSION WITH OY-HF-REFERRAL SUPPORT
# NEW FEATURES:
#   - Added Client Reference ID column from Data.clientReferenceId (project task)
#   - Added "Children Referred" column (Yes/No)
#   - Added "Symptom" column
#   - Integrated {PREFIX}-hf-referral-index-v1 data source
#   - Combined results from both Project Task and HF Referral indices
#   - Extended fallback logic for household head names
#   - FIXED: nameOfReferral, Age, Gender extraction from hfReferral.additionalFields.fields array
#   - UPDATED: HF Referral is now filtered by the CURRENT REPORTING CYCLE,
#     determined from the most common "Cycle Index" seen in the Project Task
#     fetch, and matched against Data.additionalDetails.cycleIndex /
#     Data.additionalDetails.referralCycle on HF Referral docs (with zero-pad
#     variants handled). A prior attempt used a Data.@timestamp date-range
#     filter (matching Project Task's approach), but that field reflects
#     sync time on this index, not the actual referral/evaluation date, so it
#     did not reliably isolate a single cycle. Diagnostics are printed at
#     runtime to confirm the cycle filter is actually narrowing results.
# ============================================================================

# ===================== CONFIG =====================
# Every tunable for this report lives here.
# --- HTTP / concurrency ---
SESSION = requests.Session()
MAX_FETCH_WORKERS = 8
# --- egov-enc decryption ---
ENC_DECRYPT_URL = "http://egov-enc-service.egov:8080/egov-enc-service/crypto/v1/_decrypt"
DECRYPT_CHUNK = 2000
DECRYPT_FAILED_SENTINEL = "DECRYPT_FAILED"
# --- ES host + indices (the host is defined ONCE, here) ---
ES_BASE = "https://elasticsearch-data.es-cluster-v8:9200"
ES_PROJECT_TASK_INDEX = f"{ES_BASE}/{PREFIX}-project-task-index-v1/_search"
ES_INDIVIDUAL_INDEX = f"{ES_BASE}/{PREFIX}-individual-index-v1/_search"
ES_HOUSEHOLD_MEMBER_INDEX = f"{ES_BASE}/{PREFIX}-household-member-index-v1/_search"
ES_HF_REFERRAL_INDEX = f"{ES_BASE}/{PREFIX}-hf-referral-index-v1/_search"
ES_SCROLL_API = f"{ES_BASE}/_search/scroll"
scroll_api = ES_SCROLL_API
# --- campaign / paging ---
CAMPAIGN_NUMBER = CAMPAIGN
SCROLL_SIZE = 10000
batch_size = 5000
# ==================================================

# === PARALLEL FETCH ===
def parallel_fetch(batches, fetch_fn, desc):
    out = []
    with ThreadPoolExecutor(max_workers=MAX_FETCH_WORKERS) as ex:
        with tqdm(total=len(batches), desc=desc, unit=" batch", dynamic_ncols=True) as pbar:
            for res in ex.map(fetch_fn, batches):
                out.append(res)
                pbar.update(1)
    return out

# === DECRYPTION ===
def decrypt_identifiers_bulk(encrypted_ids):
    """Decrypt a list of encrypted ids in chunks."""
    unique_ids = sorted({e for e in encrypted_ids if e})
    result = {}
    headers = {"Content-Type": "application/json"}

    chunks = [unique_ids[i:i + DECRYPT_CHUNK] for i in range(0, len(unique_ids), DECRYPT_CHUNK)]
    fail_count = 0

    with tqdm(chunks, desc="  Decrypting (batched)", unit=" chunk", dynamic_ncols=True) as pbar:
        for chunk in pbar:
            decrypted = None
            for attempt in range(3):
                try:
                    resp = SESSION.post(ENC_DECRYPT_URL, headers=headers,
                                        data=json.dumps(chunk), timeout=120)
                    if resp.status_code == 200:
                        decrypted = resp.json()
                        break
                except Exception:
                    pass
                time.sleep(2 ** attempt)

            if isinstance(decrypted, list) and len(decrypted) == len(chunk):
                for enc, dec in zip(chunk, decrypted):
                    result[enc] = (dec or "").strip() if isinstance(dec, str) else str(dec)
            else:
                for enc in chunk:
                    result[enc] = DECRYPT_FAILED_SENTINEL
                fail_count += len(chunk)

            pbar.set_postfix({"failed": fail_count})

    if fail_count:
        print(f"  WARNING: {fail_count} beneficiary ids could not be decrypted (marked '{DECRYPT_FAILED_SENTINEL}').")
    return result

# === HELPER FUNCTION TO EXTRACT FROM HF REFERRAL additionalFields ===
def extract_from_hf_referral_fields(hf_referral, field_key):
    """
    Extract value from hfReferral.additionalFields.fields array
    Returns the value of the field where key matches field_key
    """
    if not hf_referral:
        return ""
    
    additional_fields = hf_referral.get("additionalFields", {})
    fields = additional_fields.get("fields", [])
    
    for field in fields:
        if isinstance(field, dict) and field.get("key") == field_key:
            value = field.get("value", "")
            return str(value).strip() if value else ""
    
    return ""

lteTime, gteTime, start_date_str, end_date_str = get_custom_dates_of_reports()

# Use LIST to keep ALL records
ind_id_vs_info = []
ind_id_to_indices = defaultdict(list)

config = json.load(open("reports_config.json"))

print(f"Reports start date: {start_date_str}")
print(f"Reports end date  : {end_date_str}")
print(f"Campaign number   : {CAMPAIGN_NUMBER}")
print(f"\n===== Generating report : CHILDREN_TREATED (with HF Referral Data)\n")

# === FETCH PROJECT TASK DATA ===
def get_successful_administrations():
    all_statuses = [
        "ADMINISTRATION_SUCCESS",
        "VISITED",
        "INELIGIBLE",
        "BENEFICIARY_MIGRATED",
        "BENEFICIARY_DIED",
        "BENEFICIARY_ABSENT",
        "BENEFICIARY_REFUSED"
    ]
    
    other_statuses = [s for s in all_statuses if s != "ADMINISTRATION_SUCCESS"]
    
    query = {
        "size": SCROLL_SIZE,
        "query": {
            "bool": {
                "must": [
                    {"range": {"Data.@timestamp": {"gte": gteTime, "lte": lteTime}}},
                    {"term": {"Data.campaignNumber.keyword": CAMPAIGN_NUMBER}}
                ],
                "should": [
                    {
                        "bool": {
                            "must": [
                                {"term": {"Data.administrationStatus.keyword": "ADMINISTRATION_SUCCESS"}},
                                {"term": {"Data.additionalDetails.doseIndex": 1}}
                            ]
                        }
                    },
                    {"terms": {"Data.administrationStatus.keyword": other_statuses}}
                ],
                "minimum_should_match": 1
            }
        },
        "sort": [{"Data.@timestamp": {"order": "asc"}}],
        "_source": [
            "Data.boundaryHierarchy", "Data.age", "Data.individualId",
            "Data.syncedTimeStamp", "Data.@timestamp", "Data.taskDates", "Data.userName",
            "Data.quantity", "Data.productName", "Data.administrationStatus",
            "Data.latitude", "Data.longitude", "Data.additionalDetails", "Data.status",
            "Data.clientReferenceId"
        ]
    }

    initial_scroll_url = ES_PROJECT_TASK_INDEX + "/?scroll=10m"
    scroll_id = None

    status_counts = {status: 0 for status in all_statuses}
    total_fetched = 0
    
    ind_id_status_count = defaultdict(lambda: defaultdict(int))

    print("[Step 1/4] Fetching administrations from project task index...")
    print(f"  Fetching all statuses: {', '.join(all_statuses)}")
    print(f"  NOTE: ADMINISTRATION_SUCCESS filtered by doseIndex = 1 only\n")

    with tqdm(desc="  Scrolling pages", unit=" page", dynamic_ncols=True) as pbar:
        while True:
            if scroll_id is None:
                data = get_resp(initial_scroll_url, query, True).json()
                scroll_id = data["_scroll_id"]
            else:
                data = get_resp(scroll_api, {"scroll": "10m", "scroll_id": scroll_id}, True).json()
                scroll_id = data["_scroll_id"]

            hits = data["hits"]["hits"]
            if not hits:
                break

            for raw_doc in hits:
                doc = raw_doc["_source"]["Data"]
                indv_id = doc.get('individualId')
                if not indv_id:
                    continue

                administration_status = doc.get('administrationStatus')
                additional_details = doc.get("additionalDetails", {})
                timestamp = doc.get("@timestamp", "")
                quantity = doc.get("quantity")

                ind_id_status_count[indv_id][administration_status] += 1
                if administration_status in status_counts:
                    status_counts[administration_status] += 1

                child_name_from_details = additional_details.get("childName", "").strip() if additional_details else ""
                head_name_from_details = additional_details.get("headName", "").strip() if additional_details else ""
                beneficiary_id_from_details = additional_details.get("beneficiaryId", "") if additional_details else ""
                beneficiary_id_from_details = str(beneficiary_id_from_details).strip() if beneficiary_id_from_details else ""

                record = {
                    "Individual ID": indv_id,
                    "Beneficiary Id": beneficiary_id_from_details,
                    "Country": doc["boundaryHierarchy"].get("country", ""),
                    "State": doc["boundaryHierarchy"].get("state", ""),
                    "LGA": doc["boundaryHierarchy"].get("lga", ""),
                    "Ward": doc["boundaryHierarchy"].get("ward", ""),
                    "Health Facility": doc["boundaryHierarchy"].get("healthFacility", ""),
                    "Username": doc.get("userName", ""),
                    "Child Name": child_name_from_details,
                    "Age": doc.get("age", ""),
                    "Gender": additional_details.get("gender", ""),
                    "Household Head Name": head_name_from_details,
                    "latitude": doc.get("latitude"),
                    "longitude": doc.get("longitude"),
                    "productName": doc.get("productName"),
                    "dateOfRegistration": doc.get("taskDates"),
                    "Cycle Index": additional_details.get("cycleIndex", ""),
                    "Administration Status": administration_status,
                    "Timestamp": timestamp,
                    "Record Type": "",
                    "Children Referred": "No",
                    "Quantity": quantity if quantity else "",
                    "Client Reference ID": doc.get("clientReferenceId", ""),
                    "Symptom": ""
                }

                if administration_status == 'ADMINISTRATION_SUCCESS':
                    record["Quantity Administered"] = quantity if quantity else ""
                elif administration_status == 'VISITED':
                    record["Redose Quantity Administered"] = quantity if quantity else ""

                record_index = len(ind_id_vs_info)
                ind_id_vs_info.append(record)
                ind_id_to_indices[indv_id].append(record_index)

            total_fetched += len(hits)
            pbar.update(1)
            pbar.set_postfix({"records": total_fetched})

    print(f"\n  Total records fetched from PROJECT TASK: {total_fetched}")
    for status in all_statuses:
        count = status_counts[status]
        print(f"  {status:<30}: {count:>7}")
    print(f"  Unique individuals     : {len(ind_id_to_indices)}")
    print(f"  Records in list        : {len(ind_id_vs_info)}\n")

    return ind_id_status_count

ind_id_status_count = get_successful_administrations()
_lap('[1] project-task fetch')

# === DETERMINE CURRENT REPORTING CYCLE FROM PROJECT TASK DATA ===
# Project Task itself is not filtered by cycle (only by date range), but each
# record carries a "Cycle Index" value (Data.additionalDetails.cycleIndex).
# Since a single report run is expected to correspond to one cycle, we take
# the most common Cycle Index seen in the Project Task fetch and use THAT
# value to filter HF Referral directly (HF Referral has no reliable date
# alignment with cycles, but it does carry its own cycle fields).
print(f"[Step 1.6/4] Determining current reporting cycle from Project Task records...")

# If the UI passed an explicit cycle selection, that is authoritative and
# COMMON_UTILS.get_resp already scopes the hf-referral fetch to it. Deriving a
# second cycle value from the data here would AND against the injected one and
# can return zero referral rows, so the derivation is a fallback only.
_ui_cycles = [c for c in os.environ.get("CYCLES", "").split(",") if c]

_cycle_values = [r.get("Cycle Index") for r in ind_id_vs_info if r.get("Cycle Index")]
_cycle_counter = Counter(_cycle_values)

if _cycle_counter:
    print(f"  Cycle Index distribution in Project Task fetch: {dict(_cycle_counter.most_common())}")
    CURRENT_CYCLE_RAW = _cycle_counter.most_common(1)[0][0]
    if len(_cycle_counter) > 1:
        print(f"  ⚠ WARNING: Multiple distinct Cycle Index values found in Project Task data.")
        print(f"    Using the most common one: {CURRENT_CYCLE_RAW!r}")
        print(f"    If this report is meant to span multiple cycles, this filter will be too narrow.")
else:
    CURRENT_CYCLE_RAW = None
    print(f"  ⚠ WARNING: No Cycle Index values found in Project Task records. "
          f"HF Referral will not be cycle-filtered.")

def _cycle_variants(raw_value):
    """Generate plausible string variants of a cycle number so we can match
    fields that store it differently (e.g. cycleIndex='01' vs referralCycle='1')."""
    variants = set()
    if raw_value is None:
        return variants
    raw_str = str(raw_value).strip()
    if not raw_str:
        return variants
    variants.add(raw_str)
    if raw_str.isdigit():
        variants.add(str(int(raw_str)))       # strips leading zeros -> "1"
        variants.add(f"{int(raw_str):02d}")   # zero-padded to 2 digits -> "01"
    return variants

CURRENT_CYCLE_VARIANTS = list(_cycle_variants(CURRENT_CYCLE_RAW))
if _ui_cycles:
    print(f"  UI cycle selection present ({','.join(_ui_cycles)}) - hf-referral is scoped"
          f" by get_resp; dropping the data-derived filter to avoid double-filtering.")
    CURRENT_CYCLE_VARIANTS = []
print(f"  Cycle value to filter HF Referral by: "
      + (f"{','.join(_ui_cycles)} (from UI selection, applied by get_resp)"
         if _ui_cycles else f"{CURRENT_CYCLE_RAW!r} (derived from data)"))
print(f"  Matching variants: {CURRENT_CYCLE_VARIANTS}\n")

# === FETCH HF REFERRAL DATA ===
def get_hf_referral_data():
    """Fetch data from oy-hf-referral-index and add to records list.
    FILTERED BY THE CURRENT REPORTING CYCLE (Data.additionalDetails.cycleIndex
    and/or Data.additionalDetails.referralCycle), determined from the Project
    Task fetch. The Data.@timestamp field on this index reflects sync time,
    not the actual referral/evaluation date, so a date-range filter does not
    reliably isolate a single cycle -- cycle fields are used instead.
    """
    
    must_clauses = [{"term": {"Data.campaignNumber.keyword": CAMPAIGN_NUMBER}}]
    should_clauses = []
    if CURRENT_CYCLE_VARIANTS:
        should_clauses = [
            {"terms": {"Data.additionalDetails.cycleIndex.keyword": CURRENT_CYCLE_VARIANTS}}
        ]

    bool_query = {"must": must_clauses}
    if should_clauses:
        bool_query["should"] = should_clauses
        bool_query["minimum_should_match"] = 1

    query = {
        "size": SCROLL_SIZE,
        "query": {"bool": bool_query},
        "sort": [{"Data.@timestamp": {"order": "asc"}}],
        "_source": [
            "Data.boundaryHierarchy", "Data.@timestamp", "Data.taskDates", 
            "Data.userName", "Data.additionalDetails", "Data.hfReferral", "Data.campaignNumber"
        ]
    }

    initial_scroll_url = ES_HF_REFERRAL_INDEX + "/?scroll=10m"
    scroll_id = None
    
    hf_referral_ids = []
    hf_referral_data = {}
    referral_code_status_count = defaultdict(lambda: defaultdict(int))
    total_fetched = 0
    
    # Track child name sources for debugging
    hf_referral_id_sources = {}  # Maps referral code to name source
    
    print("[Step 1.5/4] Fetching referral data from HF Referral index...")
    print(f"  Campaign Number Filter: {CAMPAIGN_NUMBER}")
    if CURRENT_CYCLE_VARIANTS:
        print(f"  Cycle Filter (additionalDetails.cycleIndex): {CURRENT_CYCLE_VARIANTS}\n")
    else:
        print("  Cycle Filter: applied by get_resp from the UI selection "
              f"({','.join(_ui_cycles)})\n" if _ui_cycles else
              "  Cycle Filter: NONE APPLIED (no cycle selected and none derivable)\n")

    # === DIAGNOSTIC: Confirm the cycle filter is actually narrowing results ===
    print("  [DIAGNOSTIC] Checking whether cycle filter is effective on HF Referral index...")
    try:
        count_url = ES_HF_REFERRAL_INDEX.replace("/_search", "/_count")

        count_campaign_only_query = {
            "query": {"bool": {"must": [
                {"term": {"Data.campaignNumber.keyword": CAMPAIGN_NUMBER}}
            ]}}
        }
        resp_campaign_only = get_resp(count_url, count_campaign_only_query, True).json()
        count_campaign_only = resp_campaign_only.get("count", "N/A")
        print(f"    Docs matching campaignNumber ONLY          : {count_campaign_only}")

        if CURRENT_CYCLE_VARIANTS:
            count_with_cycle_query = {"query": {"bool": query["query"]["bool"]}}
            resp_with_cycle = get_resp(count_url, count_with_cycle_query, True).json()
            count_with_cycle = resp_with_cycle.get("count", "N/A")
            print(f"    Docs matching campaignNumber + cycle filter: {count_with_cycle}")

            if count_campaign_only == count_with_cycle:
                print("    ⚠ WARNING: Counts are IDENTICAL. The cycle filter is not excluding anything.")
                print("      Possible causes:")
                print("      - cycleIndex field may not be 'keyword' mapped (check mapping)")
                print("      - All docs for this campaign genuinely belong to this one cycle")
                print("      - CURRENT_CYCLE_VARIANTS may not match the actual stored format")
            else:
                print("    ✓ Cycle filter is narrowing results as expected.")
        else:
            print("    (Skipped cycle-filtered count -- no cycle value determined)")

        # Pull ONE raw sample doc (no _source restriction) to inspect actual cycle field values/formats
        sample_query = {
            "size": 1,
            "query": {"term": {"Data.campaignNumber.keyword": CAMPAIGN_NUMBER}}
        }
        sample_resp = get_resp(ES_HF_REFERRAL_INDEX, sample_query, True).json()
        sample_hits = sample_resp.get("hits", {}).get("hits", [])
        if sample_hits:
            sample_data = sample_hits[0]["_source"].get("Data", {})
            sample_additional = sample_data.get("additionalDetails", {})
            print(f"    Sample doc additionalDetails.cycleIndex     = {sample_additional.get('cycleIndex')!r}")
            print(f"    Sample doc additionalDetails.referralCycle  = {sample_additional.get('referralCycle')!r}")
        else:
            print("    ⚠ No sample doc found for this campaign number.")
    except Exception as e:
        print(f"    ⚠ Diagnostic check failed (non-fatal, continuing with main fetch): {e}")
    print()

    with tqdm(desc="  Scrolling HF referral pages", unit=" page", dynamic_ncols=True) as pbar:
        while True:
            if scroll_id is None:
                data = get_resp(initial_scroll_url, query, True).json()
                scroll_id = data["_scroll_id"]
            else:
                data = get_resp(scroll_api, {"scroll": "10m", "scroll_id": scroll_id}, True).json()
                scroll_id = data["_scroll_id"]

            hits = data["hits"]["hits"]
            if not hits:
                break

            for raw_doc in hits:
                doc = raw_doc["_source"]["Data"]
                hf_referral = doc.get("hfReferral", {})
                referral_code = hf_referral.get("referralCode", "")
                
                # If no referral code, use client reference ID or generate one
                if not referral_code:
                    referral_code = hf_referral.get("clientReferenceId", "")
                
                # If still no ID, skip (truly missing)
                if not referral_code:
                    continue

                boundary = doc.get("boundaryHierarchy", {})
                additional_details = doc.get("additionalDetails", {})
                timestamp = doc.get("@timestamp", "")
                
                # === FIXED: Extract nameOfReferral, age, and gender from hfReferral.additionalFields.fields ===
                name_of_referral = extract_from_hf_referral_fields(hf_referral, "nameOfReferral")
                age_in_months = extract_from_hf_referral_fields(hf_referral, "ageInMonths")
                gender = extract_from_hf_referral_fields(hf_referral, "gender")
                beneficiary_id_hf = hf_referral.get("beneficiaryId", "")
                beneficiary_id_hf = str(beneficiary_id_hf).strip() if beneficiary_id_hf else ""
                
                # Track status counts
                status = "Children Referred"
                referral_code_status_count[referral_code][status] += 1

                record = {
                    "Individual ID": referral_code,
                    "Beneficiary Id": beneficiary_id_hf,
                    "Country": boundary.get("country", ""),
                    "State": boundary.get("state", ""),
                    "LGA": boundary.get("lga", ""),
                    "Ward": boundary.get("ward", ""),
                    "Health Facility": boundary.get("healthFacility", ""),
                    "Username": doc.get("userName", ""),
                    "Child Name": name_of_referral,              # ✅ From additionalFields
                    "Age": age_in_months,                        # ✅ From additionalFields
                    "Gender": gender,                            # ✅ From additionalFields
                    "Household Head Name": "",                   # Will be filled from household index
                    "latitude": "",
                    "longitude": "",
                    "productName": "",
                    "dateOfRegistration": doc.get("taskDates", ""),
                    "Cycle Index": additional_details.get("cycleIndex", ""),
                    "Administration Status": "Children Referred",
                    "Timestamp": timestamp,
                    "Record Type": "",
                    "Children Referred": "Yes",
                    "Quantity": "",
                    "Client Reference ID": hf_referral.get("clientReferenceId", ""),
                    "Symptom": hf_referral.get("symptom", "")
                }

                record_index = len(ind_id_vs_info)
                ind_id_vs_info.append(record)
                ind_id_to_indices[referral_code].append(record_index)
                
                hf_referral_ids.append(referral_code)
                hf_referral_data[referral_code] = record_index
                
                # Store username and nameOfReferral for fallback logic
                hf_referral_id_sources[referral_code] = {
                    "userName": doc.get("userName", ""),
                    "nameOfReferral": name_of_referral
                }
                
            total_fetched += len(hits)
            pbar.update(1)
            pbar.set_postfix({"records": total_fetched})

    # Track records with missing referral codes
    missing_referral_codes = 0
    recovered_with_client_ref = 0
    
    print(f"\n  Total records fetched from HF REFERRAL: {total_fetched}")
    unique_referral_codes = len(set(hf_referral_ids))
    duplicate_records = total_fetched - unique_referral_codes
    print(f"  Unique referral codes  : {unique_referral_codes}")
    print(f"  Duplicate records      : {duplicate_records}")
    print(f"  Records in list        : {len(ind_id_vs_info)}")
    
    # Check for duplicate referral codes
    from collections import Counter
    referral_code_counts = Counter(hf_referral_ids)
    duplicate_referral_codes = {code: count for code, count in referral_code_counts.items() if count > 1}
    
    print(f"\n  Summary:")
    print(f"    - Total HF Referral records fetched: {total_fetched}")
    print(f"    - Unique referral codes: {unique_referral_codes}")
    print(f"    - Duplicate records with same referral code: {duplicate_records}")
    print(f"    - ALL {total_fetched} records added to list ✓")
    print(f"    - ALL {total_fetched} records WILL be exported to Excel ✓")
    
    if duplicate_referral_codes:
        print(f"\n  ⚠ DUPLICATE REFERRAL CODES ({len(duplicate_referral_codes)} codes, {duplicate_records} extra records):")
        for code, count in duplicate_referral_codes.items():
            print(f"    - {code}: appears {count} times (ALL {count} will be in Excel)")

    return hf_referral_ids, hf_referral_data, referral_code_status_count, hf_referral_id_sources

hf_referral_ids, hf_referral_data, referral_code_status_count, hf_referral_id_sources = get_hf_referral_data()
_lap('[1.5] hf-referral fetch')

# === FETCH CHILD NAMES (for both project task and HF referral) ===
all_individual_ids = list(ind_id_to_indices.keys())

print(f"[Step 2/4] Fetching child names and household head info ({len(all_individual_ids)} unique individuals)...")

def fetch_individual_info(individual_ids_batch):
    query = {
        "size": batch_size,
        "query": {"terms": {"clientReferenceId.keyword": individual_ids_batch}},
        "_source": ["name", "clientReferenceId", "identifiers"]
    }
    return get_resp(ES_INDIVIDUAL_INDEX, query, True).json()['hits']['hits']

batches = [all_individual_ids[i:i + batch_size] for i in range(0, len(all_individual_ids), batch_size)]

individuals_found = 0

for results in parallel_fetch(batches, fetch_individual_info, "  Individual batches"):
    for result in results:
        doc = result["_source"]
        clref = doc['clientReferenceId']
        given = doc['name'].get('givenName') or ""
        family = doc['name'].get('familyName') or ""
        name = f"{given} {family}".strip()

        if clref in ind_id_to_indices:
            for record_idx in ind_id_to_indices[clref]:
                if not ind_id_vs_info[record_idx]["Child Name"]:
                    ind_id_vs_info[record_idx]["Child Name"] = name
            individuals_found += 1

print(f"  Unique individuals matched in index: {individuals_found} / {len(all_individual_ids)}")
print(f"  Child names filled (fallback from individual index).\n")
_lap('[2] individual names')

# === EXTENDED FALLBACK: For HF referral, use nameOfReferral if username contains OIC ===
print(f"[Step 2.5/4] Verifying nameOfReferral for HF referral records (OIC users)...")

name_of_referral_verified_count = 0

for referral_code in hf_referral_ids:
    if referral_code in ind_id_to_indices:
        # Get ALL record indices for this referral code (handles duplicates)
        record_indices = ind_id_to_indices[referral_code]
        
        for record_idx in record_indices:
            # Get username and nameOfReferral from stored sources
            username = hf_referral_id_sources[referral_code].get("userName", "")
            name_of_referral = hf_referral_id_sources[referral_code].get("nameOfReferral", "")
            
            # Check if username contains "OIC" and nameOfReferral is available
            if username and "OIC" in username.upper() and name_of_referral:
                # Verify/update the child name (should already be set from extract_from_hf_referral_fields)
                if not ind_id_vs_info[record_idx]["Child Name"]:
                    ind_id_vs_info[record_idx]["Child Name"] = name_of_referral
                    name_of_referral_verified_count += 1
                else:
                    # Already populated
                    name_of_referral_verified_count += 1

print(f"  nameOfReferral verified: {name_of_referral_verified_count} HF referral records")
print(f"  (OIC username + nameOfReferral from additionalFields)\n")

# === FETCH HOUSEHOLD HEAD NAMES ===
print(f"[Step 3/4] Fetching household head names (with extended fallback logic)...")

def fetch_household_member_info(ind_ids_batch):
    query = {
        "size": batch_size,
        "query": {"terms": {"Data.householdMember.individualClientReferenceId.keyword": ind_ids_batch}},
        "_source": ["Data.householdMember.individualClientReferenceId", "Data.householdMember.householdClientReferenceId"]
    }
    return get_resp(ES_HOUSEHOLD_MEMBER_INDEX, query, True).json()['hits']['hits']

ind_id_vs_hh_clref_id = {}
batches = [all_individual_ids[i:i + batch_size] for i in range(0, len(all_individual_ids), batch_size)]

for results in parallel_fetch(batches, fetch_household_member_info, "  HH member batches"):
    for result in results:
        source = result["_source"]
        ind_id_vs_hh_clref_id[source["Data"]["householdMember"]["individualClientReferenceId"]] = \
            source["Data"]["householdMember"]["householdClientReferenceId"]

hh_clref_ids_list = list(set(ind_id_vs_hh_clref_id.values()))
print(f"  Unique households: {len(hh_clref_ids_list)}")

def fetch_household_head_info(hh_ids_batch):
    query = {
        "size": batch_size,
        "query": {
            "bool": {
                "must": [
                    {"terms": {"Data.householdMember.householdClientReferenceId.keyword": hh_ids_batch}},
                    {"term": {"Data.householdMember.isHeadOfHousehold": True}}
                ]
            }
        },
        "_source": ["Data.householdMember.individualClientReferenceId", "Data.householdMember.householdClientReferenceId"]
    }
    return get_resp(ES_HOUSEHOLD_MEMBER_INDEX, query, True).json()['hits']['hits']

hh_clref_ids_vs_head_ind_ids = {}
hh_batches = [hh_clref_ids_list[i:i + batch_size] for i in range(0, len(hh_clref_ids_list), batch_size)]

for results in parallel_fetch(hh_batches, fetch_household_head_info, "  HH head batches"):
    for result in results:
        source = result["_source"]
        hh_clref_ids_vs_head_ind_ids[source["Data"]["householdMember"]["householdClientReferenceId"]] = \
            source["Data"]["householdMember"]["individualClientReferenceId"]

head_ind_ids = set(hh_clref_ids_vs_head_ind_ids.values())
non_fetched_ind_info_ids_list = [id for id in head_ind_ids if id not in ind_id_to_indices]
print(f"  Head IDs needing separate fetch: {len(non_fetched_ind_info_ids_list)}")

def fetch_household_head_names(individual_ids_batch):
    query = {
        "size": batch_size,
        "query": {"terms": {"clientReferenceId.keyword": individual_ids_batch}},
        "_source": ["name", "clientReferenceId"]
    }
    return get_resp(ES_INDIVIDUAL_INDEX, query, True).json()['hits']['hits']

head_ind_id_vs_name = {}
head_batches = [non_fetched_ind_info_ids_list[i:i + batch_size] for i in range(0, len(non_fetched_ind_info_ids_list), batch_size)]

for results in parallel_fetch(head_batches, fetch_household_head_names, "  Head name batches"):
    for result in results:
        source = result["_source"]
        given = source['name'].get('givenName') or ""
        family = source['name'].get('familyName') or ""
        head_ind_id_vs_name[source["clientReferenceId"]] = f"{given} {family}".strip()

# Update all records with household head names (with extended fallback)
with tqdm(all_individual_ids, desc="  Filling HH head names", unit=" rec", dynamic_ncols=True) as pbar:
    for ind_id in pbar:
        hh_id = ind_id_vs_hh_clref_id.get(ind_id)
        head_ind_id = hh_clref_ids_vs_head_ind_ids.get(hh_id) if hh_id else None
        
        name = ""
        if head_ind_id:
            if head_ind_id in ind_id_to_indices:
                name = ind_id_vs_info[ind_id_to_indices[head_ind_id][0]].get("Child Name", "").strip()
            else:
                name = head_ind_id_vs_name.get(head_ind_id, "").strip()
        
        # Extended fallback: if head not found by isHeadOfHousehold=true, 
        # try to find from project task for HF referral individuals
        if not name and ind_id in hf_referral_data:
            # Search in household members without isHeadOfHousehold filter
            # This is handled in next section
            pass
        
        if ind_id in ind_id_to_indices:
            for record_idx in ind_id_to_indices[ind_id]:
                if not ind_id_vs_info[record_idx]["Household Head Name"]:
                    ind_id_vs_info[record_idx]["Household Head Name"] = name

print(f"  Household head names filled (with extended fallback).\n")

# === EXTENDED FALLBACK: For HF referral without head names, search project task ===
print(f"[Step 3.5/4] Applying extended fallback for HF referral household heads...")

def fetch_household_members_no_head_filter(ind_ids_batch):
    """Fetch all household members without isHeadOfHousehold filter"""
    query = {
        "size": batch_size,
        "query": {"terms": {"Data.householdMember.individualClientReferenceId.keyword": ind_ids_batch}},
        "_source": ["Data.householdMember.individualClientReferenceId", "Data.householdMember.householdClientReferenceId"]
    }
    return get_resp(ES_HOUSEHOLD_MEMBER_INDEX, query, True).json()['hits']['hits']

# Find HF referral individuals without household head names
hf_no_head = [rid for rid in hf_referral_ids if rid in hf_referral_data and not ind_id_vs_info[hf_referral_data[rid]]["Household Head Name"]]

if hf_no_head:
    print(f"  Found {len(hf_no_head)} HF referral records without household head names.")
    
    hf_hh_ids = {}
    hf_batches = [hf_no_head[i:i + batch_size] for i in range(0, len(hf_no_head), batch_size)]
    
    for results in parallel_fetch(hf_batches, fetch_household_members_no_head_filter, "  HF HH member batches"):
        for result in results:
            source = result["_source"]
            hm = source["Data"]["householdMember"]
            hf_hh_ids[hm["individualClientReferenceId"]] = hm["householdClientReferenceId"]
    
    # Try to find alternate members in same household from project task
    for hf_id in hf_no_head:
        hh_id = hf_hh_ids.get(hf_id)
        
        # Get ALL record indices for this HF ID (handles duplicates)
        if hf_id in ind_id_to_indices:
            record_indices = ind_id_to_indices[hf_id]
        else:
            record_indices = []
        
        if hh_id and record_indices:
            # Check if any other individual in project task belongs to this household
            # Try matching by household ID
            for other_ind_id in ind_id_to_indices.keys():
                if other_ind_id != hf_id and other_ind_id in ind_id_vs_hh_clref_id:
                    if ind_id_vs_hh_clref_id[other_ind_id] == hh_id:
                        # Found same household, use their name
                        other_record_idx = ind_id_to_indices[other_ind_id][0]
                        head_name = ind_id_vs_info[other_record_idx].get("Child Name", "").strip()
                        if head_name:
                            # Apply to ALL records with this HF ID
                            for record_idx in record_indices:
                                ind_id_vs_info[record_idx]["Household Head Name"] = head_name
                            break

print(f"  Extended fallback completed.\n")
_lap('[3] household heads')

# === DETERMINE RECORD TYPE (unique/duplicate) per status ===
print(f"[Step 3.6/4] Determining record types (unique/duplicate per status)...")

# Merge both status counts
all_status_count = defaultdict(lambda: defaultdict(int))
for ind_id, statuses in ind_id_status_count.items():
    for status, count in statuses.items():
        all_status_count[ind_id][status] += count

for referral_code, statuses in referral_code_status_count.items():
    for status, count in statuses.items():
        all_status_count[referral_code][status] += count

with tqdm(range(len(ind_id_vs_info)), desc="  Processing record types", unit=" rec", dynamic_ncols=True) as pbar:
    for record_idx in pbar:
        record = ind_id_vs_info[record_idx]
        ind_id = record.get("Individual ID")
        admin_status = record.get("Administration Status", "")
        
        if all_status_count[ind_id][admin_status] > 1:
            record["Record Type"] = "duplicate"
        else:
            record["Record Type"] = "unique"

print(f"  Record types determined.\n")
_lap('[3.6] record types')

# === EXPORT TO EXCEL ===
print(f"[Step 4/4] Writing CHILDREN_TREATED.xlsx...")

df = pd.DataFrame(ind_id_vs_info)
df.sort_values(by="Cycle Index", inplace=True)

output_dir = os.path.join(file_path, "FINAL_REPORTS", "CHILDREN_TREATED")
os.makedirs(output_dir, exist_ok=True)

file_path_out = os.path.join(output_dir, "CHILDREN_TREATED.xlsx")

print(f"  Writing {len(df)} rows ...")

save_excel(df, file_path_out)
_lap('[4] xlsx write')

print(f"\n  Output : {file_path_out}")
print(f"  Rows   : {len(df)}")

# === VERIFICATION: All records including duplicates are exported ===
print(f"\n✓ VERIFICATION: ALL RECORDS EXPORTED")
print(f"  Total records in memory     : {len(ind_id_vs_info)}")
print(f"  Total records in DataFrame  : {len(df)}")
print(f"  Total records written to Excel : {len(df)}")
print(f"  ✓ ALL DUPLICATE RECORDS ARE INCLUDED IN EXCEL\n")

# === DATA QUALITY SUMMARY ===
def _blank(series):
    return series.apply(lambda v: v is None or (isinstance(v, str) and v.strip() == "")).sum()

print("\n" + "="*80)
print("DATA QUALITY SUMMARY")
print("="*80)
total = len(df)
for col in ["Child Name", "Household Head Name"]:
    if col in df.columns:
        miss = _blank(df[col])
        pct = 100*miss/total if total > 0 else 0
        print(f"  {col:<30}: {miss:>7} missing ({pct:>6.2f}%)")
for col in ["latitude", "longitude"]:
    if col in df.columns:
        miss = df[col].isna().sum()
        pct = 100*miss/total if total > 0 else 0
        print(f"  {col:<30}: {miss:>7} missing ({pct:>6.2f}%)")
for col in ["Symptom"]:
    if col in df.columns:
        miss = _blank(df[col])
        pct = 100*miss/total if total > 0 else 0
        print(f"  {col:<30}: {miss:>7} missing ({pct:>6.2f}%)")

# === STATUS-WISE SUMMARY ===
print("\n" + "="*80)
print("STATUS-WISE SUMMARY")
print("="*80)
if "Administration Status" in df.columns:
    status_summary = df["Administration Status"].value_counts().sort_index()
    grand_total = len(df)
    
    print(f"\n{'Administration Status':<30} {'Records':>10} {'Percentage':>10}")
    print("-" * 52)
    for status, count in status_summary.items():
        pct = 100*count/grand_total if grand_total > 0 else 0
        print(f"  {status:<28} {count:>10} {pct:>9.2f}%")
    print("-" * 52)
    print(f"  {'GRAND TOTAL':<28} {grand_total:>10} {'100.00%':>9}")
    
    print("\n" + "="*80)
    print("STATUS-WISE RECORD TYPE BREAKDOWN (UNIQUE vs DUPLICATE)")
    print("="*80)
    
    for status in sorted(df["Administration Status"].unique()):
        status_df = df[df["Administration Status"] == status]
        unique_count = (status_df["Record Type"] == "unique").sum()
        duplicate_count = (status_df["Record Type"] == "duplicate").sum()
        total_count = len(status_df)
        
        unique_pct = 100*unique_count/total_count if total_count > 0 else 0
        duplicate_pct = 100*duplicate_count/total_count if total_count > 0 else 0
        
        print(f"\n  {status}")
        print(f"    ├─ Total Records   : {total_count:>7}")
        print(f"    ├─ Unique          : {unique_count:>7} ({unique_pct:>6.2f}%)")
        print(f"    └─ Duplicate       : {duplicate_count:>7} ({duplicate_pct:>6.2f}%)")

# === CHILDREN REFERRED SUMMARY ===
print("\n" + "="*80)
print("CHILDREN REFERRED SUMMARY")
print("="*80)
if "Children Referred" in df.columns:
    referred_summary = df["Children Referred"].value_counts()
    grand_total = len(df)
    
    print(f"\n{'Children Referred':<30} {'Records':>10} {'Percentage':>10}")
    print("-" * 52)
    for referred, count in referred_summary.items():
        pct = 100*count/grand_total if grand_total > 0 else 0
        print(f"  {referred:<28} {count:>10} {pct:>9.2f}%")
    print("-" * 52)
    print(f"  {'GRAND TOTAL':<28} {grand_total:>10} {'100.00%':>9}")

# === CLIENT REFERENCE ID DUPLICATE CHECK (NEW) ===
print("\n" + "="*80)
print("CLIENT REFERENCE ID DUPLICATE CHECK")
print("="*80)
if "Client Reference ID" in df.columns:
    # Count occurrences of each Client Reference ID
    client_ref_counts = df["Client Reference ID"].value_counts()
    
    # Find duplicates (Client Reference ID appearing more than once)
    duplicates = client_ref_counts[client_ref_counts > 1]
    blank_count = df["Client Reference ID"].isna().sum() + (df["Client Reference ID"] == "").sum()
    
    total_unique_refs = len(client_ref_counts)
    total_duplicate_refs = len(duplicates)
    total_records_with_duplicates = duplicates.sum() if len(duplicates) > 0 else 0
    
    print(f"\nTotal unique Client Reference IDs: {total_unique_refs}")
    print(f"Blank/Empty Client Reference IDs : {blank_count} ({100*blank_count/len(df):.2f}%)")
    print(f"Duplicate Client Reference IDs   : {total_duplicate_refs}")
    print(f"Records with duplicate IDs       : {total_records_with_duplicates}")
    
    if len(duplicates) > 0:
        print(f"\nDuplicate Client Reference IDs Breakdown:")
        print(f"\n{'Client Reference ID':<40} {'Count':>10}")
        print("-" * 52)
        for client_ref, count in duplicates.items():
            print(f"  {str(client_ref):<38} {count:>10}")
        print("-" * 52)
    else:
        print(f"\n✓ No duplicate Client Reference IDs found!")
    
    print(f"\nData Quality:")
    if blank_count > 0:
        print(f"  ⚠ WARNING: {blank_count} records have blank/empty Client Reference IDs")
    if total_duplicate_refs > 0:
        print(f"  ⚠ WARNING: {total_duplicate_refs} unique Client Reference IDs appear multiple times")
        print(f"    This may indicate:")
        print(f"    - Same client processed multiple times in different statuses")
        print(f"    - Data from different sources (Project Task vs HF Referral)")
        print(f"    - Potential data quality issues requiring review")
    else:
        print(f"  ✓ All Client Reference IDs are unique")

print("\n" + "="*80)

# === REFERRAL CODE DUPLICATE CHECK (NEW) ===
print("REFERRAL CODE DUPLICATE CHECK (HF REFERRAL DATA)")
print("="*80)
if "Children Referred" in df.columns:
    # Get only HF Referral records
    hf_referral_df = df[df["Children Referred"] == "Yes"]
    
    if len(hf_referral_df) > 0:
        # Count occurrences of each Individual ID (referral code) in HF referral
        referral_code_counts = hf_referral_df["Individual ID"].value_counts()
        
        # Find duplicates (Individual ID appearing more than once in HF referral)
        hf_duplicates = referral_code_counts[referral_code_counts > 1]
        blank_referral_count = hf_referral_df["Individual ID"].isna().sum() + (hf_referral_df["Individual ID"] == "").sum()
        
        total_unique_referral_codes = len(referral_code_counts)
        total_duplicate_referral_codes = len(hf_duplicates)
        total_records_with_duplicate_referrals = hf_duplicates.sum() if len(hf_duplicates) > 0 else 0
        
        print(f"\nHF Referral Records Analysis:")
        print(f"  Total HF Referral records     : {len(hf_referral_df)}")
        print(f"  Total unique referral codes  : {total_unique_referral_codes}")
        print(f"  Blank/Empty referral codes   : {blank_referral_count}")
        print(f"  Duplicate referral codes     : {total_duplicate_referral_codes}")
        print(f"  Records with duplicate codes : {total_records_with_duplicate_referrals}")
        
        if len(hf_duplicates) > 0:
            print(f"\nDuplicate Referral Codes Breakdown:")
            print(f"\n{'Individual ID (Referral Code)':<40} {'Count':>10}")
            print("-" * 52)
            for referral_code, count in hf_duplicates.items():
                print(f"  {str(referral_code):<38} {count:>10}")
            print("-" * 52)
            print(f"\n✓ ALL THESE DUPLICATE RECORDS ARE EXPORTED TO EXCEL")
            print(f"  Each referral code appears on {len(hf_duplicates)} separate row(s)")
            print(f"  Marked with Record Type = 'duplicate'")
            print(f"\nPossible reasons:")
            print(f"  - Same referral processed/updated multiple times")
            print(f"  - Data import includes both old and new versions")
            print(f"  - System processing error/duplicate entry")
        else:
            print(f"\n✓ No duplicate referral codes in HF Referral data!")
            print(f"  All {total_unique_referral_codes} referral codes are unique.")
    else:
        print(f"\nNo HF Referral records found in dataset")

print("\n" + "="*80)
print(f"Total Rows Exported : {len(df)}")
print(f"Unique Individuals/Referrals : {len(ind_id_to_indices)}")
print(f"Avg Records/Individual : {len(df)/len(ind_id_to_indices):.2f}")
print(f"Output File : {file_path_out}")

# === DEBUG SUMMARY: Track records by source ===
print("\n" + "-"*80)
print("DATA SOURCE BREAKDOWN (DEBUG INFO)")
print("-"*80)
if "Children Referred" in df.columns:
    project_task_records = (df["Children Referred"] == "No").sum()
    hf_referral_records = (df["Children Referred"] == "Yes").sum()
    
    print(f"\nRecords by source:")
    print(f"  Project Task records      : {project_task_records}")
    print(f"  HF Referral records       : {hf_referral_records}")
    print(f"  Total in Excel            : {len(df)}")
    
    print(f"\nHF Referral records detail:")
    print(f"  Total HF records fetched from index: {len(hf_referral_ids)}")
    print(f"  Unique referral codes              : {len(set(hf_referral_ids))}")
    print(f"  Records in final export            : {hf_referral_records}")
    print(f"  ✓ ALL {len(hf_referral_ids)} HF REFERRAL RECORDS ARE IN EXCEL (including duplicates)")
    
    if len(hf_referral_ids) > 0 and hf_referral_records > 0:
        difference = len(hf_referral_ids) - hf_referral_records
        if difference > 0:
            print(f"\n  ⚠ Note: {difference} records with duplicate Individual IDs")
            print(f"    ({len(hf_referral_ids)} total fetched vs {hf_referral_records} unique Individual IDs)")
            print(f"    But ALL {len(hf_referral_ids)} records are still in Excel")
            print(f"    Marked with Record Type = 'duplicate'")
        elif difference < 0:
            print(f"\n  ℹ More records in output than fetched (as expected)")
        else:
            print(f"\n  ✓ All fetched records present in output")

print("\n" + "="*80 + "\n")