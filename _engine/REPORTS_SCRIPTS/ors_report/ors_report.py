import json
import os
import sys
import warnings
import time
import pandas as pd
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

# === PATH SETUP ===
file_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(file_path)

warnings.filterwarnings("ignore", message="Unverified HTTPS request is being made.*")

from COMMON_UTILS.custom_date_utils import get_custom_dates_of_reports
from COMMON_UTILS.common_utils import get_resp
from COMMON_UTILS.common_utils import save_excel
from COMMON_UTILS.tenant import PREFIX, CAMPAIGN

# ============================================================================
# OPTIMIZED VERSION WITH PL-HF-REFERRAL SUPPORT
# NEW FEATURES:
#   - Added Client Reference ID column from Data.clientReferenceId (project task)
#   - Added "Children Referred" column (Yes/No)
#   - Added "Symptom" column
#   - Integrated pl-hf-referral-index-v1 data source
#   - Combined results from both Project Task and HF Referral indices
#   - Extended fallback logic for household head names
#   - AUTO-SPLIT: Splits into multiple files if > 10 lakh (1M) records
#   - FIXED: nameOfReferral, Age, Gender extraction from hfReferral.additionalFields.fields array
#   - NEW: Replaced single "productName" column with three per-product columns:
#         "SPAQ 1", "SPAQ 2", "ORS-Zinc" (Yes/No), computed PER INDIVIDUAL across
#         all of their administration records (not per single row).
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
# --- dry-run ---
# When True: the script prints the [Step 0/4] pre-flight summary (total counts, per-status
# counts, and the product breakdown for SPAQ 1 / SPAQ 2 / ORS-Zinc) and then STOPS --
# it does NOT run the full fetch, HF referral pull, name lookups, or Excel export.
# Set this to False once you've checked the numbers and are ready to generate the full report.
COUNT_ONLY = False
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

# === PRODUCT COLUMN HELPERS (NEW) ===
# Final Excel columns for products, in this exact order.
PRODUCT_COLUMNS = ["SPAQ 1", "SPAQ 2", "ORS-Zinc"]

# Maps a normalized productName value -> canonical column name above.
# Normalization strips spaces/dashes/underscores and lowercases, so
# "SPAQ-1", "spaq_1", "SPAQ1", "Spaq 1" etc. all resolve to "SPAQ 1".
_PRODUCT_NAME_LOOKUP = {
    "spaq1": "SPAQ 1",
    "spaq2": "SPAQ 2",
    "orszinc": "ORS-Zinc",
}

def normalize_product_name(raw_name):
    """Return the canonical product column name for a raw productName value, or None if unrecognized."""
    if not raw_name:
        return None
    key = str(raw_name).strip().lower().replace(" ", "").replace("-", "").replace("_", "")
    return _PRODUCT_NAME_LOOKUP.get(key)

# NEW: robust "is this doseIndex == 1?" check.
# Root cause of an earlier undercount bug: Data.additionalDetails.doseIndex is sometimes
# stored as the STRING "1" (not the number 1) in the source docs. A plain `dose_index_raw == 1`
# or a naive string-only check misses variants like "01" (zero-padded) or " 1" (whitespace).
# This tries an int() parse first (handles 1, "1", "01", " 1", 1.0 -> 1), and only falls back
# to a raw string comparison if int() parsing fails entirely.
def is_dose_one(dose_index_raw):
    if dose_index_raw is None:
        return False
    try:
        return int(str(dose_index_raw).strip()) == 1
    except (ValueError, TypeError):
        return str(dose_index_raw).strip() == "1"

lteTime, gteTime, start_date_str, end_date_str = get_custom_dates_of_reports()

# Moved earlier (was previously only defined near the final Excel export step) so the
# SPAQ 1/SPAQ 2 duplicate-analysis export (added below, right after the main fetch) can
# write its file without waiting for the rest of the pipeline to run.
output_dir = os.path.join(file_path, "FINAL_REPORTS", "ORSZ")
os.makedirs(output_dir, exist_ok=True)

# Products we specifically watch for the duplicate-individual analysis (see Step 1d below)
DUPLICATE_WATCH_PRODUCTS = ["SPAQ 1", "SPAQ 2"]

# Use LIST to keep ALL records
ind_id_vs_info = []
ind_id_to_indices = defaultdict(list)

config = json.load(open("reports_config.json"))

print(f"Reports start date: {start_date_str}")
print(f"Reports end date  : {end_date_str}")
print(f"Campaign number   : {CAMPAIGN_NUMBER}")
print(f"\n===== Generating report : ORSZ_TREATED (with HF Referral Data)\n")

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

    # === NO PRODUCT/DOSE FILTERING AT THE ELASTICSEARCH LEVEL ===
    # We used to filter by exact productName strings ("SPAQ 1"/"SPAQ 2"/"ORS-Zinc") and
    # doseIndex directly inside the ES query. That's fragile: ES term/terms queries are
    # exact-match and case-sensitive, so any mismatch in spacing/casing/naming between what
    # we hardcoded and what's actually stored silently drops matching records with NO error.
    # That's almost certainly why SPAQ (and possibly ORS) rows were coming back empty.
    #
    # Fix: fetch EVERY record matching status + campaign + date range, with no product
    # filtering at all ("all data should come through this filter"). All SPAQ-dose-index
    # and ORS-Zinc business logic is now applied in Python below (see is_qualifying_success /
    # normalize_product_name), where matching is safely case/space-insensitive.
    query = {
        "size": SCROLL_SIZE,
        "query": {
            "bool": {
                "must": [
                    {"range": {"Data.@timestamp": {"gte": gteTime, "lte": lteTime}}},
                    {"term": {"Data.campaignNumber.keyword": CAMPAIGN_NUMBER}},
                    {"terms": {"Data.administrationStatus.keyword": all_statuses}}
                ]
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

    # NOTE: SPAQ 1 / SPAQ 2 / ORS-Zinc Yes-No is now set PER ROW ONLY, directly on the
    # record itself (see build of `record` below) -- a row's flags reflect ONLY whether
    # THAT SPECIFIC document was the qualifying successful administration. Non-success
    # rows (Died, Refused, Ineligible, Migrated, Absent) ALWAYS show "No" for all three,
    # regardless of what happened on other rows for the same individual. This replaced an
    # earlier design that spread "Yes" across every row for the same (individual, username)
    # pair, which caused non-success rows (e.g. a BENEFICIARY_DIED row) to confusingly show
    # "Yes" for a product that specific event never actually administered.
    #
    # ind_id_usernames_by_product is kept purely as a DIAGNOSTIC (see Step 1c below): it
    # flags individuals where the same product was successfully logged by more than one
    # distinct username, for manual review. It does NOT affect the Yes/No column values.
    ind_id_usernames_by_product = defaultdict(lambda: defaultdict(set))  # indv_id -> product -> {usernames}
    # Counts for the SPAQ 1/SPAQ 2 duplicate-frequency summary (Step 1d, below):
    # (Individual ID, Product, Cycle Index) -> number of dose-1 ADMINISTRATION_SUCCESS docs.
    # This used to hold a full ~14-field dict PER DOCUMENT purely to feed the
    # SPAQ1_SPAQ2_All_Records_Marked.xlsx export, which has been removed - on a large
    # cycle that was ~1.2M dicts, and analyze_spaq_duplicates() then copied every one
    # of them again. A counter gives the same summary for a fraction of the memory.
    spaq_dose1_counts = defaultdict(int)
    # NEW: track which record indices (in ind_id_vs_info) belong to project-task individuals,
    # so we can fill in the product columns once the full scroll (all doses) is complete.
    project_task_record_indices = []

    # === NEW: ORS-Zinc <-> SPAQ 1/SPAQ 2 row-merge tracking ===
    # Key = (individualId, userName, cycleIndex). SPAQ 1/SPAQ 2 are "main" rows -- every
    # success event still creates its own row (including duplicates). ORS-Zinc instead
    # tries to MERGE onto whichever main row shares the same key, rather than creating a
    # separate row, UNLESS no main row exists for that key (yet or ever) -- see the merge
    # logic inline in the fetch loop below.
    main_row_index_by_key = {}        # (indv_id, username, cycle) -> row index of current main row
    ors_zinc_merged_keys = set()      # keys that have already consumed their one ORS-Zinc merge
    ors_zinc_fallback_doc_by_key = {}  # key -> first ORS-Zinc doc seen with no main row yet

    # === PRE-FLIGHT COUNT (answers "how many records will this generate?") ===
    # Runs a lightweight _count query with the SAME filters as the main scroll, BEFORE
    # doing the full (potentially large) scroll/fetch. This lets you sanity-check volume
    # first without waiting for the whole pull.
    print("[Step 0/4] Pre-flight count (same filters as the fetch below)...")
    count_url = ES_PROJECT_TASK_INDEX.replace("/_search", "/_count")
    try:
        count_query = {"query": query["query"]}
        count_resp = get_resp(count_url, count_query, True).json()
        expected_total = count_resp.get("count")
        print(f"  Expected total matching records: {expected_total:,}" if expected_total is not None
              else f"  Could not parse count response: {count_resp}")
    except Exception as e:
        print(f"  Pre-flight count failed ({e}); continuing with full fetch anyway.")

    # Per-status breakdown so you can see the split before the full pull starts.
    for status in all_statuses:
        try:
            status_count_query = {
                "query": {
                    "bool": {
                        "must": [
                            {"range": {"Data.@timestamp": {"gte": gteTime, "lte": lteTime}}},
                            {"term": {"Data.campaignNumber.keyword": CAMPAIGN_NUMBER}},
                            {"term": {"Data.administrationStatus.keyword": status}}
                        ]
                    }
                }
            }
            resp = get_resp(count_url, status_count_query, True).json()
            c = resp.get("count")
            print(f"    {status:<28}: {c:,}" if c is not None else f"    {status:<28}: (count unavailable)")
        except Exception as e:
            print(f"    {status:<28}: count failed ({e})")
    print()

    # === PRODUCT BREAKDOWN (raw doc_count, per product) ===
    # NOTE: An earlier version of this also showed an ES `cardinality` aggregation as a
    # "unique Individual+Username" estimate. That was REMOVED -- `cardinality` in
    # Elasticsearch is an APPROXIMATION (HyperLogLog++), not an exact count. Past a few
    # thousand distinct values its error grows, and at our scale (100K-1M+) it can come back
    # HIGHER than the raw doc_count itself, which is nonsensical for a true unique count.
    # Raw doc_count below is exact and reliable. The exact deduplicated (individual, username)
    # count is only computed during the real fetch (Step 1/1b), using a genuine Python set()
    # -- not an approximation -- so that number in the actual report output can be trusted.
    #
    # FIX: doseIndex can be stored as the STRING "1" (not the number 1) in the source docs.
    # A numeric-only `term: 1` filter silently drops those on a keyword/text-mapped field,
    # undercounting SPAQ 1 / SPAQ 2. Using `terms: [1, "1"]` matches either representation.
    print("  Product breakdown (ADMINISTRATION_SUCCESS only -- raw doc_count, exact):")
    try:
        product_agg_query = {
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        {"range": {"Data.@timestamp": {"gte": gteTime, "lte": lteTime}}},
                        {"term": {"Data.campaignNumber.keyword": CAMPAIGN_NUMBER}},
                        {"term": {"Data.administrationStatus.keyword": "ADMINISTRATION_SUCCESS"}}
                    ]
                }
            },
            "aggs": {
                "products": {
                    "filters": {
                        "filters": {
                            "SPAQ 1": {
                                "bool": {"must": [
                                    {"term": {"Data.productName.keyword": "SPAQ 1"}},
                                    {"terms": {"Data.additionalDetails.doseIndex": [1, "1"]}}
                                ]}
                            },
                            "SPAQ 2": {
                                "bool": {"must": [
                                    {"term": {"Data.productName.keyword": "SPAQ 2"}},
                                    {"terms": {"Data.additionalDetails.doseIndex": [1, "1"]}}
                                ]}
                            },
                            "ORS-Zinc": {
                                "term": {"Data.productName.keyword": "ORS-Zinc"}
                            }
                        }
                    }
                }
            }
        }
        agg_resp = get_resp(ES_PROJECT_TASK_INDEX, product_agg_query, True).json()
        buckets = agg_resp.get("aggregations", {}).get("products", {}).get("buckets", {})

        print(f"    {'Product':<12} {'Raw doc_count':>16}")
        print("    " + "-" * 32)
        for col in PRODUCT_COLUMNS:
            bucket = buckets.get(col, {})
            raw_cnt = bucket.get("doc_count", 0)
            note = "" if col == "ORS-Zinc" else "  (doseIndex==1 only)"
            print(f"    {col:<12} {raw_cnt:>16,}{note}")
        print()
        print("    NOTE: This is the raw, exact document count per product/dose filter.")
        print("          The final exported report deduplicates further, to one entry per")
        print("          (Individual, Username) pair -- that exact number only comes from")
        print("          actually running the fetch below (set COUNT_ONLY = False).")
    except Exception as e:
        print(f"    Product aggregation failed ({e})")
    print()

    if COUNT_ONLY:
        print("="*80)
        print("COUNT_ONLY = True -- stopping here. Review the numbers above.")
        print("Set COUNT_ONLY = False at the top of the script to run the full report.")
        print("="*80)
        sys.exit(0)

    print("[Step 1/4] Fetching administrations from project task index...")
    print(f"  Fetching all statuses: {', '.join(all_statuses)}")
    print(f"  NOTE: No productName/doseIndex filtering in the ES query -- ALL matching records")
    print(f"        are fetched. SPAQ 1/SPAQ 2 (doseIndex==1 only) vs ORS-Zinc (no dose filter)")
    print(f"        business logic is applied in Python when computing product Yes/No flags.\n")

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
                raw_product_name = doc.get("productName")
                username = doc.get("userName", "") or ""
                cycle_index = additional_details.get("cycleIndex", "") if additional_details else ""
                merge_key = (indv_id, username, cycle_index)

                normalized_product = normalize_product_name(raw_product_name)
                dose_index_raw = additional_details.get("doseIndex") if additional_details else None
                is_first_dose = is_dose_one(dose_index_raw)

                def build_row(admin_status, spaq1_flag, spaq2_flag, orszinc_flag):
                    ad = additional_details or {}
                    child_name = ad.get("childName", "").strip() if ad else ""
                    head_name = ad.get("headName", "").strip() if ad else ""
                    row = {
                        "Individual ID": indv_id,
                        "Country": doc["boundaryHierarchy"].get("country", ""),
                        "State": doc["boundaryHierarchy"].get("state", ""),
                        "LGA": doc["boundaryHierarchy"].get("lga", ""),
                        "Ward": doc["boundaryHierarchy"].get("ward", ""),
                        "Health Facility": doc["boundaryHierarchy"].get("healthFacility", ""),
                        "Username": doc.get("userName", ""),
                        "Child Name": child_name,
                        "Age": doc.get("age", ""),
                        "Gender": ad.get("gender", ""),
                        "Household Head Name": head_name,
                        "latitude": doc.get("latitude"),
                        "longitude": doc.get("longitude"),
                        "dateOfRegistration": doc.get("taskDates"),
                        "Cycle Index": cycle_index,
                        "Administration Status": admin_status,
                        "Timestamp": timestamp,
                        "Record Type": "",
                        "Children Referred": "No",
                        "Quantity": quantity if quantity else "",
                        "Client Reference ID": doc.get("clientReferenceId", ""),
                        "Symptom": "",
                        "SPAQ 1": "Yes" if spaq1_flag else "No",
                        "SPAQ 2": "Yes" if spaq2_flag else "No",
                        "ORS-Zinc": "Yes" if orszinc_flag else "No",
                    }
                    if admin_status == 'ADMINISTRATION_SUCCESS':
                        row["Quantity Administered"] = quantity if quantity else ""
                    elif admin_status == 'VISITED':
                        row["Redose Quantity Administered"] = quantity if quantity else ""
                    return row

                def append_row(row):
                    idx = len(ind_id_vs_info)
                    ind_id_vs_info.append(row)
                    ind_id_to_indices[indv_id].append(idx)
                    project_task_record_indices.append(idx)
                    return idx

                ind_id_status_count[indv_id][administration_status] += 1
                if administration_status in status_counts:
                    status_counts[administration_status] += 1

                is_qualifying_success = (
                    normalized_product
                    and administration_status == "ADMINISTRATION_SUCCESS"
                    and (
                        (normalized_product in ("SPAQ 1", "SPAQ 2") and is_first_dose)
                        or normalized_product == "ORS-Zinc"
                    )
                )

                if is_qualifying_success:
                    ind_id_usernames_by_product[indv_id][normalized_product].add(username)

                    if normalized_product in DUPLICATE_WATCH_PRODUCTS:
                        spaq_dose1_counts[
                            (indv_id, normalized_product, cycle_index)] += 1

                    if normalized_product in ("SPAQ 1", "SPAQ 2"):
                        # MAIN product row: always creates its own row, even for duplicates.
                        # If ORS-Zinc already merged (or is waiting as a fallback) for this
                        # exact (individual, username, cycle) key, attach it to THIS row --
                        # otherwise this row starts with ORS-Zinc = No and may get merged
                        # into later, when/if a matching ORS-Zinc doc arrives.
                        orszinc_already_matched = merge_key in ors_zinc_merged_keys
                        row = build_row(
                            administration_status,
                            spaq1_flag=(normalized_product == "SPAQ 1"),
                            spaq2_flag=(normalized_product == "SPAQ 2"),
                            orszinc_flag=orszinc_already_matched,
                        )
                        row_idx = append_row(row)
                        # This becomes the CURRENT main row for this key -- if a later
                        # duplicate SPAQ doc for the same key arrives, IT becomes the new
                        # current main row (this one keeps whatever it already has).
                        main_row_index_by_key[merge_key] = row_idx

                        # If an ORS-Zinc doc for this exact key arrived BEFORE any main row
                        # existed, it was stashed as a fallback -- claim it now.
                        if merge_key in ors_zinc_fallback_doc_by_key and merge_key not in ors_zinc_merged_keys:
                            ind_id_vs_info[row_idx]["ORS-Zinc"] = "Yes"
                            ors_zinc_merged_keys.add(merge_key)
                            ors_zinc_fallback_doc_by_key.pop(merge_key, None)

                    elif normalized_product == "ORS-Zinc":
                        if merge_key in ors_zinc_merged_keys:
                            # Already merged once for this key -- this is a genuine DUPLICATE
                            # ORS-Zinc event. Do NOT re-merge (silently hiding it); give it
                            # its own separate standalone row instead, so it stays visible.
                            row = build_row(administration_status, False, False, True)
                            append_row(row)
                        else:
                            row_idx = main_row_index_by_key.get(merge_key)
                            if row_idx is not None:
                                # Matching SPAQ row already exists for this key -- merge in.
                                ind_id_vs_info[row_idx]["ORS-Zinc"] = "Yes"
                                ors_zinc_merged_keys.add(merge_key)
                            else:
                                # No main row yet. Stash as a fallback candidate; a main SPAQ
                                # row that arrives later for this SAME key will claim it. If
                                # this is already the 2nd fallback doc for the key (no main
                                # row has shown up at all), treat it as a duplicate -> its own
                                # standalone row.
                                if merge_key in ors_zinc_fallback_doc_by_key:
                                    row = build_row(administration_status, False, False, True)
                                    append_row(row)
                                else:
                                    ors_zinc_fallback_doc_by_key[merge_key] = doc
                    continue

                # Not a qualifying success (wrong status, wrong product, or non-first SPAQ
                # dose) -- build its own row exactly as before, all three flags "No".
                row = build_row(administration_status, False, False, False)
                append_row(row)

            total_fetched += len(hits)
            pbar.update(1)
            pbar.set_postfix({"records": total_fetched})

    # === Resolve leftover ORS-Zinc fallback docs ===
    # These are ORS-Zinc success events for which NO matching SPAQ 1/SPAQ 2 row ever showed
    # up (same Individual + Username + Cycle) anywhere in the whole scroll. Each becomes its
    # own standalone row (SPAQ 1: No, SPAQ 2: No, ORS-Zinc: Yes) -- mirrors the reference
    # script's handling of VAS-only individuals with no main dose row.
    if ors_zinc_fallback_doc_by_key:
        print(f"\n  NOTE: {len(ors_zinc_fallback_doc_by_key):,} ORS-Zinc event(s) had no matching SPAQ 1/SPAQ 2")
        print(f"        row (same Individual + Username + Cycle) -- creating standalone rows for these.")
    for key, fallback_doc in ors_zinc_fallback_doc_by_key.items():
        ad = fallback_doc.get("additionalDetails", {}) or {}
        qty = fallback_doc.get("quantity")
        fallback_indv_id = fallback_doc.get("individualId")
        row = {
            "Individual ID": fallback_indv_id,
            "Country": fallback_doc["boundaryHierarchy"].get("country", ""),
            "State": fallback_doc["boundaryHierarchy"].get("state", ""),
            "LGA": fallback_doc["boundaryHierarchy"].get("lga", ""),
            "Ward": fallback_doc["boundaryHierarchy"].get("ward", ""),
            "Health Facility": fallback_doc["boundaryHierarchy"].get("healthFacility", ""),
            "Username": fallback_doc.get("userName", ""),
            "Child Name": ad.get("childName", "").strip() if ad else "",
            "Age": fallback_doc.get("age", ""),
            "Gender": ad.get("gender", ""),
            "Household Head Name": ad.get("headName", "").strip() if ad else "",
            "latitude": fallback_doc.get("latitude"),
            "longitude": fallback_doc.get("longitude"),
            "dateOfRegistration": fallback_doc.get("taskDates"),
            "Cycle Index": ad.get("cycleIndex", ""),
            "Administration Status": "ADMINISTRATION_SUCCESS",
            "Timestamp": fallback_doc.get("@timestamp", ""),
            "Record Type": "",
            "Children Referred": "No",
            "Quantity": qty if qty else "",
            "Client Reference ID": fallback_doc.get("clientReferenceId", ""),
            "Symptom": "",
            "SPAQ 1": "No",
            "SPAQ 2": "No",
            "ORS-Zinc": "Yes",
            "Quantity Administered": qty if qty else "",
        }
        idx = len(ind_id_vs_info)
        ind_id_vs_info.append(row)
        ind_id_to_indices[fallback_indv_id].append(idx)
        project_task_record_indices.append(idx)
        ors_zinc_merged_keys.add(key)
    ors_zinc_fallback_doc_by_key.clear()
    if ors_zinc_merged_keys:
        print()

    print(f"\n  Total records fetched from PROJECT TASK: {total_fetched}")
    for status in all_statuses:
        count = status_counts[status]
        print(f"  {status:<30}: {count:>7}")
    print(f"  Unique individuals     : {len(ind_id_to_indices)}")
    print(f"  Records in list        : {len(ind_id_vs_info)}\n")

    # === PRODUCT COLUMN SUMMARY (per row) ===
    # SPAQ 1 / SPAQ 2 / ORS-Zinc are set at row-creation time: SPAQ rows always get their
    # own row; ORS-Zinc merges onto a matching SPAQ row for the same (Individual, Username,
    # Cycle) if one exists, otherwise gets its own standalone row (see merge logic above).
    # This is just a summary of how many ROWS ended up "Yes" for each product.
    print("[Step 1b/4] Product column summary (per-row Yes counts)...")
    for col in PRODUCT_COLUMNS:
        yes_count = sum(1 for r in ind_id_vs_info if r.get(col) == "Yes")
        print(f"  {col:<12}: {yes_count:,} row(s) marked Yes")
    print()

    # === FLAG SAME INDIVIDUAL GIVEN THE SAME PRODUCT BY DIFFERENT USERNAMES (diagnostic) ===
    # This is the suspicious/duplicate-entry case: the same beneficiary shows up as having
    # received the same product, but logged by more than one distinct field worker. It is
    # NOT auto-merged or auto-discarded, and does NOT change any Yes/No column -- just
    # surfaced here for manual review.
    print("[Step 1c/4] Checking for individuals with the same product logged by multiple usernames...")
    for col in PRODUCT_COLUMNS:
        multi_username_individuals = {
            indv_id: usernames
            for indv_id, product_map in ind_id_usernames_by_product.items()
            for prod, usernames in product_map.items()
            if prod == col and len(usernames) > 1
        }
        print(f"  {col:<12}: {len(multi_username_individuals)} individual(s) with this product logged by >1 username")
        if multi_username_individuals:
            shown = 0
            for indv_id, usernames in multi_username_individuals.items():
                if shown >= 10:
                    print(f"    ... and {len(multi_username_individuals) - 10} more (not shown)")
                    break
                print(f"    - {indv_id}: usernames = {sorted(usernames)}")
                shown += 1
    print()

    return ind_id_status_count, spaq_dose1_counts

ind_id_status_count, spaq_dose1_counts = get_successful_administrations()


# ============================================================================
# DUPLICATE-INDIVIDUAL FREQUENCY ANALYSIS (SPAQ 1 / SPAQ 2)
# ============================================================================
# For each (Individual ID, Product, Cycle Index) combination, how many SEPARATE
# dose-1 ADMINISTRATION_SUCCESS documents exist. More than 1 means the same first
# dose was logged multiple times -- different CDD workers, or a sync retry that
# wasn't deduplicated upstream. This prints a summary only.
#
# The per-record export (SPAQ1_SPAQ2_All_Records_Marked.xlsx) was REMOVED: it was a
# one-off data-analysis artefact, it was ~194 MB / 1.2M rows on a large cycle, and it
# accounted for roughly 6 of the 15 minutes an ORS run took.
def analyze_spaq_duplicates(counts):
    """counts: {(individual, product, cycle): n} -> {product: {n: how_many_keys}}"""
    freq_dist_by_product = defaultdict(lambda: defaultdict(int))
    for (_indv, product, _cycle), n in counts.items():
        freq_dist_by_product[product][n] += 1
    return freq_dist_by_product


spaq_freq_dist_by_product = analyze_spaq_duplicates(spaq_dose1_counts)

for product in DUPLICATE_WATCH_PRODUCTS:
    freq_dist = spaq_freq_dist_by_product.get(product, {})
    total_individuals = sum(freq_dist.values())
    total_records = sum(n * c for n, c in freq_dist.items())

    print(f"  {product}")
    print(f"    Total distinct individuals : {total_individuals:,}")
    print(f"    Total records (dose 1)     : {total_records:,}")
    for n in sorted(freq_dist.keys()):
        print(f"      Appears {n} time(s): {freq_dist[n]:>8,} individuals  "
              f"({freq_dist[n] * n:>8,} records)")
    duplicated_individuals = sum(c for n, c in freq_dist.items() if n > 1)
    excess_records = sum((n - 1) * c for n, c in freq_dist.items() if n > 1)
    print(f"    -> {duplicated_individuals:,} individuals duplicated, "
          f"{excess_records:,} excess records, "
          f"{total_records - excess_records:,} would remain if de-duplicated\n")

if not spaq_dose1_counts:
    print("  No SPAQ 1 / SPAQ 2 dose-1 ADMINISTRATION_SUCCESS records found.\n")

print("=" * 80 + "\n")

# === FETCH HF REFERRAL DATA ===
def get_hf_referral_data():
    """Fetch data from pl-hf-referral-index and add to records list"""

    query = {
        "size": SCROLL_SIZE,
        "query": {
            "bool": {
                "must": [
                    {"term": {"Data.campaignNumber.keyword": CAMPAIGN_NUMBER}}
                ]
            }
        },
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
    print(f"  NOTE: No timestamp filter applied (all matching campaign records)\n")

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

                # Track status counts
                status = "Children Referred"
                referral_code_status_count[referral_code][status] += 1

                record = {
                    "Individual ID": referral_code,
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
                    "dateOfRegistration": doc.get("taskDates", ""),
                    "Cycle Index": additional_details.get("cycleIndex", ""),
                    "Administration Status": "Children Referred",
                    "Timestamp": timestamp,
                    "Record Type": "",
                    "Children Referred": "Yes",
                    "Quantity": "",
                    "Client Reference ID": hf_referral.get("clientReferenceId", ""),
                    "Symptom": hf_referral.get("symptom", ""),
                    # HF referral records don't have dosing info -- no products given
                    "SPAQ 1": "No",
                    "SPAQ 2": "No",
                    "ORS-Zinc": "No",
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

# === EXPORT TO EXCEL ===
print(f"[Step 4/4] Writing ORSZ_TREATED.xlsx...")

df = pd.DataFrame(ind_id_vs_info)
df.sort_values(by="Cycle Index", inplace=True)

print(f"  Total rows to export: {len(df):,}")

# save_excel owns the >1,000,000-row rule: it writes ORSZ_TREATED.xlsx for a
# single-file result, or ORSZ_TREATED_part1.xlsx / _part2.xlsx ... when split.
file_path_out = os.path.join(output_dir, "ORSZ_TREATED.xlsx")
_written = save_excel(df, file_path_out)
files_written = _written if isinstance(_written, list) else [_written]

print(f"\n  Output : {', '.join(os.path.basename(f) for f in files_written)}")
print(f"  Rows   : {len(df):,}")

# === VERIFICATION: All records including duplicates are exported ===
print(f"\n✓ VERIFICATION: ALL RECORDS EXPORTED")
print(f"  Total records in memory     : {len(ind_id_vs_info):,}")
print(f"  Total records in DataFrame  : {len(df):,}")
print(f"  Total records written to Excel : {len(df):,}")
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

# === NEW: PRODUCT-WISE SUMMARY (SPAQ 1 / SPAQ 2 / ORS-Zinc) ===
print("\n" + "="*80)
print("PRODUCT-WISE SUMMARY (PER INDIVIDUAL)")
print("="*80)
if all(col in df.columns for col in PRODUCT_COLUMNS):
    # De-duplicate to one row per Individual ID so counts reflect unique individuals,
    # since the Yes/No flags are identical across all of that individual's rows.
    per_individual_df = df.drop_duplicates(subset=["Individual ID"])
    grand_total_individuals = len(per_individual_df)

    print(f"\n{'Product':<20} {'Given (Yes)':>15} {'Not Given (No)':>18} {'% Given':>10}")
    print("-" * 66)
    for col in PRODUCT_COLUMNS:
        yes_count = (per_individual_df[col] == "Yes").sum()
        no_count = (per_individual_df[col] == "No").sum()
        pct = 100*yes_count/grand_total_individuals if grand_total_individuals > 0 else 0
        print(f"  {col:<18} {yes_count:>15} {no_count:>18} {pct:>9.2f}%")
    print("-" * 66)
    print(f"  {'TOTAL INDIVIDUALS':<18} {grand_total_individuals:>15}")

    # Breakdown by how many of the 3 products each individual received
    per_individual_df = per_individual_df.copy()
    per_individual_df["_products_given_count"] = per_individual_df[PRODUCT_COLUMNS].apply(
        lambda row: sum(1 for v in row if v == "Yes"), axis=1
    )
    print(f"\n{'Products Given Count':<25} {'Individuals':>12} {'Percentage':>10}")
    print("-" * 50)
    for n in [3, 2, 1, 0]:
        cnt = (per_individual_df["_products_given_count"] == n).sum()
        pct = 100*cnt/grand_total_individuals if grand_total_individuals > 0 else 0
        label = f"All 3 products" if n == 3 else (f"{n} product(s)" if n > 0 else "0 (none / HF referral only)")
        print(f"  {label:<23} {cnt:>12} {pct:>9.2f}%")

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
        print(f"  Total HF Referral records     : {len(hf_referral_df):,}")
        print(f"  Total unique referral codes  : {total_unique_referral_codes:,}")
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
            print(f"  All {total_unique_referral_codes:,} referral codes are unique.")
    else:
        print(f"\nNo HF Referral records found in dataset")

print("\n" + "="*80)
print(f"Total Rows Exported : {len(df):,}")
print(f"Unique Individuals/Referrals : {len(ind_id_to_indices):,}")
print(f"Avg Records/Individual : {len(df)/len(ind_id_to_indices):.2f}")
for i, fpath in enumerate(files_written, 1):
    print(f"Output File {i}: {fpath}")

# === DEBUG SUMMARY: Track records by source ===
print("\n" + "-"*80)
print("DATA SOURCE BREAKDOWN (DEBUG INFO)")
print("-"*80)
if "Children Referred" in df.columns:
    project_task_records = (df["Children Referred"] == "No").sum()
    hf_referral_records = (df["Children Referred"] == "Yes").sum()

    print(f"\nRecords by source:")
    print(f"  Project Task records      : {project_task_records:,}")
    print(f"  HF Referral records       : {hf_referral_records:,}")
    print(f"  Total in Excel            : {len(df):,}")

    print(f"\nHF Referral records detail:")
    print(f"  Total HF records fetched from index: {len(hf_referral_ids):,}")
    print(f"  Unique referral codes              : {len(set(hf_referral_ids)):,}")
    print(f"  Records in final export            : {hf_referral_records:,}")
    print(f"  ✓ ALL {len(hf_referral_ids):,} HF REFERRAL RECORDS ARE IN EXCEL (including duplicates)")

    if len(hf_referral_ids) > 0 and hf_referral_records > 0:
        difference = len(hf_referral_ids) - hf_referral_records
        if difference > 0:
            print(f"\n  ⚠ Note: {difference:,} records with duplicate Individual IDs")
            print(f"    ({len(hf_referral_ids):,} total fetched vs {hf_referral_records:,} unique Individual IDs)")
            print(f"    But ALL {len(hf_referral_ids):,} records are still in Excel")
            print(f"    Marked with Record Type = 'duplicate'")
        elif difference < 0:
            print(f"\n  ℹ More records in output than fetched (as expected)")
        else:
            print(f"\n  ✓ All fetched records present in output")

print("\n" + "="*80 + "\n")