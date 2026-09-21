"""
STOCK REPORT -- central, common to every state.

One row per Health Facility, 17 quantity columns covering the four movement legs:
    State -> HF, HF -> State (returns), HF -> Staff, Staff -> HF (returns)

Ported from zamfara-custom-reports/REPORTS_SCRIPTS/aggregation_report/zamfara_stock_report.py
(the newest of the three per-state revisions -- 7 of 10 states already ran this exact
logic; oyo/kebbi and plateau were on older revisions missing the explicit column order
and the IN_TRANSIT columns respectively).

Differences from the per-state script, and why:
  * tenant/campaign come from COMMON_UTILS.tenant -- no hardcoded prefix or CMP number.
  * NO hardcoded cycleIndex clause. COMMON_UTILS.get_resp injects the UI's cycle
    selection, but ONLY when the query carries no cycle clause of its own; leaving the
    static {"term": {...cycleIndex...}} in place would have silently pinned every run to
    one cycle whatever the user ticked.
  * the Health Facility terms aggregation (size 2000) is replaced by a COMPOSITE
    aggregation with after-key paging. Six of ten states have more than 2,000 distinct
    transacting facilities (bauchi 10,195), so the fixed size silently truncated.
  * output goes through COMMON_UTILS.save_excel (xlsxwriter constant_memory, automatic
    >1M part files, no CSV fallback) instead of df.to_excel.
  * a Data.@timestamp range is applied, same field and source as the other three reports.
    In cycle mode report_ui writes a wide-open 2000..2099 window so this is a no-op and the
    cycle does the scoping; in Custom-date-range mode it bounds the report.

NOT changed (deliberately): the three facility filters and the 17 column mappings are
byte-for-byte the zamfara logic, so this reproduces existing per-state output exactly.
Stock documents with facilityType "Central Facility" remain excluded, as they are today.
"""

import os
import sys
import warnings

import pandas as pd
from tqdm import tqdm
from datetime import datetime

# ================= SETUP =================

file_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(file_path)
warnings.filterwarnings("ignore", message="Unverified HTTPS request is being made.*")

from COMMON_UTILS.common_utils import get_resp, save_excel
from COMMON_UTILS.custom_date_utils import get_custom_dates_of_reports
from COMMON_UTILS.tenant import PREFIX, CAMPAIGN, es_index

# ===================== CONFIG =====================
# Every tunable for this report lives here.

# --- campaign / cycle ---
CAMPAIGN_NUMBER = CAMPAIGN
# Cycle scoping is applied by COMMON_UTILS.get_resp from the UI selection. This is read
# only to echo what was chosen.
SELECTED_CYCLES = [c for c in os.environ.get("CYCLES", "").split(",") if c]

# --- reporting window (same source and field as Child Treated / ORS / RI) ---
lteTime, gteTime, start_date_str, end_date_str = get_custom_dates_of_reports()

# --- ES index (host defined once, in COMMON_UTILS.tenant) ---
ES_STOCK_INDEX = es_index("stock")

# --- aggregation paging ---
AGG_PAGE_SIZE = 1000          # composite buckets per request
SUB_AGG_SIZE = 20             # stockEntryType / status cardinality is 2 and 3; 20 is slack

# --- facility filters (unchanged from the per-state script) ---
FACILITY_STATE = ["State Facility", "WAREHOUSE"]
FACILITY_STAFF = ["STAFF"]
FACILITY_HF = ["Health Facility", "WAREHOUSE"]

# --- grouping fields ---
GROUP_TRANSACTING = "Data.transactingFacilityName.keyword"
GROUP_FACILITY = "Data.facilityName.keyword"

# --- output ---
OUTPUT_DIR = os.path.join(file_path, "FINAL_REPORTS", "STOCK")
OUTPUT_XLSX = "STOCK_REPORT.xlsx"
# ==================================================


def log(message, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}", flush=True)


# ================= COLUMN MAPS =================
# (stockEntryType, status_or_None) -> output column.
# status_or_None = None means "sum across all statuses for that entry type".

STATE_FACILITY_MAP = {
    ("ISSUED", None):         "Stock Sent by State to HF",
    ("ISSUED", "ACCEPTED"):   "Stock Received by HF from State",
    ("ISSUED", "REJECTED"):   "Stock Rejected by HF from State",
    ("ISSUED", "IN_TRANSIT"): "Stock In-Transit from State to HF",
}

STAFF_FACILITY_MAP = {
    ("RETURNED", None):         "Stock Returned to HF by Staff",
    ("RETURNED", "ACCEPTED"):   "Returned Stock Received by HF",
    ("RETURNED", "REJECTED"):   "Returned Stock Rejected by HF",
    ("RETURNED", "IN_TRANSIT"): "Returned Stock In-Transit to HF",
}

HF_TO_STAFF_MAP = {
    ("ISSUED", None):         "Stock Sent by HF to Staff",
    ("ISSUED", "ACCEPTED"):   "Stock Received by Staff from HF",
    ("ISSUED", "REJECTED"):   "Stock Rejected by Staff from HF",
    ("ISSUED", "IN_TRANSIT"): "Stock In-Transit from HF to Staff",

    ("RETURNED", None):         "Stock Returned by HF to State",
    ("RETURNED", "ACCEPTED"):   "Returned Stock Accepted by State from HF",
    ("RETURNED", "REJECTED"):   "Returned Stock Rejected by State from HF",
    ("RETURNED", "IN_TRANSIT"): "Returned Stock In-Transit from HF to State",
}

FINAL_COLS = [
    "Health Facility",

    # --- State -> HF ---
    "Stock Sent by State to HF",
    "Stock Received by HF from State",
    "Stock Rejected by HF from State",
    "Stock In-Transit from State to HF",

    # --- HF -> State (returns) ---
    "Stock Returned by HF to State",
    "Returned Stock Accepted by State from HF",
    "Returned Stock Rejected by State from HF",
    "Returned Stock In-Transit from HF to State",

    # --- HF -> Staff ---
    "Stock Sent by HF to Staff",
    "Stock Received by Staff from HF",
    "Stock Rejected by Staff from HF",
    "Stock In-Transit from HF to Staff",

    # --- Staff -> HF (returns) ---
    "Stock Returned to HF by Staff",
    "Returned Stock Received by HF",
    "Returned Stock Rejected by HF",
    "Returned Stock In-Transit to HF",
]


# ================= FETCH =================

def _build_query(facility_types, group_field, after=None):
    """Composite aggregation over facilities.

    NOTE: no cycleIndex clause here on purpose -- get_resp appends the UI selection.
    """
    query = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {"term": {"Data.campaignNumber.keyword": CAMPAIGN_NUMBER}},
                    {"range": {"Data.@timestamp": {"gte": gteTime, "lte": lteTime}}},
                    {"terms": {"Data.facilityType.keyword": facility_types}},
                ]
            }
        },
        "aggs": {
            "facilities": {
                "composite": {
                    "size": AGG_PAGE_SIZE,
                    "sources": [{"hf": {"terms": {"field": group_field}}}],
                },
                "aggs": {
                    "entry_type": {
                        "terms": {
                            "field": "Data.additionalDetails.stockEntryType.keyword",
                            "size": SUB_AGG_SIZE,
                        },
                        "aggs": {
                            "status": {
                                "terms": {
                                    "field": "Data.additionalDetails.status.keyword",
                                    "size": SUB_AGG_SIZE,
                                },
                                "aggs": {
                                    "total_quantity": {"sum": {"field": "Data.physicalCount"}}
                                },
                            }
                        },
                    }
                },
            }
        },
    }
    if after:
        query["aggs"]["facilities"]["composite"]["after"] = after
    return query


def fetch_facility_buckets(facility_types, group_field, desc):
    """Page a composite aggregation to completion. Returns every bucket."""
    buckets = []
    after = None
    pages = 0
    truncated = 0

    with tqdm(desc=f"  Fetching {desc}", unit="hf") as pbar:
        while True:
            body = _build_query(facility_types, group_field, after)
            payload = get_resp(ES_STOCK_INDEX, body, es=True).json()
            agg = payload.get("aggregations", {}).get("facilities", {})
            page = agg.get("buckets", [])
            if not page:
                break

            # Guard: the nested terms aggs must never drop a value. Cardinality is 2
            # (ISSUED/RETURNED) and 3 (ACCEPTED/REJECTED/IN_TRANSIT), so a non-zero
            # sum_other_doc_count would mean an unexpected new value appeared.
            for b in page:
                et = b.get("entry_type", {})
                truncated += et.get("sum_other_doc_count", 0) or 0
                for eb in et.get("buckets", []):
                    truncated += (eb.get("status", {}) or {}).get("sum_other_doc_count", 0) or 0

            buckets.extend(page)
            pbar.update(len(page))
            pages += 1

            after = agg.get("after_key")
            if not after:
                break

    if truncated:
        log(f"{desc}: {truncated:,} documents fell outside the entryType/status "
            f"aggregation buckets -- an unexpected value has appeared in the data. "
            f"Raise SUB_AGG_SIZE and investigate.", "WARN")

    log(f"{desc}: {len(buckets):,} facilities over {pages} page(s)")
    return buckets


def parse_stock_buckets(buckets, column_map, desc):
    """Walk facility > entry type > status buckets into one row per facility."""
    rows = []
    for bucket in tqdm(buckets, desc=f"  Parsing {desc}", unit="hf"):
        key = bucket.get("key")
        hf_name = key.get("hf", "") if isinstance(key, dict) else (key or "")

        row = {"Health Facility": hf_name}
        for col_name in column_map.values():
            row[col_name] = 0

        for entry_bucket in bucket.get("entry_type", {}).get("buckets", []):
            entry_type = entry_bucket.get("key", "")
            entry_total = 0

            for status_bucket in entry_bucket.get("status", {}).get("buckets", []):
                status = status_bucket.get("key", "")
                qty = (status_bucket.get("total_quantity", {}) or {}).get("value", 0) or 0
                entry_total += qty

                mapped = column_map.get((entry_type, status))
                if mapped:
                    row[mapped] += qty

            mapped_total = column_map.get((entry_type, None))
            if mapped_total:
                row[mapped_total] += entry_total

        rows.append(row)

    return rows


def _frame(rows, column_map):
    """DataFrame from parsed rows, with the mapped columns guaranteed present."""
    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(columns=["Health Facility"] + list(column_map.values()))
    for col in column_map.values():
        if col not in df.columns:
            df[col] = 0
    return df


# ================= MAIN =================

def main():
    print(f"Campaign number   : {CAMPAIGN_NUMBER}")
    print(f"Tenant prefix     : {PREFIX}")
    print(f"Cycles            : "
          f"{','.join(SELECTED_CYCLES) if SELECTED_CYCLES else '(none selected - date window applies)'}")
    print(f"Reports start date: {start_date_str}")
    print(f"Reports end date  : {end_date_str}")
    print(f"\n===== Generating report : STOCK\n")

    log("[Step 1/5] Fetching State Facility -> HF (ISSUED) ...")
    state_buckets = fetch_facility_buckets(
        FACILITY_STATE, GROUP_TRANSACTING, "State Facility (ISSUED)")

    log("[Step 2/5] Fetching Staff -> HF (RETURNED) ...")
    staff_buckets = fetch_facility_buckets(
        FACILITY_STAFF, GROUP_TRANSACTING, "STAFF (RETURNED)")

    log("[Step 3/5] Fetching HF -> Staff movements ...")
    hf_buckets = fetch_facility_buckets(
        FACILITY_HF, GROUP_FACILITY, "HF -> Staff")

    log("[Step 4/5] Parsing and merging ...")
    state_df = _frame(parse_stock_buckets(state_buckets, STATE_FACILITY_MAP,
                                          "State Facility (ISSUED)"), STATE_FACILITY_MAP)
    staff_df = _frame(parse_stock_buckets(staff_buckets, STAFF_FACILITY_MAP,
                                          "STAFF (RETURNED)"), STAFF_FACILITY_MAP)
    hf_df = _frame(parse_stock_buckets(hf_buckets, HF_TO_STAFF_MAP,
                                       "HF -> Staff"), HF_TO_STAFF_MAP)

    merged_df = state_df.merge(staff_df, on="Health Facility", how="outer") \
                        .merge(hf_df, on="Health Facility", how="outer")

    value_cols = (list(STATE_FACILITY_MAP.values())
                  + list(STAFF_FACILITY_MAP.values())
                  + list(HF_TO_STAFF_MAP.values()))
    for col in value_cols:
        if col not in merged_df.columns:
            merged_df[col] = 0
    merged_df[value_cols] = merged_df[value_cols].fillna(0).astype("int64")

    merged_df = merged_df[FINAL_COLS].sort_values(by="Health Facility").reset_index(drop=True)

    if merged_df.empty:
        log("No stock records matched. Writing an empty workbook (headers only) so the "
            "result is visible rather than silently absent.", "WARN")

    log(f"[Step 5/5] Writing {len(merged_df):,} facility rows ...")
    out_path = os.path.join(OUTPUT_DIR, OUTPUT_XLSX)
    written = save_excel(merged_df, out_path)

    log("=" * 60)
    log("STOCK REPORT GENERATION COMPLETED")
    log(f"Output File   : {written}")
    log(f"Total HF Rows : {len(merged_df):,}")
    for col in FINAL_COLS[1:]:
        log(f"{col:<45}: {merged_df[col].sum():,.0f}")


if __name__ == "__main__":
    main()
