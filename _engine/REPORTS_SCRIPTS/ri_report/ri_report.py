import os
import sys
import warnings
import pandas as pd
from tqdm import tqdm

file_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(file_path)
warnings.filterwarnings("ignore", message="Unverified HTTPS request is being made.*")
from COMMON_UTILS.common_utils import save_excel
from COMMON_UTILS.common_utils import get_resp
from COMMON_UTILS.custom_date_utils import get_custom_dates_of_reports

# ============================================================
# RI ROW-LEVEL REPORT  -- one row per task record + one row per referral record
# Basis : flow = riDone. Cycle scoping comes from the UI selection via
#         COMMON_UTILS.get_resp (no static cycle - RI data is not present in
#         every cycle, and which cycles carry it differs per state).
#
# CHANGE (this version):
#  - "CHC response" on REFERRAL rows is no longer hardcoded to "YES".
#    It is now read from Data.additionalDetails.chcSeen on the
#    hf-referral doc itself (same field name as used on task docs).
#    Task rows are unaffected -- still a raw passthrough of chcSeen
#    from the task doc.
#
#  - Added three new columns, placed immediately after "Fully immunised":
#       Partially Immunized  <- Data.additionalDetails.partiallyImmunized
#       Unimmunized           <- Data.additionalDetails.unimmunized
#       Zero Dose             <- Data.additionalDetails.zeroDose
#    These are populated ONLY on referral rows (read from the hf-referral
#    doc's additionalDetails). Task rows leave these three columns blank.
#
# Columns: state, LGA, Ward, Health Facility, Username, Beneficiary ID,
#          Age in month, Gender, Screened for RI, CHC response,
#          Fully immunised, Partially Immunized, Unimmunized, Zero Dose,
#          hf Referral, Presentat Health facility, CDD RI referral Done,
#          riQ1..riQ5 (labeled with question text)
# ============================================================
from COMMON_UTILS.tenant import PREFIX, CAMPAIGN

# ===================== CONFIG =====================
# Every tunable for this report lives here.
# --- campaign / cycle ---
# No static cycle. RI data does not exist in every cycle and which cycles carry it
# differs per state (kogi 02/03/04, plateau 01/02/03/04, sokoto 02/03), so the old
# hard-coded "03" discarded 79% / 73% / 86% of those states' RI records.
# Cycle scoping is applied by COMMON_UTILS.get_resp from the UI selection; in
# Custom-date-range mode there is no cycle filter and the date window applies.
_cyc = [c for c in os.environ.get("CYCLES", "").split(",") if c]
CAMPAIGN_NUMBER = CAMPAIGN
# --- reporting window (same source as Child Treated / ORS) ---
# In cycle mode report_ui writes a wide-open window (2000..2099) so this is a no-op
# and the cycle does the scoping. In Custom-date-range mode it bounds the task fetch.
lteTime, gteTime, start_date_str, end_date_str = get_custom_dates_of_reports()
# --- ES host + indices (the host is defined ONCE, here) ---
ES_BASE  = "https://elasticsearch-data.es-cluster-v8:9200"
IDX_TASK = f"{ES_BASE}/{PREFIX}-project-task-index-v1/_search"
IDX_REF  = f"{ES_BASE}/{PREFIX}-hf-referral-index-v1/_search"
# --- output / paging ---
OUTPUT_DIR  = os.path.join(file_path, "FINAL_REPORTS", "RI_REPORT")
OUTPUT_XLSX = "RI_Report.xlsx"
PAGE_SIZE   = 5000
# ==================================================

RIQ_KEYS = ["riQ1", "riQ2", "riQ3", "riQ4", "riQ5"]
RIQ_LABELS = {
    "riQ1": "Has child received malaria vaccine earlier? (riQ1)",
    "riQ2": "What dose has the child received previously? (riQ2)",
    "riQ3": "Is date of previous dose administration less than 1 month? (riQ3)",
    "riQ4": "What dose has the child received now? (riQ4)",
    "riQ5": "What dose has the child received now? (riQ5)",
}

COLS = ["state", "LGA", "Ward", "Health Facility", "Username", "Beneficiary ID",
        "Age in month", "Gender", "Screened for RI", "CHC response",
        "Fully immunised", "Partially Immunized", "Unimmunized", "Zero Dose",
        "hf Referral", "Presentat Health facility", "CDD RI referral Done"
        ] + [RIQ_LABELS[k] for k in RIQ_KEYS]


def es(url, body):
    return get_resp(url, body, True).json()


def scroll(url, must, source, sort_field, desc="Fetching"):
    """Fetch all matching docs via search_after pagination."""
    after, out = None, []
    with tqdm(desc=desc, unit=" docs") as pbar:
        while True:
            body = {"size": PAGE_SIZE, "track_total_hits": False, "_source": source,
                    "sort": [{sort_field: "asc"}], "query": {"bool": {"must": must}}}
            if after:
                body["search_after"] = after
            hits = es(url, body).get("hits", {}).get("hits", [])
            if not hits:
                break
            out.extend(hits)
            pbar.update(len(hits))
            after = hits[-1]["sort"]
    return out


def build_referral_data():
    """
    Query pl-hf-referral-index-v1, filtered on:
      - Data.campaignNumber.keyword
      - Data.additionalDetails.cycleIndex.keyword
      - flow == riDone, matched via the additionalFields.fields key/value
        array (Data.hfReferral.additionalFields.fields.key.keyword == "flow"
        AND Data.hfReferral.additionalFields.fields.value.keyword == "riDone")

    riQ1-riQ5 answers AND referralReasons all live inside the same
    additionalFields.fields array as {key: "riQ1", value: "NO"} /
    {key: "referralReasons", value: "RI"} style entries -- extracted by
    key match.

    chcSeen, partiallyImmunized, unimmunized, zeroDose, fullyImmunized,
    referralReasons all live directly on Data.additionalDetails for the
    hf-referral doc (same shape as the task doc's additionalDetails).

    Also captures state/lga/ward/healthFacility/userName/ageInMonths/gender
    directly from the referral doc itself, so referral rows are fully
    self-sufficient and don't depend on a task record existing.

    Returns dict: { beneficiaryId: {
        "state": ..., "lga": ..., "ward": ..., "healthFacility": ...,
        "userName": ..., "ageInMonths": ..., "gender": ...,
        "rowVersion": <int or None>,
        "chcSeen": <str or None>,
        "partiallyImmunized": <str or None>,
        "unimmunized": <str or None>,
        "zeroDose": <str or None>,
        "referralReasons": <str or None>,
        "riQ1": <str or None>, ..., "riQ5": <str or None>
    } }
    """
    must = [
        {"term": {"Data.campaignNumber.keyword": CAMPAIGN_NUMBER}},
        {"term": {"Data.hfReferral.additionalFields.fields.key.keyword": "flow"}},
        {"term": {"Data.hfReferral.additionalFields.fields.value.keyword": "riDone"}},
    ]
    src = ["Data.boundaryHierarchy.state", "Data.boundaryHierarchy.lga", "Data.boundaryHierarchy.ward",
           "Data.boundaryHierarchy.healthFacility", "Data.userName",
           "Data.additionalDetails.ageInMonths", "Data.additionalDetails.gender",
           "Data.additionalDetails.chcSeen", "Data.additionalDetails.partiallyImmunized",
           "Data.additionalDetails.unimmunized", "Data.additionalDetails.zeroDose",
           "Data.hfReferral.beneficiaryId", "Data.hfReferral.rowVersion",
           "Data.hfReferral.additionalFields.fields", "Data.additionalDetails.referralReasons"]
    docs = scroll(IDX_REF, must, src, "Data.hfReferral.beneficiaryId.keyword",
                  desc="Fetching hf-referral")

    referral_data = {}
    for h in tqdm(docs, desc="Building referral map", unit=" docs"):
        d = h["_source"]["Data"]
        b = d.get("boundaryHierarchy", {}) or {}
        ad = d.get("additionalDetails", {}) or {}
        hf = d.get("hfReferral", {}) or {}
        bid = hf.get("beneficiaryId")
        if not bid:
            continue
        fields_list = (hf.get("additionalFields", {}) or {}).get("fields", []) or []
        riq_values = {k: None for k in RIQ_KEYS}
        referral_reasons_field = None
        for f in fields_list:
            k = f.get("key")
            if k in riq_values:
                riq_values[k] = f.get("value")
            elif k == "referralReasons":
                referral_reasons_field = f.get("value")
        referral_data[bid] = {
            "state": b.get("state"), "lga": b.get("lga"), "ward": b.get("ward"),
            "healthFacility": b.get("healthFacility"), "userName": d.get("userName"),
            "ageInMonths": ad.get("ageInMonths"), "gender": ad.get("gender"),
            "rowVersion": hf.get("rowVersion"),
            "chcSeen": ad.get("chcSeen"),
            "partiallyImmunized": ad.get("partiallyImmunized"),
            "unimmunized": ad.get("unimmunized"),
            "zeroDose": ad.get("zeroDose"),
            "referralReasons": ad.get("referralReasons") or referral_reasons_field,
            **riq_values,
        }
    return referral_data


def main():
    print(f"Campaign {CAMPAIGN_NUMBER} | RI flow=riDone | "
          f"cycles={','.join(_cyc) if _cyc else '(none selected - date window applies)'}")
    print(f"Reporting window (project-task): {start_date_str} .. {end_date_str}")

    referral_data = build_referral_data()
    print(f"  hf-referral matched: {len(referral_data):,}")

    must = [{"term": {"Data.campaignNumber.keyword": CAMPAIGN_NUMBER}},
            {"term": {"Data.additionalDetails.flow.keyword": "riDone"}},
            {"range": {"Data.@timestamp": {"gte": gteTime, "lte": lteTime}}}]
    src = ["Data.boundaryHierarchy.state", "Data.boundaryHierarchy.lga", "Data.boundaryHierarchy.ward",
           "Data.boundaryHierarchy.healthFacility", "Data.userName", "Data.additionalDetails.beneficiaryId",
           "Data.additionalDetails.individualClientReferenceId", "Data.additionalDetails.ageInMonths",
           "Data.additionalDetails.gender", "Data.additionalDetails.chcSeen",
           "Data.additionalDetails.fullyImmunized", "Data.additionalDetails.partiallyImmunized",
           "Data.additionalDetails.zeroDose", "Data.additionalDetails.unimmunized"]
    docs = scroll(IDX_TASK, must, src, "Data.additionalDetails.beneficiaryId.keyword",
                  desc="Fetching RI task records")
    print(f"  RI (riDone) records: {len(docs):,}")

    rows = []
    riq_blank = ["", "", "", "", ""]

    # ---------------------------------------------------------------
    # Task-based rows: immunisation/CHC info only. Referral columns
    # (Partially Immunized, Unimmunized, Zero Dose, hf Referral,
    # Presentat Health facility, Referral Reasons, riQ1-5) are
    # intentionally left blank here -- not applicable to task rows.
    # ---------------------------------------------------------------
    for h in tqdm(docs, desc="Building task rows", unit=" rows"):
        d = h["_source"]["Data"]
        b = d.get("boundaryHierarchy", {}) or {}
        ad = d.get("additionalDetails", {}) or {}
        bid = ad.get("beneficiaryId", "")

        rows.append([
            b.get("state"), b.get("lga"), b.get("ward"), b.get("healthFacility"), d.get("userName"),
            bid, ad.get("ageInMonths"), ad.get("gender"),
            "YES",                          # Screened for RI (hardcoded)
            ad.get("chcSeen", ""),          # CHC response (raw passthrough from task)
            ad.get("fullyImmunized", ""),   # Fully immunised (task-based)
            "", "", "",                     # Partially Immunized, Unimmunized, Zero Dose -- blank (referral-only)
            "", "", "",                     # hf Referral, Presentat Health facility, Referral Reasons -- blank
        ] + riq_blank)

    # ---------------------------------------------------------------
    # Referral-based rows: ALWAYS emitted, one per referral record,
    # regardless of whether that beneficiary also has a task row above.
    # Fully immunised is not applicable here; Partially Immunized,
    # Unimmunized, Zero Dose, and CHC response now come from the
    # hf-referral doc's own additionalDetails.
    # ---------------------------------------------------------------
    for bid, ref in tqdm(referral_data.items(), desc="Building referral rows", unit=" rows"):
        present_hf = "YES" if ref.get("rowVersion") == 2 else "NO"
        referral_reasons = ref.get("referralReasons") or ""
        riq_vals = [ref.get(k) or "" for k in RIQ_KEYS]
        rows.append([
            ref.get("state"), ref.get("lga"), ref.get("ward"), ref.get("healthFacility"),
            ref.get("userName"), bid, ref.get("ageInMonths"), ref.get("gender"),
            "YES",                              # Screened for RI (hardcoded)
            ref.get("chcSeen") or "",            # CHC response (from hf-referral additionalDetails.chcSeen)
            "",                                  # Fully immunised (not applicable on a referral row)
            ref.get("partiallyImmunized") or "", # Partially Immunized (referral-only)
            ref.get("unimmunized") or "",        # Unimmunized (referral-only)
            ref.get("zeroDose") or "",           # Zero Dose (referral-only)
            "YES", present_hf, referral_reasons,
        ] + riq_vals)

    df = pd.DataFrame(rows, columns=COLS)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    xlsx = os.path.join(OUTPUT_DIR, OUTPUT_XLSX)

    # summary counts
    unique_beneficiaries = df["Beneficiary ID"].nunique()
    summary = pd.DataFrame({
        "Metric": ["Total rows (task rows + referral rows)", "Unique beneficiaries",
                   "Screened for RI = YES", "Fully immunised = YES", "CHC response = YES",
                   "Partially Immunized = YES", "Unimmunized = YES", "Zero Dose = YES",
                   "hf Referral = YES", "Presentat Health facility = YES"],
        "Count": [len(df), unique_beneficiaries,
                  (df["Screened for RI"] == "YES").sum(),
                  (df["Fully immunised"] == "YES").sum(), (df["CHC response"] == "YES").sum(),
                  (df["Partially Immunized"] == "YES").sum(), (df["Unimmunized"] == "YES").sum(),
                  (df["Zero Dose"] == "YES").sum(),
                  (df["hf Referral"] == "YES").sum(), (df["Presentat Health facility"] == "YES").sum()]})
    save_excel(df, xlsx, extra_sheets={"Summary": summary})

    print(f"\nSaved:\n  {xlsx}\nRows: {len(df):,} (unique beneficiaries: {unique_beneficiaries:,})")


if __name__ == "__main__":
    main()