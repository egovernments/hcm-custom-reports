# hcm-custom-reports

Campaign reporting engine for DIGIT HCM health campaigns. One shared set of scripts
generates every report for every state — there is no per-state copy of any code.

This branch (`nigeria-mc-smc-prod`) holds the **Nigeria SMC** engine, in production for
10 states: Oyo, Kogi, Nasarawa, FCT Abuja, Bauchi, Borno, Kebbi, Plateau, Sokoto, Zamfara.

## Reports

| Report | Scope | Reads |
|---|---|---|
| Child Treated | all states | `project-task`, `hf-referral`, `individual`, `household-member` |
| Stock | all states | `stock` |
| ORS/ORZ | states that administered ORS-Zinc | `project-task`, `hf-referral` |
| RI | states that ran routine immunisation | `project-task`, `hf-referral` |

Which reports a state sees, and which cycles each report offers, are both resolved at run
time — see [docs/HOW_TO.md](docs/HOW_TO.md).

## Layout

```
START_HERE_SMC.ipynb          the only file a report user opens
_engine/
  report_ui.py                the form, progress, Stop button, download links
  campaign-config.json        states, tenant prefixes, campaign numbers, per-state reports
  reports_config.json         report catalog: script path, output name, cycle filters
  COMMON_UTILS/
    common_utils.py           get_resp (ES calls + cycle injection), save_excel
    tenant.py                 PREFIX / CAMPAIGN / es_index() for the selected state
    custom_date_utils.py      reporting window
  REPORTS_SCRIPTS/<report>/   one folder per report
docs/HOW_TO.{md,html,pdf}     user + developer guide, with screenshots
```

## Configuration

**No credential is stored in this repository.** Elasticsearch auth is read from the
environment at run time, and the engine refuses to start without it:

```bash
printf '%s' 'username:password' | base64          # produces <b64>
export ELASTIC_AUTH='Basic <b64>'
```

See [.env.example](.env.example). `.env` is git-ignored.

## Running

From the deployment directory (the one containing `_engine/`):

```bash
export ELASTIC_AUTH='Basic <b64>'

# in Jupyter: open START_HERE_SMC.ipynb and run the single cell
# or drive a report directly:
STATE_CODE=zamfara CYCLES=02 python3 _engine/REPORTS_SCRIPTS/stock_report/stock_report.py
```

Two environment variables drive every script:

| variable | example | meaning |
|---|---|---|
| `STATE_CODE` | `zamfara` | selected state; `tenant.py` resolves prefix + campaign number |
| `CYCLES` | `01,02` | selected cycles; empty in custom-date-range mode |

Output lands in `FINAL_REPORTS/<state>/<range>__gen_<timestamp>/` and is git-ignored —
generated campaign data must never be committed.

## Notes for anyone extending this

- Never hardcode a tenant prefix, campaign number, or cycle. Read them from
  `COMMON_UTILS.tenant` and let `get_resp` inject the cycle.
- A leftover cycle clause in a query **suppresses** cycle injection entirely and pins the
  report to one cycle regardless of what the user selected.
- Write output only through `COMMON_UTILS.save_excel` — it streams rows, splits
  automatically above 1,000,000 rows (Excel's limit is ~1,048,576) and fails loudly rather
  than silently changing format.

Full details, including how to add a report for all states / specific states / specific
cycles, are in [docs/HOW_TO.md](docs/HOW_TO.md).

## Requirements

Python 3.11+, with `pandas`, `requests`, `xlsxwriter` (or `openpyxl`), `tqdm`, `ipywidgets`,
`pytz`.
