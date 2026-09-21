"""Tenant-aware report launcher (single-state, per-state reports). START_HERE calls launch()."""
import os, re, json, shutil, datetime, subprocess, time, threading, signal
import urllib.parse

ENGINE     = os.path.dirname(os.path.abspath(__file__))
CYCLE_MIN_DOCS = 100          # ignore junk cycle buckets (e.g. a 3-document 'cycle 05')
_CYC_RE = re.compile(r'^\d{2}$')   # only zero-padded cycle ids are offered
ROOT       = os.path.dirname(ENGINE)
CFG        = os.path.join(ENGINE, "reports_config.json")     # master report catalog
STATES_CFG = os.path.join(ENGINE, "campaign-config.json")    # common_reports + states(+extra_reports)
DATECFG    = os.path.join(ENGINE, "reports_date_config.json")
STATE_FILE = os.path.join(ENGINE, "ui_state.json")
OUTDIR     = os.path.join(ROOT,   "FINAL_REPORTS")
ENGINE_OUT = os.path.join(ENGINE, "FINAL_REPORTS")
_STEP = re.compile(r"Step\s+(\d+(?:\.\d+)?)\s*(?:/|of)\s*(\d+)")
# lines emitted by tqdm - never use these as the status text: the longest stage
# (the xlsx write) emits nothing, so a latched tqdm line looks like a frozen report
_NOISE = ("%|", "it/s", "rec/s", " batch/s", " page/s", "?B/s", "row/s")
_SPIN = "|/-\\"
_MARKERS = ("Step", "Fetching", "Writing", "Download", "records", "Processing",
            "save_excel", "rows written",
            "Building", "Exporting", "Parsing", "Merging", "households", "individuals",
            "names", "Verifying", "Applying")


def _kill(proc):
    """SIGTERM the report process, escalate to SIGKILL if it ignores us.
    Only the child pid is signalled - never the process group, which would
    take the Jupyter kernel down with it."""
    if proc is None or proc.poll() is not None:
        return
    try:
        proc.terminate()
    except Exception:
        pass
    for _ in range(50):                     # up to ~5s to exit politely
        if proc.poll() is not None:
            return
        time.sleep(0.1)
    try:
        proc.kill()
    except Exception:
        pass


def _run_script(spath, env, debug, log, on_line, run=None):
    proc = subprocess.Popen(["python3", spath], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, bufsize=1, env=env)
    if run is not None:
        run["proc"] = proc
    tail = []; i = 0
    stopped = False
    try:
        for line in proc.stdout:
            if run is not None and run.get("stop"):
                stopped = True
                break
            s = line.rstrip(); tail.append(line)
            if debug: log(s)
            if on_line: on_line(s, i)
            i += 1
    finally:
        if run is not None and run.get("stop"):
            stopped = True
            _kill(proc)
        try: proc.wait(timeout=30)
        except Exception: _kill(proc)
        if run is not None:
            run["proc"] = None
    if stopped:
        log("   stopped by user")
        return -1
    if proc.returncode != 0 and not debug:
        log("   error - last lines:")
        for l in tail[-12:]:
            log("     " + l.rstrip())
    return proc.returncode


# Jupyter serves any file under the server root at <prefix>/files/<path>. ?download=1
# makes it an attachment rather than opening in a browser tab. Both values come from the
# kernel env, so nothing about the host or user is hard-coded.
_SVC_PREFIX  = os.environ.get("JUPYTERHUB_SERVICE_PREFIX", "/")
_SERVER_ROOT = os.environ.get("JUPYTER_SERVER_ROOT") or os.path.expanduser("~")


def _download_url(path):
    """A /files/ URL for an output file, or None if it sits outside the server root
    (in which case Jupyter cannot serve it and we just show the path)."""
    try:
        rel = os.path.relpath(os.path.abspath(path), _SERVER_ROOT)
    except Exception:
        return None
    if rel.startswith(".."):
        return None
    return (_SVC_PREFIX.rstrip("/") + "/files/"
            + urllib.parse.quote(rel.replace(os.sep, "/")) + "?download=1")


def _file_line(path):
    """One output row: filename, size, and a Download button when servable."""
    try: mb = os.path.getsize(path) / (1024 * 1024)
    except Exception: mb = 0
    rel = os.path.relpath(path, ROOT)
    url = _download_url(path)
    btn = ""
    if url:
        # Orange, deliberately not the green used for the filename text, so the button
        # reads as a separate control. #c2570a gives 4.50:1 against white - the AA
        # minimum for 12px text; the brighter oranges (#ff9800 = 2.16:1) do not pass.
        btn = ("<a class='dlbtn' href='%s' download target='_blank' "
               "style='margin-left:12px;padding:3px 12px;border-radius:4px;"
               "background:#c2570a;color:#fff;font-weight:600;text-decoration:none;"
               "font-size:12px;white-space:nowrap;box-shadow:0 1px 2px rgba(0,0,0,.2)'>"
               "&#11015; Download</a>" % url)
    return ("<div style='margin:3px 0'>&#128196; <code>%s</code> "
            "<span style='color:#888'>(%.1f MB)</span>%s</div>" % (rel, mb, btn))


def _purge_staging():
    """Delete anything the interrupted script left in the engine staging dir.
    A killed xlsx write leaves a truncated workbook; collecting it would hand the
    program team a corrupt file that looks like a real deliverable."""
    removed = []
    if os.path.isdir(ENGINE_OUT):
        for dp, dn, fn in os.walk(ENGINE_OUT):
            for f in fn:
                p = os.path.join(dp, f)
                try:
                    sz = os.path.getsize(p); os.remove(p)
                    removed.append((os.path.relpath(p, ENGINE_OUT), sz))
                except Exception:
                    pass
        for dp, dn, fn in os.walk(ENGINE_OUT, topdown=False):
            if os.path.isdir(dp) and dp != ENGINE_OUT and not os.listdir(dp):
                try: os.rmdir(dp)
                except Exception: pass
    return removed


def _collect_outputs(sc, rng, made):
    found = []
    if os.path.isdir(ENGINE_OUT):
        for dp, dn, fn in os.walk(ENGINE_OUT):
            for f in fn:
                if f.endswith(".xlsx"):
                    src = os.path.join(dp, f)
                    dd = os.path.join(OUTDIR, sc, rng); os.makedirs(dd, exist_ok=True)
                    nm, ext = os.path.splitext(f)
                    dest = os.path.join(dd, f"{nm}_{rng}{ext}")
                    shutil.move(src, dest); made.append(dest); found.append(dest)
        for dp, dn, fn in os.walk(ENGINE_OUT, topdown=False):
            if os.path.isdir(dp) and not os.listdir(dp) and dp != ENGINE_OUT:
                try: os.rmdir(dp)
                except Exception: pass
    return found


def launch(debug=False):
    try:
        import ipywidgets as widgets
    except ModuleNotFoundError:
        from IPython.display import display, HTML
        display(HTML("<div style='padding:12px;border:2px solid #d9534f;border-radius:8px'>"
                     "<b>One-time setup:</b> <pre>pip install --user ipywidgets</pre>"
                     "then restart the server and hard-refresh (Ctrl+Shift+R).</div>"))
        return
    from IPython.display import display, HTML, clear_output

    scfg = json.load(open(STATES_CFG)); states = scfg["states"]; ctype = scfg.get("campaign_type", "")
    scodes = list(states)
    catalog = json.load(open(CFG))
    common = scfg.get("common_reports") or list(catalog.keys())
    try:
        dc = json.load(open(DATECFG))
        d0 = datetime.datetime.strptime(dc["start_date"], "%Y-%m-%d %H:%M:%S%z").date()
        d1 = datetime.datetime.strptime(dc["end_date"], "%Y-%m-%d %H:%M:%S%z").date()
    except Exception:
        d0 = d1 = datetime.date.today()

    g = {"on": False}
    def _gw(labels, ch=8, pad=52, lo=150, hi=380):
        w = (max([len(x) for x in labels], default=10)) * ch + pad
        return max(lo, min(hi, w))

    # ---------- STATE (single-select) ----------
    st_radio = widgets.RadioButtons(
        options=[(f"{states[c]['name']} ({states[c]['tenant_id']})", c) for c in scodes],
        value=(scodes[0] if scodes else None), description="State:",
        layout=widgets.Layout(width="auto"))

    # ---------- REPORTS (dynamic per state) ----------
    rp_all = widgets.Checkbox(value=False, description="Select All Reports", indent=False)
    rp_box = widgets.Box([], layout=widgets.Layout(display="flex", flex_flow="row wrap",
                          overflow="visible", margin="0 0 0 22px"))
    rp_state = {"chk": []}

    def _reports_for(c):
        if not c: return []
        lst = list(common) + [r for r in states[c].get("extra_reports", []) if r not in common]
        excl = set(states[c].get("exclude_reports", []))
        return [r for r in lst if r in catalog and r not in excl]

    def _rlabel(r):
        cyc = catalog.get(r, {}).get("cycles")
        return f"{r}  (cycle {'/'.join(cyc)})" if cyc else r

    def _rsync(ch):
        if g["on"]: return
        rp_all.value = all(k.value for _r, k in rp_state["chk"]) if rp_state["chk"] else False

    def _rebuild_reports(*_):
        names = _reports_for(st_radio.value)
        chk = [widgets.Checkbox(value=False, description=_rlabel(r), indent=False) for r in names]
        w = _gw([k.description for k in chk]) if chk else 200
        for k in chk:
            k.layout.width = f"{w}px"; k.observe(_rsync, names="value")
        rp_state["chk"] = list(zip(names, chk))
        rp_box.children = chk
        g["on"] = True; rp_all.value = False; g["on"] = False

    def _rp_all(ch):
        if g["on"]: return
        g["on"] = True
        for _r, k in rp_state["chk"]: k.value = ch["new"]
        g["on"] = False
    rp_all.observe(_rp_all, names="value")

    # ---------- PERIOD ----------
    period_mode = widgets.RadioButtons(options=[("By cycle", "cycle"), ("Custom date range", "date")],
                     value="cycle", description="Period:", layout=widgets.Layout(width="auto"))
    load_btn   = widgets.Button(description="Load cycles", icon="refresh", layout=widgets.Layout(width="150px"))
    cyc_all    = widgets.Checkbox(value=False, description="Select All Cycles", indent=False)
    cyc_box    = widgets.Box([], layout=widgets.Layout(display="flex", flex_flow="row wrap", overflow="visible", margin="0 0 0 22px"))
    cyc_status = widgets.HTML("")
    cyc_state  = {"boxes": [], "found": {}}
    s = widgets.DatePicker(description="From:", value=d0)
    e = widgets.DatePicker(description="To:", value=d1)

    def _load_cycles(_=None):
        c = st_radio.value
        if not c:
            cyc_status.value = "<span style='color:#c00'>Pick a state.</span>"; return
        import sys, requests as _rq, json as _j, urllib3 as _u; _u.disable_warnings()
        sys.path.insert(0, ENGINE)
        from COMMON_UTILS.common_utils import ELASTIC_ENCRYPTED_PASSWORD
        pfx = states[c]["tenant_id"]; camp = states[c].get("campaign_number", "")
        if not camp:
            cyc_status.value = "<span style='color:#b8860b'>no campaign_number for this state</span>"
            cyc_state["boxes"] = []; cyc_box.children = []; return

        # Which reports to discover cycles for: the ticked ones, else every report
        # available to this state. Each report may declare a "cycle_filter" in
        # reports_config.json - RI only exists where flow=riDone, ORS only where the
        # ORS-Zinc product was administered, and those cycles differ per state - so the
        # cycle list is derived per report from live data, never hard-coded.
        try:
            ticked = [r for r, k in rp_state["chk"] if k.value]
        except Exception:
            ticked = []
        reports = ticked or _reports_for(c)

        per_report = {}      # report -> {cycle: doc_count}
        spans = {}           # cycle -> [min_taskDate, max_taskDate]
        errs = []
        for r in reports:
            # Discover cycles in the index the report actually reads. Stock lives in
            # <pfx>-stock-index-v1, so a stock cycle_filter matches nothing in
            # project-task and the report would offer no cycles at all. Defaults to
            # project-task, so the other reports are unaffected.
            _idx = (catalog.get(r) or {}).get("cycle_index") or "project-task"
            url = f"https://elasticsearch-data.es-cluster-v8:9200/{pfx}-{_idx}-index-v1/_search"
            must = [{"term": {"Data.campaignNumber.keyword": camp}}]
            cf = (catalog.get(r) or {}).get("cycle_filter")
            if cf:
                must = must + (cf if isinstance(cf, list) else [cf])
            body = {"size": 0, "query": {"bool": {"must": must}},
                    "aggs": {"c": {"terms": {"field": "Data.additionalDetails.cycleIndex.keyword",
                                             "size": 50, "min_doc_count": CYCLE_MIN_DOCS,
                                             "order": {"_key": "asc"}},
                        "aggs": {"mn": {"min": {"field": "Data.taskDates"}},
                                 "mx": {"max": {"field": "Data.taskDates"}}}}}}
            try:
                resp = _rq.post(url, data=_j.dumps(body), verify=False, timeout=120,
                                headers={"Content-Type": "application/json",
                                         "Authorization": ELASTIC_ENCRYPTED_PASSWORD})
                buckets = resp.json().get("aggregations", {}).get("c", {}).get("buckets", [])
            except Exception as ex:
                errs.append(f"{r}: {ex}"); continue
            got = {}
            for b in buckets:
                k = str(b["key"])
                if not _CYC_RE.match(k):
                    continue          # drop malformed values ('1' where the rest say '01')
                got[k] = b["doc_count"]
                lo = (b.get("mn") or {}).get("value"); hi = (b.get("mx") or {}).get("value")
                cur = spans.setdefault(k, [None, None])
                if lo is not None: cur[0] = lo if cur[0] is None else min(cur[0], lo)
                if hi is not None: cur[1] = hi if cur[1] is None else max(cur[1], hi)
            per_report[r] = got

        cycles = sorted({k for g in per_report.values() for k in g})
        cyc_state["found"] = spans
        cyc_state["per_report"] = per_report
        if errs:
            cyc_status.value = f"<span style='color:#c00'>cycle load error: {errs[0]}</span>"
            if not cycles: return

        import datetime as _dtm
        def _d(ms):
            return (_dtm.datetime.fromtimestamp(ms / 1000.0, _dtm.timezone.utc).strftime("%Y-%m-%d")
                    if ms else "?")

        boxes = []
        for k in cycles:
            lo, hi = spans.get(k, [None, None])
            boxes.append(widgets.Checkbox(
                value=False, indent=False,
                description=f"Cycle {k}  ({_d(lo)} \u2192 {_d(hi)})"))
        w = _gw([b.description for b in boxes], hi=460) if boxes else 260
        for b in boxes: b.layout.width = f"{w}px"
        cyc_state["boxes"] = list(zip(cycles, boxes))

        def _ca(ch):
            g["on"] = True
            for _c2, b in cyc_state["boxes"]: b.value = ch["new"]
            g["on"] = False
        cyc_all.unobserve_all(); cyc_all.value = False; cyc_all.observe(_ca, names="value")
        cyc_box.children = boxes

        if cycles:
            missing = [r for r in reports if not per_report.get(r)]
            msg = (f"<span style='color:#127a12'>Loaded {len(cycles)} cycle(s) for "
                   f"{', '.join(reports)}: {', '.join(cycles)}</span>")
            if missing:
                msg += (f"<br><span style='color:#b8860b'>no data in any cycle for: "
                        f"{', '.join(missing)}</span>")
            cyc_status.value = msg
        else:
            cyc_status.value = ("<span style='color:#c00'>No cycles with data for "
                                f"{', '.join(reports)} (check campaign_number).</span>")
    load_btn.on_click(_load_cycles)

    def _on_state(_=None):
        _rebuild_reports()
        if period_mode.value == "cycle":
            _load_cycles()
    st_radio.observe(_on_state, names="value")

    dbg = widgets.Checkbox(value=debug, description="Debug (full logs)")
    btn = widgets.Button(description="Generate report", button_style="success", icon="play",
                         layout=widgets.Layout(width="200px", height="40px"))
    stop_btn = widgets.Button(description="Stop report generation", button_style="danger",
                              icon="stop", disabled=True,
                              layout=widgets.Layout(width="220px", height="40px"))
    # shared between the widget thread (Stop) and the worker thread (the run)
    RUN = {"proc": None, "stop": False, "busy": False}
    progress_area = widgets.VBox([]); out = widgets.Output()

    def _row(text):
        lbl = widgets.HTML(value=text)
        bar = widgets.IntProgress(value=0, min=0, max=100, description="", bar_style="info",
                                  layout=widgets.Layout(width="70%"))
        filesw = widgets.HTML(value="")
        progress_area.children = progress_area.children + (
            widgets.VBox([lbl, bar, filesw], layout=widgets.Layout(margin="0 0 14px 0")),)
        return lbl, bar, filesw

    cyc_group  = widgets.VBox([widgets.HBox([load_btn, cyc_status]), cyc_all, cyc_box])
    date_group = widgets.VBox([widgets.HBox([s, e])])
    def _apply_mode(*_):
        oncyc = (period_mode.value == "cycle")
        cyc_group.layout.display  = "" if oncyc else "none"
        date_group.layout.display = "none" if oncyc else ""
        if oncyc and st_radio.value and not cyc_state["boxes"]:
            _load_cycles()
    period_mode.observe(_apply_mode, names="value")

    def _go_body():
        try:
            progress_area.children = ()
            # NB: out.clear_output() is a NO-OP outside a `with out:` block - it goes
            # through the display publisher, not the widget. Reset the trait directly.
            out.outputs = ()
            def log(m):
                # append_stdout is safe from a non-main thread; `with out:` is not
                out.append_stdout(f"{m}\n")
            sc = st_radio.value
            sel_reports = [r for r, k in rp_state["chk"] if k.value]
            if not sc: log("Pick a state."); return
            if not sel_reports: log("Tick at least one report."); return
            _gents = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            def _dms(ms): return (datetime.datetime.fromtimestamp(ms / 1000.0, datetime.timezone.utc).strftime("%Y-%m-%d") if ms else "NA")
            selc = []
            if period_mode.value == "cycle":
                selc = [c for c, b in cyc_state["boxes"] if b.value]
                if not selc: log("Cycle mode: tick at least one cycle (or switch to Custom date range)."); return
                CYCLES_ENV = ",".join(selc)
                _fnd = cyc_state.get("found", {})
                _los = [_fnd[c][0] for c in selc if _fnd.get(c) and _fnd[c][0] is not None]
                _his = [_fnd[c][1] for c in selc if _fnd.get(c) and _fnd[c][1] is not None]
                rng = (f"{_dms(min(_los))}_TO_{_dms(max(_his))}__gen_{_gents}" if _los and _his else f"gen_{_gents}")
                json.dump({"start_date": "2000-01-01 00:00:00+0100", "end_date": "2099-12-31 23:59:59+0100"}, open(DATECFG, "w"), indent=2)
            else:
                if not (s.value and e.value): log("Please pick both dates."); return
                if e.value < s.value: log("'To' date is before 'From' date."); return
                sstr = s.value.strftime("%Y-%m-%d"); estr = e.value.strftime("%Y-%m-%d")
                rng = f"{sstr}_TO_{estr}__gen_{_gents}"; CYCLES_ENV = ""
                json.dump({"start_date": f"{sstr} 00:00:00+0100", "end_date": f"{estr} 23:59:59+0100"}, open(DATECFG, "w"), indent=2)
            try:
                json.dump({"state": sc, "reports": sel_reports, "mode": period_mode.value, "cycles": selc,
                           "from": s.value.strftime("%Y-%m-%d") if s.value else "",
                           "to": e.value.strftime("%Y-%m-%d") if e.value else ""}, open(STATE_FILE, "w"), indent=2)
            except Exception: pass
            info = states[sc]; tag = f"{info['name']} ({info['tenant_id']})"
            if not info.get("campaign_number"):
                log(f"{tag}: skipped - no campaign_number"); return
            env = dict(os.environ); env["STATE_CODE"] = sc; env["CYCLES"] = CYCLES_ENV
            ntot = len(sel_reports)
            progress_area.children = (widgets.HTML(
                f"<b>State:</b> {tag} &nbsp;|&nbsp; <b>{ntot}</b> report(s) "
                f"<span style='color:#888'>(run one after another)</span>"),)
            # one status row per selected report - all start as pending
            rows = {}
            for idx, r in enumerate(sel_reports, 1):
                lbl, bar, filesw = _row(
                    f"<b>{idx}/{ntot} &nbsp; {r}</b> &nbsp; <span style='color:#999'>&#9203; pending</span>")
                rows[r] = (lbl, bar, filesw)
            made = []
            for idx, r in enumerate(sel_reports, 1):
                lbl, bar, filesw = rows[r]; last = {"t": "", "t0": time.time()}
                def on_line(s2, i, _lbl=lbl, _bar=bar, _r=r, _idx=idx, _last=last):
                    spin = _SPIN[i % len(_SPIN)]; m = _STEP.search(s2)
                    noisy = any(k in s2 for k in _NOISE)
                    if m and not noisy:
                        x, y = float(m.group(1)), float(m.group(2))
                        # cap at 95: 100% must mean "finished", which is set after the
                        # process exits. The final stage writes the workbook in silence.
                        if y > 0: _bar.value = min(95, int(100 * x / y))
                        _last["t"] = s2.strip()
                    elif s2.strip() and not noisy and any(k in s2 for k in _MARKERS):
                        _last["t"] = s2.strip()
                    if m or (i % 8 == 0):
                        _el = int(time.time() - _last["t0"])
                        _lbl.value = (f"<b>{_idx}/{ntot} &nbsp; {_r}</b> &nbsp; "
                                      f"<span style='color:#0a58ca'>&#9654; {spin} "
                                      f"{_el // 60}m{_el % 60:02d}s &nbsp; "
                                      f"{(_last['t'] or 'working...')[:58]}</span>")
                lbl.value = f"<b>{idx}/{ntot} &nbsp; {r}</b> &nbsp; <span style='color:#0a58ca'>&#9654; running...</span>"
                for j, rr in enumerate(sel_reports[idx:], idx + 1):
                    rows[rr][0].value = (f"<b>{j}/{ntot} &nbsp; {rr}</b> &nbsp; "
                                         f"<span style='color:#999'>&#9203; waiting (after {r})</span>")
                if dbg.value: log(f"\n===== {tag} :: {r} =====")
                before = len(made)
                for script in catalog[r]["scripts"]:
                    spath = ENGINE + catalog[r]["input"] + script
                    prev = os.getcwd(); os.chdir(ENGINE)
                    try: _run_script(spath, env, dbg.value, log, on_line, RUN)
                    finally: os.chdir(prev)
                    if RUN["stop"]: break
                if RUN["stop"]:
                    gone = _purge_staging()
                    bar.bar_style = "warning"; bar.value = 0
                    lbl.value = (f"<b style='color:#b8860b'>&#9632; {idx}/{ntot} &nbsp; {r}"
                                 f" - STOPPED by user</b>")
                    filesw.value = ("<div style='margin:2px 0 0 20px;color:#b8860b'>"
                                    "Partial output discarded"
                                    + (f" ({len(gone)} file(s))" if gone else "")
                                    + " - a half-written workbook is not a deliverable.</div>")
                    for j, rr in enumerate(sel_reports[idx:], idx + 1):
                        rows[rr][0].value = (f"<b>{j}/{ntot} &nbsp; {rr}</b> &nbsp; "
                                             f"<span style='color:#999'>&#9866; skipped (stopped)</span>")
                    break
                _collect_outputs(sc, rng, made)
                files = made[before:]; bar.value = 100
                if files:
                    bar.bar_style = "success"
                    lbl.value = f"<b style='color:green'>&#10004; {idx}/{ntot} &nbsp; {r} - done</b> &nbsp; {len(files)} file(s)"
                    filesw.value = ("<div style='margin:2px 0 0 20px;color:#127a12'>"
                                    + "".join(_file_line(d) for d in files) + "</div>")
                else:
                    bar.bar_style = "danger"
                    lbl.value = f"<b style='color:#c00'>&#9888; {idx}/{ntot} &nbsp; {r} - no files</b>"
                    filesw.value = "<div style='margin:2px 0 0 20px;color:#c00'>No files - tick Debug to see why.</div>"
            if RUN["stop"]:
                log(f"STOPPED by user - {len(made)} file(s) completed before stopping.")
            else:
                log(f"All reports finished - {len(made)} file(s) total.")
        except Exception as ex:
            out.append_stdout(f"run failed: {type(ex).__name__}: {ex}\n")
        finally:
            RUN["busy"] = False; RUN["proc"] = None
            btn.disabled = False; stop_btn.disabled = True

    def go(_):
        if RUN["busy"]:
            out.append_stdout("A report run is already in progress.\n"); return
        RUN["stop"] = False; RUN["busy"] = True
        out.outputs = (); progress_area.children = ()   # drop any previous run's text now
        btn.disabled = True; stop_btn.disabled = False
        # MUST run off the widget thread: go() used to block the kernel for the whole
        # run, so a Stop click could not be delivered until the run had already ended.
        threading.Thread(target=_go_body, daemon=True).start()

    def stop(_):
        if not RUN["busy"]:
            return
        RUN["stop"] = True
        stop_btn.disabled = True
        out.append_stdout("Stop requested - terminating the running report...\n")
        _kill(RUN.get("proc"))

    btn.on_click(go)
    stop_btn.on_click(stop)

    # ---------- restore last selections ----------
    try: _saved = json.load(open(STATE_FILE))
    except Exception: _saved = {}
    _saved_cycles = _saved.get("cycles", []) if _saved else []
    if _saved:
        if _saved.get("state") in states: st_radio.value = _saved["state"]
        if _saved.get("mode") in ("cycle", "date"): period_mode.value = _saved["mode"]
        try:
            if _saved.get("from"): s.value = datetime.datetime.strptime(_saved["from"], "%Y-%m-%d").date()
            if _saved.get("to"):   e.value = datetime.datetime.strptime(_saved["to"], "%Y-%m-%d").date()
        except Exception: pass

    _rebuild_reports()
    if _saved.get("reports"):
        g["on"] = True
        for r, k in rp_state["chk"]:
            if r in _saved["reports"]: k.value = True
        g["on"] = False

    # ---------- display ----------
    display(HTML("<style>.widget-radio-box{flex-direction:row !important;flex-wrap:wrap;} "
                 ".widget-radio-box label{margin:0 20px 2px 0 !important;width:auto !important;} "
                 "a.dlbtn:hover{background:#9a3412 !important;} "
                 "a.dlbtn:active{background:#7c2d12 !important;}</style>"
                 f"<h2 style='margin:2px 0'>{ctype} Report Generator</h2>"
                 "<p style='margin:0 0 8px;color:#555'>Pick <b>one state</b>, tick its report(s), choose cycle(s) or a "
                 "date range, then <b>Generate report</b>.</p>"))
    display(HTML("<b>State</b>")); display(st_radio)
    display(HTML("<b>Reports</b> <span style='color:#888'>(for the selected state)</span>"))
    display(widgets.VBox([rp_all, rp_box]))
    display(HTML("<b>Reporting period</b> <span style='color:#888'>(pick one)</span>")); display(period_mode)
    display(cyc_group); display(date_group)
    display(dbg); display(widgets.HBox([btn, stop_btn]))
    display(HTML("<hr style='margin:8px 0'><b>Progress</b>")); display(progress_area); display(out)
    _apply_mode()
    if period_mode.value == "cycle" and st_radio.value:
        _load_cycles()
        for _cc, _b in cyc_state["boxes"]:
            if _cc in _saved_cycles: _b.value = True
