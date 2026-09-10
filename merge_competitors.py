#!/usr/bin/env python3
"""Merge research JSONs into data_competitors.js.

v1 files (comp_*.json): {"City | Locality": [competitor, ...]}
v2 files (comp2_*.json): {"City | Locality": {"allo": {...}, "competitors": [...]}}
v2 overrides v1 for the same area (it carries GMB + Practo ratings/fees for both
Allo's clinic and each competitor). Usage: python3 merge_competitors.py <dir>
"""
import json
import sys
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
src = Path(sys.argv[1]) if len(sys.argv) > 1 else HERE

areas = {}  # key -> {"allo": {...}|None, "competitors": [...]}


def add(fname, data, v2):
    n = 0
    for k, v in data.items():
        if isinstance(v, list):
            allo, comps = None, v
        elif isinstance(v, dict):
            allo, comps = v.get("allo"), v.get("competitors") or []
        else:
            continue
        comps = [c for c in comps if isinstance(c, dict) and c.get("name")][:4]
        if not comps and not allo:
            continue
        cur = areas.setdefault(k, {"allo": None, "competitors": []})
        if v2:
            cur["competitors"] = comps or cur["competitors"]
            cur["allo"] = allo or cur["allo"]
        elif not cur["competitors"]:
            cur["competitors"] = comps
        n += len(comps)
    sys.stderr.write(f"[{fname}] {n} competitors{' (v2)' if v2 else ''}\n")


for f in sorted(src.glob("comp_*.json")):
    try:
        add(f.name, json.loads(f.read_text()), False)
    except Exception as e:
        sys.stderr.write(f"SKIP {f.name}: {e}\n")
for f in sorted(src.glob("comp2_*.json")):
    try:
        add(f.name, json.loads(f.read_text()), True)
    except Exception as e:
        sys.stderr.write(f"SKIP {f.name}: {e}\n")

n_allo = sum(1 for a in areas.values() if a["allo"])
payload = {"updated": datetime.now().strftime("%Y-%m-%d"), "areas": areas}
out = HERE / "data_competitors.js"
out.write_text("window.CLINICIAN_COMPETITORS = " + json.dumps(payload, separators=(",", ":")) + ";\n")
sys.stderr.write(f"[done] {len(areas)} areas ({n_allo} with Allo ratings) -> {out}\n")
