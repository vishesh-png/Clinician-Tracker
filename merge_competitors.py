#!/usr/bin/env python3
"""Merge the research agents' comp_*.json files into data_competitors.js.

Each input file is {"City | Locality": [ {name, specialty, qualifications,
experience_years, rating, reviews, fee, clinic, area, source}, ... ]}.
Usage: python3 merge_competitors.py <dir-with-comp_*.json>
"""
import json
import sys
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
src = Path(sys.argv[1]) if len(sys.argv) > 1 else HERE

areas = {}
for f in sorted(src.glob("comp_*.json")):
    try:
        data = json.loads(f.read_text())
    except Exception as e:
        sys.stderr.write(f"SKIP {f.name}: {e}\n")
        continue
    n = 0
    for k, v in data.items():
        if not isinstance(v, list):
            continue
        comps = [c for c in v if isinstance(c, dict) and c.get("name")]
        if comps:
            areas.setdefault(k, [])
            seen = {c["name"] for c in areas[k]}
            areas[k] += [c for c in comps if c["name"] not in seen][:4 - min(4, len(areas[k]))]
            n += len(comps)
    sys.stderr.write(f"[{f.name}] {n} competitors\n")

payload = {"updated": datetime.now().strftime("%Y-%m-%d"), "areas": areas}
out = HERE / "data_competitors.js"
out.write_text("window.CLINICIAN_COMPETITORS = " + json.dumps(payload, separators=(",", ":")) + ";\n")
sys.stderr.write(f"[done] {len(areas)} areas -> {out}\n")
