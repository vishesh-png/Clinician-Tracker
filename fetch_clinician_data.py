#!/usr/bin/env python3
"""Refresh data.js for the Clinician Tracker (Revenue/Earning tab).

Daily grain, doctor x clinic (city/locality) — the UI aggregates to
month / week / day windows client-side.

Datasets:
  blocks  : opened (bookable) minutes + shrinkage minutes per dt x doctor x clinic.
            Clinic = block's offline location via appointment_block_type_maps,
            else 'Online'. Shrinkage = overlap of the doctor's non-bookable
            blocks onto bookable ones (ops definition validated on the
            Doctor Scorecard — Dr. Adithya Jul'26).
  appts   : completed + no-show minutes and counts (SC/FU/RR/PQ) per clinic.
            No-show = not COMPLETED, not RESCHEDULED (a reschedule is never a
            no-show), and touched after start_time (MISSED + late cancels).
  revenue : total payments (consultation + treatment plan — everything paid)
            attributed to the doctor, clinic = where the doctor sat (block
            offline location first, then appointment location, else Online).
            Estimates path (invoices are dead), test + doctor-program excluded.
  earning : allo_payable.provider_payout + contract dump. The analyst role
            currently lacks the schema grant, so these queries are attempted
            and skipped gracefully — payload.earning_available says whether
            earning data made it in.

Auth: AWS profile `redshift-data` (SSO). If expired: aws sso login --profile redshift-data
Usage: python3 fetch_clinician_data.py
"""
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

PROFILE = "redshift-data"
CLUSTER = "warehouse"
DATABASE = "allo_prod"
HERE = Path(__file__).resolve().parent

START = "2026-01-01"  # IST window start

CONSULT_TYPES = "('Screening Call','Follow Up','Report Reading','Patient Queries')"

TEST_PAYMENT_IDS = ("'pay_Pi8fEI4UXiZH7B','pay_PjC9cYgU2QAUlO','pay_Pk6h5GNdahPpEh',"
                    "'pay_PmT7hIwUCifk4d','pay_PmgpgJHc1cKiRG'")


# Opened + shrinkage minutes per dt x doctor x clinic. A block carries several
# type-map rows (SC + repeat grids) pointing at the same location — MAX per
# block dedups. Blocks with no offline map are online supply.
BLOCKS_QUERY = f"""WITH blk_loc AS (
  SELECT abtm.appointment_block_id AS block_id,
         MAX(loc.city) AS city, MAX(loc.locality) AS locality
  FROM allo_consultations.appointment_block_type_maps abtm
  JOIN allo_health.locations loc ON abtm.offline_location_id = loc.id AND loc.deleted_at IS NULL
  WHERE abtm.deleted_at IS NULL
  GROUP BY 1),
blk AS (
  SELECT ab.id, ab.provider_id, ab.start_time, ab.end_time, ab.is_bookable,
         TRIM(pro.name) AS doctor,
         CAST(DATEADD(minute,330,ab.start_time) AS DATE) AS ist_date,
         COALESCE(bl.city,'Online') AS city, COALESCE(bl.locality,'Online') AS locality
  FROM allo_consultations.appointment_blocks ab
  JOIN allo_persons.providers pro ON ab.provider_id = pro.id AND pro.deleted_at IS NULL
  LEFT JOIN blk_loc bl ON bl.block_id = ab.id
  WHERE ab.deleted_at IS NULL
    AND DATEADD(minute,330,ab.start_time) >= DATE '{START}'
    -- rosters exist for future days; only count up to today (IST)
    AND CAST(DATEADD(minute,330,ab.start_time) AS DATE) <= CAST(DATEADD(minute,330,GETDATE()) AS DATE)),
ov AS (
  SELECT b.id, SUM(DATEDIFF(minute, GREATEST(b.start_time,n.start_time),
                                    LEAST(b.end_time,n.end_time))) AS mins
  FROM blk b
  JOIN blk n ON b.provider_id = n.provider_id AND b.is_bookable = 1 AND n.is_bookable = 0
   AND n.start_time < b.end_time AND n.end_time > b.start_time
  GROUP BY b.id)
SELECT TO_CHAR(b.ist_date,'YYYY-MM-DD') AS dt, b.doctor, b.city, b.locality,
       SUM(DATEDIFF(minute, b.start_time, b.end_time)) AS opened_min,
       COALESCE(SUM(ov.mins),0) AS shrink_min
FROM blk b LEFT JOIN ov ON b.id = ov.id
WHERE b.is_bookable = 1
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4"""


APPTS_QUERY = f"""SELECT
    TO_CHAR(DATEADD(minute,330,app.start_time),'YYYY-MM-DD') AS dt,
    TRIM(pro.name) AS doctor,
    COALESCE(CASE WHEN loc.type='offline' THEN loc.city END,'Online') AS city,
    COALESCE(CASE WHEN loc.type='offline' THEN loc.locality END,'Online') AS locality,
    SUM(CASE WHEN app.status='COMPLETED' THEN DATEDIFF(minute,app.start_time,app.end_time) ELSE 0 END) AS done_min,
    SUM(CASE WHEN app.status='COMPLETED' THEN 1 ELSE 0 END) AS done_appts,
    SUM(CASE WHEN app.status NOT IN ('COMPLETED','RESCHEDULED') AND app.updated_at > app.start_time
             THEN DATEDIFF(minute,app.start_time,app.end_time) ELSE 0 END) AS noshow_min,
    SUM(CASE WHEN app.status NOT IN ('COMPLETED','RESCHEDULED') AND app.updated_at > app.start_time
             THEN 1 ELSE 0 END) AS noshow_appts
FROM allo_consultations.appointments app
JOIN allo_persons.providers pro ON app.provider_id=pro.id AND pro.deleted_at IS NULL
JOIN allo_consultations.types typ ON app.type_id=typ.id AND typ.deleted_at IS NULL
LEFT JOIN allo_health.locations loc ON app.location_id=loc.id
WHERE app.deleted_at IS NULL
  AND typ.name IN {CONSULT_TYPES}
  AND DATEADD(minute,330,app.start_time) >= DATE '{START}'
  AND app.start_time <= GETDATE()
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4"""


# Total revenue (TP + consultation = every payment attributed to the doctor),
# clinic = doctor's seat: block offline location (encounter appt, then
# consultation appt), then offline appointment location, else Online.
REV_QUERY = f"""WITH doctor_location AS (
    SELECT ab.id AS block_id, MAX(loc.city) AS doc_city, MAX(loc.locality) AS doc_locality
    FROM allo_consultations.appointment_block_type_maps abtm
    JOIN allo_consultations.appointment_blocks ab ON abtm.appointment_block_id = ab.id
    JOIN allo_health.locations loc ON abtm.offline_location_id = loc.id AND loc.deleted_at IS NULL
    WHERE abtm.deleted_at IS NULL AND ab.deleted_at IS NULL
    GROUP BY ab.id
),
cons_appt AS (
    SELECT consultation_id, block_id, ap_pro, ap_loc, ap_type
    FROM (
        SELECT ap1.consultation_id, ap1.block_id, ap1.provider_id ap_pro,
               ap1.location_id ap_loc, t.name ap_type,
               ROW_NUMBER() OVER (PARTITION BY ap1.consultation_id ORDER BY ap1.created_at DESC) rn
        FROM allo_consultations.appointments ap1
            LEFT JOIN allo_consultations.types t ON ap1.type_id = t.id
        WHERE ap1.deleted_at IS NULL AND ap1.consultation_id IS NOT NULL
            AND ap1.created_at + INTERVAL '5.5 hours' >= DATE '{START}' - INTERVAL '2 month'
    ) WHERE rn = 1
),
cii AS (
    SELECT estimate_id, item_id
    FROM (
        SELECT estimate_id, id AS item_id,
               ROW_NUMBER() OVER (PARTITION BY estimate_id ORDER BY payable_amount * GREATEST(quantity,1) DESC) rn
        FROM allo_billing.estimate_items
        WHERE item_type = 'consultation' AND payable_amount > 0
          AND type_id <> 'fe5b19b4-5961-4036-bc5f-fb1009a27d64'
    ) WHERE rn = 1
),
cons_link AS (
    SELECT item_id, cons_id
    FROM (
        SELECT estimate_item_id AS item_id, id AS cons_id,
               ROW_NUMBER() OVER (PARTITION BY estimate_item_id ORDER BY created_at) rn
        FROM allo_consultations.consultations
        WHERE deleted_at IS NULL AND estimate_item_id IS NOT NULL
    ) WHERE rn = 1
)
SELECT
    TO_CHAR(p.created_at + INTERVAL '5.5 hours','YYYY-MM-DD') AS dt,
    TRIM(COALESCE(pro_enc.name, pro_cons.name)) AS doctor,
    COALESCE(dle.doc_city, dlc.doc_city,
             CASE WHEN al.type='offline' THEN al.city END,
             CASE WHEN cal.type='offline' THEN cal.city END, 'Online') AS city,
    COALESCE(dle.doc_locality, dlc.doc_locality,
             CASE WHEN al.type='offline' THEN al.locality END,
             CASE WHEN cal.type='offline' THEN cal.locality END, 'Online') AS locality,
    ROUND(SUM(p.amount) / 100) AS revenue
FROM allo_health.payments p
    LEFT JOIN allo_billing.estimate_payments epay ON epay.payment_id = p.id
    LEFT JOIN allo_billing.estimates i ON i.id = epay.estimate_id AND i.deleted_at IS NULL
    LEFT JOIN allo_encounters.encounters e ON i.encounter_id = e.id AND e.deleted_at IS NULL
    LEFT JOIN allo_consultations.appointments app ON e.appointment_id = app.id AND app.deleted_at IS NULL
    LEFT JOIN allo_health.locations al ON al.id = app.location_id
    LEFT JOIN cii ON cii.estimate_id = epay.estimate_id
    LEFT JOIN cons_link cl ON cl.item_id = cii.item_id
    LEFT JOIN cons_appt ca ON ca.consultation_id = cl.cons_id
    LEFT JOIN allo_health.locations cal ON cal.id = ca.ap_loc
    LEFT JOIN allo_persons.providers pro_enc ON e.provider_id = pro_enc.id
    LEFT JOIN allo_persons.providers pro_cons ON ca.ap_pro = pro_cons.id
    LEFT JOIN doctor_location dle ON app.block_id = dle.block_id
    LEFT JOIN doctor_location dlc ON ca.block_id = dlc.block_id
WHERE p.deleted_at IS NULL
    AND DATE(p.created_at + INTERVAL '5.5 hours') BETWEEN DATE '{START}' AND CURRENT_DATE
    AND COALESCE(pro_enc.name, pro_cons.name) IS NOT NULL
    AND COALESCE(p.razorpay_payment_id, '') NOT IN ({TEST_PAYMENT_IDS})
    AND COALESCE(p.razorpay_payment_id, '') NOT IN (
        SELECT DISTINCT id FROM allo_vendors.razorpay_payments
        WHERE notes LIKE '%name%' AND notes LIKE '%email%' AND notes LIKE '%phone%')
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4"""


# ---- allo_payable (earning) — attempted; skipped if the grant is missing ----

# The payout ledger rows are the payout BASES (consultation rows = consult
# revenue, prescription rows = TP revenue), in PAISE; net rupees =
# (credit − debit)/100. program + sc/repeat linkage are kept because the newest
# contracts (0% slab) pay fixed %s that differ by program (SH/STI vs MH call
# fee) and by SC-vs-repeat linkage (rx fee).
EARN_QUERY = f"""SELECT
    TO_CHAR(DATEADD(minute,330,pp.transaction_date),'YYYY-MM-DD') AS dt,
    TRIM(pro.name) AS doctor,
    COALESCE(CASE WHEN loc.type='offline' THEN loc.city END,'Online') AS city,
    COALESCE(CASE WHEN loc.type='offline' THEN loc.locality END,'Online') AS locality,
    pp.payout_type,
    COALESCE(pp.program,'sexual_health') AS program,
    CASE WHEN pp.appointment_type_id = 'cd02525c-1528-4047-a12c-1ad526c28c9a'
         THEN 'sc' ELSE 'rpt' END AS link,
    ROUND(SUM(CASE WHEN pp.transaction_type='credit' THEN pp.transaction_amount
                   ELSE -pp.transaction_amount END) / 100.0) AS amount
FROM allo_payable.provider_payout pp
JOIN allo_persons.providers pro ON pp.provider_id = pro.id AND pro.deleted_at IS NULL
LEFT JOIN allo_health.locations loc ON pp.location_id = loc.id
WHERE pp.deleted_at IS NULL
  AND DATEADD(minute,330,pp.transaction_date) >= DATE '{START}'
GROUP BY 1,2,3,4,5,6,7
ORDER BY 1,2,3,4"""

# Fee-split clauses for the 0%-slab contracts: fixed_call_fee % of consult
# revenue scoped by program, fixed_rx_fee % of Rx value scoped by SC-vs-repeat.
# PERCENT amounts are percent x 100 (4000 = 40%).
FEES_QUERY = """SELECT TRIM(pro.name) AS doctor,
       TO_CHAR(cc.valid_from,'YYYY-MM-DD') AS vf, TO_CHAR(cc.valid_till,'YYYY-MM-DD') AS vt,
       cc.type, cc.amount, cc.commission_unit,
       json_serialize(cc.consultation_types) AS ctypes,
       json_serialize(cc.programs) AS progs
FROM allo_payable.consultation_clause cc
JOIN allo_payable.payout_contracts pc ON cc.contract_id = pc.id
     AND pc.deleted_at IS NULL AND pc.status = 'approved'
JOIN allo_persons.providers pro ON pc.provider_id = pro.id
WHERE cc.deleted_at IS NULL AND cc.type IN ('fixed_call_fee','fixed_rx_fee')
ORDER BY 1, 2"""

# Slab grids per doctor, flattened to [doctor, valid_from, valid_till, start_rs,
# end_rs (null = top), pct]. Payout model (per Vishesh's finance sheet, validated
# on Dr. Hari Viswesh Aug'26 to the rupee): the ledger's consultation rows are
# consultation REVENUE and its prescription rows are TP REVENUE — payout before
# MG = PROGRESSIVE slab over the month's total (cons + TP) revenue; if that
# beats the minimum guarantee take it (+ additional MG), else take the MG;
# then add mock-call payout (non_clinical rate x quantity).
SLABS_QUERY = """SELECT TRIM(pro.name) AS doctor,
       TO_CHAR(pc.valid_from,'YYYY-MM-DD') AS vf, TO_CHAR(pc.valid_till,'YYYY-MM-DD') AS vt,
       sc.range_start/100.0 AS start_rs, sc.range_end/100.0 AS end_rs,
       CAST(sc.commission AS FLOAT) AS pct
FROM allo_payable.slab_clause sc
JOIN allo_payable.payout_contracts pc ON sc.contract_id = pc.id
     AND pc.deleted_at IS NULL AND pc.status = 'approved'
JOIN allo_persons.providers pro ON pc.provider_id = pro.id
WHERE sc.deleted_at IS NULL
ORDER BY 1, 2, 4"""

# Old fixed_percentage regime: one flat revenue-share % embedded in the
# subcontracts JSON -> same shape as a single 0..inf slab.
FIXED_PCT_QUERY = """SELECT TRIM(pro.name) AS doctor,
       TO_CHAR(pc.valid_from,'YYYY-MM-DD') AS vf, TO_CHAR(pc.valid_till,'YYYY-MM-DD') AS vt,
       0.0 AS start_rs, NULL AS end_rs,
       CAST(pc.subcontracts[0]."revenueShareClause"."general" AS FLOAT) AS pct
FROM allo_payable.payout_contracts pc
JOIN allo_persons.providers pro ON pc.provider_id = pro.id
WHERE pc.deleted_at IS NULL AND pc.status = 'approved' AND pc.regime = 'fixed_percentage'
ORDER BY 1, 2"""

# Monthly minimum-guarantee windows per doctor (amount is rupees for the window's
# month; windows can subdivide a month when the deal changed mid-month).
# MG is hours-adjusted at month level (per Vishesh, validated on Dr. Haritha
# Kumar Aug'26): per-hour charge = amount / expected_hours; net available >= min
# hours -> full MG; cushion <= net < min -> MG - (min - net) x rate; net <
# cushion -> net x rate.
MG_QUERY = """SELECT TRIM(pro.name) AS doctor,
       TO_CHAR(mg.valid_from,'YYYY-MM-DD') AS vf, TO_CHAR(mg.valid_till,'YYYY-MM-DD') AS vt,
       mg.amount/100.0 AS amount_rs,
       mg.minimum_working_hours, mg.expected_working_hours, mg.cushion_cutoff_hours
FROM allo_payable.min_guarantee_clause mg
JOIN allo_payable.payout_contracts pc ON mg.contract_id = pc.id
     AND pc.deleted_at IS NULL AND pc.status = 'approved'
JOIN allo_persons.providers pro ON pc.provider_id = pro.id
WHERE mg.deleted_at IS NULL AND mg.amount > 0
ORDER BY 1, 2"""

# Old-regime fixed MG embedded in subcontracts JSON.
MG_FIXED_QUERY = """SELECT TRIM(pro.name) AS doctor,
       TO_CHAR(pc.valid_from,'YYYY-MM-DD') AS vf, TO_CHAR(pc.valid_till,'YYYY-MM-DD') AS vt,
       CAST(pc.subcontracts[0]."minimumGurantee" AS FLOAT)/100.0 AS amount_rs,
       CAST(pc.subcontracts[0]."minimumWorkingHours" AS FLOAT) AS min_h,
       CAST(pc.subcontracts[0]."expectedWorkingHours" AS FLOAT) AS exp_h,
       CAST(pc.subcontracts[0]."cushionCutoffHours" AS FLOAT) AS cush_h
FROM allo_payable.payout_contracts pc
JOIN allo_persons.providers pro ON pc.provider_id = pro.id
WHERE pc.deleted_at IS NULL AND pc.status = 'approved' AND pc.regime = 'fixed_percentage'
  AND CAST(pc.subcontracts[0]."minimumGurantee" AS FLOAT) > 0
ORDER BY 1, 2"""

# Mock-call payout: the non_clinical ledger rows carry only a quantity (amount 0);
# the money is the doctor's mock_call clause rate (paise PER_ITEM) x quantity.
MOCK_QUERY = f"""WITH rated AS (
  SELECT pp.id, pp.transaction_date, pp.provider_id, pp.location_id, pp.transaction_type,
         COALESCE(pp.non_clinical_quantity, 1) AS qty, nc.rate,
         ROW_NUMBER() OVER (PARTITION BY pp.id ORDER BY nc.valid_from DESC) rn
  FROM allo_payable.provider_payout pp
  JOIN allo_payable.non_clinical_clause_type nct
       ON pp.non_clinical_type_id = nct.id AND nct.code = 'mock_call'
  JOIN allo_payable.payout_contracts pc ON pc.provider_id = pp.provider_id
       AND pc.deleted_at IS NULL AND pc.status = 'approved'
       AND pp.transaction_date >= pc.valid_from AND pp.transaction_date < pc.valid_till
  JOIN allo_payable.non_clinical_clause nc ON nc.contract_id = pc.id
       AND nc.clause_type_id = nct.id AND nc.deleted_at IS NULL
  WHERE pp.deleted_at IS NULL AND pp.payout_type = 'non_clinical'
    AND DATEADD(minute,330,pp.transaction_date) >= DATE '{START}')
SELECT TO_CHAR(DATEADD(minute,330,r.transaction_date),'YYYY-MM-DD') AS dt,
       TRIM(pro.name) AS doctor,
       COALESCE(CASE WHEN loc.type='offline' THEN loc.city END,'Online') AS city,
       COALESCE(CASE WHEN loc.type='offline' THEN loc.locality END,'Online') AS locality,
       ROUND(SUM((CASE WHEN r.transaction_type='credit' THEN 1 ELSE -1 END) * r.qty * r.rate)/100.0) AS mock_rs
FROM rated r
JOIN allo_persons.providers pro ON r.provider_id = pro.id
LEFT JOIN allo_health.locations loc ON r.location_id = loc.id
WHERE r.rn = 1
GROUP BY 1,2,3,4
ORDER BY 1,2"""

CONTRACTS_QUERY = """SELECT pc.id, TRIM(pro.name) AS doctor, pc.regime, pc.status,
       TO_CHAR(pc.valid_from,'YYYY-MM-DD') AS valid_from,
       TO_CHAR(pc.valid_till,'YYYY-MM-DD') AS valid_till,
       json_serialize(pc.subcontracts) AS subcontracts
FROM allo_payable.payout_contracts pc
JOIN allo_persons.providers pro ON pc.provider_id = pro.id
WHERE pc.deleted_at IS NULL
ORDER BY 2,5"""

CONSULT_CLAUSE_QUERY = """SELECT cc.contract_id, cc.type, cc.amount, cc.commission_unit,
       TO_CHAR(cc.valid_from,'YYYY-MM-DD') AS valid_from,
       TO_CHAR(cc.valid_till,'YYYY-MM-DD') AS valid_till,
       json_serialize(cc.consultation_types) AS consultation_types,
       json_serialize(cc.programs) AS programs,
       json_serialize(cc.location_ids) AS location_ids
FROM allo_payable.consultation_clause cc
WHERE cc.deleted_at IS NULL"""

SLAB_CLAUSE_QUERY = """SELECT contract_id, range_start, range_end, commission, commission_unit,
       TO_CHAR(created_at,'YYYY-MM-DD') AS created_at
FROM allo_payable.slab_clause WHERE deleted_at IS NULL ORDER BY contract_id, range_start"""

MIN_GUAR_QUERY = """SELECT contract_id, amount, minimum_working_hours, expected_working_hours,
       cushion_cutoff_hours,
       TO_CHAR(valid_from,'YYYY-MM-DD') AS valid_from,
       TO_CHAR(valid_till,'YYYY-MM-DD') AS valid_till
FROM allo_payable.min_guarantee_clause WHERE deleted_at IS NULL"""

NON_CLIN_QUERY = """SELECT nc.contract_id, nct.name AS clause_name, nct.code, nc.rate, nc.rate_unit,
       TO_CHAR(nc.valid_from,'YYYY-MM-DD') AS valid_from,
       TO_CHAR(nc.valid_till,'YYYY-MM-DD') AS valid_till
FROM allo_payable.non_clinical_clause nc
LEFT JOIN allo_payable.non_clinical_clause_type nct ON nc.clause_type_id = nct.id
WHERE nc.deleted_at IS NULL"""


def aws(*args):
    cmd = ["aws", "--profile", PROFILE, "--output", "json", *args]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        sys.stderr.write(f"ERROR: {' '.join(cmd[:4])}...: {res.stderr}\n")
        low = res.stderr.lower()
        if "sso" in low or "credential" in low or "token" in low:
            sys.stderr.write("Hint: run `aws sso login --profile redshift-data` and retry.\n")
        sys.exit(1)
    return json.loads(res.stdout) if res.stdout.strip() else {}


def run_query(label, sql, soft=False):
    """Run a query; on soft=True a FAILED query returns None instead of exiting."""
    sys.stderr.write(f"[{label}] executing...\n")
    stmt = aws("redshift-data", "execute-statement",
               "--cluster-identifier", CLUSTER, "--database", DATABASE, "--sql", sql)
    sid = stmt["Id"]
    for _ in range(1800):  # up to 60 min
        time.sleep(2)
        desc = aws("redshift-data", "describe-statement", "--id", sid)
        st = desc["Status"]
        if st == "FINISHED":
            break
        if st in ("FAILED", "ABORTED"):
            msg = f"[{label}] query {st}: {desc.get('Error')}\n"
            if soft:
                sys.stderr.write("SKIP: " + msg)
                return None
            sys.stderr.write("ERROR: " + msg)
            sys.exit(1)
    else:
        sys.stderr.write(f"ERROR: [{label}] query timed out\n")
        sys.exit(1)

    rows, token = [], None
    while True:
        args = ["redshift-data", "get-statement-result", "--id", sid]
        if token:
            args += ["--next-token", token]
        result = aws(*args)
        for rec in result["Records"]:
            row = []
            for cell in rec:
                if cell.get("isNull"):
                    row.append(None)
                elif "stringValue" in cell:
                    row.append(cell["stringValue"])
                elif "longValue" in cell:
                    row.append(cell["longValue"])
                elif "doubleValue" in cell:
                    row.append(cell["doubleValue"])
                else:
                    row.append(list(cell.values())[0])
            rows.append(row)
        token = result.get("NextToken")
        if not token:
            break
    sys.stderr.write(f"[{label}] {len(rows)} rows\n")
    return rows


def is_doctor(name):
    # doctors only — therapists/counsellors are "Mr."/"Ms." providers
    return (name or "").strip().startswith("Dr")


def fetch_slabs():
    """Write data_slabs.js — contract data for the earning engine: slab grids,
    minimum-guarantee windows, and mock-call payouts. Regenerate alone with:
    python3 -c 'import fetch_clinician_data as f; f.fetch_slabs()'"""
    slab_rows = []
    for label, q in (("slabs", SLABS_QUERY), ("fixed-pct", FIXED_PCT_QUERY)):
        got = run_query(label, q, soft=True)
        if got:
            slab_rows += [r[:3] + [float(r[3] or 0), None if r[4] is None else float(r[4]),
                                   float(r[5] or 0)] for r in got if is_doctor(r[0])]
    mg_rows = []
    for label, q in (("mg", MG_QUERY), ("mg-fixed", MG_FIXED_QUERY)):
        got = run_query(label, q, soft=True)
        if got:
            mg_rows += [r[:3] + [float(x or 0) for x in r[3:7]] for r in got if is_doctor(r[0])]
    mock = run_query("mock-calls", MOCK_QUERY, soft=True) or []
    mock_rows = [r[:4] + [int(float(r[4] or 0))] for r in mock if is_doctor(r[1])]
    fees = run_query("fee-clauses", FEES_QUERY, soft=True) or []
    fee_rows = [r[:4] + [float(r[4] or 0), r[5], r[6], r[7]] for r in fees if is_doctor(r[0])]
    out = HERE / "data_slabs.js"
    payload = {
        "slab_columns": ["doctor", "valid_from", "valid_till", "start_rs", "end_rs", "pct"],
        "slab_rows": slab_rows,
        "fee_columns": ["doctor", "valid_from", "valid_till", "type", "amount", "unit",
                        "ctypes", "progs"],
        "fee_rows": fee_rows,
        "mg_columns": ["doctor", "valid_from", "valid_till", "amount_rs",
                       "min_hours", "expected_hours", "cushion_hours"],
        "mg_rows": mg_rows,
        "mock_columns": ["dt", "doctor", "city", "locality", "mock_rs"],
        "mock_rows": mock_rows,
    }
    out.write_text("window.CLINICIAN_SLABS = " + json.dumps(payload, separators=(",", ":")) + ";\n")
    sys.stderr.write(f"[slabs] slabs={len(slab_rows)} mg={len(mg_rows)} mock={len(mock_rows)} -> {out}\n")


def main():
    fetch_slabs()
    blocks = [r for r in run_query("blocks", BLOCKS_QUERY) if is_doctor(r[1])]
    appts = [r for r in run_query("appts", APPTS_QUERY) if is_doctor(r[1])]
    rev = [r for r in run_query("revenue", REV_QUERY) if is_doctor(r[1])]

    earn = run_query("earning", EARN_QUERY, soft=True)
    earning_available = earn is not None
    contracts = None
    if earning_available:
        # ROUND() returns numeric, which the Data API serializes as a string
        earn = [r[:7] + [int(float(r[7] or 0))] for r in earn if is_doctor(r[1])]
        contracts = {
            "contracts": run_query("contracts", CONTRACTS_QUERY, soft=True),
            "consultation_clauses": run_query("consult-clauses", CONSULT_CLAUSE_QUERY, soft=True),
            "slab_clauses": run_query("slab-clauses", SLAB_CLAUSE_QUERY, soft=True),
            "min_guarantee_clauses": run_query("min-guarantee", MIN_GUAR_QUERY, soft=True),
            "non_clinical_clauses": run_query("non-clinical", NON_CLIN_QUERY, soft=True),
        }
        (HERE / "contracts.json").write_text(json.dumps(contracts, indent=1))
        sys.stderr.write("[contracts] dumped to contracts.json — inspect before wiring earning math\n")

    payload = {
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M IST"),
        "start": START,
        "earning_available": earning_available,
        "blocks_columns": ["dt", "doctor", "city", "locality", "opened_min", "shrink_min"],
        "blocks_rows": blocks,
        "appts_columns": ["dt", "doctor", "city", "locality", "done_min", "done_appts",
                          "noshow_min", "noshow_appts"],
        "appts_rows": appts,
        "rev_columns": ["dt", "doctor", "city", "locality", "revenue"],
        "rev_rows": rev,
        # earn amounts are net rupees (credits − debits, paise/100)
        "earn_columns": ["dt", "doctor", "city", "locality", "payout_type",
                         "program", "link", "amount"],
        "earn_rows": earn or [],
    }
    out = HERE / "data.js"
    out.write_text("window.CLINICIAN_DATA = " + json.dumps(payload, separators=(",", ":")) + ";\n")
    sys.stderr.write(f"[done] blocks={len(blocks)} appts={len(appts)} rev={len(rev)} "
                     f"earn={'n/a' if not earning_available else len(earn)} -> {out}\n")
    print(str(out))


if __name__ == "__main__":
    main()
