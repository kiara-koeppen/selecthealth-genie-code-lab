"""Reference implementation of the SelectHealth encounter DQ rules (see ../dq_rules.md).

Genie Code reads this file when the selecthealth-encounter-dq skill loads and reuses the logic.
It can also be called directly from a notebook:

    exec(open("/Workspace/Users/<you>/.assistant/skills/selecthealth-encounter-dq/scripts/run_dq_checks.py").read())
    run_dq_checks(spark, "`demo`.`selecthealth_workshop`.`kk_silver_encounters`", "`demo`.`selecthealth_workshop`.`kk_dq_results`")
"""
from pyspark.sql import functions as F

RAW_MAPPING = {
    "encounter_id": "enc_id", "member_id": "mbr_id", "admit_date": "adm_dt", "discharge_date": "dsch_dt",
    "payer": "pyr_cd", "encounter_type": "enc_typ", "total_paid": "pd_amt", "total_charges": "chg_amt",
    "readmitted_30d": "rdmt_flg", "mortality_flag": "mort_ind", "length_of_stay_days": "los_d",
}
CLEAN_MAPPING = {
    "encounter_id": "encounter_id", "member_id": "patient_id", "admit_date": "admit_date",
    "discharge_date": "discharge_date", "payer": "payer_type", "encounter_type": "encounter_type",
    "total_paid": "total_paid", "total_charges": "total_charges", "readmitted_30d": "readmitted_30d",
    "mortality_flag": "mortality_flag", "length_of_stay_days": "length_of_stay_days",
}
VALID_PAYERS = ["Commercial", "Medicare", "Medicaid", "Self-Pay", "1", "2", "3", "4"]
VALID_ENC_TYPES = ["Inpatient", "Outpatient", "Emergency", "IP", "OP", "ED"]


def _as_date(col):
    # Raw extract stores yyyyMMdd strings; clean tables store DATE. Handle both.
    # try_* variants return NULL instead of failing under ANSI mode (the serverless default).
    return F.expr(f"coalesce(try_to_date(cast(`{col}` as string), 'yyyyMMdd'), try_cast(`{col}` as date))")


def run_dq_checks(spark, table_fq, results_fq, mapping=None):
    df = spark.table(table_fq)
    cols = set(df.columns)
    if mapping is None:
        mapping = RAW_MAPPING if "enc_id" in cols else CLEAN_MAPPING
        if mapping is CLEAN_MAPPING and "patient_id" not in cols and "member_id" in cols:
            mapping = {**CLEAN_MAPPING, "member_id": "member_id"}
    m = {k: v for k, v in mapping.items() if v in cols}
    total = df.count()

    readmit = F.when(F.col(m["readmitted_30d"]).cast("string").isin("1", "Y"), 1).otherwise(0) if "readmitted_30d" in m else None

    rules = [
        ("unique_encounter_id", "BLOCKER", ["encounter_id"],
         lambda: total - df.select(m["encounter_id"]).distinct().count()),
        ("member_id_present", "BLOCKER", ["member_id"],
         lambda: df.filter(F.col(m["member_id"]).isNull() | (F.trim(F.col(m["member_id"])) == "")).count()),
        ("discharge_on_or_after_admit", "BLOCKER", ["admit_date", "discharge_date"],
         lambda: df.filter(_as_date(m["discharge_date"]) < _as_date(m["admit_date"])).count()),
        ("valid_payer", "BLOCKER", ["payer"],
         lambda: df.filter(~F.col(m["payer"]).cast("string").isin(VALID_PAYERS) | F.col(m["payer"]).isNull()).count()),
        ("valid_encounter_type", "BLOCKER", ["encounter_type"],
         lambda: df.filter(~F.col(m["encounter_type"]).isin(VALID_ENC_TYPES) | F.col(m["encounter_type"]).isNull()).count()),
        ("paid_not_above_charges", "WARN", ["total_paid", "total_charges"],
         lambda: df.filter(F.col(m["total_paid"]) > F.col(m["total_charges"])).count()),
        ("no_readmit_after_death", "WARN", ["readmitted_30d", "mortality_flag"],
         lambda: df.filter((F.col(m["mortality_flag"]) == 1) & (readmit == 1)).count()),
        ("los_matches_dates", "WARN", ["admit_date", "discharge_date", "length_of_stay_days"],
         lambda: df.filter(F.abs(F.datediff(_as_date(m["discharge_date"]), _as_date(m["admit_date"]))
                                 - F.col(m["length_of_stay_days"])) > 1).count()),
    ]

    rows = []
    for name, severity, fields, fn in rules:
        if total == 0 or any(f not in m for f in fields):
            rows.append((table_fq, name, severity, None, total, "SKIPPED"))
            continue
        failed = int(fn())
        rows.append((table_fq, name, severity, failed, total, "PASS" if failed == 0 else "FAIL"))

    results = (spark.createDataFrame(rows, "table_name string, check_name string, severity string, failed_rows long, total_rows long, status string")
               .withColumn("checked_at", F.current_timestamp()))
    results.write.mode("append").saveAsTable(results_fq)

    certified = total > 0 and all(r[5] == "PASS" for r in rows if r[2] == "BLOCKER")
    return results, certified
