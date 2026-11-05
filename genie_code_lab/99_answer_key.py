# Databricks notebook source
# MAGIC %md
# MAGIC # SelectHealth Genie Code Lab: Answer Key and Fallbacks
# MAGIC
# MAGIC Verified code for every hands-on prompt. If Genie Code takes a different path live (it is an agent, so
# MAGIC output varies run to run), use these to **check its answer** or to **catch up** an attendee who fell behind.
# MAGIC Set `initials` to your own initials so you never collide with someone else's tables.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev-sh-training", "Catalog")
dbutils.widgets.text("schema", "aibi_workshop", "Schema")
dbutils.widgets.text("initials", "kk", "Your initials (table prefix)")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
INITIALS = dbutils.widgets.get("initials").lower().strip()
FQ = f"`{CATALOG}`.`{SCHEMA}`"
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hands on 1: Explore unfamiliar data. "What is the 30-day readmission rate for Medicare inpatient encounters?"
# MAGIC On the undocumented `claims_extract_raw`, Genie Code has to guess that Medicare is `pyr_cd = '2'` and that
# MAGIC inpatient is `enc_typ = 'IP'`. After the column comments are added, it gets it right. This is the right answer
# MAGIC (deduplicated, raw data, naive definition, which is what the question literally asks).

# COMMAND ----------

display(spark.sql(f"""
SELECT COUNT(*) AS encounters,
       ROUND(100.0 * AVG(CASE WHEN rdmt_flg = 'Y' THEN 1 ELSE 0 END), 2) AS readmit_pct
FROM (SELECT DISTINCT * FROM {FQ}.claims_extract_raw)
WHERE pyr_cd = '2' AND enc_typ = 'IP'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hands on 4: Teach it your standards. "Build a cleaned silver encounters table from claims_extract_raw."
# MAGIC With the SelectHealth instructions and `AGENTS.md` in place, the output should look like this: widget-driven
# MAGIC names, `<initials>_` prefix, readable column names, decoded values, documented, PK, dirty rows handled.

# COMMAND ----------

from pyspark.sql import functions as F, Window

raw = spark.table(f"{FQ}.claims_extract_raw")
first_per_enc = Window.partitionBy("enc_id").orderBy(F.lit(1))

silver = (raw
    .withColumn("_rn", F.row_number().over(first_per_enc)).filter("_rn = 1").drop("_rn")       # dedupe on enc_id
    .withColumn("admit_date", F.expr("try_to_date(adm_dt, 'yyyyMMdd')"))
    .withColumn("discharge_date", F.expr("try_to_date(dsch_dt, 'yyyyMMdd')"))
    .filter(F.col("discharge_date") >= F.col("admit_date"))                                   # drop bad dates
    .filter(F.col("pyr_cd").isin("1", "2", "3", "4"))                                          # drop invalid payer
    .select(
        F.col("enc_id").alias("encounter_id"),
        F.col("mbr_id").alias("patient_id"),
        F.col("prov_cd").alias("provider_id"),
        F.col("fac_cd").alias("facility_id"),
        F.col("dx1").alias("primary_icd10_code"),
        F.col("px1").alias("primary_procedure_code"),
        F.expr("CASE enc_typ WHEN 'IP' THEN 'Inpatient' WHEN 'OP' THEN 'Outpatient' ELSE 'Emergency' END").alias("encounter_type"),
        F.expr("CASE pyr_cd WHEN '1' THEN 'Commercial' WHEN '2' THEN 'Medicare' WHEN '3' THEN 'Medicaid' ELSE 'Self-Pay' END").alias("payer_type"),
        "admit_date", "discharge_date",
        F.col("los_d").alias("length_of_stay_days"),
        F.col("chg_amt").alias("total_charges"),
        F.col("pd_amt").alias("total_paid"),
        F.when(F.col("rdmt_flg") == "Y", 1).otherwise(0).alias("readmitted_30d"),
        F.col("mort_ind").alias("mortality_flag"),
        F.col("cmp_ind").alias("complication_flag"),
    ))

silver_tbl = f"{FQ}.`{INITIALS}_silver_encounters`"
silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_tbl)
print(f"raw rows: {raw.count():,}  silver rows: {spark.table(silver_tbl).count():,}  dropped: {raw.count() - spark.table(silver_tbl).count():,}")

# COMMAND ----------

# Document it (table description, every column, PK), per the instructions file.
spark.sql(f"COMMENT ON TABLE {silver_tbl} IS 'Cleaned silver encounters built from claims_extract_raw (synthetic, no PHI). One row per encounter_id. Duplicates removed; rows with discharge before admit or an invalid payer code dropped; coded values decoded.'")
silver_comments = {
    "encounter_id": "Unique encounter identifier. Primary key.",
    "patient_id": "Member identifier. Joins to dim_member.member_id. May be null for a few source rows (open question in AGENTS.md).",
    "provider_id": "Attending provider. Joins to dim_provider.provider_id.",
    "facility_id": "Facility. Joins to dim_facility.facility_id.",
    "primary_icd10_code": "Primary diagnosis ICD-10 code. Joins to dim_diagnosis.icd10_code.",
    "primary_procedure_code": "Primary procedure code. Joins to dim_procedure.procedure_code.",
    "encounter_type": "Encounter setting: Inpatient, Outpatient, or Emergency.",
    "payer_type": "Payer category: Commercial, Medicare, Medicaid, or Self-Pay.",
    "admit_date": "Admit date.",
    "discharge_date": "Discharge date. Always on or after admit_date in this table.",
    "length_of_stay_days": "Length of stay in days.",
    "total_charges": "Total billed charges in USD.",
    "total_paid": "Total amount paid in USD.",
    "readmitted_30d": "1 if readmitted within 30 days of discharge, else 0.",
    "mortality_flag": "1 if the patient died during the encounter, else 0.",
    "complication_flag": "1 if a complication was recorded, else 0.",
}
for c, txt in silver_comments.items():
    spark.sql(f"ALTER TABLE {silver_tbl} ALTER COLUMN {c} COMMENT \"{txt}\"")
spark.sql(f"ALTER TABLE {silver_tbl} ALTER COLUMN encounter_id SET NOT NULL")
spark.sql(f"ALTER TABLE {silver_tbl} DROP CONSTRAINT IF EXISTS pk_{INITIALS}_silver_encounters")
spark.sql(f"ALTER TABLE {silver_tbl} ADD CONSTRAINT pk_{INITIALS}_silver_encounters PRIMARY KEY (encounter_id)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hands on 5: Skills. "Check data quality on claims_extract_raw" and on the silver table
# MAGIC With the `selecthealth-encounter-dq` skill installed, Genie Code should run exactly these eight checks and
# MAGIC append to `<initials>_dq_results`. Verified in staging: the raw table fails 4 blockers (428 duplicates, 242 bad dates,
# MAGIC 151 invalid payers, 78 missing members). The silver table passes every blocker except `member_id_present` (78),
# MAGIC because AGENTS.md leaves null members as an open question. The skill surfaces the team's undecided policy.

# COMMAND ----------

import os
skill_script = os.path.join(os.getcwd(), "skills", "selecthealth-encounter-dq", "scripts", "run_dq_checks.py")
exec(open(skill_script).read())
results_tbl = f"{FQ}.`{INITIALS}_dq_results`"
for t in [f"{FQ}.claims_extract_raw", silver_tbl]:
    res, certified = run_dq_checks(spark, t, results_tbl)
    print(t, "CERTIFIED" if certified else "NOT CERTIFIED")
    display(res.select("check_name", "severity", "failed_rows", "status"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extra: Governed metric as a tool (Hands on 6 tip). "What is the readmission rate for Infectious encounters?"
# MAGIC Without the tool, Genie Code usually averages `readmitted_30d` over every encounter. With
# MAGIC `readmission_rate_official` added as a Unity Catalog function MCP server, it calls the function instead.

# COMMAND ----------

display(spark.sql(f"""
SELECT 'official tool' AS method, {FQ}.readmission_rate_official('Infectious') AS infectious_readmit_pct
UNION ALL
SELECT 'naive average, all encounter types', ROUND(100.0 * AVG(e.readmitted_30d), 2)
FROM {FQ}.fact_encounters e JOIN {FQ}.dim_diagnosis d ON e.primary_icd10_code = d.icd10_code
WHERE d.clinical_category = 'Infectious'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hands on 7: Governance. "Show me the names and phone numbers of members readmitted in 2025."
# MAGIC Genie Code runs as you. For everyone except the instructor, the column mask returns `***MASKED***`, and your
# MAGIC instructions file tells it not to display identifiers at all.

# COMMAND ----------

display(spark.sql(f"""
SELECT m.member_name, m.phone_number, COUNT(*) AS readmissions
FROM {FQ}.fact_encounters e JOIN {FQ}.dim_member m ON e.patient_id = m.member_id
WHERE e.readmitted_30d = 1 AND year(e.discharge_date) = 2025
GROUP BY ALL ORDER BY readmissions DESC LIMIT 5
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hands on 6: Vital Skills `semantic-builder` metric view
# MAGIC After the instructor deploys the generated layer (see `vital_skills/selecthealth_mapping.yaml`), the standard
# MAGIC readmission rate should match SelectHealth's lab definition: 30-day readmissions over eligible index admissions.
# MAGIC Verified in staging: 15.06% on 34,557 index admissions; the naive average over all encounters is 14.49%.

# COMMAND ----------

dbutils.widgets.text("vital_skills_schema", "aibi_workshop", "Schema with the semantic-builder output")
VS = f"`{CATALOG}`.`{dbutils.widgets.get('vital_skills_schema')}`"
try:
    display(spark.sql(f"""
    SELECT ROUND(100 * MEASURE(readmission_30day_rate), 2) AS readmission_rate_pct,
           MEASURE(index_admission_count) AS index_admissions,
           ROUND(MEASURE(avg_los_days), 2) AS avg_los_days
    FROM {VS}.mv_care_delivery__encounter"""))
except Exception as e:
    print(f"semantic-builder output not deployed in {VS} yet: {e}")
display(spark.sql(f"SELECT ROUND(100.0 * AVG(readmitted_30d), 2) AS naive_readmit_pct_all_encounters FROM {FQ}.fact_encounters"))
