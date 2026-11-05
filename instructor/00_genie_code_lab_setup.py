# Databricks notebook source
# MAGIC %md
# MAGIC # SelectHealth Genie Code Lab: Instructor Setup
# MAGIC
# MAGIC Run this **once, as the instructor or a workspace admin, before the session**. It builds the extra
# MAGIC lab objects on top of the shared synthetic encounters dataset from the Oct 1 DBSQL workshop
# MAGIC (`data_generation/generate_workshop_data.py`). Nothing here is real member data. It is 100% synthetic, no PHI.
# MAGIC
# MAGIC | Object | Why it exists in the lab |
# MAGIC |---|---|
# MAGIC | `claims_extract_raw` | A deliberately **undocumented, messy** extract (cryptic column names, coded values, dirty rows). Hands on 1 profiles and documents it; Hands on 2 and 4 clean it; Hands on 5 checks it with the data quality skill. |
# MAGIC | `dim_member` | Synthetic member contact table with a **column mask** on name and phone: only the instructor who runs this notebook sees real values. Hands on 7 shows Genie Code inheriting your Unity Catalog permissions. |
# MAGIC | `readmission_rate_official()` | SelectHealth's official readmission definition as a Unity Catalog SQL function. Optional: add it to Genie Code as an MCP tool (Hands on 6 tip) so the metric is computed the same way every time. The Vital Skills metric view in Hands on 6 returns the same rate. |
# MAGIC
# MAGIC **Important:** `claims_extract_raw` intentionally ships with **no table description, no column comments,
# MAGIC and no keys**. That is the "before" state attendees fix in Hands on 1. The last cell of this notebook
# MAGIC (`answer key`) documents it fully if you need to reset or run a dry run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Every value is a widget. Point `catalog` and `schema` at the schema where the base encounters tables live.
# MAGIC `unmasked_user` is the one user who sees unmasked member contact columns (blank = you, the person running this).
# MAGIC It's a user rather than a group so a non-admin instructor can run this without creating a group.
# MAGIC
# MAGIC **Permissions you need** (no workspace admin required): `USE CATALOG` on the catalog and `USE SCHEMA`,
# MAGIC `CREATE TABLE`, `CREATE FUNCTION`, `SELECT` on the schema. The optional grants cell also needs you to own the
# MAGIC schema (or have `MANAGE`); if you don't, it prints the exact statements to send to an admin.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev-sh-training", "Catalog with the base encounters tables")
dbutils.widgets.text("schema", "aibi_workshop", "Schema with the base encounters tables")
dbutils.widgets.text("unmasked_user", "", "User who sees unmasked member PII (blank = you)")
dbutils.widgets.text("seed", "42", "Random seed for dirty-row injection")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
UNMASKED_USER = dbutils.widgets.get("unmasked_user").strip() or spark.sql("SELECT current_user()").first()[0]
SEED = int(dbutils.widgets.get("seed"))

# Backtick-quote names so hyphenated catalogs (for example `dev-sh-training`) work everywhere.
FQ = f"`{CATALOG}`.`{SCHEMA}`"
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")
print(f"Building Genie Code lab objects in {FQ}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the base dataset exists
# MAGIC The lab reuses `fact_encounters` and its four dimensions. If this cell fails, run the generator first.

# COMMAND ----------

required = ["fact_encounters", "dim_provider", "dim_facility", "dim_diagnosis", "dim_procedure"]
existing = {r.tableName for r in spark.sql(f"SHOW TABLES IN {FQ}").collect()}
missing = [t for t in required if t not in existing]
assert not missing, f"Missing base tables {missing}. Run data_generation/generate_workshop_data.py first."
print("Base dataset found:", ", ".join(required))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `claims_extract_raw`: the messy, undocumented extract
# MAGIC Same encounters, but shaped like a legacy file drop:
# MAGIC - cryptic column names (`rdmt_flg`, `pyr_cd`, `los_d`)
# MAGIC - **coded values Genie Code cannot guess**: `pyr_cd` is `1`=Commercial, `2`=Medicare, `3`=Medicaid, `4`=Self-Pay;
# MAGIC   `enc_typ` is `IP`/`OP`/`ED`; `rdmt_flg` is `Y`/`N`
# MAGIC - dates stored as `yyyyMMdd` strings
# MAGIC - a small number of injected dirty rows (duplicates, discharge before admit, paid greater than charges,
# MAGIC   invalid payer code `9`, null member id) so the data quality skill in Hands on 5 has something real to find

# COMMAND ----------

from pyspark.sql import functions as F

payer_code = (F.when(F.col("payer_type") == "Commercial", "1")
               .when(F.col("payer_type") == "Medicare", "2")
               .when(F.col("payer_type") == "Medicaid", "3")
               .otherwise("4"))
enc_code = (F.when(F.col("encounter_type") == "Inpatient", "IP")
             .when(F.col("encounter_type") == "Outpatient", "OP")
             .otherwise("ED"))

raw = (spark.table(f"{FQ}.fact_encounters")
       .select(
           F.col("encounter_id").alias("enc_id"),
           F.col("patient_id").alias("mbr_id"),
           F.col("provider_id").alias("prov_cd"),
           F.col("facility_id").alias("fac_cd"),
           F.col("primary_icd10_code").alias("dx1"),
           F.col("primary_procedure_code").alias("px1"),
           enc_code.alias("enc_typ"),
           payer_code.alias("pyr_cd"),
           F.date_format("admit_date", "yyyyMMdd").alias("adm_dt"),
           F.date_format("discharge_date", "yyyyMMdd").alias("dsch_dt"),
           F.col("length_of_stay_days").alias("los_d"),
           F.col("total_charges").alias("chg_amt"),
           F.col("total_paid").alias("pd_amt"),
           F.when(F.col("readmitted_30d") == 1, "Y").otherwise("N").alias("rdmt_flg"),
           F.col("mortality_flag").alias("mort_ind"),
           F.col("complication_flag").alias("cmp_ind"),
       ))

# Inject dirty rows deterministically (seeded samples of the clean extract).
dupes = raw.sample(fraction=0.005, seed=SEED)                                    # exact duplicate encounters
bad_dates = (raw.sample(fraction=0.003, seed=SEED + 1)
             .withColumn("enc_id", F.concat(F.col("enc_id"), F.lit("-D")))
             .withColumn("dsch_dt", F.date_format(F.date_sub(F.to_date("adm_dt", "yyyyMMdd"), 2), "yyyyMMdd")))
bad_paid = (raw.sample(fraction=0.002, seed=SEED + 2)
            .withColumn("enc_id", F.concat(F.col("enc_id"), F.lit("-P")))
            .withColumn("pd_amt", F.col("chg_amt") * 1.25))
bad_payer = (raw.sample(fraction=0.002, seed=SEED + 3)
             .withColumn("enc_id", F.concat(F.col("enc_id"), F.lit("-Y")))
             .withColumn("pyr_cd", F.lit("9")))
null_mbr = (raw.sample(fraction=0.001, seed=SEED + 4)
            .withColumn("enc_id", F.concat(F.col("enc_id"), F.lit("-M")))
            .withColumn("mbr_id", F.lit(None).cast("string")))

messy = raw.unionByName(dupes).unionByName(bad_dates).unionByName(bad_paid).unionByName(bad_payer).unionByName(null_mbr)
messy.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.claims_extract_raw")

# Strip any metadata a previous run or a dry run left behind, so every session starts from the same "before" state.
spark.sql(f"COMMENT ON TABLE {FQ}.claims_extract_raw IS NULL")
for c in messy.columns:
    spark.sql(f"ALTER TABLE {FQ}.claims_extract_raw ALTER COLUMN {c} COMMENT ''")

print(f"claims_extract_raw rows: {spark.table(f'{FQ}.claims_extract_raw').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `dim_member`: synthetic contact table with a column mask
# MAGIC One row per synthetic member. `member_name` and `phone_number` are masked for everyone except `unmasked_user`,
# MAGIC so attendees (and Genie Code acting as them) see `***MASKED***`. Fully documented with comments and keys.

# COMMAND ----------

first = ["Avery", "Jordan", "Riley", "Casey", "Quinn", "Morgan", "Rowan", "Sage", "Emerson", "Hayden", "Parker", "Reese"]
last = ["Larsen", "Jensen", "Nielsen", "Christensen", "Peterson", "Hansen", "Olsen", "Madsen", "Sorensen", "Andersen"]

members = (spark.table(f"{FQ}.fact_encounters").select("patient_id", "patient_age", "patient_sex").dropDuplicates(["patient_id"])
           .withColumn("h", F.abs(F.hash("patient_id")))
           .select(
               F.col("patient_id").alias("member_id"),
               F.concat_ws(" ", F.element_at(F.array(*[F.lit(x) for x in first]), (F.col("h") % len(first) + 1).cast("int")),
                           F.element_at(F.array(*[F.lit(x) for x in last]), (F.col("h") % len(last) + 1).cast("int"))).alias("member_name"),
               F.concat(F.lit("(801) 555-"), F.lpad((F.col("h") % 10000).cast("string"), 4, "0")).alias("phone_number"),
               F.col("patient_age").alias("age_at_last_encounter"),
               F.col("patient_sex").alias("sex"),
           ))
members.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.dim_member")

spark.sql(f"""CREATE OR REPLACE FUNCTION {FQ}.mask_member_pii(val STRING)
RETURNS STRING
COMMENT 'Column mask for synthetic member PII. Returns the real value only for the lab instructor ({UNMASKED_USER}); everyone else sees ***MASKED***.'
RETURN CASE WHEN current_user() = '{UNMASKED_USER}' THEN val ELSE '***MASKED***' END""")
spark.sql(f"ALTER TABLE {FQ}.dim_member ALTER COLUMN member_name SET MASK {FQ}.mask_member_pii")
spark.sql(f"ALTER TABLE {FQ}.dim_member ALTER COLUMN phone_number SET MASK {FQ}.mask_member_pii")

spark.sql(f"COMMENT ON TABLE {FQ}.dim_member IS 'Synthetic member contact dimension (no real PHI). One row per member_id. member_name and phone_number carry a column mask: only the lab instructor sees real values. Joins to fact_encounters.patient_id.'")
member_comments = {
    "member_id": "Synthetic member identifier. Primary key. Matches fact_encounters.patient_id.",
    "member_name": "Synthetic member full name. Masked by mask_member_pii for everyone except the lab instructor.",
    "phone_number": "Synthetic member phone number (555 exchange). Masked by mask_member_pii for everyone except the lab instructor.",
    "age_at_last_encounter": "Member age in years at one of their encounters.",
    "sex": "Member sex as recorded on the encounter: M or F.",
}
for c, txt in member_comments.items():
    spark.sql(f"ALTER TABLE {FQ}.dim_member ALTER COLUMN {c} COMMENT '{txt}'")
spark.sql(f"ALTER TABLE {FQ}.dim_member ALTER COLUMN member_id SET NOT NULL")
spark.sql(f"ALTER TABLE {FQ}.dim_member DROP CONSTRAINT IF EXISTS pk_dim_member")
spark.sql(f"ALTER TABLE {FQ}.dim_member ADD CONSTRAINT pk_dim_member PRIMARY KEY (member_id)")
print(f"dim_member rows: {spark.table(f'{FQ}.dim_member').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `readmission_rate_official()`: the governed metric
# MAGIC SelectHealth's (lab) definition: **30-day readmissions among inpatient index encounters, excluding encounters
# MAGIC where the patient died**, optionally for one clinical category (pass `NULL` for all). A general coding agent
# MAGIC usually averages `readmitted_30d` across every encounter, which gives a different (wrong) number. The Vital
# MAGIC Skills metric view in Hands on 6 matches it; you can also add this function to Genie Code as an MCP tool.

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE FUNCTION {FQ}.readmission_rate_official(clinical_category_filter STRING)
RETURNS DOUBLE
COMMENT 'SelectHealth official 30-day readmission rate (percent). Denominator: inpatient encounters where the patient did not die (mortality_flag = 0). Numerator: those with readmitted_30d = 1. Pass a clinical category (for example Cardiovascular) or NULL for all categories.'
RETURN (
  SELECT ROUND(100.0 * SUM(e.readmitted_30d) / COUNT(*), 2)
  FROM {FQ}.fact_encounters e
  JOIN {FQ}.dim_diagnosis d ON e.primary_icd10_code = d.icd10_code
  WHERE e.encounter_type = 'Inpatient'
    AND e.mortality_flag = 0
    AND (clinical_category_filter IS NULL OR d.clinical_category = clinical_category_filter)
)""")

display(spark.sql(f"""
SELECT 'official (inpatient, survivors)' AS definition, {FQ}.readmission_rate_official(NULL) AS readmit_pct
UNION ALL
SELECT 'naive (all encounters)', ROUND(100.0 * AVG(readmitted_30d), 2) FROM {FQ}.fact_encounters
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3b. `member_encounter_detail`: PHI-shaped table for the Vital Skills `phi-deidentifier` demo
# MAGIC One row per encounter with **synthetic** direct identifiers (name, phone, email, DOB) and quasi-identifiers
# MAGIC (ZIP, age, sex, dates, length of stay). Nothing here is real. The instructor runs `phi-deidentifier` on it in
# MAGIC Part 3 to produce a governed, de-identified view. Attendees are not granted SELECT on it (see grants below).

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE {FQ}.member_encounter_detail AS
SELECT e.encounter_id,
  e.patient_id AS member_id,
  concat(element_at(array({','.join(repr(x) for x in first)}), cast(abs(hash(e.patient_id)) % {len(first)} + 1 AS INT)), ' ',
         element_at(array({','.join(repr(x) for x in last)}), cast(abs(hash(e.patient_id, 1)) % {len(last)} + 1 AS INT))) AS member_name,
  concat('(801) 555-', lpad(cast(abs(hash(e.patient_id)) % 10000 AS STRING), 4, '0')) AS phone_number,
  concat('member', abs(hash(e.patient_id)) % 100000, '@example.com') AS email,
  date_sub(e.admit_date, e.patient_age * 365 + cast(abs(hash(e.patient_id, 2)) % 365 AS INT)) AS date_of_birth,
  element_at(array('84101','84106','84111','84123','84601','84604','84401','84770','83702','83706','84321','84097'),
             cast(abs(hash(e.patient_id, 3)) % 12 + 1 AS INT)) AS zip_code,
  e.patient_sex AS sex, e.patient_age AS age, e.admit_date, e.discharge_date, e.length_of_stay_days,
  e.encounter_type, e.primary_icd10_code, e.readmitted_30d, e.total_paid
FROM {FQ}.fact_encounters e""")

spark.sql(f"COMMENT ON TABLE {FQ}.member_encounter_detail IS 'SYNTHETIC (no real PHI) member-level encounter detail with PHI-shaped columns, built only to demo the Vital Skills phi-deidentifier skill. One row per encounter. Do not use for analytics.'")
med_comments = {
    "encounter_id": "Encounter identifier. Primary key. Matches fact_encounters.encounter_id.",
    "member_id": "Synthetic member identifier (direct identifier).",
    "member_name": "Synthetic member full name (direct identifier).",
    "phone_number": "Synthetic phone number, 555 exchange (direct identifier).",
    "email": "Synthetic email address (direct identifier).",
    "date_of_birth": "Synthetic date of birth (Safe Harbor date element).",
    "zip_code": "Synthetic 5-digit Utah/Idaho ZIP code (geographic quasi-identifier).",
    "sex": "Member sex: M or F (quasi-identifier).",
    "age": "Age in years at admission (quasi-identifier).",
    "admit_date": "Admission date (Safe Harbor date element).",
    "discharge_date": "Discharge date (Safe Harbor date element).",
    "length_of_stay_days": "Length of stay in days (derived quasi-identifier).",
    "encounter_type": "Inpatient, Outpatient, or Emergency.",
    "primary_icd10_code": "Primary diagnosis ICD-10 code.",
    "readmitted_30d": "1 if readmitted within 30 days of discharge, else 0.",
    "total_paid": "Total amount paid in USD.",
}
for c, txt in med_comments.items():
    spark.sql(f"ALTER TABLE {FQ}.member_encounter_detail ALTER COLUMN {c} COMMENT '{txt}'")
spark.sql(f"ALTER TABLE {FQ}.member_encounter_detail ALTER COLUMN encounter_id SET NOT NULL")
spark.sql(f"ALTER TABLE {FQ}.member_encounter_detail ADD CONSTRAINT pk_member_encounter_detail PRIMARY KEY (encounter_id)")
print(f"member_encounter_detail rows: {spark.table(f'{FQ}.member_encounter_detail').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Grants for the training group
# MAGIC Attendees need to read everything here and create their own `[INITIALS]_` tables in this schema.
# MAGIC Run as a user who can grant on the schema. Set `training_group` to the attendee group (account-level group bound to the workspace).

# COMMAND ----------

dbutils.widgets.text("training_group", "", "Attendee group to grant (leave blank to skip)")
TRAINING_GROUP = dbutils.widgets.get("training_group").strip()
if TRAINING_GROUP:
    stmts = [
        f"GRANT USE CATALOG ON CATALOG `{CATALOG}` TO `{TRAINING_GROUP}`",
        f"GRANT USE SCHEMA, SELECT, CREATE TABLE, CREATE FUNCTION, MODIFY ON SCHEMA {FQ} TO `{TRAINING_GROUP}`",
        f"GRANT EXECUTE ON FUNCTION {FQ}.readmission_rate_official TO `{TRAINING_GROUP}`",
    ]
    failed = []
    for stmt in stmts:
        try:
            spark.sql(stmt)
            print("OK:", stmt)
        except Exception as e:
            failed.append(stmt)
            print("COULD NOT RUN:", stmt, "\n   ", str(e).splitlines()[0][:200])
    if failed:
        # Not fatal: on Oct 1 attendees already had access to this schema. Check it with a non-admin attendee, and if
        # they can't read/create, send these statements to someone who owns the catalog and schema.
        print("\nAsk the schema/catalog owner to run:\n" + ";\n".join(failed) + ";")
    # member_encounter_detail is synthetic, but it plays the role of raw PHI in the demo. The schema-level SELECT
    # above makes it readable to attendees, which is fine for synthetic data.
else:
    print("training_group blank, skipping grants.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected numbers for THIS dataset (copy these into your instructor notes)
# MAGIC The workbook quotes approximate numbers from the staging dataset. Exact values depend on the data in this
# MAGIC workspace, so this cell computes every number attendees will see, using the same rules as `99_answer_key`
# MAGIC and the data quality skill.

# COMMAND ----------

expected = spark.sql(f"""
WITH raw AS (SELECT * FROM {FQ}.claims_extract_raw),
dated AS (SELECT *, try_to_date(adm_dt, 'yyyyMMdd') AS a, try_to_date(dsch_dt, 'yyyyMMdd') AS d FROM raw),
first_enc AS (SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY enc_id ORDER BY 1) AS rn FROM dated) WHERE rn = 1),
silver AS (SELECT * FROM first_enc WHERE d >= a AND pyr_cd IN ('1','2','3','4'))
SELECT 'Hands on 1: Medicare inpatient readmission %, deduplicated' AS check,
       CAST((SELECT ROUND(100.0 * AVG(CASE WHEN rdmt_flg = 'Y' THEN 1 ELSE 0 END), 2)
             FROM (SELECT DISTINCT * FROM raw) WHERE pyr_cd = '2' AND enc_typ = 'IP') AS STRING) AS value
UNION ALL SELECT 'Hands on 1: Medicare inpatient encounters, deduplicated',
       CAST((SELECT COUNT(*) FROM (SELECT DISTINCT * FROM raw) WHERE pyr_cd = '2' AND enc_typ = 'IP') AS STRING)
UNION ALL SELECT 'claims_extract_raw rows', CAST((SELECT COUNT(*) FROM raw) AS STRING)
UNION ALL SELECT 'Hands on 4: silver rows (answer key rules)', CAST((SELECT COUNT(*) FROM silver) AS STRING)
UNION ALL SELECT 'Hands on 5 raw: duplicate encounter rows', CAST((SELECT COUNT(*) - COUNT(DISTINCT enc_id) FROM raw) AS STRING)
UNION ALL SELECT 'Hands on 5 raw: discharge before admit', CAST((SELECT COUNT(*) FROM dated WHERE d < a) AS STRING)
UNION ALL SELECT 'Hands on 5 raw: invalid payer code', CAST((SELECT COUNT(*) FROM raw WHERE pyr_cd NOT IN ('1','2','3','4')) AS STRING)
UNION ALL SELECT 'Hands on 5 raw + silver: missing member id', CAST((SELECT COUNT(*) FROM raw WHERE mbr_id IS NULL) AS STRING)
UNION ALL SELECT 'Hands on 5 raw: paid above charges (WARN)', CAST((SELECT COUNT(*) FROM raw WHERE pd_amt > chg_amt) AS STRING)
UNION ALL SELECT 'Hands on 6: standard readmission % (index admissions)', CAST({FQ}.readmission_rate_official(NULL) AS STRING)
UNION ALL SELECT 'Hands on 6: index admissions', CAST((SELECT COUNT(*) FROM {FQ}.fact_encounters WHERE encounter_type = 'Inpatient' AND mortality_flag = 0) AS STRING)
UNION ALL SELECT 'Hands on 6: naive readmission % (all encounters)', CAST((SELECT ROUND(100.0 * AVG(readmitted_30d), 2) FROM {FQ}.fact_encounters) AS STRING)
""")
display(expected)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Answer key: fully document `claims_extract_raw` (dry runs and resets only)
# MAGIC This is what "good" looks like at the end of Hands on 1. Do **not** run it before the session, or Hands on 1's
# MAGIC "before" state disappears. Flip `apply_answer_key` to `yes` to run it.

# COMMAND ----------

dbutils.widgets.dropdown("apply_answer_key", "no", ["no", "yes"], "Apply Hands on 1 answer key?")
if dbutils.widgets.get("apply_answer_key") == "yes":
    spark.sql(f"COMMENT ON TABLE {FQ}.claims_extract_raw IS 'Legacy encounter claims extract (synthetic, no PHI). One row per encounter as delivered by the source file drop, before cleaning. Contains known dirty rows (duplicates, bad dates, invalid payer codes); use the silver table for analytics.'")
    raw_comments = {
        "enc_id": "Encounter identifier. Should be unique, but the raw extract contains duplicates.",
        "mbr_id": "Member identifier. Joins to dim_member.member_id and fact_encounters.patient_id.",
        "prov_cd": "Attending provider identifier. Joins to dim_provider.provider_id.",
        "fac_cd": "Facility identifier. Joins to dim_facility.facility_id.",
        "dx1": "Primary diagnosis ICD-10 code. Joins to dim_diagnosis.icd10_code.",
        "px1": "Primary procedure code. Joins to dim_procedure.procedure_code.",
        "enc_typ": "Encounter setting code: IP = Inpatient, OP = Outpatient, ED = Emergency.",
        "pyr_cd": "Payer code: 1 = Commercial, 2 = Medicare, 3 = Medicaid, 4 = Self-Pay. Any other value is invalid.",
        "adm_dt": "Admit date as a yyyyMMdd string. Parse with to_date(adm_dt, 'yyyyMMdd').",
        "dsch_dt": "Discharge date as a yyyyMMdd string. Should be on or after adm_dt.",
        "los_d": "Length of stay in days.",
        "chg_amt": "Total billed charges in USD.",
        "pd_amt": "Total amount paid in USD. Should not exceed chg_amt.",
        "rdmt_flg": "30-day readmission flag: Y = readmitted within 30 days of discharge, N = not readmitted.",
        "mort_ind": "Mortality indicator: 1 = patient died during the encounter, 0 = survived.",
        "cmp_ind": "Complication indicator: 1 = complication recorded, 0 = none.",
    }
    for c, txt in raw_comments.items():
        spark.sql(f"ALTER TABLE {FQ}.claims_extract_raw ALTER COLUMN {c} COMMENT \"{txt}\"")
    print("Answer key applied.")
