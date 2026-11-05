# Databricks notebook source
# MAGIC %md
# MAGIC # SelectHealth AI/BI Workshop — Synthetic Medical Dataset
# MAGIC
# MAGIC Generates a realistic (but 100% synthetic, no PHI) hospital/clinic encounters dataset
# MAGIC for the SelectHealth AI/BI vs. Tableau workshop. The schema mirrors the kind of data
# MAGIC the analysts work with day to day: **something happened** (a diagnosis / procedure),
# MAGIC **where it happened** (provider + facility), and **the outcome** (readmission,
# MAGIC mortality, complication) — designed so we can demonstrate:
# MAGIC
# MAGIC - **Aggregations** by provider and facility (their differentiator vs. Intermountain Acute Care)
# MAGIC - **Trends** over time (36 months of encounters)
# MAGIC - **Drill-downs** (region → facility → provider → encounter)
# MAGIC - **Outcome rates** (readmit / mortality / complication) for quality dashboards
# MAGIC
# MAGIC ### Output tables (star schema)
# MAGIC | Table | Grain | Purpose |
# MAGIC |---|---|---|
# MAGIC | `dim_provider` | one row per provider | provider name, specialty, region |
# MAGIC | `dim_facility` | one row per facility | hospital/clinic name, type, city/state/region |
# MAGIC | `dim_diagnosis` | one row per ICD-10 code | ICD-10 code + description + clinical category |
# MAGIC | `dim_procedure` | one row per procedure code | procedure code + description + category |
# MAGIC | `fact_encounters` | one row per patient encounter | the analytic fact table |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC All values are parameterized via widgets — never hardcoded. Adjust catalog/schema to
# MAGIC point at wherever the workshop workspace stages content.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev-sh-training", "Target catalog")
dbutils.widgets.text("schema", "aibi_workshop", "Target schema")
dbutils.widgets.text("num_encounters", "80000", "Number of encounters to generate")
dbutils.widgets.text("num_providers", "45", "Number of providers")
dbutils.widgets.text("num_facilities", "12", "Number of facilities")
dbutils.widgets.text("start_date", "2023-01-01", "Encounter start date")
dbutils.widgets.text("end_date", "2025-12-31", "Encounter end date")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
NUM_ENCOUNTERS = int(dbutils.widgets.get("num_encounters"))
NUM_PROVIDERS = int(dbutils.widgets.get("num_providers"))
NUM_FACILITIES = int(dbutils.widgets.get("num_facilities"))
START_DATE = dbutils.widgets.get("start_date")
END_DATE = dbutils.widgets.get("end_date")

print(f"Generating {NUM_ENCOUNTERS:,} encounters into {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create catalog + schema

# COMMAND ----------

# Create the target catalog if it does not already exist. On some metastores (Default
# Storage enabled with no managed location) creating a brand-new catalog fails with
# "Metastore storage root URL does not exist". If you hit that, create the catalog once
# in the Catalog UI (Catalog > Create catalog) or with an explicit MANAGED LOCATION, then
# re-run. If the catalog already exists, this is a no-op.
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
except Exception as e:
    print(f"Could not auto-create catalog '{CATALOG}'. If it does not already exist, create "
          f"it once in the Catalog UI (or with a MANAGED LOCATION) and re-run. Details: {e}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"USE `{CATALOG}`.`{SCHEMA}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference data: curated medical code lists
# MAGIC Real ICD-10 / procedure codes with plain-language descriptions, grouped into clinical
# MAGIC categories. Curated (not random) so the dashboard and Genie answers read like real
# MAGIC healthcare analytics. Includes the knee-replacement example Carla called out.

# COMMAND ----------

# (icd10_code, description, clinical_category, base_mortality, base_complication, base_readmit)
# base_* are baseline outcome probabilities used to make outcomes condition-dependent.
DIAGNOSES = [
    ("I21.4",  "Non-ST elevation myocardial infarction (NSTEMI)", "Cardiovascular", 0.06, 0.18, 0.20),
    ("I50.9",  "Heart failure, unspecified",                      "Cardiovascular", 0.05, 0.15, 0.24),
    ("I63.9",  "Cerebral infarction (stroke), unspecified",       "Cardiovascular", 0.08, 0.20, 0.18),
    ("I48.91", "Atrial fibrillation, unspecified",                "Cardiovascular", 0.02, 0.08, 0.14),
    ("J18.9",  "Pneumonia, unspecified organism",                 "Respiratory",    0.04, 0.12, 0.16),
    ("J44.1",  "COPD with acute exacerbation",                    "Respiratory",    0.03, 0.11, 0.21),
    ("J96.00", "Acute respiratory failure",                       "Respiratory",    0.10, 0.22, 0.19),
    ("M17.11", "Unilateral primary osteoarthritis, right knee",   "Orthopedic",     0.002,0.06, 0.07),
    ("M17.12", "Unilateral primary osteoarthritis, left knee",    "Orthopedic",     0.002,0.06, 0.07),
    ("M16.11", "Unilateral primary osteoarthritis, right hip",    "Orthopedic",     0.002,0.06, 0.07),
    ("S72.001A","Fracture of femur, initial encounter",           "Orthopedic",     0.02, 0.10, 0.12),
    ("E11.65", "Type 2 diabetes with hyperglycemia",              "Endocrine",      0.01, 0.07, 0.13),
    ("E11.10", "Type 2 diabetes with ketoacidosis",               "Endocrine",      0.03, 0.10, 0.17),
    ("N17.9",  "Acute kidney failure, unspecified",               "Renal",          0.07, 0.16, 0.22),
    ("N18.6",  "End stage renal disease",                         "Renal",          0.06, 0.14, 0.25),
    ("K35.80", "Acute appendicitis, unspecified",                 "Gastrointestinal",0.005,0.05, 0.06),
    ("K85.90", "Acute pancreatitis, unspecified",                 "Gastrointestinal",0.03, 0.12, 0.15),
    ("A41.9",  "Sepsis, unspecified organism",                    "Infectious",     0.14, 0.25, 0.20),
    ("O80",    "Encounter for full-term uncomplicated delivery",  "Maternity",      0.0005,0.03, 0.04),
    ("O14.90", "Pre-eclampsia, unspecified",                      "Maternity",      0.004,0.09, 0.10),
    ("C50.911","Malignant neoplasm of right breast",              "Oncology",       0.05, 0.13, 0.16),
    ("C34.90", "Malignant neoplasm of lung",                      "Oncology",       0.12, 0.20, 0.19),
    ("F32.9",  "Major depressive disorder, single episode",       "Behavioral",     0.005,0.04, 0.11),
    ("R07.9",  "Chest pain, unspecified",                         "Cardiovascular", 0.005,0.04, 0.09),
]

# (procedure_code, description, category) — CPT-style, plus the knee replacement example
PROCEDURES = [
    ("27447", "Total knee arthroplasty (knee replacement)",       "Orthopedic Surgery"),
    ("27130", "Total hip arthroplasty (hip replacement)",         "Orthopedic Surgery"),
    ("27236", "Open treatment of femoral fracture",               "Orthopedic Surgery"),
    ("92928", "Percutaneous coronary intervention (stent)",       "Cardiac Procedure"),
    ("33533", "Coronary artery bypass graft (CABG)",              "Cardiac Surgery"),
    ("93010", "Electrocardiogram interpretation",                 "Cardiac Diagnostic"),
    ("44970", "Laparoscopic appendectomy",                        "General Surgery"),
    ("47562", "Laparoscopic cholecystectomy",                     "General Surgery"),
    ("59400", "Routine obstetric care incl. vaginal delivery",    "Maternity"),
    ("59510", "Routine obstetric care incl. cesarean delivery",   "Maternity"),
    ("90935", "Hemodialysis procedure",                           "Renal"),
    ("31500", "Endotracheal intubation, emergency",               "Critical Care"),
    ("99291", "Critical care, first 30-74 minutes",               "Critical Care"),
    ("99223", "Initial hospital inpatient care, high complexity", "Evaluation & Mgmt"),
    ("99285", "Emergency department visit, high complexity",      "Emergency"),
    ("19303", "Mastectomy, simple, complete",                     "Oncology Surgery"),
    ("96413", "Chemotherapy administration, IV infusion",         "Oncology Treatment"),
    (None,    "No procedure performed",                           "None"),
]

SPECIALTIES = ["Cardiology", "Pulmonology", "Orthopedic Surgery", "Internal Medicine",
               "Nephrology", "General Surgery", "Obstetrics & Gynecology", "Oncology",
               "Emergency Medicine", "Hospitalist"]

# SelectHealth operates in the Intermountain West — use plausible regional cities.
FACILITY_SEED = [
    ("Wasatch Regional Medical Center",   "Inpatient Hospital", "Salt Lake City", "UT", "Wasatch Front"),
    ("Canyon View Hospital",              "Inpatient Hospital", "Provo",          "UT", "Wasatch Front"),
    ("Great Salt Lake Medical Center",    "Inpatient Hospital", "Ogden",          "UT", "Northern Utah"),
    ("Red Rock Regional Hospital",        "Inpatient Hospital", "St. George",     "UT", "Southern Utah"),
    ("Cache Valley Community Hospital",    "Critical Access",    "Logan",          "UT", "Northern Utah"),
    ("Treasure Valley Medical Center",    "Inpatient Hospital", "Boise",          "ID", "Idaho"),
    ("Snake River Hospital",              "Critical Access",    "Idaho Falls",    "ID", "Idaho"),
    ("High Desert Specialty Clinic",      "Outpatient Clinic",  "Las Vegas",      "NV", "Nevada"),
    ("Mountain West Surgical Center",     "Ambulatory Surgery", "Salt Lake City", "UT", "Wasatch Front"),
    ("Bonneville Family Clinic",          "Outpatient Clinic",  "Provo",          "UT", "Wasatch Front"),
    ("Summit Cardiology Institute",       "Specialty Center",   "Salt Lake City", "UT", "Wasatch Front"),
    ("Valley Women's & Children's",       "Specialty Center",   "Murray",         "UT", "Wasatch Front"),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build dimension tables

# COMMAND ----------

import random
from pyspark.sql import functions as F, types as T
from datetime import date

random.seed(42)

# --- dim_diagnosis ---
dim_diagnosis = spark.createDataFrame(
    [(c, d, cat) for (c, d, cat, *_ ) in DIAGNOSES],
    "icd10_code string, icd10_description string, clinical_category string",
)
dim_diagnosis.write.mode("overwrite").saveAsTable("dim_diagnosis")

# --- dim_procedure ---
dim_procedure = spark.createDataFrame(
    [(code if code else "NONE", desc, cat) for (code, desc, cat) in PROCEDURES],
    "procedure_code string, procedure_description string, procedure_category string",
)
dim_procedure.write.mode("overwrite").saveAsTable("dim_procedure")

# --- dim_facility ---
facility_rows = [
    (f"FAC{idx+1:03d}", name, ftype, city, state, region)
    for idx, (name, ftype, city, state, region) in enumerate(FACILITY_SEED[:NUM_FACILITIES])
]
dim_facility = spark.createDataFrame(
    facility_rows,
    "facility_id string, facility_name string, facility_type string, city string, state string, region string",
)
dim_facility.write.mode("overwrite").saveAsTable("dim_facility")

# --- dim_provider ---
FIRST = ["James","Mary","Robert","Patricia","John","Jennifer","Michael","Linda","David",
         "Elizabeth","William","Barbara","Richard","Susan","Joseph","Karen","Thomas","Nancy",
         "Maria","Carlos","Wei","Priya","Ahmed","Sofia","Hyun","Fatima","Diego","Aisha"]
LAST = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Rodriguez",
        "Martinez","Hernandez","Lopez","Gonzalez","Wilson","Anderson","Thomas","Taylor","Moore",
        "Nguyen","Patel","Kim","Chen","Okafor","Singh","Ali","Romero","Schultz","Bennett"]
provider_rows = []
for i in range(NUM_PROVIDERS):
    npi = str(random.randint(1000000000, 9999999999))  # 10-digit NPI-style id
    name = f"Dr. {random.choice(FIRST)} {random.choice(LAST)}"
    specialty = random.choice(SPECIALTIES)
    fac = random.choice(facility_rows)
    provider_rows.append((f"PRV{i+1:03d}", npi, name, specialty, fac[0]))
dim_provider = spark.createDataFrame(
    provider_rows,
    "provider_id string, npi string, provider_name string, specialty string, primary_facility_id string",
)
dim_provider.write.mode("overwrite").saveAsTable("dim_provider")

print("Dimensions written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build fact_encounters
# MAGIC Outcomes are condition-dependent (driven by the baseline rates per diagnosis) and
# MAGIC nudged by age and length of stay, so quality dashboards show meaningful variation
# MAGIC across diagnoses, providers, and facilities — not uniform noise.

# COMMAND ----------

diag_lookup = {c: (mort, comp, readm) for (c, d, cat, mort, comp, readm) in DIAGNOSES}
diag_codes = [c for (c, *_) in DIAGNOSES]
proc_codes = [code if code else "NONE" for (code, *_) in PROCEDURES]
provider_ids = [r[0] for r in provider_rows]
prov_to_fac = {r[0]: r[4] for r in provider_rows}

COMPLICATION_TYPES = ["Surgical site infection", "Post-op bleeding", "Hospital-acquired pneumonia",
                      "Venous thromboembolism", "Acute kidney injury", "Adverse drug reaction",
                      "Pressure ulcer", "Sepsis"]
ENCOUNTER_TYPES = ["Inpatient", "Outpatient", "Emergency"]
PAYER_TYPES = ["Commercial", "Medicare", "Medicaid", "Self-Pay"]

start = date.fromisoformat(START_DATE)
end = date.fromisoformat(END_DATE)
span_days = (end - start).days

def gen_row(i):
    rnd = random.random
    icd = random.choice(diag_codes)
    base_mort, base_comp, base_readm = diag_lookup[icd]

    enc_type = random.choices(ENCOUNTER_TYPES, weights=[0.45, 0.35, 0.20])[0]
    # outpatient encounters usually have no procedure / short stay
    if enc_type == "Outpatient":
        proc = random.choice(proc_codes)
        los = 0
    else:
        proc = random.choice(proc_codes)
        los = max(0, int(random.gauss(4, 3)))

    age = min(99, max(0, int(random.gauss(58, 18))))
    sex = random.choice(["F", "M"])
    prov = random.choice(provider_ids)
    fac = prov_to_fac[prov]

    # cap the admit offset so discharge (admit + los) never spills past END_DATE
    admit_offset = random.randint(0, max(0, span_days - los))
    admit = start.toordinal() + admit_offset
    admit_date = date.fromordinal(admit)
    discharge_date = date.fromordinal(admit + los)

    # age / LOS multipliers make outcomes realistic
    age_mult = 1.0 + max(0, (age - 65)) * 0.012
    los_mult = 1.0 + los * 0.03

    mortality = 1 if rnd() < base_mort * age_mult else 0
    complication = 1 if rnd() < base_comp * los_mult else 0
    comp_type = random.choice(COMPLICATION_TYPES) if complication else None
    # only survivors can be readmitted
    readmit = 1 if (mortality == 0 and rnd() < base_readm * age_mult) else 0

    charges = round(random.uniform(800, 4000) + los * random.uniform(1500, 4500)
                    + (8000 if proc != "NONE" else 0) * random.uniform(0.5, 2.5), 2)
    paid = round(charges * random.uniform(0.35, 0.85), 2)
    payer = random.choices(PAYER_TYPES, weights=[0.5, 0.3, 0.15, 0.05])[0]

    return (
        f"ENC{i+1:08d}", f"PAT{random.randint(1, NUM_ENCOUNTERS):08d}",
        prov, fac, icd, proc, enc_type, payer,
        admit_date, discharge_date, los, age, sex,
        charges, paid, readmit, mortality, complication, comp_type,
    )

schema = T.StructType([
    T.StructField("encounter_id", T.StringType()),
    T.StructField("patient_id", T.StringType()),
    T.StructField("provider_id", T.StringType()),
    T.StructField("facility_id", T.StringType()),
    T.StructField("primary_icd10_code", T.StringType()),
    T.StructField("primary_procedure_code", T.StringType()),
    T.StructField("encounter_type", T.StringType()),
    T.StructField("payer_type", T.StringType()),
    T.StructField("admit_date", T.DateType()),
    T.StructField("discharge_date", T.DateType()),
    T.StructField("length_of_stay_days", T.IntegerType()),
    T.StructField("patient_age", T.IntegerType()),
    T.StructField("patient_sex", T.StringType()),
    T.StructField("total_charges", T.DoubleType()),
    T.StructField("total_paid", T.DoubleType()),
    T.StructField("readmitted_30d", T.IntegerType()),
    T.StructField("mortality_flag", T.IntegerType()),
    T.StructField("complication_flag", T.IntegerType()),
    T.StructField("complication_type", T.StringType()),
])

# Generate in the driver in batches (80k rows is small) then parallelize.
rows = [gen_row(i) for i in range(NUM_ENCOUNTERS)]
fact = spark.createDataFrame(rows, schema)
fact.write.mode("overwrite").saveAsTable("fact_encounters")
print(f"fact_encounters written: {fact.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add table + column comments (helps Genie + AI/BI Assistant)
# MAGIC Good comments make Genie answers and AI-assisted dashboard authoring noticeably better.
# MAGIC Every column is commented, not just a few.

# COMMAND ----------

TABLE_COMMENTS = {
    "fact_encounters": "One row per patient encounter (inpatient, outpatient, or emergency). Central fact table for the SelectHealth AI/BI workshop. Synthetic data, no PHI.",
    "dim_provider": "Providers (physicians) with specialty and primary facility. One row per provider.",
    "dim_facility": "Hospitals and clinics with type, city, state, and region. One row per facility.",
    "dim_diagnosis": "ICD-10 diagnosis codes with plain-language descriptions and clinical category. One row per code.",
    "dim_procedure": "Procedure codes (CPT-style) with descriptions and category. One row per code.",
}

COLUMN_COMMENTS = {
    "fact_encounters": {
        "encounter_id": "Primary key. Unique identifier for the encounter.",
        "patient_id": "Synthetic patient identifier (a patient can have multiple encounters).",
        "provider_id": "Foreign key to dim_provider. The attending provider.",
        "facility_id": "Foreign key to dim_facility. Where the encounter happened.",
        "primary_icd10_code": "Foreign key to dim_diagnosis. Primary ICD-10 diagnosis code.",
        "primary_procedure_code": "Foreign key to dim_procedure. Primary procedure code (NONE if no procedure).",
        "encounter_type": "Encounter setting: Inpatient, Outpatient, or Emergency.",
        "payer_type": "Payer category: Commercial, Medicare, Medicaid, or Self-Pay.",
        "admit_date": "Date the patient was admitted or seen.",
        "discharge_date": "Date the patient was discharged (same day as admit for outpatient).",
        "length_of_stay_days": "Number of days between admit and discharge.",
        "patient_age": "Patient age in years at the time of the encounter.",
        "patient_sex": "Patient sex: F or M.",
        "total_charges": "Total billed charges in USD.",
        "total_paid": "Total amount paid in USD.",
        "readmitted_30d": "1 if the patient was readmitted within 30 days of discharge, else 0.",
        "mortality_flag": "1 if the patient died during the encounter, else 0.",
        "complication_flag": "1 if a complication occurred during the encounter, else 0.",
        "complication_type": "Type of complication when complication_flag = 1, else null.",
    },
    "dim_provider": {
        "provider_id": "Primary key. Unique identifier for the provider.",
        "npi": "10-digit National Provider Identifier (synthetic).",
        "provider_name": "Provider display name.",
        "specialty": "Clinical specialty (e.g., Cardiology, Orthopedic Surgery).",
        "primary_facility_id": "Foreign key to dim_facility. The provider's primary facility.",
    },
    "dim_facility": {
        "facility_id": "Primary key. Unique identifier for the facility.",
        "facility_name": "Hospital or clinic name.",
        "facility_type": "Facility type (e.g., Inpatient Hospital, Outpatient Clinic).",
        "city": "City where the facility is located.",
        "state": "Two-letter state code.",
        "region": "Business region grouping for the facility.",
    },
    "dim_diagnosis": {
        "icd10_code": "Primary key. ICD-10 diagnosis code.",
        "icd10_description": "Plain-language description of the diagnosis.",
        "clinical_category": "Clinical grouping (e.g., Cardiovascular, Orthopedic, Oncology).",
    },
    "dim_procedure": {
        "procedure_code": "Primary key. CPT-style procedure code (NONE means no procedure).",
        "procedure_description": "Plain-language description of the procedure.",
        "procedure_category": "Procedure grouping (e.g., Orthopedic Surgery, Cardiac Procedure).",
    },
}

for tbl, comment in TABLE_COMMENTS.items():
    spark.sql(f"COMMENT ON TABLE {tbl} IS '{comment.replace(chr(39), chr(39)*2)}'")
for tbl, cols in COLUMN_COMMENTS.items():
    for col, comment in cols.items():
        safe = comment.replace("'", "''")  # escape quotes so comments stay valid SQL
        spark.sql(f"ALTER TABLE {tbl} ALTER COLUMN {col} COMMENT '{safe}'")

print("Comments applied to all tables and columns.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add primary key and foreign key relationships
# MAGIC These are informational (RELY) constraints in Unity Catalog. They are not enforced at
# MAGIC write time, but Genie and the AI/BI Assistant use them to join tables automatically and
# MAGIC to understand the star schema. Primary-key columns must be NOT NULL first.

# COMMAND ----------

# 1) Make key columns NOT NULL (required before a PRIMARY KEY can be added).
NOT_NULL = {
    "dim_provider": "provider_id",
    "dim_facility": "facility_id",
    "dim_diagnosis": "icd10_code",
    "dim_procedure": "procedure_code",
    "fact_encounters": "encounter_id",
}
for tbl, col in NOT_NULL.items():
    spark.sql(f"ALTER TABLE {tbl} ALTER COLUMN {col} SET NOT NULL")

# 2) Primary keys (drop-if-exists pattern keeps the notebook re-runnable).
PRIMARY_KEYS = {
    "dim_provider": ("pk_dim_provider", "provider_id"),
    "dim_facility": ("pk_dim_facility", "facility_id"),
    "dim_diagnosis": ("pk_dim_diagnosis", "icd10_code"),
    "dim_procedure": ("pk_dim_procedure", "procedure_code"),
    "fact_encounters": ("pk_fact_encounters", "encounter_id"),
}
for tbl, (name, col) in PRIMARY_KEYS.items():
    spark.sql(f"ALTER TABLE {tbl} DROP CONSTRAINT IF EXISTS {name}")
    spark.sql(f"ALTER TABLE {tbl} ADD CONSTRAINT {name} PRIMARY KEY ({col})")

# 3) Foreign keys (fact -> dims, and provider -> facility).
FOREIGN_KEYS = [
    ("fact_encounters", "fk_enc_provider",  "provider_id",            "dim_provider",  "provider_id"),
    ("fact_encounters", "fk_enc_facility",  "facility_id",            "dim_facility",  "facility_id"),
    ("fact_encounters", "fk_enc_diagnosis", "primary_icd10_code",     "dim_diagnosis", "icd10_code"),
    ("fact_encounters", "fk_enc_procedure", "primary_procedure_code", "dim_procedure", "procedure_code"),
    ("dim_provider",    "fk_provider_facility", "primary_facility_id", "dim_facility", "facility_id"),
]
for tbl, name, col, ref_tbl, ref_col in FOREIGN_KEYS:
    spark.sql(f"ALTER TABLE {tbl} DROP CONSTRAINT IF EXISTS {name}")
    spark.sql(f"ALTER TABLE {tbl} ADD CONSTRAINT {name} FOREIGN KEY ({col}) REFERENCES {ref_tbl} ({ref_col})")

print("Primary keys and foreign keys applied. Dataset fully documented and ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick sanity checks

# COMMAND ----------

display(spark.sql("""
  SELECT f.region_check, cnt, ROUND(100.0*readmits/cnt,1) AS readmit_pct,
         ROUND(100.0*deaths/cnt,2) AS mortality_pct, ROUND(100.0*comps/cnt,1) AS complication_pct
  FROM (
    SELECT fac.region AS region_check, COUNT(*) cnt,
           SUM(readmitted_30d) readmits, SUM(mortality_flag) deaths, SUM(complication_flag) comps
    FROM fact_encounters e JOIN dim_facility fac USING (facility_id)
    GROUP BY fac.region
  ) f ORDER BY cnt DESC
"""))
