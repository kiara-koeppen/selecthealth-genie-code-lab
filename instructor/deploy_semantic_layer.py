# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy the Vital Skills `semantic-builder` metric view (Hands on 6 backup)
# MAGIC
# MAGIC In the session, attendees watch `semantic-builder` profile the data and draft a mapping. The instructor then
# MAGIC deploys **one shared copy** so the room queries the same metric view. This notebook does that deploy in one
# MAGIC run, from the tested mapping in this repo, so you aren't depending on a live agent run in front of the room.
# MAGIC
# MAGIC What it does:
# MAGIC 1. Runs the skill's own generator (`generate_semantic_layer.py`) on `genie_code_lab/vital_skills/selecthealth_mapping.yaml`
# MAGIC 2. Executes the generated SQL: 3 views (`encounter`, `provider`, `facility`) and the metric view
# MAGIC    `mv_care_delivery__encounter`, all in `target_schema`
# MAGIC 3. Queries the standard readmission rate and compares it to the naive average
# MAGIC
# MAGIC **Permissions you need:** `CREATE TABLE` (covers views) and `SELECT` on the schema. No admin needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC - `vital_skills_folder`: your Vital Skills Git folder, for example `/Workspace/Users/you@selecthealth.org/vital-skills`
# MAGIC - `catalog` / `source_schema`: where the lab encounters tables live
# MAGIC - `target_schema`: where the views go. Defaults to the lab schema: none of the 4 view names exist there.

# COMMAND ----------

dbutils.widgets.text("vital_skills_folder", "", "Vital Skills Git folder path")
dbutils.widgets.text("catalog", "dev-sh-training", "Catalog")
dbutils.widgets.text("source_schema", "aibi_workshop", "Schema with fact_encounters / dim_*")
dbutils.widgets.text("target_schema", "aibi_workshop", "Schema for the generated views")

VS_FOLDER = dbutils.widgets.get("vital_skills_folder").rstrip("/")
CATALOG = dbutils.widgets.get("catalog").strip()
SOURCE_SCHEMA = dbutils.widgets.get("source_schema").strip()
TARGET_SCHEMA = dbutils.widgets.get("target_schema").strip()
assert VS_FOLDER, "Set vital_skills_folder to your Vital Skills Git folder."
if not VS_FOLDER.startswith("/Workspace"):
    VS_FOLDER = "/Workspace" + VS_FOLDER

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Point the tested mapping at this workspace
# MAGIC The repo's mapping targets `` `dev-sh-training`.aibi_workshop ``. This rewrites catalog and schemas from the
# MAGIC widgets. Names are wrapped in backticks because the generator doesn't quote them (`dev-sh-training` has hyphens).

# COMMAND ----------

import os, re, subprocess, sys, tempfile

lab_folder = os.path.dirname(os.getcwd())  # this notebook lives in <repo>/instructor
mapping_src = os.path.join(lab_folder, "genie_code_lab", "vital_skills", "selecthealth_mapping.yaml")
generator = os.path.join(VS_FOLDER, "skills", "semantic-builder", "scripts", "generate_semantic_layer.py")
assert os.path.exists(mapping_src), f"Mapping not found at {mapping_src}. Run this notebook from the repo's instructor folder."
assert os.path.exists(generator), f"Generator not found at {generator}. Check vital_skills_folder."

m = open(mapping_src).read()
m = m.replace('"`dev-sh-training`', f'"`{CATALOG}`').replace("`dev-sh-training`.aibi_workshop.", f"`{CATALOG}`.`{SOURCE_SCHEMA}`.")
m = re.sub(r"^(  catalog:\s+).*$", rf'\1"`{CATALOG}`"', m, flags=re.M)
m = re.sub(r"^(  schema:\s+).*$", rf'\1"`{TARGET_SCHEMA}`"', m, count=1, flags=re.M)
work = tempfile.mkdtemp()
mapping = os.path.join(work, "mapping.yaml")
open(mapping, "w").write(m)
print(re.findall(r"^\s+(?:catalog|schema|source):.*$", m, flags=re.M))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate the SQL with the skill's own generator

# COMMAND ----------

out_dir = os.path.join(work, "generated")
r = subprocess.run([sys.executable, generator, "--mapping", mapping, "--out", out_dir], capture_output=True, text=True)
print(r.stdout[-2000:], r.stderr[-2000:])
assert r.returncode == 0, "Generator failed (see output above)."
print(open(os.path.join(out_dir, "plan.md")).read())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run the generated SQL
# MAGIC Silver views first, then the metric view. Statements run one at a time so any error points at the exact one.

# COMMAND ----------

def statements(path):
    sql = re.sub(r"^\s*--.*$", "", open(path).read(), flags=re.M)
    return [s.strip() for s in re.split(r";\s*\n", sql) if s.strip()]

for f in ["01_silver.sql", "03_metric_views.sql"]:
    for stmt in statements(os.path.join(out_dir, f)):
        spark.sql(stmt.rstrip(";"))
        print("OK:", stmt.splitlines()[0][:110])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Check the result
# MAGIC Expected (lab data): about **15.1%** on about 34,600 index admissions, versus about **14.5%** naive.

# COMMAND ----------

T = f"`{CATALOG}`.`{TARGET_SCHEMA}`"
display(spark.sql(f"""
SELECT 'semantic-builder standard (index admissions)' AS definition,
       ROUND(100 * MEASURE(readmission_30day_rate), 2) AS readmit_pct,
       MEASURE(index_admission_count) AS denominator
FROM {T}.mv_care_delivery__encounter
UNION ALL
SELECT 'naive (all encounters)', ROUND(100.0 * AVG(readmitted_30d), 2), COUNT(*)
FROM `{CATALOG}`.`{SOURCE_SCHEMA}`.fact_encounters
"""))
