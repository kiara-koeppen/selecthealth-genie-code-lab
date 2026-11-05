# Databricks notebook source
# MAGIC %md
# MAGIC # Jobs Demo, Task 1: Run the DQ sign-off checks
# MAGIC
# MAGIC First task of the **Encounter DQ autopilot** job used in the "Genie Code in Jobs" demo. It runs the same
# MAGIC 8 checks as the `selecthealth-encounter-dq` skill on one table, then publishes **task values** so the next
# MAGIC tasks can use them:
# MAGIC
# MAGIC | Task value | Example | Used by |
# MAGIC |---|---|---|
# MAGIC | `blocker_failures` | `4` | the `has_blockers` If/else condition |
# MAGIC | `failed_checks` | `unique_encounter_id, valid_payer, ...` | the Genie Code task prompt |
# MAGIC | `table` | `` `demo`.`selecthealth_workshop`.claims_extract_raw `` | the Genie Code task prompt |
# MAGIC | `results_table` | `` `demo`.`selecthealth_workshop`.`kk_dq_results` `` | the Genie Code task prompt |
# MAGIC
# MAGIC The Genie Code task only runs when there is something to investigate, which keeps the agent's cost proportional to the problem.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev-sh-training", "Catalog")
dbutils.widgets.text("schema", "aibi_workshop", "Schema")
dbutils.widgets.text("table", "claims_extract_raw", "Table to check")
dbutils.widgets.text("initials", "demo", "Initials (results table prefix)")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
TABLE = dbutils.widgets.get("table")
INITIALS = dbutils.widgets.get("initials").lower().strip()
FQ = f"`{CATALOG}`.`{SCHEMA}`"
table_fq = f"{FQ}.{TABLE}"
results_fq = f"{FQ}.`{INITIALS}_dq_results`"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the checks
# MAGIC Reuses the skill's reference script so the job and the interactive skill always apply identical rules.

# COMMAND ----------

import os
skill_script = os.path.join(os.getcwd(), "skills", "selecthealth-encounter-dq", "scripts", "run_dq_checks.py")
exec(open(skill_script).read())
results, certified = run_dq_checks(spark, table_fq, results_fq)
display(results.select("check_name", "severity", "failed_rows", "status"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publish task values for downstream tasks

# COMMAND ----------

rows = results.collect()
failed_blockers = [r.check_name for r in rows if r.severity == "BLOCKER" and r.status == "FAIL"]
dbutils.jobs.taskValues.set(key="blocker_failures", value=len(failed_blockers))
dbutils.jobs.taskValues.set(key="failed_checks", value=", ".join(failed_blockers) or "none")
dbutils.jobs.taskValues.set(key="table", value=table_fq)
dbutils.jobs.taskValues.set(key="results_table", value=results_fq)
print(f"{table_fq}: {'CERTIFIED' if certified else 'NOT CERTIFIED'}; blocker failures: {failed_blockers or 'none'}")
