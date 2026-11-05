# Databricks notebook source
# MAGIC %md
# MAGIC # Hands on 3 Lab: Fix and Understand a Broken Notebook
# MAGIC
# MAGIC A teammate left this notebook half-finished. It is supposed to report **readmissions by facility**, but
# MAGIC every cell has a problem. Your job is to fix it **without hand-editing the code**, using Genie Code's built-in
# MAGIC assistance: **Quick Fix**, **Diagnose error**, and the **/explain**, **/doc**, and **/optimize** slash commands.
# MAGIC
# MAGIC Run each cell top to bottom. When a cell fails, use the tool the step tells you to use.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "dev-sh-training", "Catalog")
dbutils.widgets.text("schema", "aibi_workshop", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FQ = f"`{CATALOG}`.`{SCHEMA}`"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load encounters (try **Quick Fix**)
# MAGIC This cell has a simple error. When it fails, click **Quick Fix** under the error, then **Accept and run**.

# COMMAND ----------

from pyspark.sql import functions as F

enc = spark.table(f"{FQ}.fact_encounters")
inpatient = enc.filter(F.col("encounter_type") == "Inpatient").select("encounter_id", "facility_id", "readmited_30d", "admit_date")
display(inpatient.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Join to facilities (try **Diagnose error**)
# MAGIC This one fails for a less obvious reason. Click **Diagnose error** and let Genie Code work out the cause.

# COMMAND ----------

fac = spark.table(f"{FQ}.dim_facility")
by_fac = (inpatient.join(fac, inpatient.facility_id == fac.facility_id, "inner")
          .groupBy("facility_id", "facility_name")
          .agg(F.count("*").alias("encounters"), F.avg("readmitted_30d").alias("readmit_rate")))
display(by_fac.orderBy(F.desc("readmit_rate")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Understand someone else's code (try **/explain** and **/doc**)
# MAGIC This cell runs, but nobody remembers what it does. Put your cursor in the cell, open inline Genie Code
# MAGIC (`Cmd+I` / `Ctrl+I`), and type `/explain`. Then try `/doc` to add comments, and accept the diff.

# COMMAND ----------

w = __import__("pyspark.sql.window", fromlist=["Window"]).Window.partitionBy("facility_id").orderBy(F.col("admit_date"))
x = (enc.filter("encounter_type = 'Inpatient'")
     .withColumn("m", F.date_trunc("month", "admit_date"))
     .groupBy("facility_id", "m").agg(F.avg("readmitted_30d").alias("r"))
     .withColumn("r3", F.avg("r").over(Window.partitionBy("facility_id").orderBy("m").rowsBetween(-2, 0)) if False else F.avg("r").over(__import__("pyspark.sql.window", fromlist=["Window"]).Window.partitionBy("facility_id").orderBy("m").rowsBetween(-2, 0))))
display(x.orderBy("facility_id", "m"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Make it scale (try **/optimize**)
# MAGIC This works on a sample but pulls the whole table to the driver and loops in Python. Open inline Genie Code
# MAGIC and type `/optimize`. Compare the before and after.

# COMMAND ----------

pdf = spark.table(f"{FQ}.fact_encounters").toPandas()
results = {}
for _, row in pdf.iterrows():
    key = (row["facility_id"], row["payer_type"])
    tot, re = results.get(key, (0, 0))
    results[key] = (tot + 1, re + row["readmitted_30d"])
rows = [(k[0], k[1], v[0], round(100.0 * v[1] / v[0], 2)) for k, v in results.items()]
display(spark.createDataFrame(rows, "facility_id string, payer_type string, encounters long, readmit_pct double"))
