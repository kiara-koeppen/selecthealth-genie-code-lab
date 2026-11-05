# Databricks notebook source
# MAGIC %md
# MAGIC # Copy the lab folder into every attendee's home folder
# MAGIC
# MAGIC Run this **once, as a workspace admin**, after you've created the Git folder for this repo. It copies
# MAGIC `genie_code_lab/` into `/Users/<attendee>/genie_code_lab` for every member of the attendee group, so nobody
# MAGIC has to download, clone, or import anything.
# MAGIC
# MAGIC **Why per-person copies and not one shared folder?**
# MAGIC - Hands on 3 edits `02_broken_readmissions` in place (Quick Fix, `/doc`), so a shared copy would be fixed by
# MAGIC   the first person and already working for everyone else.
# MAGIC - Hands on 4 has each person create a notebook *inside* `genie_code_lab` so Genie Code picks up `AGENTS.md`.
# MAGIC
# MAGIC Start with `dry_run = yes` to see who gets a copy, then flip it to `no`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC - `source_folder`: the `genie_code_lab` folder inside your Git folder, for example
# MAGIC   `/Workspace/Users/you@selecthealth.org/selecthealth-genie-code-lab/genie_code_lab`
# MAGIC - `attendee_group`: the account-level group assigned to this workspace (users directly in the group only)
# MAGIC - `extra_emails`: optional comma-separated list, for people not in the group
# MAGIC - `target_folder_name`: folder name created in each home folder
# MAGIC - `overwrite`: `no` skips anyone who already has the folder (safe to rerun); `yes` replaces it (resets their work)

# COMMAND ----------

dbutils.widgets.text("source_folder", "", "Source genie_code_lab folder (in your Git folder)")
dbutils.widgets.text("attendee_group", "", "Attendee group name")
dbutils.widgets.text("extra_emails", "", "Extra attendee emails (comma-separated)")
dbutils.widgets.text("target_folder_name", "genie_code_lab", "Folder name in each home folder")
dbutils.widgets.dropdown("overwrite", "no", ["no", "yes"], "Replace existing copies?")
dbutils.widgets.dropdown("dry_run", "yes", ["yes", "no"], "Dry run (list only)?")

SOURCE = dbutils.widgets.get("source_folder").rstrip("/")
GROUP = dbutils.widgets.get("attendee_group").strip()
EXTRA = [e.strip() for e in dbutils.widgets.get("extra_emails").split(",") if e.strip()]
TARGET_NAME = dbutils.widgets.get("target_folder_name").strip()
OVERWRITE = dbutils.widgets.get("overwrite") == "yes"
DRY_RUN = dbutils.widgets.get("dry_run") == "yes"
assert SOURCE, "Set source_folder to the genie_code_lab folder inside your Git folder."
assert GROUP or EXTRA, "Set attendee_group and/or extra_emails."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Who gets a copy
# MAGIC Reads the group's direct user members (nested groups are listed but not expanded) plus any extra emails.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectType, ExportFormat, ImportFormat

w = WorkspaceClient()  # runs as you, the admin running this notebook

attendees, nested = set(EXTRA), []
if GROUP:
    groups = list(w.groups.list(filter=f'displayName eq "{GROUP}"', attributes="id,displayName,members"))
    assert groups, f"Group {GROUP!r} not found in this workspace. Is the account group assigned to the workspace?"
    for m in groups[0].members or []:
        if m.ref and "Users" in m.ref:
            attendees.add(w.users.get(m.value).user_name)
        else:
            nested.append(m.display)
attendees = sorted(attendees)
print(f"{len(attendees)} attendees" + (f"; nested groups NOT expanded: {nested}" if nested else ""))
for a in attendees:
    print(" ", a)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read the source folder once
# MAGIC Notebooks are copied as source; plain files (`AGENTS.md`, the skill folder, the mapping YAML) as raw files.

# COMMAND ----------

items = []  # (relative_path, object_type, language, bytes)
for obj in w.workspace.list(SOURCE, recursive=True):
    rel = obj.path[len(SOURCE):].lstrip("/")
    if obj.object_type == ObjectType.DIRECTORY:
        items.append((rel, obj.object_type, None, None))
    elif obj.object_type == ObjectType.NOTEBOOK:
        items.append((rel, obj.object_type, obj.language, w.workspace.download(obj.path, format=ExportFormat.SOURCE).read()))
    elif obj.object_type == ObjectType.FILE:
        items.append((rel, obj.object_type, None, w.workspace.download(obj.path, format=ExportFormat.AUTO).read()))
print(f"{sum(1 for i in items if i[1] != ObjectType.DIRECTORY)} files/notebooks to copy from {SOURCE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Copy into each home folder
# MAGIC Failures (for example, someone who has never logged in and has no home folder yet) are collected and listed
# MAGIC at the end, so one bad account doesn't stop the run. Rerun later with `overwrite = no` to pick them up.

# COMMAND ----------

import io

def exists(path):
    try:
        w.workspace.get_status(path)
        return True
    except Exception:
        return False

done, skipped, failed = [], [], []
for user in attendees:
    dest = f"/Users/{user}/{TARGET_NAME}"
    if DRY_RUN:
        print("would copy to", dest)
        continue
    try:
        if exists(dest) and not OVERWRITE:
            skipped.append(user)
            continue
        w.workspace.mkdirs(dest)
        for rel, kind, lang, data in sorted(items, key=lambda i: i[0].count("/")):
            path = f"{dest}/{rel}" if rel else dest
            if kind == ObjectType.DIRECTORY:
                w.workspace.mkdirs(path)
            elif kind == ObjectType.NOTEBOOK:
                w.workspace.upload(path, io.BytesIO(data), format=ImportFormat.SOURCE, language=lang, overwrite=True)
            else:
                w.workspace.upload(path, io.BytesIO(data), format=ImportFormat.AUTO, overwrite=True)
        done.append(user)
    except Exception as e:
        failed.append((user, str(e)[:200]))

if not DRY_RUN:
    print(f"copied: {len(done)}  skipped (already had it): {len(skipped)}  failed: {len(failed)}")
    for user, err in failed:
        print("FAILED", user, "->", err)
