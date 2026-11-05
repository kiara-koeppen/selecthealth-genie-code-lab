# Genie Code Lab: SelectHealth (Nov 10, 2026)

Lab files for a 2-hour, hands-on Genie Code training. Everything here runs on **100% synthetic data**. There is no
real member data and no PHI anywhere in this repo.

Attendees don't clone this repo. The instructor creates one Git folder from it in the training workspace and runs
a one-time script that puts a personal copy of `genie_code_lab/` in every attendee's home folder.

## What the lab covers
| Part | Hands on | Uses |
|---|---|---|
| 1. What it can do | 1 Explore and document a messy extract · 2 Build a silver table · 3 Fix, explain, optimize | `claims_extract_raw`, `02_broken_readmissions` |
| 2. Make it yours | 4 Instructions + `AGENTS.md` · 5 Skills · 6 Vital Skills (healthcare) | `assistant_instructions_selecthealth.md`, `AGENTS.md`, `skills/selecthealth-encounter-dq/`, `vital_skills/` |
| 3. Run it safely, at scale | 7 Governance (+ `phi-deidentifier` demo) · Demo: Genie Code in Jobs | `dim_member`, `member_encounter_detail`, `03_dq_autopilot_check`, `instructor/jobs_demo_job.json` |

## Architecture / data flow
```
instructor/generate_workshop_data.py        (once) synthetic encounters star schema
        │   fact_encounters + dim_provider, dim_facility, dim_diagnosis, dim_procedure
        ▼
instructor/00_genie_code_lab_setup.py       (once) lab objects on top of it
        │   claims_extract_raw   messy, undocumented extract (cryptic names, codes, dirty rows)
        │   dim_member           synthetic contact table with a column mask
        │   member_encounter_detail   synthetic PHI-shaped table for the phi-deidentifier demo
        │   readmission_rate_official()   governed metric as a UC function
        ▼
attendees (in their own genie_code_lab copy)
        │   [INITIALS]_silver_v1 → [INITIALS]_silver_encounters → [INITIALS]_dq_results
        ▼
instructor (shared copies)
            Vital Skills semantic-builder metric view (one shared schema)
            Encounter DQ autopilot job: run_dq → has_blockers → Genie Code task
```

## Folder structure
```
genie_code_lab/                     ← copied to every attendee: /Users/<attendee>/genie_code_lab
  02_broken_readmissions.py         Hands on 3: notebook with planted bugs
  03_dq_autopilot_check.py          Jobs demo task 1: runs the DQ checks, publishes task values
  99_answer_key.py                  Tested fallback for every prompt
  AGENTS.md                         Hands on 4: project context Genie Code reads automatically
  assistant_instructions_selecthealth.md   Hands on 4: paste into your personal instructions file
  skills/selecthealth-encounter-dq/ Hands on 5: SKILL.md, dq_rules.md, scripts/run_dq_checks.py
  vital_skills/selecthealth_mapping.yaml   Hands on 6: tested semantic-builder mapping (instructor deploys)
instructor/                         ← instructor only
  generate_workshop_data.py         Builds the base synthetic dataset
  00_genie_code_lab_setup.py        Builds the lab objects and grants
  copy_lab_to_attendees.py          Copies genie_code_lab/ into every attendee's home folder
  jobs_demo_job.json                Job definition for the Genie Code in Jobs demo
```
`03_dq_autopilot_check` and `99_answer_key` load the skill's script by relative path, so keep them in
`genie_code_lab/` next to `skills/`.

## Prerequisites
- A Databricks workspace with Unity Catalog, serverless compute, and a SQL warehouse
- **Genie Code with partner-powered AI enabled** (account and workspace). Without it, Genie Code has no agentic
  features and the lab won't work. On Azure, check the cross-Geo processing setting.
- Workspace admin (or equivalent) for the instructor: to run the copy script, create grants, and set the column mask
- An **account-level** attendee group assigned to the workspace
- For Hands on 6: the [Vital Skills](https://github.com/databricks-industry-solutions/vital-skills) repo (public)

## Setup (instructor, before the session)
1. **Create a Git folder** from this repo: Workspace → Create → Git folder →
   `https://github.com/kiara-koeppen/selecthealth-genie-code-lab`.
2. **Base data** (skip if the tables already exist): run `instructor/generate_workshop_data.py` with the
   `catalog` and `schema` widgets set (for example `dev-sh-training` / `aibi_workshop`).
3. **Lab objects:** run `instructor/00_genie_code_lab_setup.py` with the same `catalog` / `schema`,
   `admin_group` = a group attendees are **not** in, `training_group` = the attendee group, `apply_answer_key = no`.
4. **Attendee copies:** run `instructor/copy_lab_to_attendees.py` with `source_folder` = this Git folder's
   `genie_code_lab` path and `attendee_group` = the attendee group. Run with `dry_run = yes` first, then `no`.
   It's safe to rerun (it skips people who already have a copy unless `overwrite = yes`).
5. **Vital Skills:** create one Git folder from the Vital Skills repo in a shared location (for example
   `/Workspace/Shared/vital-skills`) and grant the attendee group **Can Read**. Attendees register its `skills`
   subfolder under Genie Code → Customizations → Add folder.
6. **semantic-builder (Hands on 6):** edit `genie_code_lab/vital_skills/selecthealth_mapping.yaml` (target schema,
   `warehouse_id`), run the skill's `generate_semantic_layer.py --mapping <yaml>`, and deploy the generated SQL to
   that schema. The catalog name is wrapped in backticks in the YAML because the generator doesn't quote names.
7. **Jobs demo:** create a job from `instructor/jobs_demo_job.json` (replace `<PATH_TO_GIT_FOLDER>`), then add the
   Genie Code task in the Jobs UI (Add task → Genie Code; prompt is in the workbook). Do one full run the day before.

## Configuration
Every notebook is parameterized with `dbutils.widgets`; nothing is hardcoded.

| Notebook | Widgets |
|---|---|
| `generate_workshop_data` | `catalog`, `schema`, `num_encounters`, `num_providers`, `num_facilities`, `start_date`, `end_date` |
| `00_genie_code_lab_setup` | `catalog`, `schema`, `admin_group`, `seed`, `training_group`, `apply_answer_key` |
| `copy_lab_to_attendees` | `source_folder`, `attendee_group`, `extra_emails`, `target_folder_name`, `overwrite`, `dry_run` |
| `02_broken_readmissions`, `03_dq_autopilot_check`, `99_answer_key` | `catalog`, `schema` (+ `table`, `initials`, `vital_skills_schema` where used) |

## Resetting between sessions
Rerun `00_genie_code_lab_setup` (it strips the comments attendees add to `claims_extract_raw`) and rerun the copy
script with `overwrite = yes` to give everyone a fresh, still-broken `02_broken_readmissions`.
