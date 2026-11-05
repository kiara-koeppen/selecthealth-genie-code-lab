# Genie Code Lab: SelectHealth (Nov 10, 2026)

Lab files for a 2-hour, hands-on Genie Code training. Everything here runs on **100% synthetic data**. There is no
real member data and no PHI anywhere in this repo.

Each attendee creates **one Git folder** from this repo in the training workspace. That gives everyone a personal
copy (the lab edits notebooks in place and creates files inside `genie_code_lab/`, so copies can't be shared).
The instructor runs two setup notebooks beforehand. **No workspace admin rights are needed for either.**

## What the lab covers
| Part | Hands on | Uses |
|---|---|---|
| 1. What it can do | 1 Explore and document a messy extract · 2 Build a silver table · 3 Fix, explain, optimize | `claims_extract_raw`, `02_broken_readmissions` |
| 2. Make it yours | 4 Instructions + `AGENTS.md` · 5 Skills · 6 Vital Skills (healthcare) | `assistant_instructions_selecthealth.md`, `AGENTS.md`, `skills/selecthealth-encounter-dq/`, `vital_skills/` |
| 3. Run it safely, at scale | 7 Governance (+ `phi-deidentifier` demo) · Demo: Genie Code in Jobs | `dim_member`, `member_encounter_detail`, `03_dq_autopilot_check`, `instructor/jobs_demo_job.json` |

## Architecture / data flow
```
Base dataset (already in the workspace from the Oct 1 workshop; instructor/generate_workshop_data.py rebuilds it)
        │   `dev-sh-training`.aibi_workshop: fact_encounters + dim_provider, dim_facility, dim_diagnosis, dim_procedure
        ▼
instructor/00_genie_code_lab_setup.py        (instructor, once) adds the lab objects to the same schema
        │   claims_extract_raw        messy, undocumented extract (cryptic names, codes, dirty rows)
        │   dim_member                synthetic contact table, column mask (only the instructor sees real values)
        │   member_encounter_detail   synthetic PHI-shaped table for the phi-deidentifier demo
        │   readmission_rate_official()   governed metric as a UC function
        ▼
attendees, each in their own Git folder (…/selecthealth-genie-code-lab/genie_code_lab)
        │   [INITIALS]_silver_v1 → [INITIALS]_silver_encounters → [INITIALS]_dq_results
        ▼
instructor (one shared copy each)
            instructor/deploy_semantic_layer.py → Vital Skills metric view mv_care_delivery__encounter
            Encounter DQ autopilot job: run_dq → has_blockers → Genie Code task
```

## Folder structure
```
genie_code_lab/                     ← what attendees work in
  02_broken_readmissions.py         Hands on 3: notebook with planted bugs
  03_dq_autopilot_check.py          Jobs demo task 1: runs the DQ checks, publishes task values
  99_answer_key.py                  Tested fallback for every prompt
  AGENTS.md                         Hands on 4: project context Genie Code reads automatically
  assistant_instructions_selecthealth.md   Hands on 4: paste into your personal instructions file
  skills/selecthealth-encounter-dq/ Hands on 5: SKILL.md, dq_rules.md, scripts/run_dq_checks.py
  vital_skills/selecthealth_mapping.yaml   Hands on 6: tested semantic-builder mapping
instructor/                         ← instructor only
  generate_workshop_data.py         Rebuilds the base synthetic dataset (only if it's missing)
  00_genie_code_lab_setup.py        Builds the lab objects (and optional grants)
  deploy_semantic_layer.py          Deploys the Vital Skills metric view from the tested mapping
  jobs_demo_job.json                Job definition for the Genie Code in Jobs demo
```
`03_dq_autopilot_check` and `99_answer_key` load the skill's script by relative path, so keep them in
`genie_code_lab/` next to `skills/`.

## Prerequisites
- Databricks workspace with Unity Catalog, serverless compute, and a SQL warehouse
- **Genie Code with partner-powered AI enabled** (account and workspace). Without it, Genie Code has no agentic
  features and the lab won't work. On Azure, check the cross-Geo processing setting.
- Workspace access to **public GitHub** for Git folders (this repo and Vital Skills)
- Instructor: `USE CATALOG`, plus `USE SCHEMA`, `SELECT`, `CREATE TABLE`, `CREATE FUNCTION` on the lab schema
- Attendees: `USE CATALOG`, plus `USE SCHEMA`, `SELECT`, `CREATE TABLE`, `MODIFY` on the lab schema (the Oct 1 setup)

## Setup (instructor, before the session)
1. **Your Git folder:** Workspace → Create → Git folder → `https://github.com/kiara-koeppen/selecthealth-genie-code-lab`.
2. **Base data:** confirm `` `dev-sh-training`.aibi_workshop.fact_encounters `` has ~80,000 rows. Only if it's
   missing, run `instructor/generate_workshop_data.py`.
3. **Lab objects:** run `instructor/00_genie_code_lab_setup.py` (defaults already point at
   `` `dev-sh-training`.aibi_workshop ``). Leave `unmasked_user` blank (you) and `apply_answer_key = no`. Set
   `training_group` only if you own the schema; otherwise the cell prints the grant statements to send to an owner.
4. **Vital Skills:** create your own Git folder from `https://github.com/databricks-industry-solutions/vital-skills`
   (needed by step 5). Attendees create their own in Hands on 6 and register its `skills` subfolder under
   Genie Code → Customizations → Add folder.
5. **Metric view backup:** run `instructor/deploy_semantic_layer.py` with `vital_skills_folder` set. Expect ~15.1%
   standard vs ~14.5% naive. (Or deploy live in the session with the skill, and keep this as the fallback.)
6. **Jobs demo:** create a job from `instructor/jobs_demo_job.json` (replace `<PATH_TO_GIT_FOLDER>` with your Git
   folder path), then add the Genie Code task in the Jobs UI (Add task → Genie Code; prompt is in the workbook).
   Do one full run the day before.

## Attendee setup (first 2 minutes of the session)
Workspace → Create → Git folder → paste `https://github.com/kiara-koeppen/selecthealth-genie-code-lab` → Create.
Everything they need is in `genie_code_lab/` inside it. In Hands on 5 they register `genie_code_lab/skills` under
Genie Code → Customizations → Add folder, and in Hands on 6 they add a second Git folder for Vital Skills. Every notebook's widgets already default to
`` `dev-sh-training`.aibi_workshop ``.

## Configuration
Every notebook is parameterized with `dbutils.widgets`; nothing is hardcoded.

| Notebook | Widgets |
|---|---|
| `generate_workshop_data` | `catalog`, `schema`, `num_encounters`, `num_providers`, `num_facilities`, `start_date`, `end_date` |
| `00_genie_code_lab_setup` | `catalog`, `schema`, `unmasked_user`, `seed`, `training_group`, `apply_answer_key` |
| `deploy_semantic_layer` | `vital_skills_folder`, `catalog`, `source_schema`, `target_schema` |
| `02_broken_readmissions`, `03_dq_autopilot_check`, `99_answer_key` | `catalog`, `schema` (+ `table`, `initials`, `vital_skills_schema` where used) |

## Resetting between sessions
Rerun `00_genie_code_lab_setup` (it strips the comments attendees add to `claims_extract_raw`). Attendees who want a
fresh, still-broken `02_broken_readmissions` can discard their changes in the Git folder (Git dialog → Reset).
