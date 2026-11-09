---
name: selecthealth-encounter-dq
description: Run SelectHealth's standard data quality checks on an encounters or claims table and write the results to a dq_results table. Use when someone asks to check data quality, validate, profile issues, or certify an encounters, claims, or silver encounters table.
---

# SelectHealth encounter data quality checks

Use this skill whenever a user asks you to check, validate, or certify the quality of an encounter-level table
(raw extract, silver encounters, or fact_encounters). These are the checks our data team signs off on. Do not
invent extra checks or skip any of these, and always use the severities below.

## Steps

1. Identify the table and map its columns to the logical fields in `dq_rules.md` (the raw extract uses
   cryptic names like `enc_id`, `pyr_cd`, `adm_dt`; the silver and fact tables use full names). If a field is
   missing, report the check as `SKIPPED` rather than guessing.
2. Run every rule in `dq_rules.md`. The reference implementation is `scripts/run_dq_checks.py`. Read it, then
   put its code **into a notebook cell** (the functions plus your call to `run_dq_checks`), adapting only the column
   mapping, and run that cell. Do **not** run the file with `exec(open(...).read())`, `%run`, or `subprocess`:
   code that isn't visible in the cell can't be reviewed, and approval mode blocks it.
3. Write one row per rule to a results table named `<user initials>_dq_results` in the same schema, with exactly
   these columns: `table_name`, `check_name`, `severity`, `failed_rows`, `total_rows`, `status`, `checked_at`.
   Append, do not overwrite, so we keep history. Add a table description and column comments if you create it.
4. Summarize for the user in a short table: each check, failed rows, and PASS / FAIL / SKIPPED.
   Call out every `BLOCKER` failure first and say plainly whether the table is certified.

## Certification rule
A table is **certified** only if every `BLOCKER` check passes. `WARN` failures are reported but do not block.

## Example
User: "Check data quality on kk_silver_encounters."
You: map columns, run all rules, append to `kk_dq_results`, then reply:

| Check | Severity | Failed rows | Status |
|---|---|---|---|
| member_id_present | BLOCKER | 78 | FAIL |
| unique_encounter_id | BLOCKER | 0 | PASS |
| discharge_on_or_after_admit | BLOCKER | 0 | PASS |
| paid_not_above_charges | WARN | 148 | FAIL |
| ... | | | |

"kk_silver_encounters is **not certified**: 1 blocker failed (78 rows with no member id). Everything else passes;
148 rows have paid above charges (warning)."

## Edge cases
- Never display member names, phone numbers, or identifiers in the summary. Counts only.
- If the table is empty, every check is `SKIPPED` and the table is not certified.
- Coded values: payer may be text (`Commercial`) or a code (`1` to `4`); readmission may be `0/1` or `Y/N`.
  See `dq_rules.md` for the accepted values of each.
