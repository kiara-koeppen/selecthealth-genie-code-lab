# Project: Encounters data product (Genie Code lab)

Genie Code reads this file automatically when you open a notebook or file in this folder (or any subfolder).
Use it for **this project only**. Personal, cross-project preferences belong in `.assistant_instructions.md`.

## Goal
Turn the raw encounter extract into a clean, documented silver table, check its quality, and report the
SelectHealth readmission rate the official way.

## Data
- Raw input: `claims_extract_raw` (legacy file drop, contains dirty rows).
- Clean reference model: `fact_encounters` plus `dim_provider`, `dim_facility`, `dim_diagnosis`, `dim_procedure`.
- Silver output: `<initials>_silver_encounters`, one row per unique `encounter_id`.

## Decisions
- Deduplicate on `enc_id`, keep the first row.
- Drop rows where discharge is before admit, or the payer code is not 1 to 4. Log how many were dropped.
- Readmission rate is ALWAYS computed with the `readmission_rate_official` function. Do not write your own formula.

## Open questions
- Should rows with a null member id be quarantined instead of dropped?
