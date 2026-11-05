# SelectHealth encounter DQ rules

| Check name | Severity | Logical fields | Rule (a row FAILS when...) |
|---|---|---|---|
| `unique_encounter_id` | BLOCKER | encounter_id | the encounter id appears more than once (count every extra copy) |
| `member_id_present` | BLOCKER | member_id | member id is null or blank |
| `discharge_on_or_after_admit` | BLOCKER | admit_date, discharge_date | discharge date is before admit date |
| `valid_payer` | BLOCKER | payer | payer is not one of: Commercial, Medicare, Medicaid, Self-Pay (or codes 1, 2, 3, 4) |
| `valid_encounter_type` | BLOCKER | encounter_type | not one of: Inpatient, Outpatient, Emergency (or codes IP, OP, ED) |
| `paid_not_above_charges` | WARN | total_paid, total_charges | paid amount is greater than charges |
| `no_readmit_after_death` | WARN | readmitted_30d, mortality_flag | patient died (mortality = 1) but is flagged readmitted |
| `los_matches_dates` | WARN | admit_date, discharge_date, length_of_stay_days | length of stay differs from datediff(discharge, admit) by more than 1 day |

## Column mapping

| Logical field | Raw extract (`claims_extract_raw`) | Silver / fact tables |
|---|---|---|
| encounter_id | `enc_id` | `encounter_id` |
| member_id | `mbr_id` | `patient_id` or `member_id` |
| admit_date | `adm_dt` (yyyyMMdd string) | `admit_date` |
| discharge_date | `dsch_dt` (yyyyMMdd string) | `discharge_date` |
| payer | `pyr_cd` | `payer_type` |
| encounter_type | `enc_typ` | `encounter_type` |
| total_paid | `pd_amt` | `total_paid` |
| total_charges | `chg_amt` | `total_charges` |
| readmitted_30d | `rdmt_flg` (Y/N) | `readmitted_30d` (0/1) |
| mortality_flag | `mort_ind` | `mortality_flag` |
| length_of_stay_days | `los_d` | `length_of_stay_days` |
