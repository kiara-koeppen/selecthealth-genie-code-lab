# SelectHealth data team conventions (paste into your `.assistant_instructions.md`)

## Who I am
- I work on the SelectHealth data team. Our data is health plan data: members, encounters, claims, providers.
- Explain your plan briefly before building anything that writes tables.

## Python and SQL
- Use PySpark DataFrames or Spark SQL. Never use pandas on full tables, and never call `toPandas()` on more than a 1,000-row sample.
- Never hardcode catalog or schema names. Read them from `dbutils.widgets` (`catalog`, `schema`) and always wrap names in backticks, because our catalogs can contain hyphens (for example `` `dev-sh-training` ``).
- Use snake_case for every table and column name.

## Tables I create
- Prefix every table I create with my initials and an underscore, for example `kk_silver_encounters`.
- Every table must have a table description and a comment on every column.
- Add a PRIMARY KEY constraint on the natural key (set the column NOT NULL first).
- Parse yyyyMMdd string dates with `to_date(col, 'yyyyMMdd')`.

## Safety
- Never print or display member names, phone numbers, or other identifiers in results. Show aggregates or IDs only.
- Ask before dropping or overwriting any table I did not create in this chat.
