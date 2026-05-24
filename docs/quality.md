# Data Quality

Canonical reference: [docs/architecture.md](architecture.md).

## Soda Core role

- Bronze: minimal checks (row_count > 0, freshness).
- Silver: structural checks (types, duplicates, nulls).
- Gold: business checks (KPI bounds, dimensional integrity).

## Example checks

checks for users:
  - row_count > 0
  - duplicate_count(id) = 0
  - missing_count(email) = 0

## Quality gates

- Bronze gate: fail if ingestion is empty or stale.
- Silver gate: fail on duplicates or invalid types.
- Gold gate: fail on business rule violations.
