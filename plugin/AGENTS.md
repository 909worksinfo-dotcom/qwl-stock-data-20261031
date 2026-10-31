# QWL Stock Data Plugin Rules

- Treat the canonical QWL runtime as the only implementation of collection, PIT, normalization, and auditing. Do not reimplement providers inside this plugin.
- The allowed universe is exactly the 60 symbols produced by `qwl_list_fixed_60`. Never collect, persist, train on, or silently substitute an out-of-universe stock.
- Market-wide endpoints may be used only by the canonical collectors. They must be filtered in memory and must never persist non-whitelisted records.
- Never interpret a provider failure, empty response, stale series, ineligible security, or non-reportable position as a numeric zero.
- Use `available_on`, not report period or event date, for point-in-time decisions.
- Preserve raw provider values when quarantining anomalies. Derived cleaned fields may be null, but source evidence must remain auditable.
- Run `qwl_data_coverage_status(refresh=true)` after a refresh workflow. Report every known irreducible gap.
- Do not claim that public data is commercially complete. State the exact source window and source limitation.
- Do not modify portfolio, prediction, Champion, or trading configuration while performing a data request.
- Do not place orders or connect a broker. This plugin is data-only.
