"""QWL data collection layer (M1).

Symbol-scoped multi-source collectors with strict fallback and a hard data
quality gate. The plugin NEVER fetches the whole market: it either queries a
per-symbol endpoint, or (only when an interface is market-wide-only) fetches
once and immediately filters to the 12 universe members.
"""
