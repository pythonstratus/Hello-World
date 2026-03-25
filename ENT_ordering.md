# ENT Grade Discrepancy — Stakeholder Summary

## Issue Reported

When excluding TINSID, the ENT value returns **4** on the modernized system (Exadata) but is expected to be **0** (as observed on the legacy M7 system).

---

## Root Cause

This discrepancy is a **known, previously documented** issue caused by **platform-level behavioral differences between the legacy M7 database and the modern Oracle Exadata environment** — not a defect in the application code or business logic.

### What's Happening

The underlying SQL logic retrieves the **first matching record** from a table (`E3TMP`) using Oracle's `ROWNUM = 1` construct. However, `ROWNUM = 1` simply returns whichever record Oracle's query engine encounters first — **it does not guarantee a specific ordering**.

- **On M7 (Legacy):** The older query optimizer tends to return records in a consistent, insertion-based order. For this data, the first record encountered yields a grade that ultimately resolves ENT to **0**.
- **On Exadata (Modern):** The newer query optimizer uses different execution strategies (hash-based operations, parallel execution, smart scan offloading). For the same data, the first record encountered yields a grade that resolves ENT to **4**.

**Both results are technically valid** — the SQL never specified *which* record should be selected when multiple records exist for the same TIN, TIN Type, and File Source Code combination. The legacy system simply happened to return one order consistently, and the modern system returns a different order.

### Why Multiple Records Exist

The `E3TMP` table can contain **more than one record** for the same combination of `tin`, `tintype`, and `filesourcecd`. When the query asks for `ROWNUM = 1` without an `ORDER BY` clause, the database is free to return **any one** of those records. The record it chooses determines the downstream `grade` value and, ultimately, the ENT result.

---

## Connection to Previously Documented Ordering Issue

This is the **same architectural root cause** identified during DIAL validation, where approximately 0.08% of records showed discrepancies due to non-deterministic ordering between M7 and Exadata. Key findings from that analysis:

- The M7 query optimizer uses **sort-based** execution, which tends to preserve insertion order as an implicit tie-breaker.
- The Exadata query optimizer uses **hash-based** execution with aggressive parallelism, where tie-breaking order is non-deterministic.
- Neither platform is "wrong" — the SQL simply does not specify a deterministic order, and each platform fills that ambiguity differently.

---

## Resolution Path

The fix is straightforward: **add an explicit ORDER BY** within the subquery so that the same record is consistently selected regardless of which database platform executes it. This ensures deterministic behavior on both M7 and Exadata.

This is a targeted code change — not a redesign — and aligns with the same remediation approach successfully applied to earlier DIAL ordering discrepancies.

---

## Key Takeaway

> This discrepancy is **not a bug in the modernized system**. It is a consequence of migrating from a legacy platform (M7) to a modern platform (Exadata) where implicit, undocumented ordering assumptions in the original code are no longer preserved. The fix involves making those assumptions explicit, which actually **improves** the overall reliability and determinism of the system.
