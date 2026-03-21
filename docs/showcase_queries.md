# Showcase Queries

This document contains a small set of analytical showcase queries for the ISO deliverables pipeline.

The goal is not to replace BI or reporting tools. The goal is to demonstrate that the warehouse model is:

- loaded correctly,
- queryable,
- useful for analysis,
- designed around both **current-state access** and **snapshot history**.

The SQL file with all showcase queries is stored in:

- `sql/analytics/showcase_queries.sql`

---

## How to use these queries

Recommended order:

1. Run a few **current-state** queries against `warehouse.vw_deliverables_current`
2. Run a few **child-table** queries against languages / ICS / relations
3. Run 1–2 **snapshot history** queries to demonstrate append-mode behavior

For interviews or demos, 4–6 queries are usually enough.

---

## Suggested core showcase set

### 1. Current published vs withdrawn summary
**Question:** How many deliverables in the latest snapshot are published, withdrawn, or still non-withdrawn?

**Why it matters:** Demonstrates that the current-state view works and exposes meaningful business flags.

**Main object used:** `warehouse.vw_deliverables_current`

---

### 2. Distribution by deliverable type
**Question:** What is the distribution of deliverables by type?

**Why it matters:** Demonstrates dimensional grouping on curated warehouse fields.

**Main object used:** `warehouse.vw_deliverables_current`

---

### 3. Top owner committees
**Question:** Which committees own the largest number of deliverables in the current snapshot?

**Why it matters:** Shows that the warehouse model supports aggregation by business dimensions.

**Main object used:** `warehouse.vw_deliverables_current`

---

### 4. Top language codes or top ICS codes
**Question:** Which languages or ICS codes are most common in the latest snapshot?

**Why it matters:** Demonstrates why exploded child tables are useful and why the model is better than a flat CSV copy.

**Main objects used:**
- `warehouse.bridge_deliverable_snapshot_languages`
- `warehouse.bridge_deliverable_snapshot_ics`

---

### 5. Relationship lineage examples
**Question:** Which deliverables replace or are replaced by others?

**Why it matters:** Shows that the project models relationships, not just flat rows.

**Main objects used:**
- `warehouse.factless_deliverable_relation_snapshot`
- `warehouse.fact_deliverable_snapshot`

---

### 6. Snapshot history by batch
**Question:** How many rows were loaded into each snapshot batch?

**Why it matters:** Demonstrates append-mode behavior and proves that the project stores multiple snapshots instead of overwriting prior loads.

**Main object used:** `warehouse.fact_deliverable_snapshot`

---

## Why these queries matter in an interview

These queries help demonstrate that the project is not only able to:

- ingest a CSV,
- clean it,
- load it into PostgreSQL,

but also to:

- preserve batch history,
- expose a latest-snapshot view,
- normalize multi-valued fields,
- model relationships between deliverables,
- support downstream analytical usage.

That is the main difference between a simple file load and a more complete mini data engineering pipeline.

---

## Recommended demo flow

A short and effective walkthrough could look like this:

1. Explain the layers: `raw -> staging -> warehouse`
2. Explain the batch concept: one file load = one snapshot batch
3. Show the current-state view: `warehouse.vw_deliverables_current`
4. Run 3–4 business-facing queries
5. Run 1 history query showing multiple batches
6. Explain the two load toggles:
   - `full_refresh` vs `append`
   - `executemany` vs `copy`

That is usually enough to make the project feel structured and intentional.
