# Multi-Source E-Commerce Batch ETL Pipeline

A batch ETL pipeline built on Databricks that ingests, cleans, and integrates two independent real-world e-commerce datasets using the Medallion (Bronze → Silver → Gold) architecture — demonstrating multi-source data engineering, data quality handling, and cross-source analytical integration.

## Overview

| | |
|---|---|
| **Sources** | Olist Brazilian E-Commerce (transactional) + REES46 Multi-Category Store (clickstream) |
| **Stack** | Databricks Free Edition (Serverless), PySpark, Delta Lake, Unity Catalog Volumes, Databricks Jobs |
| **Architecture** | Medallion (Bronze → Silver → Gold) |
| **Orchestration** | Databricks Jobs — 2-task pipeline, scheduled daily |
| **Validation** | 19 automated data quality checks (row counts, nulls, duplicates, referential integrity) |

## Why Two Sources?

Most portfolio ETL projects use a single, clean dataset. This project intentionally combines **two independent, real-world businesses** with different schemas and no shared entity keys, to demonstrate a common real-world DE challenge: reconciling data that doesn't cleanly join.

- **Olist** — a real Brazilian e-commerce marketplace, with 9 relational tables (orders, items, payments, reviews, products, sellers, customers, geolocation, category translations).
- **REES46** — real clickstream data (views/carts/purchases) from a separate multi-category online store. Sampled to the first 7 days of October 2019 (~8.8M events) from the full ~42M-row month, for compute-tractability on Databricks Free Edition's serverless environment.

Because the two sources are different businesses, there is no entity-level key (e.g. no shared `customer_id`). Integration is done at the **category level** instead — see [Integration Strategy](#integration-strategy) below.

## Architecture

```
[Olist CSVs]                    [REES46 CSV: Oct 2019]
      │                                  │
      ▼                                  ▼
┌─────────────────────── BRONZE ────────────────────────┐
│  Raw ingestion, as-is, both sources                    │
│  + ingestion metadata (timestamp, source file)         │
│  REES46 sampled to first 7 days (~8.8M of ~42M rows)   │
└─────────────────────────────────────────────────────────┘
      │                                  │
      ▼                                  ▼
┌─────────────────────── SILVER ────────────────────────┐
│  Olist: cleaned, deduplicated, conformed types          │
│  REES46: category hierarchy parsed, nulls handled       │
│  Category mapping table: Olist (71 categories) →        │
│  REES46 buckets (14 categories), documented assumptions │
└─────────────────────────────────────────────────────────┘
      │                                  │
      ▼                                  ▼
┌─────────────────────── GOLD ──────────────────────────┐
│  Olist: monthly sales, customer LTV, seller performance,│
│  review score trends                                    │
│  REES46: overall conversion funnel                       │
│  Combined: category-level comparison (browsing interest  │
│  vs. actual sales, across both stores)                   │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
              Databricks Jobs (2-task, scheduled)
                          │
                          ▼
              19-check automated validation suite
```

Storage: all layers live under `/Volumes/dbacademy/default/ecommerce_project/{raw,bronze,silver,gold}/` — Unity Catalog Volumes, not DBFS (see [Design Decisions](#design-decisions)).

## Integration Strategy

Olist's 71 product categories and REES46's 14 top-level categories don't share a naming convention (e.g. Olist's `computers_accessories` vs. REES46's `computers.peripherals`). Rather than attempt a fragile fuzzy-match, a **manual, explicit mapping table** was built: each of the 71 Olist categories is mapped to its closest REES46 top-level bucket, or to `no_match` where no reasonable equivalent exists. This mapping is stored as its own Silver table (`category_mapping`) and is fully transparent/auditable — every mapping decision is a single line in the source dictionary.

This produced a Gold table (`category_comparison`) joining REES46 browsing/funnel metrics against Olist sales, per category bucket. It surfaces a genuine cross-business insight: **electronics** is REES46's top browsing category (3.2M views) but only 9th in Olist sales revenue, while **furniture** is Olist's #1 sales category despite modest REES46 browsing interest — consistent with these being two different real-world stores, not a data quality issue.

## Data Quality Issues Found and Fixed

Two non-trivial issues were caught during Silver-layer development, both worth calling out as they reflect realistic production debugging rather than clean-path development:

1. **Silent CSV parsing corruption (`olist_order_reviews`)** — initial ingestion showed ~2,380 rows with nulls in supposedly mandatory fields (`order_id`, `review_score`). Inspecting the raw rows (not just null counts) revealed that free-text review comments containing embedded commas/newlines were not being handled by the CSV parser, causing column values to shift. Fixed by re-reading with `multiLine=True`, `quote='"'`, `escape='"'`, and re-ingesting Bronze with `overwriteSchema=true` (the corrected data changed `review_score` from string to integer). Row count changed from 104,162 to 99,224 as incorrectly-split rows were merged back into single rows.
2. **Hidden low-cardinality anomalies (`olist_order_payments`)** — a `not_defined` payment type (3 rows) and `payment_installments = 0` on 2 credit card rows were found via distinct-value and min/max checks, not obvious from a null-count scan alone. The 3 ambiguous rows were dropped; the 2 zero-installment rows were corrected to 1.

## Design Decisions

- **Unity Catalog Volumes over DBFS** — Databricks Free Edition disables the public DBFS root; all raw/bronze/silver/gold data is stored under a Unity Catalog managed Volume instead, which is the current recommended pattern regardless of edition.
- **REES46 sampled to 7 days** — the full REES46 dataset (~42M rows for October alone, ~285M across all available months) was reduced to a 7-day window to keep processing tractable on Free Edition's serverless compute, while retaining enough volume for meaningful funnel analysis. This is documented in the `source_file` metadata column on the Bronze table itself.
- **`customer_unique_id` vs. `customer_id`** — Olist assigns a new `customer_id` per order but a stable `customer_unique_id` per actual person. Customer lifetime value is computed on `customer_unique_id` to avoid treating repeat customers as new customers each time.
- **`is_delivered` flag and sales filtering** — all revenue/sales aggregates filter to `order_status == 'delivered'`, so cancelled/pending orders aren't counted as realized sales.
- **Nulls filled, not dropped, where the missing value is informative** — e.g. Olist products missing category/photos (610 rows) were filled with `"unknown"` / `0` rather than dropped, since dropping would lose otherwise-valid transactional history; REES46 events missing `category_code` (~31.5% of rows) were filled with `"uncategorized"` for the same reason.
- **Geolocation deduplication** — the raw Olist geolocation table has ~1M rows for ~19K unique zip codes (multiple lat/long readings per zip). Silver collapses this to one row per zip code via averaged coordinates, since downstream joins need a single representative point.

## Pipeline Structure

| Notebook | Responsibility |
|---|---|
| `01_bronze_ingestion` + `02_silver_transformation` (combined) | Downloads both datasets via `kagglehub`, stages them to Volumes, ingests to Bronze with metadata columns, then cleans/conforms both sources into Silver (including the category mapping table) |
| `03_gold_aggregation` | Builds all 6 Gold tables: combined category comparison, Olist monthly sales, customer LTV, seller performance, review trends, and REES46 overall funnel |
| `04_validation` | Runs 19 automated checks across row counts, null constraints, duplicate keys, referential integrity, and Gold table readability |

Orchestrated as a 2-task Databricks Job (`bronze_ingestion+silver_transformation` → `gold_aggregation`), scheduled to run daily.

## Gold Layer Tables

| Table | Description |
|---|---|
| `category_comparison` | REES46 views/carts/purchases + Olist revenue/items sold, per mapped category bucket |
| `olist_monthly_sales` | Revenue and items sold by month (Sep 2016 – Oct 2018) |
| `olist_customer_ltv` | Lifetime spend and order count per unique customer |
| `olist_seller_performance` | Revenue and items sold per seller, with state |
| `olist_review_trends` | Average review score by purchase month |
| `rees46_overall_funnel` | View→cart, cart→purchase, and overall conversion rates |

## Known Limitations

- REES46 data is a 7-day sample, not the full dataset — absolute volumes aren't comparable 1:1 with Olist's multi-year history, though rates/proportions are meaningful.
- Category mapping is a manual, judgment-based approximation, not an authoritative taxonomy crosswalk — documented per-category in the mapping table.
- Some Gold aggregates (e.g. review trends for Sep–Oct 2018) are based on very small sample sizes (under 20 reviews) near the edges of the Olist dataset's time range and shouldn't be over-interpreted.
