# BIS Procurement Intelligence Platform

An end-to-end data analytics project transforming UK Government expenditure data into actionable procurement intelligence.

![Python](https://img.shields.io/badge/Python-3.x-blue?logo=python&logoColor=white)
![SQLite](https://img.shields.io/badge/Database-SQLite-lightgrey?logo=sqlite)
![PowerBI](https://img.shields.io/badge/Dashboard-Power%20BI-F2C811?logo=powerbi&logoColor=black)
![License](https://img.shields.io/badge/Licence-Open%20Gov%20v3.0-green)
![Status](https://img.shields.io/badge/Status-Complete-brightgreen)

---

## Overview

This project builds a production-style procurement analytics system using publicly available expenditure data from the **UK Department for Business, Innovation and Skills (BIS)** — a central government department later merged into BEIS in July 2016.

The pipeline ingests and consolidates **12 monthly transaction files** covering BIS spending above £25,000 across FY 2015/16, representing **42,380 transactions** and **£38.4 billion** in government financial flows across **1,889 unique suppliers**.

---

## Key Findings

| Finding | Detail |
|---|---|
| Top 5 suppliers = 80% of total spend | Student Loans Company alone = 37.5% (£14.4B) |
| CAPITA refund rate = 54% | £2.9M refunded on £5.4M billed — billing anomaly |
| Q4 refunds = £55.4M | vs £2–2.5M in other quarters — pre-restructure signal |
| Cash Transfers H2 spike = 1,218% | £139M → £1.84B — year-end treasury movement |
| April and January are outlier months | Fiscal year-start and mid-year disbursement spikes |

---

## Business Questions Answered

- Which suppliers are receiving the most public money — and is there dangerous over-reliance on any single vendor?
- How does spend vary across BIS cost centers and internal entities?
- Are there patterns in refunds that indicate process failures or contract disputes?
- Which expense categories spike in H2 — suggesting end-of-year budget burning?
- Were there unusual spending patterns in the lead-up to the 2016 departmental restructure?

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Ingestion | Python — requests, pandas |
| Data Warehouse | SQLite — Star Schema |
| ETL Pipeline | Python — pandas, openpyxl |
| Analytical Layer | SQL — CTEs, Window Functions, Stored Views |
| BI Dashboard | Power BI Desktop |
| Version Control | Git + GitHub |

---

## Project Structure

```
bis-procurement-analytics/
├── etl/
│   ├── download_data.py        # Downloads all 12 source CSV files
│   └── etl_pipeline.py         # Extract, clean, transform, load
├── warehouse/
│   ├── build_schema.py         # Star schema + dimension/fact builder
│   └── create_views.py         # Reporting views for Power BI
├── sql/
│   ├── analytical_queries.sql  # All business SQL queries
│   └── run_queries.py          # Query runner with formatted output
├── dashboard/
│   └── BIS.pbix                # Power BI dashboard
├── notebooks/
│   └── 01_eda_profiling.ipynb  # Exploratory data analysis
├── docs/
│   └── project_report.md       # Full project report
├── requirements.txt
└── README.md
```

---

## Warehouse Schema

Star schema with 1 fact table and 4 dimension tables.

| Table | Rows | Description |
|---|---|---|
| fact_transactions | 42,380 | One row per transaction |
| dim_date | 251 | Date hierarchy with UK fiscal calendar |
| dim_supplier | 1,889 | Supplier details and classification |
| dim_expense | 2,138 | Expense type and area combinations |
| dim_entity | 81 | BIS cost centers with group and subgroup |

---

## How to Run

**Prerequisites:** Python 3.8+ and Power BI Desktop

```bash
# 1. Clone the repository
git clone https://github.com/Shahbaz-Ahmed999/bis-procurement-analytics.git
cd bis-procurement-analytics

# 2. Create virtual environment
python -m venv venv
venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Download raw data (12 CSV files from data.gov.uk)
python etl/download_data.py

# 5. Run ETL pipeline
python etl/etl_pipeline.py

# 6. Build warehouse
python warehouse/build_schema.py

# 7. Create reporting views
python warehouse/create_views.py

# 8. Run analytical queries
python sql/run_queries.py
```

Open `dashboard/BIS.pbix` in Power BI Desktop to explore the dashboard.

> The warehouse database is rebuilt locally by running the pipeline above.
> Raw CSV files are not stored in this repository — they are downloaded automatically via `download_data.py`.

---

## Dashboard

**Page 1 — Executive Overview**

KPI cards showing total net spend, transaction count, unique suppliers, and refunds issued. Monthly spend trend line chart with fiscal quarter colouring. Supplier type donut chart. Quarterly summary table with conditional formatting on refund rate.

**Page 2 — Supplier and Spend Intelligence**

Top 10 suppliers by gross spend. Supplier type concentration donut. Monthly spend trend with average anomaly line. Refund risk table with conditional formatting.

---

## Data Source

| Attribute | Detail |
|---|---|
| Source | [data.gov.uk](https://www.data.gov.uk/dataset/22a8f668-9cf5-43b6-b097-8be0303ad74d/financial-transactions-spend-data-bis) |
| Publisher | Department for Business, Innovation and Skills |
| Coverage | April 2015 – March 2016 |
| Licence | Open Government Licence v3.0 |

---

## Data Challenges Handled

| Challenge | Resolution |
|---|---|
| 839–880 blank footer rows per file | Dropped rows where all core fields null |
| Inconsistent column counts (12–14) | Schema reconciliation with canonical mapping |
| Supplier name inconsistencies | Regex normalization — LIMITED to LTD etc |
| Transaction numbers not unique | Surrogate row_id key created |
| Mixed date formats | Parsed with dayfirst=True |
| dim_entity returning 1 record | Repurposed to expense_area — 81 cost centers |

---

## Author

**Shahbaz Ahmed**

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue?logo=linkedin)](https://linkedin.com/in/YOUR-LINKEDIN-HANDLE)

---

## Licence

Data: [Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)

Code: MIT
