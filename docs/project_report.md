# BIS Procurement Intelligence — Project Report

## 1. Problem Statement

The UK Department for Business, Innovation and Skills (BIS) published monthly 
transaction data as 12 separate CSV files with inconsistent schemas, blank rows, 
and no unified analytical layer. This project consolidates that fragmented data 
into a queryable warehouse and delivers executive-level procurement insights.

## 2. Data Challenges Encountered

| Challenge | How Resolved |
|---|---|
| 839–880 blank/footer rows per file | Dropped rows where all core fields null |
| august_2015 missing 2 columns | Added missing columns as NULL in ETL |
| Supplier name inconsistencies | Regex normalization (LIMITED→LTD) |
| Transaction numbers not unique | Created surrogate row_id key |
| 100% null Contract/Project columns | Retained for schema completeness |
| Mixed date formats | Parsed with dayfirst=True, errors=coerce |

## 3. Warehouse Design Decisions

- **Star schema** chosen for query performance and BI tool compatibility
- **UK fiscal calendar** (April–March) used throughout — not calendar year
- **dim_entity repurposed** from department (100% identical) to expense_area 
  (81 distinct cost centers) — a deliberate design improvement mid-project
- **Refunds preserved** as signed negative values + boolean flag — not removed

## 4. Key Analytical Findings

### Supplier Concentration Risk
The top 5 suppliers account for 80% of total spend. Student Loans Company 
Limited alone represents 37.5% (£14.4B). This is an extreme concentration 
that would typically trigger a procurement risk review.

### CAPITA Refund Anomaly
CAPITA Business Services received £5.4M in payments but £2.9M was refunded — 
a 54% refund rate across 118 refund transactions. This pattern strongly suggests 
either systematic overbilling or significant service delivery failures.

### Q4 Refund Spike
Q4 (Jan–Mar 2016) shows £55.4M in refunds versus £2–2.5M in other quarters. 
This coincides with the announcement of BIS's dissolution into BEIS. Contract 
unwinding near departmental closure is the most likely explanation.

### H2 Budget Burning
"Cash Transfers Paid Over to HMT" spiked 1,218% from H1 to H2 — from £139M 
to £1.84B. This pattern, combined with similar H2 spikes in NPF Agencies 
General Fund, is consistent with year-end budget utilization behavior.

## 5. Limitations

- Dataset covers only FY 2015/16 — trend comparison across years not possible
- Supplier postcode 59.5% null — geographic analysis not reliable
- Contract Number and Project Code 100% null — contract-level drill-down unavailable
- Student Loans/Post Office transactions dominate totals — operational spend 
  analysis requires filtering these out separately

## 6. Potential Extensions

- Add prior years (2013–2015 data available on same source)
- Build anomaly detection model using IQR or Z-score thresholds
- Geocode suppliers using postcode data for geographic spend mapping
- Automate pipeline refresh with Apache Airflow or GitHub Actions