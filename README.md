\# BIS Procurement Intelligence Platform



> An end-to-end data analytics project transforming UK Government expenditure 

> data into actionable procurement intelligence.



!\[Python](https://img.shields.io/badge/Python-3.x-blue?logo=python)

!\[SQL](https://img.shields.io/badge/SQL-SQLite-lightgrey?logo=sqlite)

!\[Power BI](https://img.shields.io/badge/Dashboard-Power%20BI-yellow?logo=powerbi)

!\[License](https://img.shields.io/badge/License-Open%20Gov%20v3.0-green)

!\[Status](https://img.shields.io/badge/Status-Complete-brightgreen)



\---



\## Project Overview



This project builds a production-style procurement analytics system using 

publicly available expenditure data from the \*\*UK Department for Business, 

Innovation and Skills (BIS)\*\* — a central government department later merged 

into BEIS in July 2016.



The system ingests, consolidates, and analyzes \*\*12 monthly transaction files\*\* 

covering BIS spending above £25,000 across FY 2015/16 — representing 

\*\*42,380 transactions\*\* and \*\*£38.4 billion\*\* in government financial flows 

across 1,889 unique suppliers.



\### Business Questions Answered

\- Which suppliers are receiving the most public money — and are we over-reliant on any single vendor?

\- How does spend vary across BIS cost centers and internal entities?

\- Are there patterns in refunds or credits that indicate process failures?

\- Which expense categories spike in H2 — suggesting end-of-year budget burning?

\- Were there unusual spending patterns in the lead-up to the 2016 restructure?



\---



\## Key Findings



| Finding | Detail |

|---|---|

| \*\*Top 5 suppliers = 80% of spend\*\* | Student Loans Company alone = 37.5% (£14.4B) |

| \*\*CAPITA refund rate = 54%\*\* | £2.9M refunded on £5.4M billed — billing dispute signal |

| \*\*Q4 refunds = £55.4M\*\* | vs \~£2.4M in other quarters — pre-restructure anomaly |

| \*\*Cash Transfers H2 spike = 1,218%\*\* | £139M → £1.84B — year-end treasury movement |

| \*\*April \& January outlier months\*\* | Fiscal year-start and mid-year disbursement spikes |



\---



\## Tech Stack



| Layer | Technology |

|---|---|

| Data Ingestion | Python (requests, pandas) |

| Data Warehouse | SQLite (Star Schema) |

| ETL Pipeline | Python (pandas, openpyxl) |

| Analytical Layer | SQL (CTEs, Window Functions) |

| BI Dashboard | Power BI Desktop |

| Version Control | Git + GitHub |



\---



\## Project Architecture

bis-procurement-analytics/

│

├── data/

│   ├── raw/                    # Downloaded monthly CSV files (see setup)

│   └── processed/              # ETL summary outputs

│

├── etl/

│   ├── download\_data.py        # Downloads all 12 source files

│   └── etl\_pipeline.py         # Full ETL: extract → clean → load

│

├── warehouse/

│   ├── build\_schema.py         # Star schema DDL + dimension/fact builder

│   └── create\_views.py         # Reporting views for Power BI

│

├── sql/

│   ├── analytical\_queries.sql  # All business SQL queries

│   └── run\_queries.py          # Query runner with formatted output

│

├── dashboard/

│   └── BIS\_Procurement\_Dashboard.pbix   # Power BI dashboard file

│

├── notebooks/

│   └── 01\_eda\_profiling.ipynb  # Exploratory data analysis

│

├── docs/

│   └── project\_report.md       # Full project report

│

└── README.md



\---



\## Data Source



| Attribute | Detail |

|---|---|

| \*\*Source\*\* | \[data.gov.uk](https://www.data.gov.uk/dataset/22a8f668-9cf5-43b6-b097-8be0303ad74d/financial-transactions-spend-data-bis) |

| \*\*Publisher\*\* | Department for Business, Innovation and Skills |

| \*\*Coverage\*\* | April 2015 – March 2016 (FY 2015/16) |

| \*\*Transactions\*\* | 42,380 (after cleaning) |

| \*\*Licence\*\* | Open Government Licence v3.0 |



> Raw data files are not stored in this repository.  

> Run `python etl/download\_data.py` to download them automatically.



\---



\## How to Run This Project



\### Prerequisites

\- Python 3.8+

\- Power BI Desktop (free — \[download here](https://powerbi.microsoft.com/desktop))



\### Setup



```bash

\# 1. Clone the repository

git clone https://github.com/YOUR\_USERNAME/bis-procurement-analytics.git

cd bis-procurement-analytics



\# 2. Create and activate virtual environment

python -m venv venv

venv\\Scripts\\activate        # Windows

source venv/bin/activate     # Mac/Linux



\# 3. Install dependencies

pip install -r requirements.txt



\# 4. Download raw data

python etl/download\_data.py



\# 5. Run ETL pipeline

python etl/etl\_pipeline.py



\# 6. Build warehouse schema

python warehouse/build\_schema.py



\# 7. Create reporting views

python warehouse/create\_views.py



\# 8. Run analytical queries

python sql/run\_queries.py

```



\### Open Dashboard

Open `dashboard/BIS\_Procurement\_Dashboard.pbix` in Power BI Desktop.  

The dashboard connects to the local SQLite database at `warehouse/bis\_warehouse.db`.



\---



\## Data Pipeline

Raw CSV Files (12 months)

↓

ETL Pipeline

• Schema reconciliation

• Blank row removal

• Date normalization

• Supplier name standardization

• Refund flagging

↓

SQLite Warehouse

┌─────────────────┐

│  fact\_transactions│

│  dim\_date        │

│  dim\_supplier    │

│  dim\_expense     │

│  dim\_entity      │

└─────────────────┘

↓

Reporting Views (6)

↓

Power BI Dashboard



\---



\## Warehouse Schema



\*\*Star Schema — 1 Fact Table, 4 Dimension Tables\*\*



| Table | Rows | Description |

|---|---|---|

| `fact\_transactions` | 42,380 | One row per transaction |

| `dim\_date` | 251 | Date hierarchy with UK fiscal calendar |

| `dim\_supplier` | 1,889 | Supplier details and classification |

| `dim\_expense` | 2,138 | Expense type and area combinations |

| `dim\_entity` | 81 | BIS cost centers with group/subgroup |



\---



\## Dashboard Pages



\*\*Page 1 — Executive Overview\*\*

KPI cards, monthly spend trend, supplier type breakdown, quarterly summary table.



\*\*Page 2 — Supplier \& Spend Intelligence\*\*

Top 10 suppliers, concentration risk, monthly trend with anomaly line, refund risk table.



\---



\## Author



\*\*Shahbaz Ahmed Khan\*\*  

www.linkedin.com/in/shahbaz-ahmed-0s  





\---



\## License



Data: \[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)  

Code: MIT License



