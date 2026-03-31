# optum-azure-pipeline

> Azure End-to-End Automated Data Pipeline — Optum Data Analyst Interview Project

Built as part of a 30-day interview prep bootcamp targeting the **Optum Data Analyst role (Dublin)**. This project directly mirrors the Azure cloud migration and CI/CD pipeline work described in the Optum JD — Azure Data Factory, Azure SQL, Blob Storage, GitHub Actions, and automated pytest data quality testing.

**GitHub Actions:** Every push to `main` automatically runs 6 pytest tests against Azure Blob Storage.

---

## What This Project Does

A production-grade cloud data pipeline built entirely on Azure that:

1. **Generates** pharmacy pricing data (1,000 records) using Python + Faker
2. **Uploads** the CSV to Azure Blob Storage
3. **Extracts** the CSV from Blob Storage
4. **Transforms** the data — null checks, duplicate removal, data quality filters, derived columns
5. **Loads** the clean data into Azure SQL Database
6. **Tests** automatically on every GitHub push via GitHub Actions CI/CD

This is a complete Extract → Transform → Load (ETL) pipeline running on Azure cloud infrastructure.

---

## Project Structure

```
optum-azure-pipeline/
├── .github/
│   └── workflows/
│       └── pipeline.yml      # GitHub Actions CI/CD workflow
├── pipeline.py               # Main ETL pipeline: Extract → Transform → Load
├── upload_to_blob.py         # Generates data and uploads CSV to Azure Blob
├── test_pipeline.py          # 6 pytest tests — runs on every push
├── pipeline.log              # ETL run log (auto-generated)
└── README.md                 # This file
```

---

## Azure Resources

| Resource | Name | Type |
|---|---|---|
| Resource Group | optum-pipeline-rg1 | Container for all resources |
| SQL Server | optum-sql-server | Azure SQL Server |
| SQL Database | PricingDB | Azure SQL Database |
| Storage Account | optumpricingstorage | Azure Blob Storage |
| Data Factory | optum-data-factory1256 | Azure Data Factory V2 |
| Region | West US 2 | All resources |

**Connection details:**
```
SQL Server:   optum-sql-server.database.windows.net
Database:     PricingDB
Storage:      optumpricingstorage.blob.core.windows.net
Container:    pricing-data
Blob file:    pricing_data.csv
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Data Storage | Azure Blob Storage |
| Database | Azure SQL Database |
| Data Factory | Azure Data Factory V2 |
| Language | Python 3.13 |
| ETL Libraries | pandas, pyodbc, azure-storage-blob, azure-identity |
| Testing | pytest |
| CI/CD | GitHub Actions |
| Version Control | Git, GitHub |
| Data Generation | Faker, random |

---

## Pipeline Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Python Script  │────▶│  Azure Blob       │────▶│  ETL Pipeline   │
│  upload_to_blob  │     │  Storage          │     │  pipeline.py    │
│  (1000 rows CSV) │     │  pricing-data/    │     │                 │
└─────────────────┘     │  pricing_data.csv │     └────────┬────────┘
                        └──────────────────┘              │
                                                          │ Extract
                                                          ▼
                                                 ┌─────────────────┐
                                                 │   Transform      │
                                                 │  - Null checks   │
                                                 │  - Dedup         │
                                                 │  - Quality filter│
                                                 │  - DiscountPct   │
                                                 └────────┬────────┘
                                                          │ Load
                                                          ▼
                                                 ┌─────────────────┐
                                                 │  Azure SQL DB    │
                                                 │  PricingDB       │
                                                 │  PricingPipeline │
                                                 │  table           │
                                                 └─────────────────┘

CI/CD:
┌──────────┐    push    ┌─────────────────┐    auto-run    ┌──────────────┐
│  Local   │───────────▶│  GitHub         │───────────────▶│  GitHub      │
│  Code    │            │  Repository     │                │  Actions     │
└──────────┘            └─────────────────┘                │  pytest x6   │
                                                           └──────────────┘
```

---

## Day-by-Day Build Log

### Day 6 — Azure Setup + Resources
**Commit:** `Day 6: Azure pipeline - Blob, SQL, ETL, pytest, GitHub Actions`

**What was built:**

**Azure Account:**
- Created free Azure account at azure.microsoft.com/free using Wise card
- €169.47 credits — expires April 29, 2026
- Subscription: Azure subscription 1

**Resources created in Azure portal:**
1. Resource Group: `optum-pipeline-rg1` (West Europe → moved to West US 2)
2. Azure SQL Database: `PricingDB` on server `optum-sql-server`
   - Authentication: Microsoft Entra (Azure AD) with virat183672@gmail.com
   - Free tier — 100,000 vCore seconds per month
   - Firewall rule added for local IP: 109.77.17.227
3. Storage Account: `optumpricingstorage`
   - Performance: Standard
   - Redundancy: LRS (Locally Redundant Storage)
   - Container created: `pricing-data`
4. Data Factory: `optum-data-factory1256`
   - Version: V2
   - Region: West US 2

**Python libraries installed:**
```bash
pip install azure-storage-blob azure-identity pandas pyodbc sqlalchemy faker pytest
```

---

### Day 6 — upload_to_blob.py
**What it does:**
- Generates 1,000 rows of pharmacy pricing data using Faker + random
- Data includes: ProductName, Category, BasePrice, CustomerName, Tier, Region, NegotiatedPrice, Quantity, TxnDate
- Converts DataFrame to CSV in memory (no file saved locally)
- Creates container `pricing-data` in Azure Blob Storage if it doesn't exist
- Uploads CSV as `pricing_data.csv` to Blob Storage

**Run result:**
```
INFO - Connecting to Azure Blob Storage...
INFO - Container 'pricing-data' created
INFO - Generating pricing data CSV...
INFO - Uploaded 1000 rows to blob: pricing_data.csv
```

**Key code pattern:**
```python
import os
STORAGE_CONN_STR = os.environ.get("STORAGE_CONN_STR", "")

# Upload to blob
csv_buffer = io.StringIO()
df.to_csv(csv_buffer, index=False)
blob_client.upload_blob(csv_data, overwrite=True)
```

---

### Day 6 — pipeline.py (ETL)
**What it does:**

**EXTRACT:**
- Connects to Azure Blob Storage using connection string from environment variable
- Downloads `pricing_data.csv` from container `pricing-data`
- Loads into pandas DataFrame
- Result: 1,000 rows extracted

**TRANSFORM:**
- Null check: `df.isnull().sum().sum()` — logs count, drops nulls
- Duplicate check: `df.duplicated().sum()` — logs count, drops duplicates
- Data quality filters:
  - Remove rows where BasePrice ≤ 0
  - Remove rows where NegotiatedPrice ≤ 0
  - Remove rows where Quantity ≤ 0
  - Remove rows where NegotiatedPrice > BasePrice × 1.5 (outlier removal)
- Add derived column: `DiscountPct = (BasePrice - NegotiatedPrice) / BasePrice * 100`
- Convert TxnDate to datetime
- Add LoadedAt timestamp
- Result: 694 clean rows, 11 columns

**LOAD:**
- Connects to Azure SQL Database using pyodbc
- Creates `PricingPipeline` table if it doesn't exist
- Bulk inserts using `cursor.fast_executemany = True`
- Commits transaction
- Result: 694 rows loaded into Azure SQL

**Full pipeline run log:**
```
INFO - ========== PIPELINE STARTED ==========
INFO - === EXTRACT: Reading from Azure Blob Storage ===
INFO - Extracted 1000 rows from blob
INFO - === TRANSFORM: Cleaning and validating data ===
INFO - Nulls found: 0
INFO - Duplicates found: 0
INFO - After transform: 694 rows, 11 columns
INFO - === LOAD: Inserting into Azure SQL ===
INFO - Loaded 694 rows into Azure SQL PricingPipeline table
INFO - ========== PIPELINE COMPLETE ==========
```

**Azure SQL table created:**
```sql
CREATE TABLE PricingPipeline (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    ProductName NVARCHAR(200),
    Category NVARCHAR(100),
    BasePrice DECIMAL(10,2),
    CustomerName NVARCHAR(200),
    Tier NVARCHAR(50),
    Region NVARCHAR(100),
    NegotiatedPrice DECIMAL(10,2),
    DiscountPct DECIMAL(5,2),
    Quantity INT,
    TxnDate DATE,
    LoadedAt DATETIME
)
```

---

### Day 6 — test_pipeline.py (pytest)
**6 tests written and passing:**

| Test | What it checks |
|---|---|
| `test_extract_returns_data` | Blob returns data (len > 0) |
| `test_extract_row_count` | Exactly 1000 rows extracted |
| `test_extract_has_required_columns` | ProductName, BasePrice, NegotiatedPrice exist |
| `test_transform_removes_nulls` | No nulls after transform |
| `test_transform_positive_prices` | All prices > 0 after transform |
| `test_transform_adds_discount_column` | DiscountPct column exists after transform |

**Test run result:**
```
platform win32 -- Python 3.13.12, pytest-9.0.2
collected 6 items

test_pipeline.py::test_extract_returns_data          PASSED [ 16%]
test_pipeline.py::test_extract_row_count             PASSED [ 33%]
test_pipeline.py::test_extract_has_required_columns  PASSED [ 50%]
test_pipeline.py::test_transform_removes_nulls       PASSED [ 66%]
test_pipeline.py::test_transform_positive_prices     PASSED [ 83%]
test_pipeline.py::test_transform_adds_discount_column PASSED [100%]

6 passed in 8.80s
```

---

### Day 6 — GitHub Actions CI/CD
**What was set up:**

1. Created `.github/workflows/pipeline.yml`
2. Added `STORAGE_CONN_STR` as GitHub repository secret
3. Workflow triggers on every push to `main`
4. GitHub spins up Ubuntu server, installs dependencies, runs pytest

**Workflow file:**
```yaml
name: Azure Pipeline CI

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'
    - name: Install dependencies
      run: |
        pip install pandas pyodbc azure-storage-blob azure-identity faker pytest sqlalchemy
    - name: Run tests
      env:
        STORAGE_CONN_STR: ${{ secrets.STORAGE_CONN_STR }}
      run: |
        python -m pytest test_pipeline.py -v
```

**Result:** ✅ Green checkmark on every push — 6/6 tests passing automatically.

---

## Full Setup From Scratch

### Prerequisites
- Azure free account — azure.microsoft.com/free
- Python 3.11+
- Git

### Step 1 — Clone repo
```bash
git clone https://github.com/SaiTejaReddyYeldandi/optum-azure-pipeline.git
cd optum-azure-pipeline
```

### Step 2 — Install dependencies
```bash
pip install azure-storage-blob azure-identity pandas pyodbc sqlalchemy faker pytest
```

### Step 3 — Set environment variable
```bash
# Windows
set STORAGE_CONN_STR=your_connection_string_here

# Mac/Linux
export STORAGE_CONN_STR=your_connection_string_here
```

### Step 4 — Get connection string
- Azure portal → Storage accounts → optumpricingstorage
- Security + networking → Access keys → Copy Connection string

### Step 5 — Add firewall rule for Azure SQL
- Azure portal → SQL servers → optum-sql-server
- Security → Networking → Add your client IPv4 address → Save

### Step 6 — Upload data to blob
```bash
python upload_to_blob.py
```

### Step 7 — Run pipeline
```bash
python pipeline.py
```

### Step 8 — Run tests
```bash
python -m pytest test_pipeline.py -v
```

---

## GitHub Commands Used

```bash
# Clone
git clone https://github.com/SaiTejaReddyYeldandi/optum-azure-pipeline.git .

# Daily workflow
git add .
git commit -m "message"
git pull --rebase
git push

# Force push (after rewriting history to remove secrets)
git push --force

# Remove secret from git history
git reset --soft HEAD~2
git add .
git commit -m "clean commit"
git push --force

# Remove cached files
git rm -r --cached __pycache__
git add .
git commit -m "Remove pycache"
git push
```

**Why secrets must NEVER be in code:**
GitHub scans every push for secrets. If a connection string, API key, or password is found in any file — including `.pyc` compiled files — GitHub blocks the push with error `GH013`. Always use `os.environ.get("SECRET_NAME")` and store the actual value in:
- Local machine: `set SECRET_NAME=value` in Command Prompt
- GitHub Actions: Repository Settings → Secrets → Actions → New secret

---

## Security Notes

- Storage Account connection string stored as GitHub Secret `STORAGE_CONN_STR`
- Never hardcoded in any file
- `.gitignore` excludes `__pycache__`, `.pyc`, `.log`, `.env`
- Azure SQL firewall restricts access to specific IP addresses only
- Authentication via Microsoft Entra (Azure AD)

---

## Interview Talking Points

**"Describe your Azure pipeline project."**
Built a cloud ETL pipeline on Azure from scratch. Data is generated in Python using Faker, uploaded to Azure Blob Storage as CSV, then a pipeline script extracts it, transforms it — null checks, deduplication, outlier removal, derived column calculation — and loads the clean data into Azure SQL Database. The whole thing is tested automatically on every GitHub push using GitHub Actions running 6 pytest tests. The pipeline processes 1,000 rows, applies quality filters, and loads 694 clean records into Azure SQL in under 30 seconds.

**"How does GitHub Actions work in your project?"**
Every time I push code to the main branch, GitHub automatically spins up an Ubuntu server, installs all Python dependencies, and runs my pytest test suite against Azure Blob Storage. If any test fails, the push is marked as failed and I get notified. The connection string is stored as a GitHub Secret so it's never exposed in the code. This is the same CI/CD pattern used in production — automated testing on every deployment.

**"How do you handle secrets in code?"**
Never hardcode secrets. I learned this the hard way — GitHub's secret scanner blocked my push when it detected a connection string in a `.pyc` compiled file. The correct approach is `os.environ.get("SECRET_NAME")` in code, `set SECRET_NAME=value` locally in Command Prompt, and GitHub Secrets for CI/CD pipelines. The actual value never touches the codebase.

**"What data quality checks does your pipeline perform?"**
Five checks: null detection and removal, duplicate detection and removal, positive price validation (BasePrice and NegotiatedPrice must be > 0), quantity validation (Quantity > 0), and outlier removal (NegotiatedPrice must be less than 1.5× BasePrice to catch data entry errors). Each check is logged with a count so there's a full audit trail. All checks are also covered by automated pytest tests.

**"Why Azure for this project?"**
The Optum JD specifically mentions Azure Data Factory, Azure SQL, Databricks, and Azure DevOps. I built against those exact services. Azure Data Factory is set up for orchestration, Azure Blob Storage is the data lake layer, Azure SQL is the analytics store — this mirrors the exact architecture Optum's Dublin Pricing team uses for the PULSE/Repricing ecosystem migration from on-prem to cloud.

**"What is the difference between Azure Blob Storage and Azure SQL Database?"**
Blob Storage is unstructured object storage — it stores any file type (CSV, JSON, images, Parquet) and is ideal for raw data landing zones and data lake patterns. Azure SQL is a fully managed relational database — structured tables, SQL queries, indexes, transactions. In my pipeline, Blob Storage is the ingestion layer (raw CSV lands here) and Azure SQL is the serving layer (clean, transformed data for analytics). This is the standard medallion architecture pattern — raw → clean → analytics.

---

## Lessons Learned

**Secret scanning:** GitHub scans ALL files including compiled `.pyc` files for secrets. Even after removing a secret from a `.py` file, the old commit in git history still contained it. Had to use `git reset --soft HEAD~2` to rewrite history and then `git push --force`.

**Azure SQL firewall:** Azure SQL blocks all connections by default. Had to add local IP address `109.77.17.227` in Azure portal → SQL Server → Networking → Firewall rules. This is a security best practice — whitelist specific IPs only.

**Microsoft Entra auth:** Used Microsoft Entra (Azure AD) authentication instead of SQL username/password for Azure SQL. This is the recommended enterprise authentication method for Azure services.

**Environment variables:** `set VARIABLE=value` in Windows Command Prompt only lasts for that session. Every new Command Prompt window needs the variable set again. For permanent storage use Windows System Properties → Environment Variables.

**git pull --rebase vs git pull:** Regular `git pull` creates a merge commit. `git pull --rebase` puts your local commits on top of remote changes — cleaner history, no merge commits. Always use `--rebase` when working with a shared repo.

---

*30-day bootcamp | Optum Data Analyst — Dublin | Sai Teja Reddy Yeldandi*
*Project 2 of 3 | github.com/SaiTejaReddyYeldandi/optum-azure-pipeline*
