# Tax Training Data Pipeline

A production-ready data extraction pipeline that generates realistic household distribution tables from US Census PUMS and Bureau of Labor Statistics data for tax preparation training applications.

## 📊 What This Pipeline Does

Extracts and processes data to create **21 distribution tables** across three stages:

### Stage 1: PUMS Extraction (16 tables)
- Household patterns (married couples, single parents, blended families, multigenerational)
- Employment distributions by age and sex
- Children and parent age relationships
- Social Security and retirement income
- Interest, dividend, and property tax distributions
- Mortgage interest patterns
- Education and disability distributions
- Public assistance income
- Adult-child, stepchild, and unmarried partner patterns

### Stage 2: BLS Extraction (1 table)
- Occupation wage distributions by state from OEWS data

### Stage 3: Derived Tables (3 tables)
- Education × Occupation probability matrices
- Age-based income adjustment multipliers
- Self-employment probability by occupation

---

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- PostgreSQL database (optional, for database mode)
- Git

### Local Installation

```bash
# Clone repository
git clone https://github.com/yourusername/tax-training-data-pipeline.git
cd tax-training-data-pipeline

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

---

## 💻 Local Usage

### Extract Data for a Single State

```bash
# Stage 1: Extract PUMS distributions
python scripts/extract_pums.py --state HI --year 2022 --output sql

# Stage 2: Extract BLS occupation wages
python scripts/extract_bls.py --state HI --year 2023 --output sql

# Stage 3: Extract derived tables (requires stages 1 & 2 to be run first)
python scripts/extract_derived.py --state HI --pums-year 2022 --bls-year 2023 --output sql
```

**Output:** SQL files in `output/` directory ready for import

### Import to Local Database

```bash
# Import all tables
psql -d mydb -f output/pums_distributions_HI_2022.sql
psql -d mydb -f output/bls_occupation_wages_HI_2023.sql
psql -d mydb -f output/derived_distributions_HI_pums_2022_bls_2023.sql
```

### Direct Database Upload (Local)

```bash
# Set database connection
export DATABASE_URL="postgresql://user:password@localhost:5432/mydb"

# Run with database output
python scripts/extract_pums.py --state HI --year 2022 --output database
python scripts/extract_bls.py --state HI --year 2023 --output database
python scripts/extract_derived.py --state HI --pums-year 2022 --bls-year 2023 --output database
```

---

## ☁️ GitHub Actions Usage

### Setup

1. **Add Database Secret** (if using database mode)
   - Go to: `Settings` → `Secrets and variables` → `Actions`
   - Click `New repository secret`
   - Name: `DATABASE_URL`
   - Value: Your Neon PostgreSQL connection string
     ```
     postgresql://user:password@host.neon.tech:5432/dbname?sslmode=require
     ```

### Manual Workflow Trigger

1. Go to `Actions` tab in your repository
2. Select `Extract Tax Training Data` workflow
3. Click `Run workflow`
4. Configure parameters:

   | Parameter | Description | Example |
   |-----------|-------------|---------|
   | **States** | Comma-separated state codes | `HI` or `HI,CA,TX` |
   | **PUMS Year** | Census PUMS data year | `2022` |
   | **BLS Year** | BLS OEWS data year | `2023` |
   | **Output Mode** | `sql` (artifacts) or `database` (upload) | `sql` |
   | **Run Stages** | `all`, `pums_only`, `bls_only`, `derived_only` | `all` |

### Example Scenarios

#### Scenario 1: Generate SQL Files for Single State
```
States: HI
PUMS Year: 2022
BLS Year: 2023
Output Mode: sql
Run Stages: all
```
**Result:** Download SQL files from workflow artifacts (90-day retention)

#### Scenario 2: Upload Data to Neon Database
```
States: HI
PUMS Year: 2022
BLS Year: 2023
Output Mode: database
Run Stages: all
```
**Result:** Tables automatically uploaded to your Neon PostgreSQL database

#### Scenario 3: Process Multiple States
```
States: HI,CA,TX,NY,FL
PUMS Year: 2022
BLS Year: 2023
Output Mode: sql
Run Stages: all
```
**Result:** Parallel processing, separate artifacts per state

#### Scenario 4: Re-run Only Derived Tables
```
States: HI
PUMS Year: 2022
BLS Year: 2023
Output Mode: database
Run Stages: derived_only
```
**Result:** Skips PUMS/BLS, only runs derived stage (uses cached data)

---

## 📁 Repository Structure

```
tax-training-data-pipeline/
├── .github/
│   └── workflows/
│       └── extract-data.yml          # Main GitHub Actions workflow
│
├── scripts/
│   ├── extract_pums.py              # Stage 1: PUMS extraction
│   ├── extract_bls.py               # Stage 2: BLS extraction
│   └── extract_derived.py           # Stage 3: Derived tables
│
├── .gitignore                        # Ignore cache and output directories
├── requirements.txt                  # Python dependencies
└── README.md                         # This file

# Runtime directories (not in git)
├── pums_cache/                      # Census PUMS downloads (cached)
├── bls_cache/                       # BLS OEWS downloads (cached)
└── output/                          # Generated SQL files
```

---

## 🔧 Configuration

### Supported States

All 50 US states + DC. Use two-letter postal codes:

```
AK, AL, AR, AZ, CA, CO, CT, DC, DE, FL, GA, HI, IA, ID, IL, IN, KS, KY,
LA, MA, MD, ME, MI, MN, MO, MS, MT, NC, ND, NE, NH, NJ, NM, NV, NY, OH,
OK, OR, PA, RI, SC, SD, TN, TX, UT, VA, VT, WA, WI, WV, WY
```

### Data Years

- **PUMS:** 2017-2022 (5-year estimates)
- **BLS OEWS:** 2018-2023

### Database Table Naming

Tables follow this naming convention:

```
PUMS tables:    {table_name}_{STATE}_{YEAR}
                Example: household_patterns_HI_2022

BLS tables:     bls_occupation_wages_{STATE}_{YEAR}
                Example: bls_occupation_wages_HI_2023

Derived tables: {table_name}_{STATE}_pums_{PUMS_YEAR}_bls_{BLS_YEAR}
                Example: education_occupation_probabilities_HI_pums_2022_bls_2023
```

---

## 🎯 Workflow Features

### Smart Caching
- **PUMS data** cached for 7 days (large files ~100MB per state)
- **BLS data** cached for 7 days (shared across states)
- Automatic cache invalidation on year change

### Parallel Processing
- Multiple states run in parallel
- Independent PUMS and BLS extraction
- Derived stage waits for both dependencies

### Resource Optimization
- **Single state:** ~5-10 minutes
- **5 states:** ~10-15 minutes (parallel)
- **50 states:** ~60-90 minutes (matrix strategy)

### Error Handling
- Individual state failures don't stop entire workflow
- Transaction rollback on database upload errors
- Clear error messages with actionable steps

---

## 📊 Database Schema

### Sample Table: `household_patterns_HI_2022`

| Column | Type | Description |
|--------|------|-------------|
| state_code | VARCHAR(2) | State code |
| pattern | TEXT | Household pattern type |
| percentage | DECIMAL(5,2) | Percentage of households |
| weight | INTEGER | Population weight |
| year | INTEGER | Data year |

**Pattern Types:**
- `married_couple_no_children`
- `married_couple_with_children`
- `blended_family`
- `single_parent`
- `multigenerational`
- `unmarried_partners`
- `single_adult`

---

## 🛠️ Troubleshooting

### Local Development

**Issue:** `FileNotFoundError: PUMS cache not found`
```bash
# Solution: Run PUMS extraction first
python scripts/extract_pums.py --state HI --year 2022 --output sql
```

**Issue:** `ImportError: No module named 'pandas'`
```bash
# Solution: Install dependencies
pip install -r requirements.txt
```

**Issue:** BLS download fails with 403 error
```bash
# Solution: Use manual download
# 1. Visit: https://www.bls.gov/oes/special.requests/oesm23st.zip
# 2. Save to: bls_cache/oesm23st.zip
# 3. Re-run script
```

### GitHub Actions

**Issue:** Workflow fails with "DATABASE_URL not set"
```
Solution: Add DATABASE_URL to repository secrets
Settings → Secrets and variables → Actions → New repository secret
```

**Issue:** PUMS download timeout
```
Solution: Re-run workflow - cached data will be used
```

**Issue:** "derived_only" stage fails
```
Solution: Ensure PUMS and BLS caches exist (run "all" stages first)
```

---

## 🔐 Security

- Database credentials stored as GitHub secrets (never in code)
- Connection strings not logged in workflow output
- SQL injection protection via parameterized queries
- TLS/SSL required for database connections

---

## 📈 Scaling to All 50 States

### Batch Processing

Create a configuration file with multiple states:

```bash
# Process multiple states
python scripts/extract_pums.py --state CA --year 2022 --output database
python scripts/extract_pums.py --state TX --year 2022 --output database
# ... etc
```

### GitHub Actions Matrix

For all 50 states, modify workflow input:

```
States: HI,AK,AL,AR,AZ,CA,CO,CT,DC,DE,FL,GA,IA,ID,IL,IN,KS,KY,LA,MA,MD,ME,MI,MN,MO,MS,MT,NC,ND,NE,NH,NJ,NM,NV,NY,OH,OK,OR,PA,RI,SC,SD,TN,TX,UT,VA,VT,WA,WI,WV,WY
```

**Note:** GitHub Actions free tier = 2,000 minutes/month. 50 states = ~60-90 minutes.

---

## 🤝 Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

---

## 📝 License

[Choose your license - MIT, Apache 2.0, etc.]

---

## 📞 Support

- **Issues:** [GitHub Issues](https://github.com/yourusername/tax-training-data-pipeline/issues)
- **Documentation:** This README
- **Data Sources:**
  - [Census PUMS](https://www.census.gov/programs-surveys/acs/microdata.html)
  - [BLS OEWS](https://www.bls.gov/oes/)

---

## 🎓 Data Sources & Attribution

This pipeline uses public data from:

- **US Census Bureau** - American Community Survey (ACS) Public Use Microdata Sample (PUMS)
- **Bureau of Labor Statistics** - Occupational Employment and Wage Statistics (OEWS)

Data is subject to the respective agencies' terms of use.
