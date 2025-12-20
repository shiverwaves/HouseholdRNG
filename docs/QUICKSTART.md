# Quick Start Guide

Get up and running with Tax Prep Data Extraction in 5 minutes.

## Prerequisites

- Python 3.9 or higher
- PostgreSQL (optional - for importing data)
- Git

## Installation

### 1. Clone Repository

```bash
git clone https://github.com/yourusername/taxprep-data-extraction.git
cd taxprep-data-extraction
```

### 2. Install Dependencies

```bash
# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install requirements
pip install -r requirements.txt
```

## Basic Usage

### Extract Data for One State

```bash
# Extract for Hawaii (HI)
python scripts/extract_pums_distributions.py --state HI --year 2022
python scripts/extract_bls_distributions.py --state HI --year 2023
python scripts/extract_derived_distributions.py --state HI
```

**What happens:**
- Downloads Census PUMS data (~50-150 MB depending on state)
- Downloads BLS OEWS data (~10 MB, shared across all states)
- Processes and creates SQL files in `output/` directory

**Runtime:** 3-10 minutes depending on state size and internet speed

### Check Output

```bash
ls -lh output/
```

You should see three SQL files:
- `pums_distributions_HI_2022.sql` (~100-200 KB)
- `bls_occupation_wages_HI_2023.sql` (~50-100 KB)
- `derived_probabilities_HI_2023.sql` (~20-50 KB)

### Import to Database (Optional)

```bash
# Make sure PostgreSQL is running and database exists
createdb taxapp_dev  # If database doesn't exist

# Import all three SQL files
./database/import_all.sh HI

# Verify import
psql -d taxapp_dev -c "\dt"
```

## Using GitHub Actions

### Run in the Cloud

1. Fork/clone repository to your GitHub account
2. Go to **Actions** tab
3. Click **Extract Distribution Tables**
4. Click **Run workflow**
5. Enter state code (e.g., `HI`)
6. Click **Run workflow**
7. Wait 5-15 minutes
8. Download SQL files from **Artifacts**

**Benefits:**
- No local setup required
- Runs on GitHub's servers
- Caches data for fast re-runs
- Process multiple states in parallel

## Extract Multiple States

### Local Script

```bash
# Create extraction script
cat > extract_multiple.sh << 'EOF'
#!/bin/bash
STATES=(HI CA TX NY FL)

for state in "${STATES[@]}"; do
    echo "Processing $state..."
    python scripts/extract_pums_distributions.py --state $state --year 2022
    python scripts/extract_bls_distributions.py --state $state --year 2023
    python scripts/extract_derived_distributions.py --state $state
done
EOF

chmod +x extract_multiple.sh
./extract_multiple.sh
```

### Using Cache

**Good news:** After first state downloads BLS data, subsequent states reuse it (instant!)

```
HI: Downloads PUMS-HI + BLS (first time) - 5 min
CA: Downloads PUMS-CA, uses BLS cache - 3 min
TX: Downloads PUMS-TX, uses BLS cache - 3 min
```

## Common Tasks

### Clear Cache and Re-download

```bash
# Clear all caches
rm -rf cache/pums_cache/*
rm -rf cache/bls_cache/*

# Next run will re-download everything
```

### Change Data Year

```bash
# Extract 2021 PUMS data instead of 2022
python scripts/extract_pums_distributions.py --state HI --year 2021
```

### Extract for Different State

```bash
# California
python scripts/extract_pums_distributions.py --state CA --year 2022
python scripts/extract_bls_distributions.py --state CA --year 2023
python scripts/extract_derived_distributions.py --state CA
```

## Troubleshooting

### Download Fails

**Problem:** Network timeout or connection error

**Solution:** Just re-run the script - it will resume from cache

### Import Fails

**Problem:** `psql: FATAL: database "taxapp_dev" does not exist`

**Solution:** Create database first:
```bash
createdb taxapp_dev
```

### Permission Denied

**Problem:** `./database/import_all.sh: Permission denied`

**Solution:** Make script executable:
```bash
chmod +x database/import_all.sh
```

## Next Steps

- Read [PIPELINE_GUIDE.md](PIPELINE_GUIDE.md) for complete documentation
- Review [EXTRACTION_LOGIC.md](EXTRACTION_LOGIC.md) for how extraction works
- Check out [GitHub Actions workflow](.github/workflows/extract-distributions.yml)

## Need Help?

- Check the [troubleshooting guide](PIPELINE_GUIDE.md#troubleshooting)
- Open an issue on GitHub
- Review the example SQL files in `output/`
