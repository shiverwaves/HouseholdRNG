# Setup Guide

Complete setup instructions for the Tax Training Data Pipeline repository.

---

## Table of Contents

1. [Initial Repository Setup](#initial-repository-setup)
2. [Local Development Setup](#local-development-setup)
3. [GitHub Actions Setup](#github-actions-setup)
4. [Neon Database Setup](#neon-database-setup)
5. [First Workflow Run](#first-workflow-run)
6. [Verification](#verification)

---

## Initial Repository Setup

### 1. Create GitHub Repository

```bash
# Create a new repository on GitHub
# Name: tax-training-data-pipeline
# Description: Data extraction pipeline for tax training household generation
# Visibility: Private (recommended) or Public

# Clone locally
git clone https://github.com/yourusername/tax-training-data-pipeline.git
cd tax-training-data-pipeline
```

### 2. Add Project Files

```bash
# Copy all files into repository
# Structure should look like:
# .github/workflows/
# scripts/
# config/
# requirements.txt
# .gitignore
# README.md

# Initialize git (if not already)
git init

# Add files
git add .
git commit -m "Initial commit: Tax training data pipeline"
git push origin main
```

---

## Local Development Setup

### 1. Python Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # On macOS/Linux
# OR
venv\Scripts\activate     # On Windows
```

### 2. Install Dependencies

```bash
# Install all required packages
pip install -r requirements.txt

# Verify installation
python -c "import pandas; import psycopg2; print('✓ Dependencies installed')"
```

### 3. Test Local Extraction (Optional)

```bash
# Run a test extraction (generates SQL file)
python scripts/extract_pums.py --state HI --year 2022 --output sql

# Check output
ls output/
# Should see: pums_distributions_HI_2022.sql
```

---

## GitHub Actions Setup

### 1. Enable GitHub Actions

1. Go to your repository on GitHub
2. Click `Settings` tab
3. Click `Actions` → `General`
4. Under "Actions permissions", select:
   - ✅ Allow all actions and reusable workflows
5. Click `Save`

### 2. Configure Repository Secrets

#### Add DATABASE_URL Secret

1. Go to `Settings` → `Secrets and variables` → `Actions`
2. Click `New repository secret`
3. Configure:
   - **Name:** `DATABASE_URL`
   - **Secret:** Your Neon connection string (see below)
4. Click `Add secret`

**Format for Neon PostgreSQL:**
```
postgresql://user:password@ep-xxx-xxx.us-west-2.aws.neon.tech:5432/dbname?sslmode=require
```

**Where to find your Neon connection string:**
1. Log in to [Neon Console](https://console.neon.tech)
2. Select your project
3. Go to `Connection Details`
4. Copy the connection string (make sure it includes `?sslmode=require`)

---

## Neon Database Setup

### 1. Create Neon Account

1. Visit [neon.tech](https://neon.tech)
2. Sign up (free tier available)
3. Create a new project

### 2. Create Database

```sql
-- Option 1: Use default database (neondb)
-- No action needed

-- Option 2: Create dedicated database
CREATE DATABASE tax_training_data;
```

### 3. Configure Connection

**Connection Details:**
- **Host:** `ep-xxx-xxx.us-west-2.aws.neon.tech`
- **Database:** `tax_training_data` (or `neondb`)
- **User:** Auto-generated username
- **Password:** Auto-generated password
- **SSL Mode:** Required (`sslmode=require`)

**Connection String Format:**
```
postgresql://[username]:[password]@[host]:5432/[database]?sslmode=require
```

### 4. Test Connection Locally

```bash
# Set environment variable
export DATABASE_URL="postgresql://user:password@host:5432/dbname?sslmode=require"

# Test with psql (if installed)
psql "$DATABASE_URL" -c "SELECT version();"

# Or test with Python
python -c "
import psycopg2
import os
conn = psycopg2.connect(os.getenv('DATABASE_URL'))
print('✓ Database connection successful')
conn.close()
"
```

---

## First Workflow Run

### Test Run: Single State (SQL Mode)

**Recommended for first run - generates SQL files without touching database**

1. Go to `Actions` tab
2. Select `Extract Tax Training Data` workflow
3. Click `Run workflow`
4. Configure:
   ```
   States: HI
   PUMS Year: 2022
   BLS Year: 2023
   Output Mode: sql
   Run Stages: all
   ```
5. Click `Run workflow` (green button)

**Expected Duration:** ~5-10 minutes

**Expected Results:**
- ✅ 3 workflow jobs complete successfully
- ✅ 3 artifacts available for download:
  - `pums-sql-HI-2022`
  - `bls-sql-HI-2023`
  - `derived-sql-HI-pums2022-bls2023`

### Production Run: Database Upload

**Only after verifying SQL mode works**

1. Go to `Actions` tab
2. Select `Extract Tax Training Data` workflow
3. Click `Run workflow`
4. Configure:
   ```
   States: HI
   PUMS Year: 2022
   BLS Year: 2023
   Output Mode: database
   Run Stages: all
   ```
5. Click `Run workflow`

**Expected Results:**
- ✅ Data uploaded directly to Neon database
- ✅ 20 new tables in database (16 PUMS + 1 BLS + 3 Derived)

---

## Verification

### Verify SQL Files (SQL Mode)

1. Download artifacts from workflow run
2. Unzip files
3. Inspect SQL files:
   ```bash
   head -50 pums_distributions_HI_2022.sql
   ```

4. Import to local PostgreSQL (optional):
   ```bash
   psql -d mydb -f pums_distributions_HI_2022.sql
   psql -d mydb -f bls_occupation_wages_HI_2023.sql
   psql -d mydb -f derived_distributions_HI_pums_2022_bls_2023.sql
   ```

### Verify Database Tables (Database Mode)

**Connect to Neon:**
```bash
psql "$DATABASE_URL"
```

**List all tables:**
```sql
\dt

-- You should see tables like:
-- household_patterns_HI_2022
-- employment_by_age_HI_2022
-- bls_occupation_wages_HI_2023
-- education_occupation_probabilities_HI_pums_2022_bls_2023
-- etc.
```

**Count records:**
```sql
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE tablename LIKE '%_HI_%'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

**Sample data:**
```sql
SELECT * FROM household_patterns_HI_2022 LIMIT 10;
SELECT * FROM bls_occupation_wages_HI_2023 LIMIT 10;
SELECT * FROM education_occupation_probabilities_HI_pums_2022_bls_2023 LIMIT 10;
```

---

## Troubleshooting

### Issue: Workflow fails with "DATABASE_URL not set"

**Solution:**
```
1. Check secret name is exactly: DATABASE_URL (case-sensitive)
2. Verify secret is in "Actions" secrets (not "Dependabot" or "Codespaces")
3. Re-run workflow
```

### Issue: Database connection refused

**Solution:**
```
1. Verify Neon project is active (not paused)
2. Check connection string has ?sslmode=require
3. Test connection locally first
4. Check Neon dashboard for connection status
```

### Issue: PUMS download timeout

**Solution:**
```
1. Re-run workflow (cache will speed up subsequent runs)
2. If persistent, try different state
3. Check Census website status
```

### Issue: BLS download fails with 403

**Solution:**
```
1. BLS server may be blocking automated downloads
2. Manual workaround:
   - Download from: https://www.bls.gov/oes/special.requests/oesm23st.zip
   - Upload as workflow artifact
   - Re-run workflow
3. This is cached for 7 days after first successful download
```

---

## Next Steps

### Scale to Multiple States

```yaml
# Process 5 states
States: HI,CA,TX,NY,FL
```

### Batch Processing

Use the `Extract All States (Batch)` workflow:
- `test`: 1 state (HI)
- `small`: 5 states
- `large`: Top 10 by population
- `full`: All 50 states + DC

### Schedule Regular Updates

Add to workflow file:
```yaml
on:
  workflow_dispatch:
  schedule:
    - cron: '0 2 1 * *'  # 2 AM on 1st of each month
```

### Monitor Usage

- GitHub Actions free tier: 2,000 minutes/month
- Single state run: ~10 minutes
- All 50 states: ~90 minutes
- Track usage: `Settings` → `Billing` → `Usage this month`

---

## Support

- **Repository Issues:** [Create an issue](https://github.com/yourusername/tax-training-data-pipeline/issues)
- **GitHub Actions Docs:** [GitHub Actions Documentation](https://docs.github.com/actions)
- **Neon Support:** [Neon Documentation](https://neon.tech/docs)

---

## Security Checklist

- ✅ DATABASE_URL stored as secret (never committed to code)
- ✅ Connection uses SSL/TLS (`?sslmode=require`)
- ✅ Repository is private (if handling sensitive data)
- ✅ Secrets never logged in workflow output
- ✅ Local `.env` files in `.gitignore`

---

## Success Criteria

You've successfully set up the pipeline when:

✅ Local extraction works (`python scripts/extract_pums.py ...`)  
✅ GitHub Actions workflow runs without errors  
✅ SQL artifacts can be downloaded  
✅ Database mode uploads tables to Neon  
✅ Tables contain expected data (spot check)  

**Congratulations!** Your data pipeline is ready for production use.
