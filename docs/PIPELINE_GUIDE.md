# Complete Data Extraction Pipeline

## Overview

The household generation pipeline requires three types of distribution tables:

1. **PUMS distributions** - Census demographic data
2. **BLS distributions** - Occupation and wage data  
3. **Derived distributions** - Combined PUMS + BLS probabilities

We have **three extraction scripts** that work together:

```
extract_pums_distributions.py    (Script 1)
    ↓
extract_bls_distributions.py     (Script 2)
    ↓
extract_derived_distributions.py (Script 3) ← Uses cache from 1 & 2
```

---

## 🏠 Local Development Workflow

### Step 1: Extract PUMS Distributions

```bash
python extract_pums_distributions.py --state HI --year 2022
```

**What happens:**
- Downloads: `pums_cache/2022_csv_hhi.zip` + `2022_csv_phi.zip` (~150 MB)
- Processes: 9 distribution tables
- Outputs: `output/pums_distributions_HI_2022.sql`
- Cache persists: Future runs use cache (no re-download)

### Step 2: Extract BLS Distributions

```bash
python extract_bls_distributions.py --state HI --year 2023
```

**What happens:**
- Downloads: `bls_cache/oews_2023_state_data.xlsx` (~10 MB)
- Processes: 1 occupation wage table
- Outputs: `output/bls_occupation_wages_HI_2023.sql`
- Cache persists: Future runs use cache (no re-download)

### Step 3: Extract Derived Distributions

```bash
python extract_derived_distributions.py --state HI --pums-year 2022 --bls-year 2023
```

**What happens:**
- **NO downloads!** Reads from `pums_cache/` and `bls_cache/`
- Combines PUMS person data + BLS occupation data
- Creates 3 derived probability tables
- Outputs: `output/derived_probabilities_HI_2023.sql`

### Step 4: Import to Database

```bash
# Import all three SQL files
psql -d taxapp_dev -f output/pums_distributions_HI_2022.sql
psql -d taxapp_dev -f output/bls_occupation_wages_HI_2023.sql
psql -d taxapp_dev -f output/derived_probabilities_HI_2023.sql
```

**Result:** Database has all tables needed for household generation!

---

## 🔄 How Caching Works

### Local Development

```
First run:
  Script 1: Download PUMS → Save to cache → Process → Output SQL
  Script 2: Download BLS → Save to cache → Process → Output SQL
  Script 3: Load from cache → Combine → Output SQL

Second run (same state/year):
  Script 1: Load from cache → Process → Output SQL (fast!)
  Script 2: Load from cache → Process → Output SQL (fast!)
  Script 3: Load from cache → Combine → Output SQL (fast!)
```

**Cache directories:**
```
pums_cache/
├── 2022_csv_hhi.zip    (Hawaii households)
└── 2022_csv_phi.zip    (Hawaii persons)

bls_cache/
└── oews_2023_state_data.xlsx  (ALL states - one file)
```

**Key insight:** BLS cache contains ALL states in one file. Once cached, processing any state is instant!

---

## ☁️ GitHub Actions Workflow

### The Challenge

**Problem:** GitHub Actions starts each job with a clean environment (no cache).

**Solution:** Run all three scripts in the **same job** so cache persists between steps.

### Single-Job Approach (Recommended)

```yaml
jobs:
  generate-all-distributions:
    runs-on: ubuntu-latest
    steps:
      - name: Extract PUMS
        run: python extract_pums_distributions.py --state HI --year 2022
        # Creates: pums_cache/ and output/pums_*.sql
      
      - name: Extract BLS  
        run: python extract_bls_distributions.py --state HI --year 2023
        # Creates: bls_cache/ and output/bls_*.sql
      
      - name: Extract Derived
        run: python extract_derived_distributions.py --state HI
        # Uses: pums_cache/ + bls_cache/ (from previous steps!)
        # Creates: output/derived_*.sql
      
      - name: Upload SQL files
        uses: actions/upload-artifact@v3
        with:
          name: all-distributions-HI
          path: output/*.sql
```

**Why this works:**
- ✅ All steps run in same container
- ✅ Filesystem (including cache) persists between steps
- ✅ Script 3 can read cache created by scripts 1 & 2
- ✅ Simple and fast

### Multi-Job Approach (Alternative)

If you need jobs to run in parallel or have other constraints:

```yaml
jobs:
  extract-pums:
    steps:
      - run: python extract_pums_distributions.py --state HI
      - name: Upload cache
        uses: actions/upload-artifact@v3
        with:
          name: pums-cache-HI
          path: pums_cache/

  extract-bls:
    steps:
      - run: python extract_bls_distributions.py --state HI
      - name: Upload cache
        uses: actions/upload-artifact@v3
        with:
          name: bls-cache-HI
          path: bls_cache/

  extract-derived:
    needs: [extract-pums, extract-bls]
    steps:
      - name: Download PUMS cache
        uses: actions/download-artifact@v3
        with:
          name: pums-cache-HI
          path: pums_cache/
      
      - name: Download BLS cache
        uses: actions/download-artifact@v3
        with:
          name: bls-cache-HI
          path: bls_cache/
      
      - run: python extract_derived_distributions.py --state HI
```

**Trade-offs:**
- ✅ Jobs can run in parallel (PUMS and BLS download simultaneously)
- ❌ More complex
- ❌ Artifact upload/download overhead
- ❌ Uses more GitHub Actions minutes

**Recommendation:** Use single-job approach unless you need parallelism.

---

## 📊 Complete Pipeline Example

### Local: Process Multiple States

```bash
#!/bin/bash
# process_all_states.sh

STATES=("HI" "CA" "TX" "NY" "FL")
PUMS_YEAR=2022
BLS_YEAR=2023

for state in "${STATES[@]}"; do
    echo "Processing $state..."
    
    # Step 1: PUMS
    python extract_pums_distributions.py --state $state --year $PUMS_YEAR
    
    # Step 2: BLS
    python extract_bls_distributions.py --state $state --year $BLS_YEAR
    
    # Step 3: Derived
    python extract_derived_distributions.py \
        --state $state \
        --pums-year $PUMS_YEAR \
        --bls-year $BLS_YEAR
    
    echo "✅ $state complete"
    echo ""
done

echo "🎉 All states processed!"
```

### GitHub Actions: Process Multiple States in Parallel

```yaml
jobs:
  generate-all-distributions:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        state: [HI, CA, TX, NY, FL]
    
    steps:
      - run: python extract_pums_distributions.py --state ${{ matrix.state }}
      - run: python extract_bls_distributions.py --state ${{ matrix.state }}
      - run: python extract_derived_distributions.py --state ${{ matrix.state }}
      - uses: actions/upload-artifact@v3
        with:
          name: distributions-${{ matrix.state }}
          path: output/*.sql
```

**Result:** All 5 states process simultaneously (much faster!)

---

## 🗂️ Final Database Schema

After running all three scripts and importing, your database has:

### PUMS Tables (9 tables)
```sql
household_patterns
employment_by_age
children_by_parent_age
child_age_distributions
social_security
retirement_income
interest_and_dividend_income
property_taxes
mortgage_interest
```

### BLS Tables (1 table)
```sql
bls_occupation_wages
```

### Derived Tables (3 tables)
```sql
education_occupation_probabilities
age_income_adjustments
occupation_self_employment_probability
```

**Total: 13 distribution tables** ready for household generation!

---

## 🎯 Integration with Blueprint

### Blueprint Stage 4: Occupation & Income Assignment

```python
# Generate household member
member = {
    'age': 35,
    'education_level': 'bachelors'
}

# Use derived table to select occupation
occupation = sample_from_distribution(
    'education_occupation_probabilities',
    filters={'education_level': 'bachelors'},
    weight_column='percentage'
)
# Result: occupation.soc_major_group = '29' (Healthcare)

# Use BLS table to get wage for that occupation in state
wage_dist = get_from_distribution(
    'bls_occupation_wages',
    filters={'state_code': 'HI', 'soc_code': '29-1141'}  # Registered Nurses
)
# Result: median_wage = $106,900

# Use derived table to adjust for age
age_adjustment = get_from_distribution(
    'age_income_adjustments',
    filters={'age_bracket': '35-39'}
)
# Result: multiplier = 1.10x

# Calculate final wage
member['wage'] = wage_dist.median_annual_wage * age_adjustment.income_multiplier
# Result: $106,900 × 1.10 = $117,590

# Use derived table to determine if has SE income
se_prob = get_from_distribution(
    'occupation_self_employment_probability',
    filters={'soc_major': '29'}
)
# Result: se_probability = 2.3% (nurses rarely self-employed)

if random.random() * 100 < se_prob.se_probability:
    member['self_employment_income'] = generate_se_income()
```

---

## 📈 Resource Requirements

### Local Development

| State | PUMS Download | BLS Download | Total Time | Total Disk |
|-------|---------------|--------------|------------|------------|
| HI    | 50 MB        | 10 MB (shared) | 3-5 min   | 60 MB      |
| CA    | 200 MB       | 10 MB (shared) | 8-12 min  | 210 MB     |
| TX    | 180 MB       | 10 MB (shared) | 7-10 min  | 190 MB     |

**Note:** BLS file is shared across all states (download once, use for all).

### GitHub Actions (Free Tier)

**Limits:**
- RAM: 7 GB per job
- Time: 6 hours per job
- Storage: 500 MB per artifact
- Minutes: 2000/month

**Usage per state:**
- RAM: 2-4 GB peak (well under limit)
- Time: 5-15 minutes (well under limit)
- Storage: ~500 KB SQL files (well under limit)
- Minutes: ~10 min × 5 states = 50 minutes/run

**Conclusion:** ✅ Easily runs all 50 states if needed!

---

## 🔍 Troubleshooting

### Script 3 Says "Cache not found"

**Error:**
```
FileNotFoundError: PUMS cache not found: pums_cache/2022_csv_phi.zip
Please run: python extract_pums_distributions.py --state HI --year 2022
```

**Solution:** Run scripts in order (1 → 2 → 3)

### Different Years Between Scripts

**This is OK!** You can use:
- PUMS data from 2022
- BLS data from 2023

The derived script accepts both:
```bash
python extract_derived_distributions.py \
    --state HI \
    --pums-year 2022 \
    --bls-year 2023
```

### Cache Taking Too Much Space

```bash
# Clear PUMS cache (will re-download next run)
rm -rf pums_cache/

# Clear BLS cache (will re-download next run)
rm -rf bls_cache/

# Clear all caches
rm -rf pums_cache/ bls_cache/
```

---

## 📋 Summary

**Three scripts, one pipeline:**

1. `extract_pums_distributions.py` - Downloads & processes PUMS → 9 tables
2. `extract_bls_distributions.py` - Downloads & processes BLS → 1 table  
3. `extract_derived_distributions.py` - Combines cached data → 3 tables

**Caching makes it fast:**
- First run: Downloads everything
- Subsequent runs: Uses cache (90% faster)

**Works everywhere:**
- ✅ Local development
- ✅ GitHub Actions  
- ✅ Any CI/CD system

**Result:**
- 13 distribution tables
- Ready for household generation
- Fully reproducible
- Following the blueprint exactly!
