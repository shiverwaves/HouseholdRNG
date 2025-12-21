# Tax Training Data Pipeline - Complete Repository Structure

## 📁 Final Structure

```
tax-training-data-pipeline/
│
├── .github/
│   └── workflows/
│       ├── extract-data.yml              # Main manual workflow (flexible, single/multi-state)
│       └── extract-all-states.yml        # Batch processing (test/small/large/full)
│
├── scripts/
│   ├── extract_pums.py                   # Stage 1: PUMS extraction (16 tables)
│   ├── extract_bls.py                    # Stage 2: BLS extraction (1 table)
│   └── extract_derived.py                # Stage 3: Derived tables (3 tables)
│
├── config/
│   └── states.yml                        # State configurations & batch definitions
│
├── .gitignore                            # Ignore cache/output/env files
├── requirements.txt                      # Python dependencies (pinned versions)
├── README.md                            # Main documentation
└── SETUP.md                             # Step-by-step setup guide

# Runtime directories (created automatically, not in git)
├── pums_cache/                          # Census PUMS data (cached)
├── bls_cache/                           # BLS OEWS data (cached)
└── output/                              # Generated SQL files
```

---

## 🎯 Quick Start Guide

### For Local Development

```bash
# 1. Clone repository
git clone https://github.com/yourusername/tax-training-data-pipeline.git
cd tax-training-data-pipeline

# 2. Setup Python environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt

# 3. Run extraction
python scripts/extract_pums.py --state HI --year 2022 --output sql
python scripts/extract_bls.py --state HI --year 2023 --output sql
python scripts/extract_derived.py --state HI --pums-year 2022 --bls-year 2023 --output sql

# 4. SQL files generated in output/
```

### For GitHub Actions (CI/CD)

```bash
# 1. Push code to GitHub
git push origin main

# 2. Add DATABASE_URL secret
Settings → Secrets → Actions → New secret
Name: DATABASE_URL
Value: postgresql://user:pass@host:5432/db?sslmode=require

# 3. Run workflow
Actions → Extract Tax Training Data → Run workflow
Configure: States=HI, Years, Output Mode → Run

# 4. Download SQL artifacts OR data in database
```

---

## 🔄 Workflow Comparison

### `extract-data.yml` - Main Workflow
**Use for:** Day-to-day operations, single state testing, custom state combinations

**Features:**
- Manual trigger with full parameter control
- State selection: Single or comma-separated (e.g., HI,CA,TX)
- Choose output mode: SQL artifacts or database upload
- Select stages: all, pums_only, bls_only, derived_only
- Smart caching and parallel processing

**Example Use Cases:**
- Testing: `States=HI, Mode=sql, Stages=all`
- Production: `States=HI,CA,TX, Mode=database, Stages=all`
- Update derived: `States=HI, Mode=database, Stages=derived_only`

### `extract-all-states.yml` - Batch Workflow
**Use for:** Bulk processing, annual refreshes, database rebuilds

**Features:**
- Predefined batch sizes (test/small/large/full)
- Optimized for all 50 states processing
- Parallel processing with rate limiting
- Progress tracking and summary reports

**Batch Sizes:**
- `test`: 1 state (HI) - ~10 min
- `small`: 5 states - ~15 min
- `large`: 10 states - ~30 min
- `full`: 51 states - ~90 min

---

## 📊 Data Pipeline Summary

### Stage 1: PUMS (16 tables per state)
```
Census PUMS Data → 16 Distribution Tables
- household_patterns
- employment_by_age
- children_by_parent_age
- child_age_distributions
- social_security
- retirement_income
- interest_and_dividend_income
- property_taxes
- mortgage_interest
- education_by_age
- disability_by_age
- other_income_by_employment_status
- public_assistance_income
- adult_child_ages
- stepchild_patterns
- multigenerational_patterns
- unmarried_partner_patterns
```

### Stage 2: BLS (1 table per state)
```
BLS OEWS Data → 1 Occupation Wage Table
- bls_occupation_wages
```

### Stage 3: Derived (3 tables per state)
```
PUMS + BLS Combined → 3 Probability Tables
- education_occupation_probabilities
- age_income_adjustments
- occupation_self_employment_probability
```

**Total: 20 tables per state**

---

## 🗄️ Database Schema Example

After running for Hawaii (HI):

```sql
-- PUMS tables (16)
household_patterns_HI_2022
employment_by_age_HI_2022
children_by_parent_age_HI_2022
-- ... (13 more)

-- BLS table (1)
bls_occupation_wages_HI_2023

-- Derived tables (3)
education_occupation_probabilities_HI_pums_2022_bls_2023
age_income_adjustments_HI_pums_2022_bls_2023
occupation_self_employment_probability_HI_pums_2022_bls_2023
```

---

## 🔐 Secrets & Configuration

### Required GitHub Secrets

| Secret Name | Description | Example |
|------------|-------------|---------|
| `DATABASE_URL` | Neon PostgreSQL connection | `postgresql://user:pass@host.neon.tech:5432/db?sslmode=require` |

### Optional Environment Variables (Local)

```bash
# For local database mode
export DATABASE_URL="postgresql://localhost:5432/mydb"
```

---

## 📈 Scaling Considerations

### GitHub Actions Free Tier
- **Limit:** 2,000 minutes/month
- **Single state:** ~10 minutes
- **5 states:** ~15 minutes
- **50 states:** ~90 minutes
- **Monthly capacity:** ~20 full runs or 200 single-state runs

### Neon Free Tier
- **Storage:** 0.5 GB
- **Compute:** Shared
- **Tables:** Unlimited
- **Estimated storage per state:** ~5-10 MB (20 tables)
- **Capacity:** ~50-100 states

### Optimization Tips
1. Use SQL mode for development/testing (free)
2. Use database mode for production refreshes
3. Cache persists 7 days - subsequent runs are faster
4. Process states in batches during off-peak hours
5. Consider upgrading tiers for heavy usage

---

## 🎓 Learning Path

### Beginner
1. Read `SETUP.md` for initial setup
2. Run single state with SQL mode locally
3. Import SQL to local PostgreSQL
4. Examine table structures and data

### Intermediate
1. Set up Neon database
2. Configure GitHub Actions secrets
3. Run workflow in database mode
4. Query data from Neon console

### Advanced
1. Process multiple states in parallel
2. Schedule automated monthly refreshes
3. Create views/materialized views on top of tables
4. Integrate with household generation system

---

## 📚 Documentation Index

- **README.md** - Main documentation, usage examples
- **SETUP.md** - Step-by-step setup guide
- **states.yml** - State configurations
- **Workflow files** - GitHub Actions automation
- **Script docstrings** - Inline documentation

---

## ✅ Success Checklist

- [ ] Repository created and code pushed
- [ ] Python dependencies installed locally
- [ ] Local SQL extraction tested (HI)
- [ ] GitHub Actions enabled
- [ ] DATABASE_URL secret configured
- [ ] Neon database created and tested
- [ ] First workflow run successful (SQL mode)
- [ ] Database upload tested (database mode)
- [ ] Tables verified in Neon console
- [ ] Data quality spot-checked

---

## 🚀 You're Ready!

Your tax training data pipeline is production-ready. You can now:

✅ Extract data for any US state  
✅ Generate SQL files for manual import  
✅ Auto-upload to Neon database  
✅ Scale to all 50 states  
✅ Schedule regular updates  
✅ Support household generation pipeline  

**Next step:** Integrate with your household generation system!

---

## 📞 Support Resources

- GitHub Issues (recommended)
- Census PUMS Documentation
- BLS OEWS Documentation  
- Neon PostgreSQL Documentation
- GitHub Actions Documentation

**Good luck building your tax training application!** 🎉
