# PUMS Data Extraction Approach

## How This Script Works (NOT Using Census API)

### Data Source
We download **complete CSV files** from the Census Bureau's FTP server, not using the Census API.

**Source URL:** `https://www2.census.gov/programs-surveys/acs/data/pums/2022/5-Year/`

### Files Downloaded

For each state (e.g., Hawaii):

1. **Household File:** `csv_hhi.zip`
   - Contains: One large CSV with ALL household records for Hawaii
   - Size: ~30-50 MB compressed
   - Records: ~50,000 households

2. **Person File:** `csv_phi.zip`
   - Contains: One large CSV with ALL person records for Hawaii
   - Size: ~80-150 MB compressed  
   - Records: ~100,000 persons

### Processing Steps

```
1. DOWNLOAD complete ZIP files from Census FTP
   ↓
2. EXTRACT CSV files from ZIPs
   ↓
3. LOAD entire CSVs into pandas DataFrames (all records in memory)
   ↓
4. PROCESS & AGGREGATE into distribution tables
   ↓
5. EXPORT to SQL file
```

### Why This Approach?

✅ **Simple:** Download once, process many times  
✅ **Fast:** No API rate limits or pagination  
✅ **Cacheable:** Files stored locally for repeated runs  
✅ **Complete:** Get ALL data for comprehensive distributions  
✅ **Reproducible:** Same files = same results  

### What We DON'T Use

❌ Census API (no API calls)  
❌ Streaming/chunked reads (load all data at once)  
❌ Database during extraction (pure pandas → SQL export)  

### Memory Requirements

For typical state:
- Hawaii: ~2 GB RAM
- California: ~4 GB RAM
- Texas: ~3.5 GB RAM

All processing happens in memory using pandas DataFrames.

---

## Extraction Logic Summary

### 1. household_patterns
**Purpose:** Distribution of household types (married, single parent, multigenerational, etc.)

**Logic:**
- Join household + person files on SERIALNO
- Count relationship types per household (stepchildren, grandchildren, partners)
- Classify into patterns using HHT and relationship counts
- Weight by WGTP (household weight)

**Output:** Percentage of households in each pattern

---

### 2. employment_by_age
**Purpose:** Employment status probability given age and sex

**Logic:**
- Filter to adults (18+)
- Create age brackets
- Map ESR codes to employment categories
- Group by age, sex, employment status
- Weight by PWGTP (person weight)

**Output:** P(employment_status | age, sex)

---

### 3. children_by_parent_age
**Purpose:** Number of children probability given parent age

**Logic:**
- Get householder age (RELSHIPP=20)
- Join with household NOC (number of children)
- Group by parent age and number of children
- Weight by WGTP

**Output:** P(num_children | parent_age, household_type)

---

### 4. child_age_distributions
**Purpose:** Child age probability given parent age

**Logic:**
- Filter to children (RELSHIPP 22/23/24)
- Link to householder age via SERIALNO
- Create parent age brackets and child age groups
- Group and calculate distribution
- Weight by PWGTP

**Output:** P(child_age_group | parent_age)

**Example:** 30-year-old parents mostly have kids 0-5; 45-year-olds have teenagers

---

### 5. social_security
**Purpose:** Typical Social Security amounts by age

**Logic:**
- Filter to people with SSP or SSIP > 0
- Group by age bracket (focus 62+)
- Calculate mean/median amounts
- Weight by PWGTP

**Output:** Mean/median SS income by age

---

### 6. retirement_income
**Purpose:** Typical retirement income (pension/IRA) by age

**Logic:**
- Filter to people with RETP > 0
- Group by age bracket (focus 55+)
- Calculate mean/median amounts
- Weight by PWGTP

**Output:** Mean/median retirement income by age

---

### 7. interest_and_dividend_income
**Purpose:** Distribution of investment income amounts (interest + dividends combined)

**Logic:**
- Filter to people with INTP > 0
- **IMPORTANT:** PUMS combines interest and dividends into single INTP variable
- There is NO separate DIVP variable in PUMS data
- Create income brackets ($1-500, $500-2000, etc.)
- Count percentage in each bracket
- Weight by PWGTP

**Output:** Percentage distribution across income brackets

**Tax Implication:** 
- Both interest and dividends reported on Schedule B if over $1,500 total
- For household generation, can split INTP into separate interest/dividend amounts
- Typical split: ~60% interest, ~40% dividends (based on aggregate IRS data)

**Household Generation Example:**
```python
# Sample from distribution
total_investment = sample_from_distribution('interest_and_dividend_income')

# Split for separate reporting (if needed for tax scenario)
interest_income = total_investment * 0.60
dividend_income = total_investment * 0.40
```

---

### 8. property_taxes
**Purpose:** Typical property tax amounts by household income

**Logic:**
- Filter to homeowners (TEN 1/2) with TAXAMT > 0
- Create household income brackets
- Group by income bracket
- Calculate mean/median property tax
- Weight by WGTP

**Output:** Mean/median property tax by income level

**Use:** Assign realistic property tax deduction based on household income

---

### 9. mortgage_interest
**Purpose:** Typical mortgage interest by household income

**Logic:**
- Filter to homeowners with mortgage (TEN=1, MRGP > 0)
- Estimate annual mortgage interest: MRGP × 12 × 0.7
  (Rough estimate: 70% of payment is interest)
- Create household income brackets
- Calculate mean/median
- Weight by WGTP

**Output:** Mean/median mortgage interest by income level

**Use:** Assign realistic mortgage interest deduction based on household income

---

## Key PUMS Variables Used

### Household File (csv_hXX.zip)
- **SERIALNO:** Household ID (links to person file)
- **WGTP:** Household weight (for population estimates)
- **HHT:** Household type (1=married, 2=male householder, 3=female householder)
- **NOC:** Number of own children under 18
- **NP:** Number of persons in household
- **HINCP:** Household income
- **TEN:** Tenure (1=owned w/ mortgage, 2=owned free, 3=rented)
- **TAXAMT:** Property taxes paid
- **MRGP:** Monthly mortgage payment

### Person File (csv_pXX.zip)
- **SERIALNO:** Household ID (links to household file)
- **PWGTP:** Person weight (for population estimates)
- **RELSHIPP:** Relationship to householder (20=householder, 22=bio child, 24=stepchild, etc.)
- **AGEP:** Age
- **SEX:** Sex (1=male, 2=female)
- **ESR:** Employment status (1-6)
- **WAGP:** Wage/salary income
- **SEMP:** Self-employment income
- **SSP:** Social Security income
- **SSIP:** Supplemental Security Income
- **RETP:** Retirement income (pension/IRA/401k)
- **INTP:** Interest AND dividend income (COMBINED - no separate DIVP exists)
- **PAP:** Public assistance income
- **OIP:** Other income

---

## Weighting Explained

PUMS data includes weights because not everyone is surveyed. Weights scale up the sample to represent the full population.

**Example:** A household with WGTP=50 represents 50 similar households in the state.

When calculating distributions, we:
1. Group records by the dimension of interest
2. Sum weights within each group
3. Calculate percentage = (group_weight / total_weight) × 100

This ensures distributions match real-world proportions, not just sample proportions.
