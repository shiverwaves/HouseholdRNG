#!/usr/bin/env python3
"""
PUMS Distribution Extraction Script
Extracts tax-relevant distribution tables from Census PUMS data

APPROACH: Downloads complete PUMS CSV files (not API), extracts, and processes locally.
- Downloads: Full household + person ZIP files from Census FTP server
- Processing: Loads entire datasets into pandas DataFrames
- Output: PostgreSQL-compatible SQL file

Usage:
    python extract_pums_distributions.py --state HI --year 2022
"""

import argparse
import os
import requests
import zipfile
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

# Census FTP server with PUMS CSV files (NOT the Census API)
# These are direct downloads of the complete CSV files
PUMS_BASE_URL = "https://www2.census.gov/programs-surveys/acs/data/pums/{year}/5-Year/"
CACHE_DIR = Path("./cache/pums_cache")
OUTPUT_DIR = Path("./output")

# PUMS file naming convention:
# Household: csv_hXX.zip where XX is lowercase state code (e.g., csv_hhi.zip for Hawaii)
# Person: csv_pXX.zip where XX is lowercase state code (e.g., csv_phi.zip for Hawaii)
# Each ZIP contains a single large CSV file with all records for that state


# =============================================================================
# DOWNLOAD PUMS FILES
# =============================================================================

def download_pums_files(state_code: str, year: int) -> Tuple[Path, Path]:
    """
    Download complete PUMS household and person ZIP files for a state.
    
    NOTE: This downloads the FULL CSV files (50-200MB compressed), NOT using Census API.
    Files are cached locally to avoid re-downloading on subsequent runs.
    
    Args:
        state_code: Two-letter state code (e.g., 'HI')
        year: Year of data (e.g., 2022)
        
    Returns:
        Tuple of (household_zip_path, person_zip_path)
    """
    CACHE_DIR.mkdir(exist_ok=True)
    
    state_lower = state_code.lower()
    household_filename = f"csv_h{state_lower}.zip"
    person_filename = f"csv_p{state_lower}.zip"
    
    household_path = CACHE_DIR / f"{year}_{household_filename}"
    person_path = CACHE_DIR / f"{year}_{person_filename}"
    
    base_url = PUMS_BASE_URL.format(year=year)
    
    # Download household file if not cached
    if not household_path.exists():
        logger.info(f"Downloading household file for {state_code}...")
        url = base_url + household_filename
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(household_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"Downloaded {household_filename}")
    else:
        logger.info(f"Using cached household file: {household_path}")
    
    # Download person file if not cached
    if not person_path.exists():
        logger.info(f"Downloading person file for {state_code}...")
        url = base_url + person_filename
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(person_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"Downloaded {person_filename}")
    else:
        logger.info(f"Using cached person file: {person_path}")
    
    return household_path, person_path


# =============================================================================
# LOAD PUMS DATA
# =============================================================================

def load_pums_data(household_zip: Path, person_zip: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load PUMS household and person data from ZIP files.
    
    Extracts the CSV from each ZIP and loads the ENTIRE dataset into memory.
    Uses optimized dtypes to reduce memory footprint by ~40-50%.
    
    For a typical state, this means:
    - Household file: ~50,000-200,000 records
    - Person file: ~100,000-500,000 records
    
    DTYPE OPTIMIZATION:
    Instead of using int64/float64 for everything (8 bytes each), we use:
    - int8 (1 byte) for small categorical values: age, sex, household type
    - int16 (2 bytes) for medium counts
    - int32 (4 bytes) for weights and larger counts
    - float32 (4 bytes) for income/dollar amounts
    
    This reduces memory usage by 40-50% with no loss of precision.
    Critical for large states (CA, TX) to fit in GitHub Actions 7GB RAM limit.
    
    Returns:
        Tuple of (households_df, persons_df)
    """
    # Household file dtype optimization
    # Only specify dtypes for columns we actually use
    household_dtypes = {
        'SERIALNO': 'str',      # Household ID - keep as string
        'WGTP': 'int32',        # Household weight (0-10,000 range) - int32 is plenty
        'NP': 'int8',           # Number of persons (1-20 range) - int8 sufficient
        'NOC': 'int8',          # Number of own children (0-10 range) - int8 sufficient
        'HHT': 'int8',          # Household type (1-7 categorical) - int8 sufficient
        'TEN': 'int8',          # Tenure (1-4 categorical) - int8 sufficient
        'HINCP': 'float32',     # Household income - float32 gives us $0-$16M range with good precision
        'TAXAMT': 'float32',    # Property tax amount - float32 sufficient
        'MRGP': 'float32',      # Monthly mortgage payment - float32 sufficient
    }
    
    # Person file dtype optimization  
    person_dtypes = {
        'SERIALNO': 'str',      # Household ID - keep as string to match household file
        'PWGTP': 'int32',       # Person weight (0-10,000 range) - int32 is plenty
        'AGEP': 'int8',         # Age (0-99 range) - int8 sufficient
        'SEX': 'int8',          # Sex (1-2 categorical) - int8 sufficient
        'ESR': 'int8',          # Employment status (1-6 categorical) - int8 sufficient
        'RELSHIPP': 'int8',     # Relationship code (20-38 range) - int8 sufficient
        'SCHL': 'int8',         # Education level (1-24 categorical) - int8 sufficient
        'WAGP': 'float32',      # Wage/salary income - float32 gives good range
        'SEMP': 'float32',      # Self-employment income - float32 sufficient
        'RETP': 'float32',      # Retirement income - float32 sufficient
        'SSP': 'float32',       # Social Security income - float32 sufficient
        'SSIP': 'float32',      # Supplemental Security Income - float32 sufficient
        'INTP': 'float32',      # Interest/dividend income - float32 sufficient
    }
    
    logger.info("Loading household data with optimized dtypes...")
    with zipfile.ZipFile(household_zip, 'r') as z:
        csv_name = [name for name in z.namelist() if name.endswith('.csv')][0]
        with z.open(csv_name) as f:
            households = pd.read_csv(f, dtype=household_dtypes, low_memory=False)
    
    logger.info("Loading person data with optimized dtypes...")
    with zipfile.ZipFile(person_zip, 'r') as z:
        csv_name = [name for name in z.namelist() if name.endswith('.csv')][0]
        with z.open(csv_name) as f:
            persons = pd.read_csv(f, dtype=person_dtypes, low_memory=False)
    
    # Log memory usage for monitoring
    hh_memory_mb = households.memory_usage(deep=True).sum() / 1024**2
    person_memory_mb = persons.memory_usage(deep=True).sum() / 1024**2
    total_memory_mb = hh_memory_mb + person_memory_mb
    
    logger.info(f"Loaded {len(households):,} households and {len(persons):,} persons")
    logger.info(f"Memory usage - Households: {hh_memory_mb:.1f} MB")
    logger.info(f"Memory usage - Persons: {person_memory_mb:.1f} MB")
    logger.info(f"Total memory: {total_memory_mb:.1f} MB ({total_memory_mb/1024:.2f} GB)")
    
    return households, persons


# =============================================================================
# DISTRIBUTION TABLE EXTRACTION FUNCTIONS
# =============================================================================

def extract_household_patterns(households: pd.DataFrame, persons: pd.DataFrame, state_code: str, year: int) -> pd.DataFrame:
    """
    Extract household pattern distributions.
    
    From Blueprint: household_patterns showing distribution of:
    - Married couples (with/without children)
    - Single parents
    - Blended families
    - Multigenerational households
    - Unmarried partners
    - Single adults
    
    LOGIC:
    1. Join person data to household data on SERIALNO to get relationship info
    2. Count relationship types per household (biological children, stepchildren, grandchildren, etc.)
    3. Classify each household into a pattern based on:
       - HHT code (married/male householder/female householder)
       - NOC (number of own children under 18)
       - Presence of specific relationship types (RELSHIPP codes)
    4. Calculate weighted percentages using household weights (WGTP)
    5. Return distribution showing how common each household pattern is
    
    KEY PUMS VARIABLES:
    - HHT: Household type (1=married, 2=male householder, 3=female householder)
    - NOC: Number of own children under 18
    - RELSHIPP: Relationship to householder (22=bio child, 24=stepchild, 27=grandchild, etc.)
    - WGTP: Household weight for population estimates
    """
    logger.info("Extracting household patterns...")
    
    # Join person data to get relationship info
    # Get householder (RELSHIPP == 20) and count relationships
    persons_subset = persons[['SERIALNO', 'RELSHIPP', 'AGEP', 'PWGTP']].copy()
    
    # Aggregate relationship counts per household
    relationship_counts = persons_subset.groupby(['SERIALNO', 'RELSHIPP']).size().unstack(fill_value=0)
    
    # Merge with household data
    hh_with_rels = households.merge(relationship_counts, on='SERIALNO', how='left')
    
    def classify_household(row):
        """Classify household into pattern categories"""
        # HHT codes from PUMS:
        # 1 = Married couple household
        # 2 = Male householder, no spouse
        # 3 = Female householder, no spouse
        
        hht = row.get('HHT', 0)
        noc = row.get('NOC', 0)  # Number of own children under 18
        
        # Relationship codes (RELSHIPP):
        # 20 = Householder
        # 21 = Spouse
        # 22 = Biological child
        # 23 = Adopted child
        # 24 = Stepchild
        # 25 = Sibling
        # 26 = Parent
        # 27 = Grandchild
        # 33 = Unmarried partner
        
        bio_children = row.get(22, 0)
        step_children = row.get(24, 0)
        grandchildren = row.get(27, 0)
        parents = row.get(26, 0)
        unmarried_partner = row.get(33, 0)
        
        # Classify patterns
        if hht == 1 and noc == 0:
            return 'married_couple_no_children'
        elif hht == 1 and step_children > 0 and bio_children > 0:
            return 'married_with_bio_and_step'
        elif hht == 1 and step_children > 0:
            return 'married_with_stepchildren'
        elif hht == 1 and noc > 0:
            return 'married_couple_with_children'
        elif hht == 3 and noc > 0:
            return 'single_parent_female'
        elif hht == 2 and noc > 0:
            return 'single_parent_male'
        elif parents > 0 and grandchildren > 0:
            return 'multigenerational'
        elif unmarried_partner > 0 and noc > 0:
            return 'unmarried_partners_with_children'
        elif unmarried_partner > 0:
            return 'unmarried_partners_no_children'
        elif hht == 3 and noc == 0:
            return 'single_female_no_children'
        elif hht == 2 and noc == 0:
            return 'single_male_no_children'
        else:
            return 'other'
    
    hh_with_rels['pattern'] = hh_with_rels.apply(classify_household, axis=1)
    
    # Calculate weighted distributions
    pattern_dist = hh_with_rels.groupby('pattern').agg({
        'WGTP': 'sum',  # Sum of household weights
        'SERIALNO': 'count'  # Count of records
    }).reset_index()
    
    pattern_dist.columns = ['pattern_id', 'weighted_count', 'sample_count']
    pattern_dist['percentage'] = (pattern_dist['weighted_count'] / pattern_dist['weighted_count'].sum() * 100).round(2)
    pattern_dist['state_code'] = state_code
    pattern_dist['year'] = year
    
    # Calculate average household size per pattern
    avg_size = hh_with_rels.groupby('pattern')['NP'].mean().round(1)
    pattern_dist = pattern_dist.merge(avg_size.rename('avg_household_size'), left_on='pattern_id', right_index=True)
    
    logger.info(f"Extracted {len(pattern_dist)} household patterns")
    
    return pattern_dist[['pattern_id', 'state_code', 'percentage', 'weighted_count', 'sample_count', 'avg_household_size', 'year']]


def extract_employment_by_age(persons: pd.DataFrame, state_code: str, year: int) -> pd.DataFrame:
    """
    Extract employment status distribution by age bracket.
    
    From Blueprint: Employment status by age needed for Stage 2 adult generation
    
    LOGIC:
    1. Filter to adults (age 18+) from person file
    2. Create age brackets (18-24, 25-34, 35-44, etc.)
    3. Map ESR (Employment Status Recode) to simplified categories:
       - employed (ESR 1,2)
       - unemployed (ESR 3)
       - armed_forces (ESR 4,5)
       - not_in_labor_force (ESR 6)
    4. Group by age bracket and sex
    5. Calculate weighted percentage within each age/sex group using person weights (PWGTP)
    6. Return distribution showing employment status probability given age and sex
    
    KEY PUMS VARIABLES:
    - AGEP: Age in years
    - ESR: Employment status recode (1-6)
    - SEX: Sex (1=male, 2=female)
    - PWGTP: Person weight for population estimates
    """
    logger.info("Extracting employment by age...")
    
    # Filter to adults (18+)
    adults = persons[persons['AGEP'] >= 18].copy()
    
    # Create age brackets
    adults['age_bracket'] = pd.cut(
        adults['AGEP'],
        bins=[18, 25, 35, 45, 55, 65, 75, 100],
        labels=['18-24', '25-34', '35-44', '45-54', '55-64', '65-74', '75+']
    )
    
    # Map ESR (Employment Status Recode) to simple categories
    # ESR codes:
    # 1 = Employed
    # 2 = Employed, not at work
    # 3 = Unemployed
    # 4 = Armed forces
    # 5 = Armed forces, not at work
    # 6 = Not in labor force
    
    def classify_employment(esr):
        if pd.isna(esr):
            return 'unknown'
        elif esr in [1, 2]:
            return 'employed'
        elif esr == 3:
            return 'unemployed'
        elif esr in [4, 5]:
            return 'armed_forces'
        elif esr == 6:
            return 'not_in_labor_force'
        else:
            return 'unknown'
    
    adults['employment_status'] = adults['ESR'].apply(classify_employment)
    
    # Calculate weighted distribution
    employment_dist = adults.groupby(['age_bracket', 'SEX', 'employment_status'], observed=True).agg({
        'PWGTP': 'sum',
        'SERIALNO': 'count'
    }).reset_index()
    
    # Calculate percentage within each age/sex group
    totals = employment_dist.groupby(['age_bracket', 'SEX'], observed=True)['PWGTP'].sum()
    employment_dist = employment_dist.merge(
        totals.rename('total_weight'),
        left_on=['age_bracket', 'SEX'],
        right_index=True
    )
    employment_dist['percentage'] = (employment_dist['PWGTP'] / employment_dist['total_weight'] * 100).round(2)
    
    # Map sex codes
    employment_dist['sex'] = employment_dist['SEX'].map({1: 'male', 2: 'female'})
    employment_dist['state_code'] = state_code
    employment_dist['year'] = year
    
    logger.info(f"Extracted {len(employment_dist)} employment distribution records")
    
    return employment_dist[['state_code', 'age_bracket', 'sex', 'employment_status', 'percentage', 'PWGTP', 'year']]


def extract_children_by_parent_age(households: pd.DataFrame, persons: pd.DataFrame, state_code: str, year: int) -> pd.DataFrame:
    """
    Extract distribution of number of children by parent age.
    
    From Blueprint: Needed for Stage 3 child generation
    
    LOGIC:
    1. Get householder age from person file (RELSHIPP=20)
    2. Join with household file to get NOC (number of own children)
    3. Filter to households with householder age 18-80
    4. Create parent age brackets (18-24, 25-29, 30-34, etc.)
    5. Group by household type, parent age bracket, and number of children
    6. Calculate weighted percentage within each household_type/age bracket using WGTP
    7. Return distribution showing P(num_children | parent_age, household_type)
    
    PURPOSE: When generating a household with a 35-year-old parent, this tells us
    how likely they are to have 0, 1, 2, 3, etc. children based on real data.
    
    KEY PUMS VARIABLES:
    - AGEP: Age of householder (from person file where RELSHIPP=20)
    - HHT: Household type (1=married, 2=male householder, 3=female householder)
    - NOC: Number of own children under 18
    - WGTP: Household weight
    """
    logger.info("Extracting children by parent age...")
    
    # Get householder age from person file
    householders = persons[persons['RELSHIPP'] == 20][['SERIALNO', 'AGEP', 'PWGTP']].copy()
    householders.columns = ['SERIALNO', 'householder_age', 'person_weight']
    
    # Merge with household file to get number of children
    hh_with_age = households.merge(householders, on='SERIALNO', how='inner')
    
    # Only include households with potential for children (householder 18-80)
    hh_with_age = hh_with_age[(hh_with_age['householder_age'] >= 18) & (hh_with_age['householder_age'] <= 80)]
    
    # Create age brackets
    hh_with_age['parent_age_bracket'] = pd.cut(
        hh_with_age['householder_age'],
        bins=[18, 25, 30, 35, 40, 45, 50, 55, 60, 80],
        labels=['18-24', '25-29', '30-34', '35-39', '40-44', '45-49', '50-54', '55-59', '60+']
    )
    
    # Use NOC (number of own children under 18)
    hh_with_age['num_children'] = hh_with_age['NOC'].fillna(0).astype(int)
    
    # Group by household type as well
    hh_with_age['household_type'] = hh_with_age['HHT'].map({
        1: 'married_couple',
        2: 'male_householder',
        3: 'female_householder'
    })
    
    # Calculate distribution
    children_dist = hh_with_age.groupby(['household_type', 'parent_age_bracket', 'num_children'], observed=True).agg({
        'WGTP': 'sum',
        'SERIALNO': 'count'
    }).reset_index()
    
    # Calculate percentage within each household_type/age_bracket
    totals = children_dist.groupby(['household_type', 'parent_age_bracket'], observed=True)['WGTP'].sum()
    children_dist = children_dist.merge(
        totals.rename('total_weight'),
        left_on=['household_type', 'parent_age_bracket'],
        right_index=True
    )
    children_dist['percentage'] = (children_dist['WGTP'] / children_dist['total_weight'] * 100).round(2)
    
    children_dist['state_code'] = state_code
    children_dist['year'] = year
    
    logger.info(f"Extracted {len(children_dist)} children distribution records")
    
    return children_dist[['state_code', 'household_type', 'parent_age_bracket', 'num_children', 'percentage', 'WGTP', 'year']]


def extract_income_sources(persons: pd.DataFrame, state_code: str, year: int) -> Dict[str, pd.DataFrame]:
    """
    Extract various income source distributions.
    
    From Blueprint: Social Security, retirement income, interest, dividends
    
    LOGIC:
    Social Security (SSP, SSIP):
    - Filter to people with SSP or SSIP > 0
    - Group by age bracket
    - Calculate mean/median amounts by age
    - Returns distribution for assigning realistic SS amounts based on age
    
    Retirement Income (RETP):
    - Filter to people with RETP > 0 (pension/IRA/401k distributions)
    - Group by age bracket (focus on 55+)
    - Calculate mean/median amounts
    - Returns distribution for assigning retirement income based on age
    
    Interest and Dividend Income (INTP):
    - NOTE: PUMS combines interest and dividends into single INTP variable
    - There is NO separate DIVP variable in PUMS data
    - Filter to people with INTP > 0
    - Create income brackets ($1-500, $500-2000, $2000-5000, etc.)
    - Calculate percentage in each bracket
    - Returns distribution for randomly assigning investment income amounts
    - For tax purposes, can split into interest/dividends in household generation
    
    KEY PUMS VARIABLES:
    - SSP: Social Security income
    - SSIP: Supplemental Security Income
    - RETP: Retirement income (pensions, IRA, 401k)
    - INTP: Interest AND dividend income (COMBINED - no separate DIVP variable)
    - AGEP: Age (for age-based distributions)
    - PWGTP: Person weight
    """
    logger.info("Extracting income sources...")
    
    results = {}
    
    # Social Security by age
    ss_recipients = persons[(persons['SSP'] > 0) | (persons['SSIP'] > 0)].copy()
    if len(ss_recipients) > 0:
        ss_recipients['age_bracket'] = pd.cut(
            ss_recipients['AGEP'],
            bins=[0, 25, 35, 45, 55, 62, 67, 75, 100],
            labels=['<25', '25-34', '35-44', '45-54', '55-61', '62-66', '67-74', '75+']
        )
        
        ss_dist = ss_recipients.groupby('age_bracket', observed=True).agg({
            'SSP': ['mean', 'median', 'count'],
            'PWGTP': 'sum'
        }).reset_index()
        ss_dist.columns = ['age_bracket', 'mean_ss', 'median_ss', 'count', 'weighted_count']
        ss_dist['state_code'] = state_code
        ss_dist['year'] = year
        results['social_security'] = ss_dist
    
    # Retirement income by age
    retirement_recipients = persons[persons['RETP'] > 0].copy()
    if len(retirement_recipients) > 0:
        retirement_recipients['age_bracket'] = pd.cut(
            retirement_recipients['AGEP'],
            bins=[0, 55, 60, 65, 70, 75, 100],
            labels=['<55', '55-59', '60-64', '65-69', '70-74', '75+']
        )
        
        ret_dist = retirement_recipients.groupby('age_bracket', observed=True).agg({
            'RETP': ['mean', 'median', 'count'],
            'PWGTP': 'sum'
        }).reset_index()
        ret_dist.columns = ['age_bracket', 'mean_retirement', 'median_retirement', 'count', 'weighted_count']
        ret_dist['state_code'] = state_code
        ret_dist['year'] = year
        results['retirement_income'] = ret_dist
    
    # Interest and dividend income (combined in INTP)
    interest_recipients = persons[persons['INTP'] > 0].copy()
    if len(interest_recipients) > 0:
        inv_dist = pd.DataFrame({
            'income_bracket': ['$1-$500', '$500-$2000', '$2000-$5000', '$5000-$10000', '$10000+'],
            'state_code': state_code,
            'year': year
        })
        
        interest_recipients['bracket'] = pd.cut(
            interest_recipients['INTP'],
            bins=[0, 500, 2000, 5000, 10000, 1000000],
            labels=['$1-$500', '$500-$2000', '$2000-$5000', '$5000-$10000', '$10000+']
        )
        
        bracket_counts = interest_recipients.groupby('bracket', observed=True)['PWGTP'].sum()
        inv_dist = inv_dist.merge(bracket_counts.rename('weighted_count'), left_on='income_bracket', right_index=True, how='left')
        inv_dist['weighted_count'] = inv_dist['weighted_count'].fillna(0)
        inv_dist['percentage'] = (inv_dist['weighted_count'] / inv_dist['weighted_count'].sum() * 100).round(2)
        results['interest_and_dividend_income'] = inv_dist
    
    logger.info(f"Extracted {len(results)} income source distributions")
    
    return results


def extract_housing_costs(households: pd.DataFrame, state_code: str, year: int) -> Dict[str, pd.DataFrame]:
    """
    Extract property taxes and mortgage interest by income level.
    
    From Blueprint: Needed for Stage 5 tax-relevant expenses
    
    LOGIC:
    Property Taxes (TAXAMT):
    - Filter to homeowners (TEN in [1,2]) with TAXAMT > 0
    - Create household income brackets (<$25k, $25k-$50k, etc.)
    - Group by income bracket
    - Calculate mean/median property tax amounts
    - Returns distribution showing typical property tax given income level
    
    Mortgage Interest (MRGP):
    - Filter to homeowners with mortgage (TEN=1) and MRGP > 0
    - MRGP is monthly mortgage payment
    - Estimate annual mortgage interest as: MRGP * 12 * 0.7
      (assumes ~70% of payment is interest, 30% is principal - rough estimate)
    - Create household income brackets
    - Calculate mean/median mortgage interest
    - Returns distribution showing typical mortgage interest given income level
    
    PURPOSE: When generating a household with $100k income, this tells us realistic
    property tax and mortgage interest amounts for tax deductions.
    
    KEY PUMS VARIABLES:
    - TAXAMT: Annual property taxes paid
    - MRGP: Monthly mortgage payment (first mortgage)
    - TEN: Tenure (1=owned with mortgage, 2=owned free and clear, 3=rented)
    - HINCP: Household income (for income brackets)
    - WGTP: Household weight
    """
    logger.info("Extracting housing costs...")
    
    results = {}
    
    # Property taxes (TAXAMT)
    homeowners = households[(households['TEN'].isin([1, 2])) & (households['TAXAMT'] > 0)].copy()
    if len(homeowners) > 0:
        # Create income brackets
        homeowners['income_bracket'] = pd.cut(
            homeowners['HINCP'],
            bins=[0, 25000, 50000, 75000, 100000, 150000, 200000, 1000000],
            labels=['<$25k', '$25k-$50k', '$50k-$75k', '$75k-$100k', '$100k-$150k', '$150k-$200k', '$200k+']
        )
        
        prop_tax_dist = homeowners.groupby('income_bracket', observed=True).agg({
            'TAXAMT': ['mean', 'median', 'count'],
            'WGTP': 'sum'
        }).reset_index()
        prop_tax_dist.columns = ['income_bracket', 'mean_property_tax', 'median_property_tax', 'count', 'weighted_count']
        prop_tax_dist['state_code'] = state_code
        prop_tax_dist['year'] = year
        results['property_taxes'] = prop_tax_dist
    
    # Mortgage interest (MRGP = monthly mortgage payment, estimate interest as ~70% of payment)
    homeowners_mortgage = households[(households['TEN'] == 1) & (households['MRGP'] > 0)].copy()
    if len(homeowners_mortgage) > 0:
        # Estimate annual mortgage interest (rough estimate)
        homeowners_mortgage['estimated_mortgage_interest'] = homeowners_mortgage['MRGP'] * 12 * 0.7
        
        homeowners_mortgage['income_bracket'] = pd.cut(
            homeowners_mortgage['HINCP'],
            bins=[0, 25000, 50000, 75000, 100000, 150000, 200000, 1000000],
            labels=['<$25k', '$25k-$50k', '$50k-$75k', '$75k-$100k', '$100k-$150k', '$150k-$200k', '$200k+']
        )
        
        mortgage_dist = homeowners_mortgage.groupby('income_bracket', observed=True).agg({
            'estimated_mortgage_interest': ['mean', 'median', 'count'],
            'WGTP': 'sum'
        }).reset_index()
        mortgage_dist.columns = ['income_bracket', 'mean_mortgage_interest', 'median_mortgage_interest', 'count', 'weighted_count']
        mortgage_dist['state_code'] = state_code
        mortgage_dist['year'] = year
        results['mortgage_interest'] = mortgage_dist
    
    logger.info(f"Extracted {len(results)} housing cost distributions")
    
    return results


def extract_child_age_distributions(persons: pd.DataFrame, state_code: str, year: int) -> pd.DataFrame:
    """
    Extract age distribution of children, conditional on parent age.
    
    From Blueprint: Needed to generate realistic child ages
    
    LOGIC:
    1. Filter person file to children under 18 with RELSHIPP in [22,23,24]
       (biological, adopted, or stepchildren)
    2. Get householder age from person file (RELSHIPP=20)
    3. Join children to their householder's age via SERIALNO
    4. Create parent age brackets (18-24, 25-29, 30-34, etc.)
    5. Create child age groups (0-2, 3-5, 6-10, 11-13, 14-17)
    6. Group by parent age bracket and child age group
    7. Calculate weighted percentage within each parent age bracket
    8. Return distribution showing P(child_age_group | parent_age)
    
    PURPOSE: When generating a household with a 35-year-old parent and 2 children,
    this tells us the realistic ages for those children. Younger parents typically
    have younger children; older parents may have teenagers.
    
    EXAMPLE: A 30-year-old parent is most likely to have children ages 0-5,
    while a 45-year-old parent is more likely to have teenagers.
    
    KEY PUMS VARIABLES:
    - AGEP: Age of both children and householder
    - RELSHIPP: Relationship (20=householder, 22=bio child, 23=adopted, 24=step)
    - SERIALNO: Household ID (links children to their parent)
    - PWGTP: Person weight
    """
    logger.info("Extracting child age distributions...")
    
    # Get all children (under 18, biological/adopted/step)
    children = persons[
        (persons['AGEP'] < 18) &
        (persons['RELSHIPP'].isin([22, 23, 24]))  # Bio child, adopted, step
    ].copy()
    
    # Get householder ages for each household
    householders = persons[persons['RELSHIPP'] == 20][['SERIALNO', 'AGEP']].copy()
    householders.columns = ['SERIALNO', 'parent_age']
    
    # Merge to get parent age for each child
    children_with_parent = children.merge(householders, on='SERIALNO', how='inner')
    
    # Create parent age brackets
    children_with_parent['parent_age_bracket'] = pd.cut(
        children_with_parent['parent_age'],
        bins=[18, 25, 30, 35, 40, 45, 50, 55, 100],
        labels=['18-24', '25-29', '30-34', '35-39', '40-44', '45-49', '50-54', '55+']
    )
    
    # Create child age groups
    children_with_parent['child_age_group'] = pd.cut(
        children_with_parent['AGEP'],
        bins=[-1, 2, 5, 10, 13, 17],
        labels=['0-2', '3-5', '6-10', '11-13', '14-17']
    )
    
    # Calculate distribution
    child_age_dist = children_with_parent.groupby(['parent_age_bracket', 'child_age_group'], observed=True).agg({
        'PWGTP': 'sum',
        'SERIALNO': 'count'
    }).reset_index()
    
    # Calculate percentage within each parent age bracket
    totals = child_age_dist.groupby('parent_age_bracket', observed=True)['PWGTP'].sum()
    child_age_dist = child_age_dist.merge(
        totals.rename('total_weight'),
        left_on='parent_age_bracket',
        right_index=True
    )
    child_age_dist['percentage'] = (child_age_dist['PWGTP'] / child_age_dist['total_weight'] * 100).round(2)
    
    child_age_dist['state_code'] = state_code
    child_age_dist['year'] = year
    
    logger.info(f"Extracted {len(child_age_dist)} child age distribution records")
    
    return child_age_dist[['state_code', 'parent_age_bracket', 'child_age_group', 'percentage', 'PWGTP', 'year']]


# =============================================================================
# SQL EXPORT
# =============================================================================

def dataframe_to_sql_insert(df: pd.DataFrame, table_name: str) -> str:
    """
    Convert a DataFrame to SQL INSERT statements.
    """
    sql_lines = []
    
    # Create table structure based on dtypes
    sql_lines.append(f"\n-- Table: {table_name}")
    sql_lines.append(f"CREATE TABLE IF NOT EXISTS {table_name} (")
    
    columns = []
    for col, dtype in df.dtypes.items():
        if dtype == 'object':
            columns.append(f"    {col} VARCHAR(100)")
        elif dtype == 'int64':
            columns.append(f"    {col} INTEGER")
        elif dtype == 'float64':
            columns.append(f"    {col} DECIMAL(12,2)")
        else:
            columns.append(f"    {col} TEXT")
    
    sql_lines.append(",\n".join(columns))
    sql_lines.append(");\n")
    
    # Generate INSERT statements
    sql_lines.append(f"-- Data for {table_name}")
    
    for _, row in df.iterrows():
        values = []
        for val in row:
            if pd.isna(val):
                values.append('NULL')
            elif isinstance(val, str):
                # Escape single quotes
                val_escaped = val.replace("'", "''")
                values.append(f"'{val_escaped}'")
            else:
                values.append(str(val))
        
        sql_lines.append(f"INSERT INTO {table_name} VALUES ({', '.join(values)});")
    
    return '\n'.join(sql_lines)


def export_to_sql(distributions: Dict[str, pd.DataFrame], state_code: str, year: int, output_path: Path):
    """
    Export all distribution tables to a single SQL file.
    """
    logger.info(f"Exporting distributions to SQL: {output_path}")
    
    with open(output_path, 'w') as f:
        # Write header
        f.write(f"""-- PUMS Distribution Tables
-- State: {state_code}
-- Year: {year}
-- Generated: {pd.Timestamp.now()}
--
-- This file contains tax-relevant distribution tables extracted from Census PUMS data
-- for use in household generation pipeline

BEGIN;

""")
        
        # Write each distribution table
        for table_name, df in distributions.items():
            sql = dataframe_to_sql_insert(df, table_name)
            f.write(sql)
            f.write('\n\n')
        
        f.write("COMMIT;\n")
    
    logger.info(f"Exported {len(distributions)} tables to {output_path}")


# =============================================================================
# VALIDATION
# =============================================================================

def validate_distributions(distributions: Dict[str, pd.DataFrame]):
    """
    Run basic validation checks on extracted distributions.
    """
    logger.info("Validating distributions...")
    
    issues = []
    
    for table_name, df in distributions.items():
        # Check for empty tables
        if len(df) == 0:
            issues.append(f"WARNING: {table_name} is empty")
        
        # Check for percentage columns that don't sum to ~100%
        if 'percentage' in df.columns:
            # Group by relevant dimensions and check sums
            # This is a simplified check - real validation would be more sophisticated
            total_pct = df['percentage'].sum()
            if total_pct > 0 and (total_pct < 95 or total_pct > 105):
                issues.append(f"WARNING: {table_name} percentages sum to {total_pct:.1f}% (expected ~100%)")
    
    if issues:
        logger.warning(f"Validation found {len(issues)} issues:")
        for issue in issues:
            logger.warning(f"  {issue}")
    else:
        logger.info("All distributions passed validation")
    
    return len(issues) == 0


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def main(state_code: str, year: int):
    """
    Main pipeline to extract PUMS distributions.
    
    APPROACH:
    - Downloads complete PUMS ZIP files from Census FTP (NOT using Census API)
    - Extracts and loads entire CSVs into pandas DataFrames
    - Processes all records to generate distribution tables
    - Exports to PostgreSQL-compatible SQL
    """
    logger.info(f"Starting PUMS extraction for {state_code} ({year})")
    logger.info("Method: Download complete PUMS files (no API calls)")
    
    # Step 1: Download PUMS files (complete ZIP files with CSVs inside)
    household_zip, person_zip = download_pums_files(state_code, year)
    
    # Step 2: Load into DataFrames
    households, persons = load_pums_data(household_zip, person_zip)
    
    # Step 3: Extract distribution tables
    distributions = {}
    
    distributions['household_patterns'] = extract_household_patterns(households, persons, state_code, year)
    distributions['employment_by_age'] = extract_employment_by_age(persons, state_code, year)
    distributions['children_by_parent_age'] = extract_children_by_parent_age(households, persons, state_code, year)
    distributions['child_age_distributions'] = extract_child_age_distributions(persons, state_code, year)
    
    # Income sources (returns dict of multiple tables)
    income_dists = extract_income_sources(persons, state_code, year)
    distributions.update(income_dists)
    
    # Housing costs (returns dict of multiple tables)
    housing_dists = extract_housing_costs(households, state_code, year)
    distributions.update(housing_dists)
    
    # Step 4: Validate
    validate_distributions(distributions)
    
    # Step 5: Export to SQL
    OUTPUT_DIR.mkdir(exist_ok=True)
    output_path = OUTPUT_DIR / f"pums_distributions_{state_code}_{year}.sql"
    export_to_sql(distributions, state_code, year, output_path)
    
    # Print summary
    logger.info("\n" + "="*60)
    logger.info("EXTRACTION COMPLETE")
    logger.info("="*60)
    logger.info(f"State: {state_code}")
    logger.info(f"Year: {year}")
    logger.info(f"Tables extracted: {len(distributions)}")
    logger.info(f"Output file: {output_path}")
    logger.info(f"File size: {output_path.stat().st_size / 1024:.1f} KB")
    logger.info("\nExtracted tables:")
    for table_name, df in distributions.items():
        logger.info(f"  - {table_name}: {len(df)} rows")
    
    return distributions


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extract PUMS distribution tables')
    parser.add_argument('--state', type=str, required=True, help='Two-letter state code (e.g., HI)')
    parser.add_argument('--year', type=int, default=2022, help='Year of PUMS data (default: 2022)')
    
    args = parser.parse_args()
    
    main(args.state.upper(), args.year)
