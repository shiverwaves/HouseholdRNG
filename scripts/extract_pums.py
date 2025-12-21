#!/usr/bin/env python3
"""
PUMS Distribution Extraction Script
Extracts tax-relevant distribution tables from Census PUMS data

Usage:
    # Generate SQL file for local import
    python extract_pums.py --state HI --year 2022 --output sql
    
    # Upload directly to database (GitHub Actions)
    python extract_pums.py --state HI --year 2022 --output database --connection-string $DATABASE_URL
    
    # Or use environment variable
    export DATABASE_URL="postgresql://user:pass@host:5432/dbname?sslmode=require"
    python extract_pums.py --state HI --year 2022 --output database
"""

import argparse
import os
import sys
import requests
import zipfile
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Tuple
from io import StringIO
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

PUMS_BASE_URL = "https://www2.census.gov/programs-surveys/acs/data/pums/{year}/5-Year/"
CACHE_DIR = Path("./pums_cache")
OUTPUT_DIR = Path("./output")

# Distribution tables to extract
DISTRIBUTION_TABLES = [
    'household_patterns',
    'employment_by_age',
    'children_by_parent_age',
    'child_age_distributions',
    'social_security',
    'retirement_income',
    'interest_and_dividend_income',
    'property_taxes',
    'mortgage_interest',
    'education_by_age',
    'disability_by_age',
    'other_income_by_employment_status',
    'public_assistance_income',
    'adult_child_ages',
    'stepchild_patterns',
    'multigenerational_patterns',
    'unmarried_partner_patterns'
]


# =============================================================================
# DOWNLOAD PUMS FILES
# =============================================================================

def download_pums_files(state_code: str, year: int) -> Tuple[Path, Path]:
    """
    Download PUMS household and person ZIP files for a state.
    Files are cached locally to avoid re-downloading.
    """
    CACHE_DIR.mkdir(exist_ok=True)
    
    state_lower = state_code.lower()
    household_filename = f"csv_h{state_lower}.zip"
    person_filename = f"csv_p{state_lower}.zip"
    
    household_path = CACHE_DIR / f"{year}_{household_filename}"
    person_path = CACHE_DIR / f"{year}_{person_filename}"
    
    base_url = PUMS_BASE_URL.format(year=year)
    
    # Download household file
    if not household_path.exists():
        logger.info(f"  → Downloading household file ({household_filename})...")
        url = base_url + household_filename
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        total_size = 0
        with open(household_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                total_size += len(chunk)
        
        logger.info(f"    Downloaded {total_size / 1024 / 1024:.1f} MB")
    else:
        logger.info(f"  → Using cached household file")
    
    # Download person file
    if not person_path.exists():
        logger.info(f"  → Downloading person file ({person_filename})...")
        url = base_url + person_filename
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        total_size = 0
        with open(person_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                total_size += len(chunk)
        
        logger.info(f"    Downloaded {total_size / 1024 / 1024:.1f} MB")
    else:
        logger.info(f"  → Using cached person file")
    
    return household_path, person_path


# =============================================================================
# LOAD PUMS DATA
# =============================================================================

def load_pums_data(household_zip: Path, person_zip: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load PUMS household and person data from ZIP files into memory.
    Uses default dtypes for flexibility during processing.
    """
    logger.info("  → Loading household data from ZIP...")
    with zipfile.ZipFile(household_zip, 'r') as z:
        csv_name = [name for name in z.namelist() if name.endswith('.csv')][0]
        with z.open(csv_name) as f:
            households = pd.read_csv(f, low_memory=False)
    logger.info(f"    Loaded {len(households):,} households")
    
    logger.info("  → Loading person data from ZIP...")
    with zipfile.ZipFile(person_zip, 'r') as z:
        csv_name = [name for name in z.namelist() if name.endswith('.csv')][0]
        with z.open(csv_name) as f:
            persons = pd.read_csv(f, low_memory=False)
    logger.info(f"    Loaded {len(persons):,} persons")
    
    return households, persons


# =============================================================================
# DISTRIBUTION EXTRACTION FUNCTIONS
# =============================================================================

def extract_household_patterns(households: pd.DataFrame, persons: pd.DataFrame, 
                               state_code: str, year: int) -> pd.DataFrame:
    """
    Extract household pattern distributions.
    Shows distribution of married couples, single parents, multigenerational, etc.
    """
    # Join person data to get relationship info
    persons_subset = persons[['SERIALNO', 'RELSHIPP']].copy()
    relationship_counts = persons_subset.groupby(['SERIALNO', 'RELSHIPP']).size().unstack(fill_value=0)
    
    # Merge with household data
    hh_with_rels = households.merge(relationship_counts, on='SERIALNO', how='left')
    
    def classify_household(row):
        """Classify household into pattern categories"""
        hht = row.get('HHT', 0)
        noc = row.get('NOC', 0)
        
        bio_children = row.get(22, 0)
        step_children = row.get(24, 0)
        grandchildren = row.get(27, 0)
        parents = row.get(26, 0)
        unmarried_partner = row.get(33, 0)
        
        # Classify patterns
        if hht == 1 and noc == 0:
            return 'married_couple_no_children'
        elif hht == 1 and noc > 0 and step_children == 0:
            return 'married_couple_with_children'
        elif hht == 1 and step_children > 0:
            return 'blended_family'
        elif hht in [2, 3] and noc > 0:
            return 'single_parent'
        elif grandchildren > 0 or parents > 0:
            return 'multigenerational'
        elif unmarried_partner > 0:
            return 'unmarried_partners'
        else:
            return 'single_adult'
    
    hh_with_rels['pattern'] = hh_with_rels.apply(classify_household, axis=1)
    
    # Calculate weighted distribution
    pattern_dist = hh_with_rels.groupby('pattern').agg({
        'WGTP': 'sum',
        'SERIALNO': 'count'
    }).reset_index()
    
    total_weight = pattern_dist['WGTP'].sum()
    pattern_dist['percentage'] = (pattern_dist['WGTP'] / total_weight * 100).round(2)
    pattern_dist['state_code'] = state_code
    pattern_dist['year'] = year
    
    return pattern_dist[['state_code', 'pattern', 'percentage', 'WGTP', 'year']].rename(columns={'WGTP': 'weight'})


def extract_employment_by_age(persons: pd.DataFrame, state_code: str, year: int) -> pd.DataFrame:
    """
    Extract employment status probability given age and sex.
    """
    # Filter to adults
    adults = persons[persons['AGEP'] >= 18].copy()
    
    # Create age brackets
    adults['age_bracket'] = pd.cut(
        adults['AGEP'],
        bins=[17, 24, 34, 44, 54, 64, 120],
        labels=['18-24', '25-34', '35-44', '45-54', '55-64', '65+']
    )
    
    # Map ESR codes to employment categories
    employment_map = {
        1: 'employed',
        2: 'employed',
        3: 'unemployed',
        4: 'armed_forces',
        5: 'armed_forces',
        6: 'not_in_labor_force'
    }
    adults['employment_status'] = adults['ESR'].map(employment_map)
    
    # Calculate distribution
    emp_dist = adults.groupby(['age_bracket', 'SEX', 'employment_status'], observed=True).agg({
        'PWGTP': 'sum',
        'SERIALNO': 'count'
    }).reset_index()
    
    # Calculate percentage within each age/sex group
    totals = emp_dist.groupby(['age_bracket', 'SEX'], observed=True)['PWGTP'].sum()
    emp_dist = emp_dist.merge(
        totals.rename('total_weight'),
        left_on=['age_bracket', 'SEX'],
        right_index=True
    )
    emp_dist['percentage'] = (emp_dist['PWGTP'] / emp_dist['total_weight'] * 100).round(2)
    
    emp_dist['state_code'] = state_code
    emp_dist['year'] = year
    emp_dist['sex'] = emp_dist['SEX'].map({1: 'male', 2: 'female'})
    
    return emp_dist[['state_code', 'age_bracket', 'sex', 'employment_status', 'percentage', 'PWGTP', 'year']].rename(columns={'PWGTP': 'weight'})


def extract_children_by_parent_age(households: pd.DataFrame, persons: pd.DataFrame,
                                   state_code: str, year: int) -> pd.DataFrame:
    """
    Extract number of children probability given parent age.
    """
    # Get householder age
    householders = persons[persons['RELSHIPP'] == 20][['SERIALNO', 'AGEP']].copy()
    householders = householders.rename(columns={'AGEP': 'householder_age'})
    
    # Join with household NOC
    hh_with_age = households.merge(householders, on='SERIALNO', how='inner')
    
    # Create age brackets
    hh_with_age['parent_age_bracket'] = pd.cut(
        hh_with_age['householder_age'],
        bins=[17, 24, 29, 34, 39, 44, 49, 120],
        labels=['18-24', '25-29', '30-34', '35-39', '40-44', '45-49', '50+']
    )
    
    # Calculate distribution
    children_dist = hh_with_age.groupby(['parent_age_bracket', 'NOC'], observed=True).agg({
        'WGTP': 'sum',
        'SERIALNO': 'count'
    }).reset_index()
    
    # Calculate percentage within each parent age bracket
    totals = children_dist.groupby('parent_age_bracket', observed=True)['WGTP'].sum()
    children_dist = children_dist.merge(
        totals.rename('total_weight'),
        left_on='parent_age_bracket',
        right_index=True
    )
    children_dist['percentage'] = (children_dist['WGTP'] / children_dist['total_weight'] * 100).round(2)
    
    children_dist['state_code'] = state_code
    children_dist['year'] = year
    
    return children_dist[['state_code', 'parent_age_bracket', 'NOC', 'percentage', 'WGTP', 'year']].rename(columns={'WGTP': 'weight', 'NOC': 'num_children'})


def extract_child_age_distributions(persons: pd.DataFrame, state_code: str, year: int) -> pd.DataFrame:
    """
    Extract child age probability given parent age.
    """
    # Get children (biological, adopted, step)
    children = persons[persons['RELSHIPP'].isin([22, 23, 24])].copy()
    
    # Get householder ages
    householders = persons[persons['RELSHIPP'] == 20][['SERIALNO', 'AGEP']].copy()
    householders = householders.rename(columns={'AGEP': 'parent_age'})
    
    # Join children with parent ages
    children_with_parent = children.merge(householders, on='SERIALNO', how='inner')
    
    # Create brackets
    children_with_parent['parent_age_bracket'] = pd.cut(
        children_with_parent['parent_age'],
        bins=[17, 29, 34, 39, 44, 49, 120],
        labels=['18-29', '30-34', '35-39', '40-44', '45-49', '50+']
    )
    
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
    
    return child_age_dist[['state_code', 'parent_age_bracket', 'child_age_group', 'percentage', 'PWGTP', 'year']].rename(columns={'PWGTP': 'weight'})


def extract_social_security(persons: pd.DataFrame, state_code: str, year: int) -> pd.DataFrame:
    """
    Extract typical Social Security amounts by age.
    """
    # Filter to people with SS income
    ss_recipients = persons[(persons['SSP'] > 0) | (persons['SSIP'] > 0)].copy()
    ss_recipients['total_ss'] = ss_recipients['SSP'].fillna(0) + ss_recipients['SSIP'].fillna(0)
    
    # Create age brackets (focus on 62+)
    ss_recipients['age_bracket'] = pd.cut(
        ss_recipients['AGEP'],
        bins=[61, 64, 69, 74, 120],
        labels=['62-64', '65-69', '70-74', '75+']
    )
    
    # Calculate mean and median
    ss_dist = ss_recipients.groupby('age_bracket', observed=True).apply(
        lambda x: pd.Series({
            'mean_amount': np.average(x['total_ss'], weights=x['PWGTP']),
            'median_amount': x['total_ss'].median(),
            'weight': x['PWGTP'].sum(),
            'count': len(x)
        }),
        include_groups=False
    ).reset_index()
    
    ss_dist['state_code'] = state_code
    ss_dist['year'] = year
    
    return ss_dist[['state_code', 'age_bracket', 'mean_amount', 'median_amount', 'weight', 'year']]


def extract_retirement_income(persons: pd.DataFrame, state_code: str, year: int) -> pd.DataFrame:
    """
    Extract typical retirement income (pension/IRA) by age.
    """
    # Filter to people with retirement income
    retirees = persons[persons['RETP'] > 0].copy()
    
    # Create age brackets (focus on 55+)
    retirees['age_bracket'] = pd.cut(
        retirees['AGEP'],
        bins=[54, 61, 64, 69, 74, 120],
        labels=['55-61', '62-64', '65-69', '70-74', '75+']
    )
    
    # Calculate mean and median
    ret_dist = retirees.groupby('age_bracket', observed=True).apply(
        lambda x: pd.Series({
            'mean_amount': np.average(x['RETP'], weights=x['PWGTP']),
            'median_amount': x['RETP'].median(),
            'weight': x['PWGTP'].sum(),
            'count': len(x)
        }),
        include_groups=False
    ).reset_index()
    
    ret_dist['state_code'] = state_code
    ret_dist['year'] = year
    
    return ret_dist[['state_code', 'age_bracket', 'mean_amount', 'median_amount', 'weight', 'year']]


def extract_interest_dividend(persons: pd.DataFrame, state_code: str, year: int) -> pd.DataFrame:
    """
    Extract distribution of investment income (interest + dividends combined).
    PUMS combines these into INTP variable.
    """
    # Filter to people with investment income
    investors = persons[persons['INTP'] > 0].copy()
    
    # Create income brackets
    investors['income_bracket'] = pd.cut(
        investors['INTP'],
        bins=[0, 500, 2000, 5000, 10000, 20000, float('inf')],
        labels=['$1-500', '$500-2K', '$2K-5K', '$5K-10K', '$10K-20K', '$20K+']
    )
    
    # Calculate distribution
    inv_dist = investors.groupby('income_bracket', observed=True).agg({
        'PWGTP': 'sum',
        'SERIALNO': 'count'
    }).reset_index()
    
    total_weight = inv_dist['PWGTP'].sum()
    inv_dist['percentage'] = (inv_dist['PWGTP'] / total_weight * 100).round(2)
    inv_dist['state_code'] = state_code
    inv_dist['year'] = year
    
    return inv_dist[['state_code', 'income_bracket', 'percentage', 'PWGTP', 'year']].rename(columns={'PWGTP': 'weight'})


def extract_property_taxes(households: pd.DataFrame, state_code: str, year: int) -> pd.DataFrame:
    """
    Extract typical property tax amounts by household income.
    """
    # Filter to homeowners with property tax
    homeowners = households[(households['TEN'].isin([1, 2])) & (households['TAXAMT'] > 0)].copy()
    
    # Create household income brackets
    homeowners['income_bracket'] = pd.cut(
        homeowners['HINCP'],
        bins=[0, 25000, 50000, 75000, 100000, 150000, 200000, float('inf')],
        labels=['<$25K', '$25-50K', '$50-75K', '$75-100K', '$100-150K', '$150-200K', '$200K+']
    )
    
    # Calculate mean and median
    prop_tax_dist = homeowners.groupby('income_bracket', observed=True).apply(
        lambda x: pd.Series({
            'mean_amount': np.average(x['TAXAMT'], weights=x['WGTP']),
            'median_amount': x['TAXAMT'].median(),
            'weight': x['WGTP'].sum(),
            'count': len(x)
        }),
        include_groups=False
    ).reset_index()
    
    prop_tax_dist['state_code'] = state_code
    prop_tax_dist['year'] = year
    
    return prop_tax_dist[['state_code', 'income_bracket', 'mean_amount', 'median_amount', 'weight', 'year']]


def extract_mortgage_interest(households: pd.DataFrame, state_code: str, year: int) -> pd.DataFrame:
    """
    Extract typical mortgage interest by household income.
    Estimates annual interest as monthly payment × 12 × 0.7
    """
    # Filter to homeowners with mortgage
    mortgaged = households[(households['TEN'] == 1) & (households['MRGP'] > 0)].copy()
    
    # Estimate annual mortgage interest (70% of payment is interest)
    mortgaged['estimated_interest'] = mortgaged['MRGP'] * 12 * 0.7
    
    # Create household income brackets
    mortgaged['income_bracket'] = pd.cut(
        mortgaged['HINCP'],
        bins=[0, 25000, 50000, 75000, 100000, 150000, 200000, float('inf')],
        labels=['<$25K', '$25-50K', '$50-75K', '$75-100K', '$100-150K', '$150-200K', '$200K+']
    )
    
    # Calculate mean and median
    mort_int_dist = mortgaged.groupby('income_bracket', observed=True).apply(
        lambda x: pd.Series({
            'mean_amount': np.average(x['estimated_interest'], weights=x['WGTP']),
            'median_amount': x['estimated_interest'].median(),
            'weight': x['WGTP'].sum(),
            'count': len(x)
        }),
        include_groups=False
    ).reset_index()
    
    mort_int_dist['state_code'] = state_code
    mort_int_dist['year'] = year
    
    return mort_int_dist[['state_code', 'income_bracket', 'mean_amount', 'median_amount', 'weight', 'year']]


def extract_education_by_age(persons: pd.DataFrame, state_code: str, year: int) -> pd.DataFrame:
    """
    Extract education attainment distribution by age.
    
    CRITICAL: Needed for derived education_occupation_probabilities table.
    
    SCHL values:
    - 01-15: No high school diploma
    - 16-17: High school graduate  
    - 18-19: Some college, no degree
    - 20: Associate's degree
    - 21: Bachelor's degree
    - 22: Master's degree
    - 23: Professional degree
    - 24: Doctorate degree
    """
    # Filter to adults with education data
    adults = persons[(persons['AGEP'] >= 18) & (persons['SCHL'].notna())].copy()
    
    # Simplify education levels
    def map_education(schl):
        schl = int(schl)
        if schl <= 15:
            return 'no_hs_diploma'
        elif schl in [16, 17]:
            return 'hs_graduate'
        elif schl in [18, 19]:
            return 'some_college'
        elif schl == 20:
            return 'associates'
        elif schl == 21:
            return 'bachelors'
        elif schl == 22:
            return 'masters'
        elif schl in [23, 24]:
            return 'professional_doctorate'
        else:
            return 'unknown'
    
    adults['education_level'] = adults['SCHL'].apply(map_education)
    
    # Create age brackets
    adults['age_bracket'] = pd.cut(
        adults['AGEP'],
        bins=[18, 25, 30, 35, 40, 45, 50, 55, 60, 65, 75, 100],
        labels=['18-24', '25-29', '30-34', '35-39', '40-44', '45-49', '50-54', '55-59', '60-64', '65-74', '75+']
    )
    
    # Group and calculate distributions
    edu_dist = adults.groupby(['age_bracket', 'education_level'], observed=True).agg({
        'PWGTP': 'sum',
        'SERIALNO': 'count'
    }).reset_index()
    
    edu_dist.columns = ['age_bracket', 'education_level', 'weighted_count', 'sample_count']
    
    # Calculate percentage within each age bracket
    totals = edu_dist.groupby('age_bracket', observed=True)['weighted_count'].sum()
    edu_dist = edu_dist.merge(
        totals.rename('total_weight'),
        left_on='age_bracket',
        right_index=True
    )
    edu_dist['percentage'] = (edu_dist['weighted_count'] / edu_dist['total_weight'] * 100).round(2)
    
    edu_dist['state_code'] = state_code
    edu_dist['year'] = year
    
    return edu_dist[['state_code', 'age_bracket', 'education_level', 'percentage', 'weighted_count', 'year']]


def extract_disability_by_age(persons: pd.DataFrame, state_code: str, year: int) -> pd.DataFrame:
    """
    Extract disability prevalence by age.
    
    DIS values:
    - 1: With disability
    - 2: Without disability
    
    Important for:
    - SSI eligibility (SSIP)
    - Disability tax credits
    - Work limitations
    """
    # Filter to people with disability status
    has_dis = persons[persons['DIS'].notna()].copy()
    
    # Create age brackets
    has_dis['age_bracket'] = pd.cut(
        has_dis['AGEP'],
        bins=[0, 18, 25, 35, 45, 55, 65, 75, 100],
        labels=['<18', '18-24', '25-34', '35-44', '45-54', '55-64', '65-74', '75+']
    )
    
    # Map disability status
    has_dis['has_disability'] = (has_dis['DIS'] == 1).astype(int)
    
    # Group by age bracket
    dis_dist = has_dis.groupby('age_bracket', observed=True).apply(
        lambda x: pd.Series({
            'total_weighted': x['PWGTP'].sum(),
            'disabled_weighted': (x['has_disability'] * x['PWGTP']).sum(),
            'sample_count': len(x)
        }),
        include_groups=False
    ).reset_index()
    
    dis_dist['disability_percentage'] = (dis_dist['disabled_weighted'] / dis_dist['total_weighted'] * 100).round(2)
    
    dis_dist['state_code'] = state_code
    dis_dist['year'] = year
    
    return dis_dist[['state_code', 'age_bracket', 'disability_percentage', 'total_weighted', 'year']]


def extract_other_income_by_employment_status(persons: pd.DataFrame, state_code: str, year: int) -> pd.DataFrame:
    """
    Extract OIP (other income) distribution by employment status.
    
    IMPORTANT: OIP is a catch-all that includes:
    - Unemployment compensation (taxable)
    - Alimony received (taxable)  
    - Child support (NOT taxable)
    - Other miscellaneous income
    
    Cannot separate these types in PUMS data.
    
    Use with ESR (employment status) to infer likely source:
    - ESR=3 (unemployed) + OIP → likely unemployment comp
    - ESR=1,2 (employed) + OIP → likely alimony/other
    
    ESR values:
    - 1: Civilian employed, at work
    - 2: Civilian employed, with job but not at work
    - 3: Unemployed
    - 4: Armed forces, at work
    - 5: Armed forces, with job but not at work
    - 6: Not in labor force
    """
    # Filter to people with other income
    has_other = persons[(persons['OIP'].notna()) & (persons['OIP'] > 0)].copy()
    
    # Create income brackets
    has_other['income_bracket'] = pd.cut(
        has_other['OIP'],
        bins=[0, 2500, 5000, 10000, 15000, 20000, 30000, 50000, float('inf')],
        labels=['<$2.5K', '$2.5-5K', '$5-10K', '$10-15K', '$15-20K', '$20-30K', '$30-50K', '$50K+']
    )
    
    # Group by employment status and income bracket
    other_dist = has_other.groupby(['ESR', 'income_bracket'], observed=True).agg({
        'PWGTP': 'sum',
        'OIP': ['mean', 'median'],
        'SERIALNO': 'count'
    }).reset_index()
    
    other_dist.columns = ['employment_status', 'income_bracket', 'weighted_count', 'mean_amount', 'median_amount', 'sample_count']
    
    # Calculate percentage within each employment status
    totals = other_dist.groupby('employment_status', observed=True)['weighted_count'].sum()
    other_dist = other_dist.merge(
        totals.rename('total_weight'),
        left_on='employment_status',
        right_index=True
    )
    other_dist['percentage'] = (other_dist['weighted_count'] / other_dist['total_weight'] * 100).round(2)
    
    other_dist['state_code'] = state_code
    other_dist['year'] = year
    
    return other_dist[['state_code', 'employment_status', 'income_bracket', 'percentage', 'mean_amount', 'median_amount', 'weighted_count', 'year']]


def extract_public_assistance_income(persons: pd.DataFrame, state_code: str, year: int) -> pd.DataFrame:
    """
    Extract PAP (public assistance) distribution.
    
    PAP is a PERSON-level variable (not household-level).
    
    PAP includes:
    - TANF (Temporary Assistance for Needy Families)
    - General welfare assistance
    - Other public assistance
    
    Does NOT include:
    - Social Security (SSP)
    - SSI (SSIP)
    - Unemployment compensation (in OIP)
    
    NOTE: PAP income is NOT taxable (unlike unemployment).
    """
    # Filter to people with public assistance
    has_pap = persons[(persons['PAP'].notna()) & (persons['PAP'] > 0)].copy()
    
    # Create income brackets
    has_pap['income_bracket'] = pd.cut(
        has_pap['PAP'],
        bins=[0, 1000, 2000, 3000, 5000, 10000, 15000, float('inf')],
        labels=['<$1K', '$1-2K', '$2-3K', '$3-5K', '$5-10K', '$10-15K', '$15K+']
    )
    
    # Calculate distribution
    pap_dist = has_pap.groupby('income_bracket', observed=True).agg({
        'PWGTP': 'sum',
        'PAP': ['mean', 'median'],
        'SERIALNO': 'count'
    }).reset_index()
    
    pap_dist.columns = ['income_bracket', 'weighted_count', 'mean_amount', 'median_amount', 'sample_count']
    
    # Calculate percentage
    total_weight = pap_dist['weighted_count'].sum()
    pap_dist['percentage'] = (pap_dist['weighted_count'] / total_weight * 100).round(2)
    
    pap_dist['state_code'] = state_code
    pap_dist['year'] = year
    
    return pap_dist[['state_code', 'income_bracket', 'percentage', 'mean_amount', 'median_amount', 'weighted_count', 'year']]


# =============================================================================
# COMPLEX HOUSEHOLD PATTERN EXTRACTIONS
# =============================================================================

def extract_adult_child_ages(persons: pd.DataFrame, state_code: str, year: int) -> pd.DataFrame:
    """
    Extract age distribution of adult children (18+) living at home.
    
    Adult children = RELSHIPP in [22,23,24,36] AND AGEP >= 18
    - 22: Biological child
    - 23: Adopted child  
    - 24: Stepchild
    - 36: Foster child
    
    Tax relevance:
    - Qualifying child vs qualifying relative determination
    - Support test calculations
    - Dependency exemption eligibility
    """
    # Filter to adult children
    adult_children = persons[
        (persons['RELSHIPP'].isin([22, 23, 24, 36])) &
        (persons['AGEP'] >= 18)
    ].copy()
    
    if len(adult_children) == 0:
        # Return empty DataFrame with correct structure
        return pd.DataFrame(columns=['state_code', 'age_bracket', 'percentage', 'weighted_count', 'year'])
    
    # Create age brackets
    adult_children['age_bracket'] = pd.cut(
        adult_children['AGEP'],
        bins=[18, 21, 24, 29, 34, 100],
        labels=['18-20', '21-23', '24-28', '29-33', '34+']
    )
    
    # Calculate distribution
    age_dist = adult_children.groupby('age_bracket', observed=True).agg({
        'PWGTP': 'sum',
        'SERIALNO': 'count'
    }).reset_index()
    
    age_dist.columns = ['age_bracket', 'weighted_count', 'sample_count']
    
    # Calculate percentages
    total = age_dist['weighted_count'].sum()
    age_dist['percentage'] = (age_dist['weighted_count'] / total * 100).round(2)
    
    age_dist['state_code'] = state_code
    age_dist['year'] = year
    
    return age_dist[['state_code', 'age_bracket', 'percentage', 'weighted_count', 'year']]


def extract_stepchild_patterns(households: pd.DataFrame, persons: pd.DataFrame, 
                                state_code: str, year: int) -> pd.DataFrame:
    """
    Extract stepchild patterns for blended families.
    
    Identifies households with stepchildren (RELSHIPP=24)
    and their composition (number of bio vs step children).
    
    Tax relevance:
    - Stepchild dependency rules
    - Blended family filing scenarios
    - Child tax credit eligibility
    """
    # Count children by type per household
    children = persons[persons['RELSHIPP'].isin([22, 23, 24])].copy()
    
    if len(children) == 0:
        return pd.DataFrame(columns=['state_code', 'pattern', 'percentage', 'weighted_count', 'avg_total_children', 'year'])
    
    child_counts = children.groupby('SERIALNO').apply(
        lambda x: pd.Series({
            'bio_children': ((x['RELSHIPP'] == 22) | (x['RELSHIPP'] == 23)).sum(),
            'step_children': (x['RELSHIPP'] == 24).sum(),
            'total_children': len(x)
        }),
        include_groups=False
    ).reset_index()
    
    # Only households with stepchildren
    blended = child_counts[child_counts['step_children'] > 0].copy()
    
    if len(blended) == 0:
        return pd.DataFrame(columns=['state_code', 'pattern', 'percentage', 'weighted_count', 'avg_total_children', 'year'])
    
    # Merge with household data for weights
    blended = blended.merge(households[['SERIALNO', 'WGTP']], on='SERIALNO')
    
    # Create pattern categories
    def categorize_pattern(row):
        bio = int(row['bio_children'])
        step = int(row['step_children'])
        
        if bio == 0 and step == 1:
            return 'only_step_1'
        elif bio == 0 and step >= 2:
            return 'only_step_2plus'
        elif bio == 1 and step == 1:
            return 'bio_1_step_1'
        elif bio >= 2 and step == 1:
            return 'bio_2plus_step_1'
        elif bio == 1 and step >= 2:
            return 'bio_1_step_2plus'
        else:
            return 'bio_2plus_step_2plus'
    
    blended['pattern'] = blended.apply(categorize_pattern, axis=1)
    
    # Calculate distribution
    pattern_dist = blended.groupby('pattern').agg({
        'WGTP': 'sum',
        'total_children': 'mean',
        'SERIALNO': 'count'
    }).reset_index()
    
    pattern_dist.columns = ['pattern', 'weighted_count', 'avg_total_children', 'sample_count']
    
    total = pattern_dist['weighted_count'].sum()
    pattern_dist['percentage'] = (pattern_dist['weighted_count'] / total * 100).round(2)
    
    pattern_dist['state_code'] = state_code
    pattern_dist['year'] = year
    
    return pattern_dist[['state_code', 'pattern', 'percentage', 'weighted_count', 'avg_total_children', 'year']]


def extract_multigenerational_patterns(households: pd.DataFrame, persons: pd.DataFrame,
                                       state_code: str, year: int) -> pd.DataFrame:
    """
    Extract multigenerational household patterns.
    
    Identifies households with 3+ generations:
    - Parent (RELSHIPP=26): Householder's parent
    - Grandchild (RELSHIPP=27): Householder's grandchild
    
    Presence of either/both indicates multigenerational household.
    
    Tax relevance:
    - Multiple tax filing units in one household
    - Dependent parent rules
    - Grandparent raising grandchildren scenarios
    """
    # Count relationship types per household
    rel_counts = persons.groupby('SERIALNO').apply(
        lambda x: pd.Series({
            'has_parent': (x['RELSHIPP'] == 26).any(),
            'has_grandchild': (x['RELSHIPP'] == 27).any()
        }),
        include_groups=False
    ).reset_index()
    
    # Determine number of generations
    def count_generations(row):
        if row['has_grandchild'] and row['has_parent']:
            return 4  # Both = 4 generations
        elif row['has_grandchild'] or row['has_parent']:
            return 3  # Either = 3 generations
        else:
            return 2  # Standard (householder + spouse/children)
    
    rel_counts['num_generations'] = rel_counts.apply(count_generations, axis=1)
    
    # Filter to 3+ generations
    multigenerational = rel_counts[rel_counts['num_generations'] >= 3].copy()
    
    if len(multigenerational) == 0:
        return pd.DataFrame(columns=['state_code', 'pattern', 'percentage', 'weighted_count', 'avg_household_size', 'year'])
    
    # Merge with household data
    multigenerational = multigenerational.merge(
        households[['SERIALNO', 'WGTP', 'NP']], 
        on='SERIALNO'
    )
    
    # Categorize patterns
    def categorize_multigens(row):
        if row['has_parent'] and row['has_grandchild']:
            return 'four_generations'
        elif row['has_grandchild']:
            return 'grandparent_with_grandchildren'
        elif row['has_parent']:
            return 'adult_with_parent'
        else:
            return 'other'
    
    multigenerational['pattern'] = multigenerational.apply(categorize_multigens, axis=1)
    
    # Calculate distribution
    pattern_dist = multigenerational.groupby('pattern').agg({
        'WGTP': 'sum',
        'NP': 'mean',
        'SERIALNO': 'count'
    }).reset_index()
    
    pattern_dist.columns = ['pattern', 'weighted_count', 'avg_household_size', 'sample_count']
    
    total = pattern_dist['weighted_count'].sum()
    pattern_dist['percentage'] = (pattern_dist['weighted_count'] / total * 100).round(2)
    
    pattern_dist['state_code'] = state_code
    pattern_dist['year'] = year
    
    return pattern_dist[['state_code', 'pattern', 'percentage', 'weighted_count', 'avg_household_size', 'year']]


def extract_unmarried_partner_patterns(households: pd.DataFrame, persons: pd.DataFrame,
                                       state_code: str, year: int) -> pd.DataFrame:
    """
    Extract unmarried partner (cohabiting couple) patterns.
    
    Identifies households with unmarried partner (RELSHIPP=33)
    and their compositions.
    
    Tax relevance:
    - Cannot file jointly (must file separately or as single/HOH)
    - Head of Household qualification complexity
    - Dependency rules (partner cannot be dependent)
    - Who claims children in household
    """
    # Identify households with unmarried partners
    has_partner = persons[persons['RELSHIPP'] == 33]['SERIALNO'].unique()
    
    if len(has_partner) == 0:
        return pd.DataFrame(columns=['state_code', 'pattern', 'percentage', 'weighted_count', 'avg_adults', 'avg_children', 'year'])
    
    # Get all members of these households
    partner_households = persons[persons['SERIALNO'].isin(has_partner)].copy()
    
    # Count household composition
    hh_composition = partner_households.groupby('SERIALNO').apply(
        lambda x: pd.Series({
            'num_adults': (x['AGEP'] >= 18).sum(),
            'num_children': (x['AGEP'] < 18).sum(),
            'has_bio_children': (x['RELSHIPP'].isin([22, 23])).any(),
            'has_step_children': (x['RELSHIPP'] == 24).any(),
            'has_other_adults': (x['RELSHIPP'].isin([25, 30, 34])).any()
        }),
        include_groups=False
    ).reset_index()
    
    # Merge with household data
    hh_composition = hh_composition.merge(
        households[['SERIALNO', 'WGTP']], 
        on='SERIALNO'
    )
    
    # Categorize patterns
    def categorize_partner_pattern(row):
        if row['num_children'] == 0:
            return 'couple_no_children'
        elif row['has_bio_children'] and not row['has_step_children']:
            return 'couple_bio_children_only'
        elif row['has_step_children']:
            return 'couple_blended_family'
        elif row['has_other_adults']:
            return 'couple_with_other_adults'
        else:
            return 'couple_with_children'
    
    hh_composition['pattern'] = hh_composition.apply(categorize_partner_pattern, axis=1)
    
    # Calculate distribution
    pattern_dist = hh_composition.groupby('pattern').agg({
        'WGTP': 'sum',
        'num_adults': 'mean',
        'num_children': 'mean',
        'SERIALNO': 'count'
    }).reset_index()
    
    pattern_dist.columns = ['pattern', 'weighted_count', 'avg_adults', 'avg_children', 'sample_count']
    
    total = pattern_dist['weighted_count'].sum()
    pattern_dist['percentage'] = (pattern_dist['weighted_count'] / total * 100).round(2)
    
    pattern_dist['state_code'] = state_code
    pattern_dist['year'] = year
    
    return pattern_dist[['state_code', 'pattern', 'percentage', 'weighted_count', 'avg_adults', 'avg_children', 'year']]


# =============================================================================
# DTYPE OPTIMIZATION
# =============================================================================

def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optimize data types for final distribution tables to reduce SQL file size.
    Only called on small output tables, not on large input data.
    """
    optimized = df.copy()
    
    # Convert low-cardinality strings to categorical
    for col in optimized.select_dtypes(include='object'):
        if optimized[col].nunique() < 50:
            optimized[col] = optimized[col].astype('category')
    
    # Downcast integers
    for col in optimized.select_dtypes(include=['int64']):
        optimized[col] = pd.to_numeric(optimized[col], downcast='integer')
    
    # Downcast floats
    for col in optimized.select_dtypes(include=['float64']):
        optimized[col] = pd.to_numeric(optimized[col], downcast='float')
    
    return optimized


# =============================================================================
# EXTRACT ALL DISTRIBUTIONS
# =============================================================================

def extract_all_distributions(households: pd.DataFrame, persons: pd.DataFrame,
                              state_code: str, year: int) -> Dict[str, pd.DataFrame]:
    """
    Extract all distribution tables with progress logging.
    """
    distributions = {}
    
    extraction_funcs = [
        ('household_patterns', lambda: extract_household_patterns(households, persons, state_code, year)),
        ('employment_by_age', lambda: extract_employment_by_age(persons, state_code, year)),
        ('children_by_parent_age', lambda: extract_children_by_parent_age(households, persons, state_code, year)),
        ('child_age_distributions', lambda: extract_child_age_distributions(persons, state_code, year)),
        ('social_security', lambda: extract_social_security(persons, state_code, year)),
        ('retirement_income', lambda: extract_retirement_income(persons, state_code, year)),
        ('interest_and_dividend_income', lambda: extract_interest_dividend(persons, state_code, year)),
        ('property_taxes', lambda: extract_property_taxes(households, state_code, year)),
        ('mortgage_interest', lambda: extract_mortgage_interest(households, state_code, year)),
        ('education_by_age', lambda: extract_education_by_age(persons, state_code, year)),
        ('disability_by_age', lambda: extract_disability_by_age(persons, state_code, year)),
        ('other_income_by_employment_status', lambda: extract_other_income_by_employment_status(persons, state_code, year)),
        ('public_assistance_income', lambda: extract_public_assistance_income(persons, state_code, year)),
        ('adult_child_ages', lambda: extract_adult_child_ages(persons, state_code, year)),
        ('stepchild_patterns', lambda: extract_stepchild_patterns(households, persons, state_code, year)),
        ('multigenerational_patterns', lambda: extract_multigenerational_patterns(households, persons, state_code, year)),
        ('unmarried_partner_patterns', lambda: extract_unmarried_partner_patterns(households, persons, state_code, year)),
    ]
    
    for table_name, extract_func in extraction_funcs:
        logger.info(f"  → Extracting {table_name}...")
        df = extract_func()
        distributions[table_name] = df
        logger.info(f"    ({len(df)} rows)")
    
    return distributions


# =============================================================================
# SQL EXPORT
# =============================================================================

def create_table_ddl(df: pd.DataFrame, table_name: str) -> str:
    """
    Generate CREATE TABLE statement based on DataFrame schema.
    """
    lines = [f"CREATE TABLE {table_name} ("]
    
    columns = []
    for col, dtype in df.dtypes.items():
        if isinstance(dtype, pd.CategoricalDtype) or dtype == 'object':
            columns.append(f"    {col} VARCHAR(100)")
        elif pd.api.types.is_integer_dtype(dtype):
            columns.append(f"    {col} INTEGER")
        elif pd.api.types.is_float_dtype(dtype):
            columns.append(f"    {col} DECIMAL(12,2)")
        else:
            columns.append(f"    {col} TEXT")
    
    lines.append(",\n".join(columns))
    lines.append(");")
    
    return "\n".join(lines)


def export_to_sql_file(distributions: Dict[str, pd.DataFrame], state_code: str, year: int):
    """
    Export all distribution tables to a single SQL file using COPY statements.
    """
    OUTPUT_DIR.mkdir(exist_ok=True)
    output_path = OUTPUT_DIR / f"pums_distributions_{state_code}_{year}.sql"
    
    logger.info(f"  → Creating SQL file: {output_path.name}")
    
    with open(output_path, 'w') as f:
        # Write header
        f.write(f"""-- PUMS Distribution Tables
-- State: {state_code}
-- Year: {year}
-- Generated: {pd.Timestamp.now()}
--
-- This file contains tax-relevant distribution tables extracted from Census PUMS data
-- Import with: psql -d mydb -f {output_path.name}

BEGIN;

""")
        
        # Drop all tables first
        f.write("-- Drop existing tables\n")
        for table_name in distributions.keys():
            full_table = f"{table_name}_{state_code}_{year}"
            f.write(f"DROP TABLE IF EXISTS {full_table} CASCADE;\n")
        f.write("\n")
        
        # Create and populate each table
        for table_name, df in distributions.items():
            full_table = f"{table_name}_{state_code}_{year}"
            
            logger.info(f"  → Writing {full_table}...")
            
            # CREATE TABLE
            f.write(f"-- Table: {full_table}\n")
            f.write(create_table_ddl(df, full_table))
            f.write("\n\n")
            
            # COPY data
            f.write(f"COPY {full_table} FROM stdin;\n")
            
            # Write data in tab-delimited format
            for _, row in df.iterrows():
                values = []
                for val in row:
                    if pd.isna(val):
                        values.append('\\N')
                    else:
                        values.append(str(val))
                f.write('\t'.join(values) + '\n')
            
            f.write("\\.\n\n")
        
        f.write("COMMIT;\n")
    
    file_size_kb = output_path.stat().st_size / 1024
    logger.info(f"  → File size: {file_size_kb:.1f} KB")
    logger.info(f"\n✓ SQL file created: {output_path}")


# =============================================================================
# DATABASE UPLOAD
# =============================================================================

def upload_to_database(distributions: Dict[str, pd.DataFrame], state_code: str, 
                       year: int, connection_string: str):
    """
    Upload all distribution tables to database using COPY statements.
    All-or-nothing transaction: rollback if any table fails.
    """
    try:
        import psycopg2
    except ImportError:
        logger.error("psycopg2 not installed. Install with: pip install psycopg2-binary")
        sys.exit(1)
    
    logger.info(f"  → Connecting to database...")
    
    try:
        conn = psycopg2.connect(connection_string)
        cur = conn.cursor()
        
        logger.info(f"  → Connected successfully")
        
        # Process all tables in a single transaction
        for table_name, df in distributions.items():
            full_table = f"{table_name}_{state_code}_{year}"
            
            logger.info(f"  → Uploading {full_table}...")
            
            # Drop and create table
            cur.execute(f"DROP TABLE IF EXISTS {full_table} CASCADE")
            cur.execute(create_table_ddl(df, full_table))
            
            # Use COPY for fast bulk insert
            buffer = StringIO()
            df.to_csv(buffer, sep='\t', header=False, index=False, na_rep='\\N')
            buffer.seek(0)
            
            cur.copy_expert(f"COPY {full_table} FROM stdin", buffer)
            
            logger.info(f"    ✓ {len(df)} rows uploaded")
        
        # Commit all changes
        conn.commit()
        logger.info(f"\n✓ All tables uploaded successfully")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"\n✗ Upload failed: {e}")
        logger.error("Transaction rolled back - no changes made to database")
        raise
    
    finally:
        conn.close()


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Extract PUMS distribution tables',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate SQL file for local import
  python extract_pums.py --state HI --year 2022 --output sql
  
  # Upload to database (using connection string)
  python extract_pums.py --state HI --year 2022 --output database --connection-string "postgresql://..."
  
  # Upload to database (using DATABASE_URL environment variable)
  export DATABASE_URL="postgresql://user:pass@host:5432/dbname"
  python extract_pums.py --state HI --year 2022 --output database
        """
    )
    
    parser.add_argument('--state', type=str, required=True,
                       help='Two-letter state code (e.g., HI, CA, TX)')
    parser.add_argument('--year', type=int, default=2022,
                       help='Year of PUMS data (default: 2022)')
    parser.add_argument('--output', type=str, required=True, choices=['sql', 'database'],
                       help='Output mode: sql (generate file) or database (upload directly)')
    parser.add_argument('--connection-string', type=str,
                       help='PostgreSQL connection string (or use DATABASE_URL env var)')
    
    args = parser.parse_args()
    
    # Validate database output requirements
    if args.output == 'database':
        conn_string = args.connection_string or os.getenv('DATABASE_URL')
        if not conn_string:
            logger.error("Database output requires --connection-string or DATABASE_URL environment variable")
            sys.exit(1)
    else:
        conn_string = None
    
    # Print header
    logger.info("="*60)
    logger.info(f"PUMS EXTRACTION: {args.state.upper()} ({args.year})")
    logger.info(f"Output mode: {args.output}")
    logger.info("="*60)
    
    try:
        # Phase 1: Download
        logger.info("\n[1/5] Downloading PUMS files...")
        household_zip, person_zip = download_pums_files(args.state.upper(), args.year)
        
        # Phase 2: Load
        logger.info("\n[2/5] Loading data into memory...")
        households, persons = load_pums_data(household_zip, person_zip)
        
        # Phase 3: Extract
        logger.info("\n[3/5] Extracting distribution tables...")
        distributions = extract_all_distributions(households, persons, args.state.upper(), args.year)
        
        # Phase 4: Optimize
        logger.info("\n[4/5] Optimizing data types...")
        distributions = {name: optimize_dtypes(df) for name, df in distributions.items()}
        logger.info("  → Data types optimized for all tables")
        
        # Phase 5: Output
        if args.output == 'sql':
            logger.info("\n[5/5] Exporting to SQL file...")
            export_to_sql_file(distributions, args.state.upper(), args.year)
        else:
            logger.info("\n[5/5] Uploading to database...")
            upload_to_database(distributions, args.state.upper(), args.year, conn_string)
        
        logger.info("\n" + "="*60)
        logger.info("✓ EXTRACTION COMPLETE")
        logger.info("="*60)
        logger.info(f"Tables extracted: {len(distributions)}")
        for table_name, df in distributions.items():
            full_table = f"{table_name}_{args.state.upper()}_{args.year}"
            logger.info(f"  - {full_table}: {len(df)} rows")
        
    except Exception as e:
        logger.error(f"\n✗ Extraction failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
