#!/usr/bin/env python3
"""
Derived Distribution Extraction Script
Creates combined PUMS + BLS probability tables for household generation

APPROACH: Loads processed PUMS and BLS data (from cache), creates derived tables
- Loads: Cached PUMS person data (no re-download)
- Loads: Cached BLS occupation data (no re-download)
- Creates: Education-occupation probabilities, age-income adjustments, SE probabilities
- Output: PostgreSQL-compatible SQL file

This is the THIRD step in the data pipeline:
1. extract_pums_distributions.py  → PUMS data tables
2. extract_bls_distributions.py   → BLS occupation tables  
3. extract_derived_distributions.py → Derived probability tables (THIS SCRIPT)

Usage:
    python extract_derived_distributions.py --state HI --pums-year 2022 --bls-year 2023
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
import zipfile
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

PUMS_CACHE_DIR = Path("./cache/pums_cache")
BLS_CACHE_DIR = Path("./cache/bls_cache")
OUTPUT_DIR = Path("./output")


# =============================================================================
# LOAD CACHED PUMS DATA
# =============================================================================

def load_pums_person_data(state_code: str, year: int) -> pd.DataFrame:
    """
    Load PUMS person data from cache (no re-download).
    
    Uses the same cache created by extract_pums_distributions.py
    If cache doesn't exist, provides helpful error message.
    
    Returns:
        DataFrame with person-level PUMS data
    """
    logger.info(f"Loading PUMS person data for {state_code} ({year}) from cache...")
    
    state_lower = state_code.lower()
    person_zip = PUMS_CACHE_DIR / f"{year}_csv_p{state_lower}.zip"
    
    if not person_zip.exists():
        raise FileNotFoundError(
            f"PUMS cache not found: {person_zip}\n"
            f"Please run: python extract_pums_distributions.py --state {state_code} --year {year}"
        )
    
    # Load person data from cached ZIP
    with zipfile.ZipFile(person_zip, 'r') as z:
        csv_name = [name for name in z.namelist() if name.endswith('.csv')][0]
        with z.open(csv_name) as f:
            # Only load columns we need for derived tables
            needed_cols = [
                'SERIALNO', 'PWGTP', 'AGEP', 'SCHL', 'ESR', 'OCCP',
                'WAGP', 'SEMP', 'RELSHIPP'
            ]
            persons = pd.read_csv(f, usecols=lambda x: x in needed_cols, low_memory=False)
    
    # Convert numeric columns
    numeric_cols = ['PWGTP', 'AGEP', 'SCHL', 'ESR', 'WAGP', 'SEMP']
    for col in numeric_cols:
        if col in persons.columns:
            persons[col] = pd.to_numeric(persons[col], errors='coerce')
    
    logger.info(f"Loaded {len(persons):,} person records")
    logger.info(f"Memory: {persons.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
    
    return persons


# =============================================================================
# LOAD CACHED BLS DATA
# =============================================================================

def load_bls_occupation_data(state_code: str, year: int) -> pd.DataFrame:
    """
    Load BLS OEWS data from cache (no re-download).
    
    Uses the same cache created by extract_bls_distributions.py
    If cache doesn't exist, provides helpful error message.
    
    Returns:
        DataFrame with occupation-level BLS data
    """
    logger.info(f"Loading BLS occupation data ({year}) from cache...")
    
    excel_path = BLS_CACHE_DIR / f"oews_{year}_state_data.xlsx"
    
    if not excel_path.exists():
        raise FileNotFoundError(
            f"BLS cache not found: {excel_path}\n"
            f"Please run: python extract_bls_distributions.py --state {state_code} --year {year}"
        )
    
    # Load BLS data from cached Excel
    df = pd.read_excel(excel_path, engine='openpyxl')
    
    # Convert numeric columns (same as BLS script)
    numeric_columns = ['TOT_EMP', 'A_MEDIAN']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Filter to state
    state_names = {
        'HI': 'Hawaii', 'CA': 'California', 'TX': 'Texas', 'NY': 'New York',
        'FL': 'Florida', 'IL': 'Illinois', 'PA': 'Pennsylvania', 'OH': 'Ohio',
        'GA': 'Georgia', 'NC': 'North Carolina', 'MI': 'Michigan', 'NJ': 'New Jersey',
        'VA': 'Virginia', 'WA': 'Washington', 'AZ': 'Arizona', 'MA': 'Massachusetts',
        'TN': 'Tennessee', 'IN': 'Indiana', 'MO': 'Missouri', 'MD': 'Maryland',
        'WI': 'Wisconsin', 'CO': 'Colorado', 'MN': 'Minnesota', 'SC': 'South Carolina',
        'AL': 'Alabama', 'LA': 'Louisiana', 'KY': 'Kentucky', 'OR': 'Oregon',
        'OK': 'Oklahoma', 'CT': 'Connecticut', 'UT': 'Utah', 'IA': 'Iowa',
        'NV': 'Nevada', 'AR': 'Arkansas', 'MS': 'Mississippi', 'KS': 'Kansas',
        'NM': 'New Mexico', 'NE': 'Nebraska', 'WV': 'West Virginia', 'ID': 'Idaho',
        'HI': 'Hawaii', 'NH': 'New Hampshire', 'ME': 'Maine', 'RI': 'Rhode Island',
        'MT': 'Montana', 'DE': 'Delaware', 'SD': 'South Dakota', 'ND': 'North Dakota',
        'AK': 'Alaska', 'DC': 'District of Columbia', 'VT': 'Vermont', 'WY': 'Wyoming'
    }
    
    state_name = state_names.get(state_code.upper())
    if not state_name:
        raise ValueError(f"Invalid state code: {state_code}")
    
    state_df = df[df['AREA_TITLE'] == state_name].copy()
    
    # Filter to detailed occupations with data
    state_df = state_df[~state_df['OCC_CODE'].str.endswith('0000')].copy()
    state_df = state_df[state_df['TOT_EMP'].notna() & (state_df['TOT_EMP'] > 0)].copy()
    state_df = state_df[state_df['A_MEDIAN'].notna()].copy()
    
    logger.info(f"Loaded {len(state_df):,} occupation records for {state_name}")
    
    return state_df


# =============================================================================
# DERIVED TABLE 1: EDUCATION → OCCUPATION PROBABILITIES
# =============================================================================

def create_education_occupation_probabilities(persons: pd.DataFrame, occupations: pd.DataFrame, state_code: str, year: int) -> pd.DataFrame:
    """
    Create probability distribution: P(occupation | education level)
    
    From Blueprint: education_occupation_probabilities
    
    LOGIC:
    1. Filter PUMS to employed people with occupation codes
    2. Map PUMS OCCP codes to BLS SOC codes (major groups)
    3. Group by education level (SCHL) and occupation
    4. Calculate weighted percentage within each education level
    5. Return distribution for sampling occupations based on education
    
    PURPOSE: When generating a person with Bachelor's degree, this tells us
    which occupations they're most likely to have.
    
    PUMS EDUCATION LEVELS (SCHL):
    - 01-15: No high school diploma
    - 16-17: High school graduate
    - 18-19: Some college, no degree
    - 20: Associate's degree
    - 21: Bachelor's degree
    - 22: Master's degree
    - 23: Professional degree
    - 24: Doctorate degree
    """
    logger.info("Creating education → occupation probabilities...")
    
    # Filter to employed people with occupation codes
    employed = persons[
        (persons['ESR'].isin([1, 2, 4, 5])) &  # Employed or armed forces
        (persons['OCCP'].notna()) &
        (persons['SCHL'].notna())
    ].copy()
    
    logger.info(f"Filtered to {len(employed):,} employed people with occupation codes")
    
    # Map PUMS OCCP to SOC major groups (first 2 digits)
    # PUMS uses 4-digit codes like "1050" → SOC "10" (Management)
    employed['soc_major'] = employed['OCCP'].astype(str).str[:2]
    
    # Simplify education levels into broader categories
    def map_education(schl):
        if pd.isna(schl):
            return 'unknown'
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
    
    employed['education_level'] = employed['SCHL'].apply(map_education)
    
    # Group by education and occupation, calculate weighted counts
    edu_occ = employed.groupby(['education_level', 'soc_major'], observed=True).agg({
        'PWGTP': 'sum',
        'SERIALNO': 'count'
    }).reset_index()
    
    edu_occ.columns = ['education_level', 'soc_major_group', 'weighted_count', 'sample_count']
    
    # Calculate percentage within each education level
    totals = edu_occ.groupby('education_level', observed=True)['weighted_count'].sum()
    edu_occ = edu_occ.merge(
        totals.rename('total_weight'),
        left_on='education_level',
        right_index=True
    )
    edu_occ['percentage'] = (edu_occ['weighted_count'] / edu_occ['total_weight'] * 100).round(2)
    
    # Add state and year
    edu_occ['state_code'] = state_code.upper()
    edu_occ['year'] = year
    
    # Sort by education level and percentage
    edu_occ = edu_occ.sort_values(['education_level', 'percentage'], ascending=[True, False])
    
    logger.info(f"Created {len(edu_occ)} education-occupation probability records")
    logger.info(f"Education levels: {edu_occ['education_level'].nunique()}")
    logger.info(f"Occupation groups: {edu_occ['soc_major_group'].nunique()}")
    
    return edu_occ[['education_level', 'soc_major_group', 'percentage', 'weighted_count', 'state_code', 'year']]


# =============================================================================
# DERIVED TABLE 2: AGE → INCOME ADJUSTMENTS
# =============================================================================

def create_age_income_adjustments(persons: pd.DataFrame, state_code: str, year: int) -> pd.DataFrame:
    """
    Create income adjustment multipliers by age.
    
    From Blueprint: age_income_adjustments
    
    LOGIC:
    1. Filter to employed people with wage income
    2. Calculate median wage by age group
    3. Normalize to overall median (create multiplier)
    4. Return adjustment factors for realistic age-based income variation
    
    PURPOSE: A 25-year-old nurse earns less than a 45-year-old nurse.
    This provides age-based multipliers to adjust base occupation wages.
    
    EXAMPLE OUTPUT:
    - Age 22-24: 0.75x median
    - Age 35-44: 1.10x median  
    - Age 55-64: 1.15x median
    """
    logger.info("Creating age → income adjustments...")
    
    # Filter to employed people with positive wage income
    wage_earners = persons[
        (persons['ESR'].isin([1, 2])) &  # Employed
        (persons['WAGP'].notna()) &
        (persons['WAGP'] > 0) &
        (persons['AGEP'].notna()) &
        (persons['AGEP'] >= 18)
    ].copy()
    
    logger.info(f"Filtered to {len(wage_earners):,} wage earners")
    
    # Create age brackets
    wage_earners['age_bracket'] = pd.cut(
        wage_earners['AGEP'],
        bins=[18, 25, 30, 35, 40, 45, 50, 55, 60, 65, 75, 100],
        labels=['18-24', '25-29', '30-34', '35-39', '40-44', '45-49', '50-54', '55-59', '60-64', '65-74', '75+']
    )
    
    # Calculate weighted median wage by age bracket
    age_wages = []
    for age_bracket in wage_earners['age_bracket'].cat.categories:
        bracket_data = wage_earners[wage_earners['age_bracket'] == age_bracket]
        
        # Weighted median calculation
        sorted_data = bracket_data.sort_values('WAGP')
        cumsum = sorted_data['PWGTP'].cumsum()
        total_weight = sorted_data['PWGTP'].sum()
        median_wage = sorted_data.loc[cumsum >= total_weight / 2, 'WAGP'].iloc[0]
        
        age_wages.append({
            'age_bracket': age_bracket,
            'median_wage': median_wage,
            'sample_count': len(bracket_data),
            'weighted_count': total_weight
        })
    
    age_income_df = pd.DataFrame(age_wages)
    
    # Calculate overall median for normalization
    overall_median = wage_earners['WAGP'].median()
    
    # Create adjustment multiplier (normalize to overall median)
    age_income_df['income_multiplier'] = (age_income_df['median_wage'] / overall_median).round(3)
    
    # Add state and year
    age_income_df['state_code'] = state_code.upper()
    age_income_df['year'] = year
    
    logger.info(f"Created {len(age_income_df)} age-income adjustment records")
    logger.info(f"Income multiplier range: {age_income_df['income_multiplier'].min():.2f}x - {age_income_df['income_multiplier'].max():.2f}x")
    
    return age_income_df[['age_bracket', 'median_wage', 'income_multiplier', 'weighted_count', 'state_code', 'year']]


# =============================================================================
# DERIVED TABLE 3: OCCUPATION → SELF-EMPLOYMENT PROBABILITY
# =============================================================================

def create_occupation_se_probability(persons: pd.DataFrame, state_code: str, year: int) -> pd.DataFrame:
    """
    Create self-employment probability by occupation.
    
    From Blueprint: occupation_self_employment_probability
    
    LOGIC:
    1. Filter to employed people with occupation codes
    2. Identify who has self-employment income (SEMP > 0)
    3. Group by occupation major group
    4. Calculate percentage with SE income within each occupation
    5. Return probability for realistic SE income assignment
    
    PURPOSE: Construction workers are more likely to have self-employment
    income than nurses. This provides occupation-specific SE probabilities.
    
    EXAMPLE OUTPUT:
    - Construction laborers: 35% have SE income
    - Registered nurses: 2% have SE income
    - Real estate agents: 80% have SE income
    """
    logger.info("Creating occupation → self-employment probabilities...")
    
    # Filter to employed people with occupation codes
    employed = persons[
        (persons['ESR'].isin([1, 2])) &
        (persons['OCCP'].notna())
    ].copy()
    
    logger.info(f"Filtered to {len(employed):,} employed people")
    
    # Map to SOC major groups
    employed['soc_major'] = employed['OCCP'].astype(str).str[:2]
    
    # Identify self-employment (SEMP > 0)
    employed['has_se_income'] = (employed['SEMP'].fillna(0) > 0).astype(int)
    
    # Calculate weighted SE percentage by occupation
    occ_se = employed.groupby('soc_major', observed=True).apply(
        lambda x: pd.Series({
            'total_weighted': x['PWGTP'].sum(),
            'se_weighted': (x['has_se_income'] * x['PWGTP']).sum(),
            'sample_count': len(x)
        })
    ).reset_index()
    
    occ_se['se_probability'] = (occ_se['se_weighted'] / occ_se['total_weighted'] * 100).round(2)
    
    # Add state and year
    occ_se['state_code'] = state_code.upper()
    occ_se['year'] = year
    
    # Sort by SE probability
    occ_se = occ_se.sort_values('se_probability', ascending=False)
    
    logger.info(f"Created {len(occ_se)} occupation SE probability records")
    logger.info(f"SE probability range: {occ_se['se_probability'].min():.1f}% - {occ_se['se_probability'].max():.1f}%")
    
    return occ_se[['soc_major', 'se_probability', 'total_weighted', 'state_code', 'year']]


# =============================================================================
# SQL EXPORT
# =============================================================================

def dataframe_to_sql_insert(df: pd.DataFrame, table_name: str) -> str:
    """Convert a DataFrame to SQL INSERT statements."""
    sql_lines = []
    
    sql_lines.append(f"\n-- Table: {table_name}")
    sql_lines.append(f"CREATE TABLE IF NOT EXISTS {table_name} (")
    
    columns = []
    for col, dtype in df.dtypes.items():
        if dtype == 'object' or str(dtype) == 'category':
            columns.append(f"    {col} VARCHAR(100)")
        elif 'int' in str(dtype):
            columns.append(f"    {col} INTEGER")
        elif 'float' in str(dtype):
            columns.append(f"    {col} DECIMAL(12,2)")
        else:
            columns.append(f"    {col} TEXT")
    
    sql_lines.append(",\n".join(columns))
    sql_lines.append(");\n")
    
    sql_lines.append(f"-- Data for {table_name}")
    
    for _, row in df.iterrows():
        values = []
        for val in row:
            if pd.isna(val):
                values.append('NULL')
            elif isinstance(val, str):
                val_escaped = str(val).replace("'", "''")
                values.append(f"'{val_escaped}'")
            else:
                values.append(str(val))
        
        sql_lines.append(f"INSERT INTO {table_name} VALUES ({', '.join(values)});")
    
    return '\n'.join(sql_lines)


def export_to_sql(distributions: dict, state_code: str, year: int, output_path: Path):
    """Export all derived distribution tables to SQL file."""
    logger.info(f"Exporting distributions to SQL: {output_path}")
    
    with open(output_path, 'w') as f:
        f.write(f"""-- Derived Distribution Tables (PUMS + BLS Combined)
-- State: {state_code}
-- Year: {year}
-- Generated: {pd.Timestamp.now()}
--
-- This file contains derived probability tables created by combining
-- PUMS person data and BLS occupation data for household generation

BEGIN;

""")
        
        for table_name, df in distributions.items():
            sql = dataframe_to_sql_insert(df, table_name)
            f.write(sql)
            f.write('\n\n')
        
        f.write("COMMIT;\n")
    
    logger.info(f"Exported {len(distributions)} tables to {output_path}")


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def main(state_code: str, pums_year: int, bls_year: int):
    """
    Main pipeline to create derived distribution tables.
    
    APPROACH:
    - Loads PUMS person data from cache (created by extract_pums_distributions.py)
    - Loads BLS occupation data from cache (created by extract_bls_distributions.py)
    - Creates 3 derived probability tables
    - Exports to PostgreSQL-compatible SQL
    """
    logger.info(f"Starting derived distribution extraction for {state_code}")
    logger.info(f"PUMS year: {pums_year}, BLS year: {bls_year}")
    logger.info("Method: Load from cached data (no downloads)")
    
    # Step 1: Load PUMS person data from cache
    persons = load_pums_person_data(state_code, pums_year)
    
    # Step 2: Load BLS occupation data from cache
    occupations = load_bls_occupation_data(state_code, bls_year)
    
    # Step 3: Create derived distribution tables
    distributions = {}
    
    distributions['education_occupation_probabilities'] = create_education_occupation_probabilities(
        persons, occupations, state_code, bls_year
    )
    
    distributions['age_income_adjustments'] = create_age_income_adjustments(
        persons, state_code, pums_year
    )
    
    distributions['occupation_self_employment_probability'] = create_occupation_se_probability(
        persons, state_code, pums_year
    )
    
    # Step 4: Export to SQL
    OUTPUT_DIR.mkdir(exist_ok=True)
    output_path = OUTPUT_DIR / f"derived_probabilities_{state_code}_{bls_year}.sql"
    export_to_sql(distributions, state_code, bls_year, output_path)
    
    # Print summary
    logger.info("\n" + "="*60)
    logger.info("EXTRACTION COMPLETE")
    logger.info("="*60)
    logger.info(f"State: {state_code}")
    logger.info(f"Year: {bls_year}")
    logger.info(f"Tables created: {len(distributions)}")
    logger.info(f"Output file: {output_path}")
    logger.info(f"File size: {output_path.stat().st_size / 1024:.1f} KB")
    logger.info("\nDerived tables:")
    for table_name, df in distributions.items():
        logger.info(f"  - {table_name}: {len(df)} rows")
    
    return distributions


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extract derived distribution tables (PUMS + BLS)')
    parser.add_argument('--state', type=str, required=True, help='Two-letter state code (e.g., HI)')
    parser.add_argument('--pums-year', type=int, default=2022, help='Year of PUMS data (default: 2022)')
    parser.add_argument('--bls-year', type=int, default=2023, help='Year of BLS data (default: 2023)')
    
    args = parser.parse_args()
    
    main(args.state.upper(), args.pums_year, args.bls_year)
