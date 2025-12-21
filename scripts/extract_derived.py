#!/usr/bin/env python3
"""
Derived Distribution Extraction Script
Creates combined PUMS + BLS probability tables for household generation

This is the THIRD step in the data pipeline:
1. extract_pums.py  → PUMS distribution tables (13 tables)
2. extract_bls.py   → BLS occupation wages (1 table)
3. extract_derived.py → Derived probability tables (THIS SCRIPT - 3 tables)

IMPORTANT: Relationship to extract_pums.py tables
---------------------------------------------------
extract_pums.py creates:
- education_by_age: Education distribution by age (P(education | age))
- disability_by_age: Disability prevalence by age
- other_income_by_employment_status: OIP distribution by ESR
- public_assistance_income: PAP (welfare) distribution
- ... and 9 other PUMS tables

THIS script creates DIFFERENT tables:
- education_occupation_probabilities: P(occupation | education) 
  NOTE: This cross-tabulates education × occupation, which is different
  from the simple education_by_age distribution in extract_pums.py
  
- age_income_adjustments: Income multipliers by age for realistic wage adjustment
  
- occupation_self_employment_probability: P(has SE income | occupation)

These derived tables combine raw PUMS person data with BLS occupation data
to create conditional probability distributions for household generation.

Usage:
    # Generate SQL file for local import
    python extract_derived.py --state HI --pums-year 2022 --bls-year 2023 --output sql
    
    # Upload directly to database (GitHub Actions)
    python extract_derived.py --state HI --pums-year 2022 --bls-year 2023 --output database
    
    # Or use environment variable
    export DATABASE_URL="postgresql://user:pass@host:5432/dbname"
    python extract_derived.py --state HI --pums-year 2022 --bls-year 2023 --output database
"""

import argparse
import os
import sys
import zipfile
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict
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

PUMS_CACHE_DIR = Path("./pums_cache")
BLS_CACHE_DIR = Path("./bls_cache")
OUTPUT_DIR = Path("./output")


# =============================================================================
# LOAD CACHED PUMS DATA
# =============================================================================

def load_pums_person_data(state_code: str, year: int) -> pd.DataFrame:
    """
    Load PUMS person data from cache (no re-download).
    
    Uses the same cache created by extract_pums.py
    If cache doesn't exist, provides helpful error message.
    
    NOTE: This loads RAW person records to create derived cross-tabulations.
    The education_by_age table from extract_pums.py is different - that's
    just education distribution by age, while we need education × occupation
    cross-tabulation here.
    
    Required PUMS variables:
    - SERIALNO: Household ID
    - PWGTP: Person weight (for population estimates)
    - AGEP: Age
    - SCHL: Educational attainment (for education_occupation_probabilities)
    - ESR: Employment status (for filtering employed people)
    - OCCP: Occupation code (for education_occupation_probabilities)
    - WAGP: Wage/salary income (for age_income_adjustments)
    - SEMP: Self-employment income (for occupation_se_probability)
    - RELSHIPP: Relationship to householder
    """
    state_lower = state_code.lower()
    person_zip = PUMS_CACHE_DIR / f"{year}_csv_p{state_lower}.zip"
    
    if not person_zip.exists():
        logger.error(f"\n✗ PUMS cache not found: {person_zip}")
        logger.error(f"\n📍 Local Development:")
        logger.error(f"  Run: python extract_pums.py --state {state_code} --year {year} --output sql")
        logger.error(f"\n☁️  GitHub Actions:")
        logger.error(f"  Ensure PUMS workflow has run and uploaded cache artifact")
        raise FileNotFoundError(f"PUMS cache not found: {person_zip}")
    
    logger.info(f"  → Loading PUMS person data from cache...")
    
    # Load person data from cached ZIP (only needed columns)
    with zipfile.ZipFile(person_zip, 'r') as z:
        csv_name = [name for name in z.namelist() if name.endswith('.csv')][0]
        with z.open(csv_name) as f:
            needed_cols = ['SERIALNO', 'PWGTP', 'AGEP', 'SCHL', 'ESR', 'OCCP', 'WAGP', 'SEMP', 'RELSHIPP']
            persons = pd.read_csv(f, usecols=lambda x: x in needed_cols, low_memory=False)
    
    # Convert numeric columns
    numeric_cols = ['PWGTP', 'AGEP', 'SCHL', 'ESR', 'WAGP', 'SEMP']
    for col in numeric_cols:
        if col in persons.columns:
            persons[col] = pd.to_numeric(persons[col], errors='coerce')
    
    logger.info(f"    Loaded {len(persons):,} person records")
    
    return persons


# =============================================================================
# LOAD CACHED BLS DATA
# =============================================================================

def load_bls_occupation_data(state_code: str, year: int) -> pd.DataFrame:
    """
    Load BLS OEWS data from cache (no re-download).
    
    Uses the same cache created by extract_bls.py
    If cache doesn't exist, provides helpful error message.
    """
    excel_path = BLS_CACHE_DIR / f"oews_{year}_state_data.xlsx"
    
    if not excel_path.exists():
        logger.error(f"\n✗ BLS cache not found: {excel_path}")
        logger.error(f"\n📍 Local Development:")
        logger.error(f"  Run: python extract_bls.py --state {state_code} --year {year} --output sql")
        logger.error(f"\n☁️  GitHub Actions:")
        logger.error(f"  Ensure BLS workflow has run and uploaded cache artifact")
        raise FileNotFoundError(f"BLS cache not found: {excel_path}")
    
    logger.info(f"  → Loading BLS occupation data from cache...")
    
    # Load BLS data from cached Excel
    df = pd.read_excel(excel_path, engine='openpyxl')
    
    # Convert numeric columns
    numeric_columns = ['TOT_EMP', 'A_MEDIAN']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # State name mapping (minimal - just for lookup)
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
        'NH': 'New Hampshire', 'ME': 'Maine', 'RI': 'Rhode Island', 'MT': 'Montana',
        'DE': 'Delaware', 'SD': 'South Dakota', 'ND': 'North Dakota', 'AK': 'Alaska',
        'DC': 'District of Columbia', 'VT': 'Vermont', 'WY': 'Wyoming'
    }
    
    state_name = state_names.get(state_code.upper())
    if not state_name:
        raise ValueError(f"Invalid state code: {state_code}")
    
    # Filter to state and detailed occupations
    state_df = df[df['AREA_TITLE'] == state_name].copy()
    state_df = state_df[~state_df['OCC_CODE'].str.endswith('0000')].copy()
    state_df = state_df[state_df['TOT_EMP'].notna() & (state_df['TOT_EMP'] > 0)].copy()
    state_df = state_df[state_df['A_MEDIAN'].notna()].copy()
    
    logger.info(f"    Loaded {len(state_df):,} occupation records for {state_name}")
    
    return state_df


# =============================================================================
# DERIVED TABLE 1: EDUCATION → OCCUPATION PROBABILITIES
# =============================================================================

def extract_education_occupation_probabilities(persons: pd.DataFrame, occupations: pd.DataFrame, 
                                               state_code: str, pums_year: int, bls_year: int) -> pd.DataFrame:
    """
    Create probability distribution: P(occupation | education level)
    
    Shows which occupations people with different education levels tend to have.
    """
    logger.info("  → Extracting education-occupation probabilities...")
    
    # Filter to employed people with occupation codes
    employed = persons[
        (persons['ESR'].isin([1, 2, 4, 5])) &
        (persons['OCCP'].notna()) &
        (persons['SCHL'].notna())
    ].copy()
    
    # Map PUMS OCCP to SOC major groups (first 2 digits)
    employed['soc_major'] = employed['OCCP'].astype(str).str[:2]
    
    # Simplify education levels
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
    
    # Group by education and occupation
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
    
    edu_occ['state_code'] = state_code.upper()
    edu_occ['pums_year'] = pums_year
    edu_occ['bls_year'] = bls_year
    
    # Sort by education level and percentage
    edu_occ = edu_occ.sort_values(['education_level', 'percentage'], ascending=[True, False])
    
    logger.info(f"    ({len(edu_occ)} rows)")
    
    return edu_occ[['education_level', 'soc_major_group', 'percentage', 'weighted_count', 'state_code', 'pums_year', 'bls_year']]


# =============================================================================
# DERIVED TABLE 2: AGE → INCOME ADJUSTMENTS
# =============================================================================

def extract_age_income_adjustments(persons: pd.DataFrame, state_code: str, pums_year: int, bls_year: int) -> pd.DataFrame:
    """
    Create income adjustment multipliers by age.
    
    Provides age-based wage multipliers (e.g., 25-year-olds earn 0.75x median).
    """
    logger.info("  → Extracting age-income adjustments...")
    
    # Filter to employed people with positive wage income
    wage_earners = persons[
        (persons['ESR'].isin([1, 2])) &
        (persons['WAGP'].notna()) &
        (persons['WAGP'] > 0) &
        (persons['AGEP'].notna()) &
        (persons['AGEP'] >= 18)
    ].copy()
    
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
        
        if total_weight > 0:
            median_wage = sorted_data.loc[cumsum >= total_weight / 2, 'WAGP'].iloc[0]
        else:
            median_wage = 0
        
        age_wages.append({
            'age_bracket': age_bracket,
            'median_wage': median_wage,
            'sample_count': len(bracket_data),
            'weighted_count': total_weight
        })
    
    age_income_df = pd.DataFrame(age_wages)
    
    # Calculate overall median for normalization
    overall_median = wage_earners['WAGP'].median()
    
    # Create adjustment multiplier
    age_income_df['income_multiplier'] = (age_income_df['median_wage'] / overall_median).round(3)
    
    age_income_df['state_code'] = state_code.upper()
    age_income_df['pums_year'] = pums_year
    age_income_df['bls_year'] = bls_year
    
    logger.info(f"    ({len(age_income_df)} rows)")
    
    return age_income_df[['age_bracket', 'median_wage', 'income_multiplier', 'weighted_count', 'state_code', 'pums_year', 'bls_year']]


# =============================================================================
# DERIVED TABLE 3: OCCUPATION → SELF-EMPLOYMENT PROBABILITY
# =============================================================================

def extract_occupation_se_probability(persons: pd.DataFrame, state_code: str, pums_year: int, bls_year: int) -> pd.DataFrame:
    """
    Create self-employment probability by occupation.
    
    Shows percentage of people in each occupation who have self-employment income.
    """
    logger.info("  → Extracting occupation self-employment probabilities...")
    
    # Filter to employed people with occupation codes
    employed = persons[
        (persons['ESR'].isin([1, 2])) &
        (persons['OCCP'].notna())
    ].copy()
    
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
        }),
        include_groups=False
    ).reset_index()
    
    occ_se['se_probability'] = (occ_se['se_weighted'] / occ_se['total_weighted'] * 100).round(2)
    
    occ_se['state_code'] = state_code.upper()
    occ_se['pums_year'] = pums_year
    occ_se['bls_year'] = bls_year
    
    # Sort by SE probability
    occ_se = occ_se.sort_values('se_probability', ascending=False)
    
    logger.info(f"    ({len(occ_se)} rows)")
    
    return occ_se[['soc_major', 'se_probability', 'total_weighted', 'state_code', 'pums_year', 'bls_year']]


# =============================================================================
# DTYPE OPTIMIZATION
# =============================================================================

def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optimize data types for final distribution table to reduce SQL file size.
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
            columns.append(f"    {col} DECIMAL(12,4)")
        else:
            columns.append(f"    {col} TEXT")
    
    lines.append(",\n".join(columns))
    lines.append(");")
    
    return "\n".join(lines)


def export_to_sql_file(distributions: Dict[str, pd.DataFrame], state_code: str, 
                       pums_year: int, bls_year: int):
    """
    Export all derived distribution tables to SQL file using COPY statements.
    """
    OUTPUT_DIR.mkdir(exist_ok=True)
    output_path = OUTPUT_DIR / f"derived_probabilities_{state_code}_pums_{pums_year}_bls_{bls_year}.sql"
    
    logger.info(f"  → Creating SQL file: {output_path.name}")
    
    with open(output_path, 'w') as f:
        # Write header
        f.write(f"""-- Derived Distribution Tables (PUMS + BLS Combined)
-- State: {state_code}
-- PUMS Year: {pums_year}
-- BLS Year: {bls_year}
-- Generated: {pd.Timestamp.now()}
--
-- This file contains derived probability tables created by combining
-- PUMS person data and BLS occupation data for household generation
-- Import with: psql -d mydb -f {output_path.name}

BEGIN;

""")
        
        # Drop all tables first
        f.write("-- Drop existing tables\n")
        for table_name in distributions.keys():
            full_table = f"{table_name}_{state_code}_pums_{pums_year}_bls_{bls_year}"
            f.write(f"DROP TABLE IF EXISTS {full_table} CASCADE;\n")
        f.write("\n")
        
        # Create and populate each table
        for table_name, df in distributions.items():
            full_table = f"{table_name}_{state_code}_pums_{pums_year}_bls_{bls_year}"
            
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
                       pums_year: int, bls_year: int, connection_string: str):
    """
    Upload all derived distribution tables to database using COPY statements.
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
            full_table = f"{table_name}_{state_code}_pums_{pums_year}_bls_{bls_year}"
            
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
        description='Extract derived distribution tables (PUMS + BLS combined)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate SQL file for local import
  python extract_derived.py --state HI --pums-year 2022 --bls-year 2023 --output sql
  
  # Upload to database (using connection string)
  python extract_derived.py --state HI --pums-year 2022 --bls-year 2023 --output database --connection-string "postgresql://..."
  
  # Upload to database (using DATABASE_URL environment variable)
  export DATABASE_URL="postgresql://user:pass@host:5432/dbname"
  python extract_derived.py --state HI --pums-year 2022 --bls-year 2023 --output database

Prerequisites:
  Must have run both:
    - python extract_pums.py --state HI --year 2022
    - python extract_bls.py --state HI --year 2023
  (Creates cache files that this script reads)
        """
    )
    
    parser.add_argument('--state', type=str, required=True,
                       help='Two-letter state code (e.g., HI, CA, TX)')
    parser.add_argument('--pums-year', type=int, default=2022,
                       help='Year of PUMS data (default: 2022)')
    parser.add_argument('--bls-year', type=int, default=2023,
                       help='Year of BLS data (default: 2023)')
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
    logger.info(f"DERIVED DISTRIBUTIONS: {args.state.upper()}")
    logger.info(f"PUMS year: {args.pums_year}, BLS year: {args.bls_year}")
    logger.info(f"Output mode: {args.output}")
    logger.info("="*60)
    
    try:
        # Phase 1: Load PUMS data
        logger.info("\n[1/5] Loading PUMS person data from cache...")
        persons = load_pums_person_data(args.state.upper(), args.pums_year)
        
        # Phase 2: Load BLS data
        logger.info("\n[2/5] Loading BLS occupation data from cache...")
        occupations = load_bls_occupation_data(args.state.upper(), args.bls_year)
        
        # Phase 3: Extract derived tables
        logger.info("\n[3/5] Extracting derived distribution tables...")
        distributions = {}
        
        distributions['education_occupation_probabilities'] = extract_education_occupation_probabilities(
            persons, occupations, args.state.upper(), args.pums_year, args.bls_year
        )
        
        distributions['age_income_adjustments'] = extract_age_income_adjustments(
            persons, args.state.upper(), args.pums_year, args.bls_year
        )
        
        distributions['occupation_self_employment_probability'] = extract_occupation_se_probability(
            persons, args.state.upper(), args.pums_year, args.bls_year
        )
        
        # Phase 4: Optimize
        logger.info("\n[4/5] Optimizing data types...")
        distributions = {name: optimize_dtypes(df) for name, df in distributions.items()}
        logger.info("  → Data types optimized for all tables")
        
        # Phase 5: Output
        if args.output == 'sql':
            logger.info("\n[5/5] Exporting to SQL file...")
            export_to_sql_file(distributions, args.state.upper(), args.pums_year, args.bls_year)
        else:
            logger.info("\n[5/5] Uploading to database...")
            upload_to_database(distributions, args.state.upper(), args.pums_year, args.bls_year, conn_string)
        
        logger.info("\n" + "="*60)
        logger.info("✓ EXTRACTION COMPLETE")
        logger.info("="*60)
        logger.info(f"State: {args.state.upper()}")
        logger.info(f"PUMS year: {args.pums_year}, BLS year: {args.bls_year}")
        logger.info(f"Tables created: {len(distributions)}")
        logger.info("")
        
        # Table descriptions for clarity
        table_descriptions = {
            'education_occupation_probabilities': 'P(occupation | education level)',
            'age_income_adjustments': 'Age-based income multipliers',
            'occupation_self_employment_probability': 'P(self-employment | occupation)'
        }
        
        for table_name, df in distributions.items():
            full_table = f"{table_name}_{args.state.upper()}_pums_{args.pums_year}_bls_{args.bls_year}"
            desc = table_descriptions.get(table_name, '')
            logger.info(f"  - {full_table}")
            logger.info(f"    {desc} ({len(df)} rows)")
        
    except Exception as e:
        logger.error(f"\n✗ Extraction failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
