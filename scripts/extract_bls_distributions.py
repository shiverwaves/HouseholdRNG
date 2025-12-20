#!/usr/bin/env python3
"""
BLS OEWS Distribution Extraction Script
Extracts wage and employment data from Bureau of Labor Statistics OEWS

APPROACH: Downloads complete OEWS Excel file, extracts state data, and processes locally.
- Downloads: Full state data file from BLS (all occupations, all states)
- Processing: Filters to specified state and extracts wage distributions
- Output: PostgreSQL-compatible SQL file

Data Source: BLS Occupational Employment and Wage Statistics (OEWS)
URL: https://www.bls.gov/oes/special.requests/oesm{YY}st.zip

Usage:
    python extract_bls_distributions.py --state HI --year 2023
"""

import argparse
import os
import requests
import zipfile
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple
from io import BytesIO
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

# BLS OEWS data URL pattern (ZIP file with state-level Excel data)
# Example: https://www.bls.gov/oes/special.requests/oesm23st.zip (May 2023)
BLS_BASE_URL = "https://www.bls.gov/oes/special.requests/oesm{year_short}st.zip"
CACHE_DIR = Path("./cache/bls_cache")
OUTPUT_DIR = Path("./output")

# OEWS file contains all states in one Excel file
# We'll download once and cache, then filter to requested state


# =============================================================================
# DOWNLOAD BLS OEWS FILE
# =============================================================================

def download_bls_oews_file(year: int) -> Path:
    """
    Download complete BLS OEWS state data file (ZIP containing Excel).
    
    NOTE: This downloads the FULL file with all states (~5-10 MB), NOT using BLS API.
    Files are cached locally to avoid re-downloading.
    
    The ZIP contains one Excel file with data for ALL states.
    We download once and filter to the requested state.
    
    Args:
        year: Year of data (e.g., 2023 for May 2023 data)
        
    Returns:
        Path to cached Excel file
    """
    CACHE_DIR.mkdir(exist_ok=True)
    
    year_short = str(year)[2:]  # 2023 -> "23"
    zip_url = BLS_BASE_URL.format(year_short=year_short)
    
    cached_excel = CACHE_DIR / f"oews_{year}_state_data.xlsx"
    
    if cached_excel.exists():
        logger.info(f"Using cached OEWS file: {cached_excel}")
        return cached_excel
    
    logger.info(f"Downloading OEWS data for {year}...")
    logger.info(f"URL: {zip_url}")
    
    # Add browser-like headers to avoid being blocked
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
    }
    
    try:
        # Download ZIP file
        response = requests.get(zip_url, headers=headers, timeout=120, stream=True)
        response.raise_for_status()
        logger.info("Downloaded ZIP file")
        
        # Extract Excel file from ZIP
        with zipfile.ZipFile(BytesIO(response.content)) as zf:
            # Find Excel file in ZIP
            excel_files = [f for f in zf.namelist() if f.endswith(('.xlsx', '.xls'))]
            
            if not excel_files:
                raise ValueError("No Excel files found in ZIP")
            
            excel_filename = excel_files[0]
            logger.info(f"Extracting: {excel_filename}")
            
            # Extract to cache
            with zf.open(excel_filename) as ef:
                with open(cached_excel, 'wb') as out:
                    out.write(ef.read())
        
        logger.info(f"Cached OEWS file: {cached_excel}")
        return cached_excel
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Download failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Error processing file: {e}")
        raise


# =============================================================================
# LOAD BLS DATA
# =============================================================================

def load_bls_data(excel_path: Path) -> pd.DataFrame:
    """
    Load BLS OEWS data from Excel file.
    
    The Excel file contains all states, all occupations.
    Typical size: ~800 occupations × 50 states = ~40,000 rows
    
    Returns:
        DataFrame with all OEWS data
    """
    logger.info("Loading OEWS data...")
    
    # Read Excel file (no dtype specification - let pandas infer)
    df = pd.read_excel(excel_path, engine='openpyxl')
    
    # Convert numeric columns from string to numeric
    # BLS uses "*", "**", "#" for suppressed/unavailable data
    numeric_columns = [
        'TOT_EMP', 'EMP_PRSE',
        'H_MEAN', 'A_MEAN', 'MEAN_PRSE',
        'H_MEDIAN', 'A_MEDIAN',
        'H_PCT10', 'H_PCT25', 'H_PCT75', 'H_PCT90',
        'A_PCT10', 'A_PCT25', 'A_PCT75', 'A_PCT90'
    ]
    
    for col in numeric_columns:
        if col in df.columns:
            # Convert to numeric, coercing errors (e.g., "*" becomes NaN)
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    logger.info(f"Converted {len(numeric_columns)} columns to numeric types")
    
    # Log memory usage
    memory_mb = df.memory_usage(deep=True).sum() / 1024**2
    
    logger.info(f"Loaded {len(df):,} occupation-state records")
    logger.info(f"Memory usage: {memory_mb:.1f} MB")
    
    return df


# =============================================================================
# EXTRACT STATE DATA
# =============================================================================

def extract_state_occupations(df: pd.DataFrame, state_code: str, year: int) -> pd.DataFrame:
    """
    Extract occupation and wage data for a specific state.
    
    From Blueprint: bls_wages_by_occupation_state
    
    LOGIC:
    1. Filter to specified state using AREA_TITLE (e.g., "Hawaii")
    2. Keep only detailed occupations (filter out summary categories)
    3. Clean and validate wage data
    4. Return distribution table with:
       - SOC code
       - Occupation title
       - State code
       - Employment count
       - Median annual wage
       - Wage percentiles (10th, 25th, 75th, 90th)
    
    PURPOSE: When generating a household with a specific occupation,
    this provides realistic wage distributions for that occupation in the state.
    
    KEY BLS VARIABLES:
    - OCC_CODE: Standard Occupational Classification code
    - OCC_TITLE: Occupation name
    - TOT_EMP: Total employment in occupation for state
    - A_MEDIAN: Median annual wage
    - A_PCT10/25/75/90: Annual wage percentiles
    """
    logger.info(f"Extracting occupation data for {state_code}...")
    
    # Map state code to full state name for AREA_TITLE
    state_names = {
        'HI': 'Hawaii',
        'AK': 'Alaska',
        'AL': 'Alabama',
        'AR': 'Arkansas',
        'AZ': 'Arizona',
        'CA': 'California',
        'CO': 'Colorado',
        'CT': 'Connecticut',
        'DC': 'District of Columbia',
        'DE': 'Delaware',
        'FL': 'Florida',
        'GA': 'Georgia',
        'IA': 'Iowa',
        'ID': 'Idaho',
        'IL': 'Illinois',
        'IN': 'Indiana',
        'KS': 'Kansas',
        'KY': 'Kentucky',
        'LA': 'Louisiana',
        'MA': 'Massachusetts',
        'MD': 'Maryland',
        'ME': 'Maine',
        'MI': 'Michigan',
        'MN': 'Minnesota',
        'MO': 'Missouri',
        'MS': 'Mississippi',
        'MT': 'Montana',
        'NC': 'North Carolina',
        'ND': 'North Dakota',
        'NE': 'Nebraska',
        'NH': 'New Hampshire',
        'NJ': 'New Jersey',
        'NM': 'New Mexico',
        'NV': 'Nevada',
        'NY': 'New York',
        'OH': 'Ohio',
        'OK': 'Oklahoma',
        'OR': 'Oregon',
        'PA': 'Pennsylvania',
        'RI': 'Rhode Island',
        'SC': 'South Carolina',
        'SD': 'South Dakota',
        'TN': 'Tennessee',
        'TX': 'Texas',
        'UT': 'Utah',
        'VA': 'Virginia',
        'VT': 'Vermont',
        'WA': 'Washington',
        'WI': 'Wisconsin',
        'WV': 'West Virginia',
        'WY': 'Wyoming',
    }
    
    state_name = state_names.get(state_code.upper())
    if not state_name:
        raise ValueError(f"Invalid state code: {state_code}")
    
    # Filter to state
    state_df = df[df['AREA_TITLE'] == state_name].copy()
    logger.info(f"Found {len(state_df):,} occupation records for {state_name}")
    
    # Filter to detailed occupations only (6-digit SOC codes)
    # Exclude summary categories (ending in 0000)
    state_df = state_df[~state_df['OCC_CODE'].str.endswith('0000')].copy()
    logger.info(f"After filtering summaries: {len(state_df):,} detailed occupations")
    
    # Keep only rows with employment data
    state_df = state_df[state_df['TOT_EMP'].notna() & (state_df['TOT_EMP'] > 0)].copy()
    logger.info(f"With employment data: {len(state_df):,} occupations")
    
    # Keep only rows with wage data
    state_df = state_df[state_df['A_MEDIAN'].notna()].copy()
    logger.info(f"With wage data: {len(state_df):,} occupations")
    
    # Create clean distribution table
    occupation_dist = pd.DataFrame({
        'soc_code': state_df['OCC_CODE'],
        'occupation_title': state_df['OCC_TITLE'],
        'state_code': state_code.upper(),
        'employment_count': state_df['TOT_EMP'].astype('int'),
        'median_annual_wage': state_df['A_MEDIAN'].astype('int'),
        'p10_annual_wage': state_df['A_PCT10'].fillna(0).astype('int'),
        'p25_annual_wage': state_df['A_PCT25'].fillna(0).astype('int'),
        'p75_annual_wage': state_df['A_PCT75'].fillna(0).astype('int'),
        'p90_annual_wage': state_df['A_PCT90'].fillna(0).astype('int'),
        'year': year
    })
    
    # Sort by employment (most common occupations first)
    occupation_dist = occupation_dist.sort_values('employment_count', ascending=False).reset_index(drop=True)
    
    logger.info(f"Extracted {len(occupation_dist)} occupation distributions")
    
    # Show summary
    logger.info(f"\nTop 5 occupations by employment:")
    for i, row in occupation_dist.head(5).iterrows():
        logger.info(f"  {row['occupation_title']:50s} - {row['employment_count']:,} jobs, ${row['median_annual_wage']:,}/yr")
    
    return occupation_dist


# =============================================================================
# SQL EXPORT
# =============================================================================

def dataframe_to_sql_insert(df: pd.DataFrame, table_name: str) -> str:
    """
    Convert a DataFrame to SQL INSERT statements.
    """
    sql_lines = []
    
    # Create table structure
    sql_lines.append(f"\n-- Table: {table_name}")
    sql_lines.append(f"CREATE TABLE IF NOT EXISTS {table_name} (")
    
    columns = []
    for col, dtype in df.dtypes.items():
        if dtype == 'object':
            columns.append(f"    {col} VARCHAR(200)")
        elif dtype == 'int32' or dtype == 'int64':
            columns.append(f"    {col} INTEGER")
        elif dtype == 'float32' or dtype == 'float64':
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
                val_escaped = val.replace("'", "''")
                values.append(f"'{val_escaped}'")
            else:
                values.append(str(val))
        
        sql_lines.append(f"INSERT INTO {table_name} VALUES ({', '.join(values)});")
    
    return '\n'.join(sql_lines)


def export_to_sql(occupation_dist: pd.DataFrame, state_code: str, year: int, output_path: Path):
    """
    Export occupation distribution table to SQL file.
    """
    logger.info(f"Exporting distribution to SQL: {output_path}")
    
    with open(output_path, 'w') as f:
        # Write header
        f.write(f"""-- BLS OEWS Distribution Table
-- State: {state_code}
-- Year: {year}
-- Generated: {pd.Timestamp.now()}
--
-- This file contains occupation and wage distribution data from BLS OEWS
-- for use in household generation pipeline

BEGIN;

""")
        
        # Write occupation distribution table
        sql = dataframe_to_sql_insert(occupation_dist, 'bls_occupation_wages')
        f.write(sql)
        f.write('\n\n')
        
        f.write("COMMIT;\n")
    
    logger.info(f"Exported to {output_path}")


# =============================================================================
# VALIDATION
# =============================================================================

def validate_distribution(df: pd.DataFrame):
    """
    Run basic validation checks on extracted distribution.
    """
    logger.info("Validating distribution...")
    
    issues = []
    
    # Check for empty table
    if len(df) == 0:
        issues.append("WARNING: Distribution table is empty")
    
    # Check wage reasonableness
    if df['median_annual_wage'].min() < 15000:
        issues.append(f"WARNING: Some wages seem very low (min: ${df['median_annual_wage'].min():,})")
    
    if df['median_annual_wage'].max() > 500000:
        logger.info(f"Note: Some high wages detected (max: ${df['median_annual_wage'].max():,}) - this is normal for executives/doctors")
    
    # Check employment totals
    total_emp = df['employment_count'].sum()
    logger.info(f"Total employment across all occupations: {total_emp:,}")
    
    if issues:
        logger.warning(f"Validation found {len(issues)} issues:")
        for issue in issues:
            logger.warning(f"  {issue}")
    else:
        logger.info("Distribution passed validation")
    
    return len(issues) == 0


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def main(state_code: str, year: int):
    """
    Main pipeline to extract BLS OEWS distributions.
    
    APPROACH:
    - Downloads complete OEWS file from BLS (NOT using BLS API)
    - Extracts and loads Excel data into pandas
    - Filters to specified state
    - Exports to PostgreSQL-compatible SQL
    """
    logger.info(f"Starting BLS OEWS extraction for {state_code} ({year})")
    logger.info("Method: Download complete OEWS file (no API calls)")
    
    # Step 1: Download OEWS file (cached)
    excel_path = download_bls_oews_file(year)
    
    # Step 2: Load into DataFrame
    oews_df = load_bls_data(excel_path)
    
    # Step 3: Extract state distribution
    occupation_dist = extract_state_occupations(oews_df, state_code, year)
    
    # Step 4: Validate
    validate_distribution(occupation_dist)
    
    # Step 5: Export to SQL
    OUTPUT_DIR.mkdir(exist_ok=True)
    output_path = OUTPUT_DIR / f"bls_occupation_wages_{state_code}_{year}.sql"
    export_to_sql(occupation_dist, state_code, year, output_path)
    
    # Print summary
    logger.info("\n" + "="*60)
    logger.info("EXTRACTION COMPLETE")
    logger.info("="*60)
    logger.info(f"State: {state_code}")
    logger.info(f"Year: {year}")
    logger.info(f"Occupations extracted: {len(occupation_dist)}")
    logger.info(f"Output file: {output_path}")
    logger.info(f"File size: {output_path.stat().st_size / 1024:.1f} KB")
    logger.info(f"\nTotal employment: {occupation_dist['employment_count'].sum():,}")
    logger.info(f"Median wage range: ${occupation_dist['median_annual_wage'].min():,} - ${occupation_dist['median_annual_wage'].max():,}")
    
    return occupation_dist


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extract BLS OEWS distribution tables')
    parser.add_argument('--state', type=str, required=True, help='Two-letter state code (e.g., HI)')
    parser.add_argument('--year', type=int, default=2023, help='Year of OEWS data (default: 2023)')
    
    args = parser.parse_args()
    
    main(args.state.upper(), args.year)
