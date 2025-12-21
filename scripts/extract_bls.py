#!/usr/bin/env python3
"""
BLS OEWS Distribution Extraction Script
Extracts occupation wage data from Bureau of Labor Statistics

Usage:
    # Generate SQL file for local import
    python extract_bls.py --state HI --year 2023 --output sql
    
    # Upload directly to database (GitHub Actions)
    python extract_bls.py --state HI --year 2023 --output database --connection-string $DATABASE_URL
    
    # Or use environment variable
    export DATABASE_URL="postgresql://user:pass@host:5432/dbname?sslmode=require"
    python extract_bls.py --state HI --year 2023 --output database
"""

import argparse
import os
import sys
import subprocess
import shutil
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

BLS_BASE_URL = "https://www.bls.gov/oes/special.requests/oesm{year_short}st.zip"
CACHE_DIR = Path("./bls_cache")
OUTPUT_DIR = Path("./output")

# State code to name mapping (BLS uses full state names in AREA_TITLE)
STATE_NAMES = {
    'AK': 'Alaska', 'AL': 'Alabama', 'AR': 'Arkansas', 'AZ': 'Arizona',
    'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut',
    'DC': 'District of Columbia', 'DE': 'Delaware', 'FL': 'Florida',
    'GA': 'Georgia', 'HI': 'Hawaii', 'IA': 'Iowa', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'KS': 'Kansas', 'KY': 'Kentucky',
    'LA': 'Louisiana', 'MA': 'Massachusetts', 'MD': 'Maryland', 'ME': 'Maine',
    'MI': 'Michigan', 'MN': 'Minnesota', 'MO': 'Missouri', 'MS': 'Mississippi',
    'MT': 'Montana', 'NC': 'North Carolina', 'ND': 'North Dakota',
    'NE': 'Nebraska', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
    'NM': 'New Mexico', 'NV': 'Nevada', 'NY': 'New York', 'OH': 'Ohio',
    'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island',
    'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee',
    'TX': 'Texas', 'UT': 'Utah', 'VA': 'Virginia', 'VT': 'Vermont',
    'WA': 'Washington', 'WI': 'Wisconsin', 'WV': 'West Virginia', 'WY': 'Wyoming'
}


# =============================================================================
# DOWNLOAD BLS OEWS FILE
# =============================================================================

def download_bls_oews_file(year: int) -> Path:
    """
    Download BLS OEWS state data file using wget.
    Falls back to manual download instructions if wget unavailable or fails.
    """
    CACHE_DIR.mkdir(exist_ok=True)
    
    year_short = str(year)[2:]  # 2023 -> "23"
    zip_url = BLS_BASE_URL.format(year_short=year_short)
    
    cached_zip = CACHE_DIR / f"oesm{year_short}st.zip"
    cached_excel = CACHE_DIR / f"oews_{year}_state_data.xlsx"
    
    # Check if Excel already extracted
    if cached_excel.exists():
        logger.info(f"Using cached OEWS file: {cached_excel}")
        return cached_excel
    
    # Check if ZIP already downloaded (from manual upload or previous run)
    if cached_zip.exists():
        logger.info(f"Extracting Excel from cached ZIP: {cached_zip}")
        _extract_excel_from_zip(cached_zip, cached_excel)
        return cached_excel
    
    # Download with wget
    logger.info(f"Downloading OEWS data for {year}...")
    logger.info(f"URL: {zip_url}")
    
    if not shutil.which('wget'):
        _print_manual_download_instructions(zip_url, cached_zip, year)
        raise RuntimeError("wget not available. See manual download instructions above.")
    
    try:
        _download_with_wget(zip_url, cached_zip)
        _extract_excel_from_zip(cached_zip, cached_excel)
        return cached_excel
        
    except subprocess.CalledProcessError as e:
        logger.error(f"wget failed: {e.stderr if e.stderr else str(e)}")
        _print_manual_download_instructions(zip_url, cached_zip, year)
        raise RuntimeError("Download failed. See manual download instructions above.")


def _download_with_wget(url: str, output_zip: Path):
    """Download using wget with browser-like headers"""
    logger.info("  → Downloading with wget...")
    
    result = subprocess.run([
        'wget',
        '--user-agent=Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0',
        '--timeout=120',
        '--tries=3',
        '--waitretry=5',
        '-O', str(output_zip),
        url
    ], check=True, capture_output=True, text=True)
    
    file_size_mb = output_zip.stat().st_size / 1024 / 1024
    logger.info(f"    Downloaded {file_size_mb:.1f} MB")


def _extract_excel_from_zip(zip_path: Path, output_excel: Path):
    """Extract Excel file from ZIP"""
    logger.info("  → Extracting Excel file from ZIP...")
    
    with zipfile.ZipFile(zip_path, 'r') as zf:
        excel_files = [f for f in zf.namelist() if f.endswith(('.xlsx', '.xls'))]
        
        if not excel_files:
            raise ValueError("No Excel files found in ZIP")
        
        excel_filename = excel_files[0]
        logger.info(f"    Extracting: {excel_filename}")
        
        with zf.open(excel_filename) as ef:
            with open(output_excel, 'wb') as out:
                out.write(ef.read())
    
    logger.info(f"    Cached to: {output_excel.name}")


def _print_manual_download_instructions(url: str, zip_path: Path, year: int):
    """Print manual download instructions"""
    logger.error("\n" + "="*70)
    logger.error("AUTOMATED DOWNLOAD FAILED")
    logger.error("="*70)
    logger.error("wget is unavailable or BLS server blocked the request.")
    logger.error("")
    logger.error("MANUAL DOWNLOAD INSTRUCTIONS:")
    logger.error("")
    logger.error("For Local Development:")
    logger.error(f"  1. Visit: {url}")
    logger.error(f"  2. Download the ZIP file")
    logger.error(f"  3. Save to: {zip_path}")
    logger.error(f"  4. Re-run this script")
    logger.error("")
    logger.error("For GitHub Actions:")
    logger.error(f"  1. Download ZIP from: {url}")
    logger.error(f"  2. Upload as workflow artifact named: bls-oews-{year}")
    logger.error(f"  3. Re-run the workflow")
    logger.error("="*70 + "\n")


# =============================================================================
# LOAD BLS DATA
# =============================================================================

def load_bls_data(excel_path: Path) -> pd.DataFrame:
    """
    Load BLS OEWS data from Excel file.
    Uses default dtypes for flexibility during processing.
    """
    logger.info("  → Loading OEWS data from Excel...")
    
    # Read Excel file
    df = pd.read_excel(excel_path, engine='openpyxl')
    
    # Convert numeric columns (BLS uses "*", "**", "#" for suppressed data)
    numeric_columns = [
        'TOT_EMP', 'EMP_PRSE',
        'H_MEAN', 'A_MEAN', 'MEAN_PRSE',
        'H_MEDIAN', 'A_MEDIAN',
        'H_PCT10', 'H_PCT25', 'H_PCT75', 'H_PCT90',
        'A_PCT10', 'A_PCT25', 'A_PCT75', 'A_PCT90'
    ]
    
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    logger.info(f"    Loaded {len(df):,} occupation-state records")
    
    return df


# =============================================================================
# EXTRACT STATE OCCUPATIONS
# =============================================================================

def extract_state_occupations(df: pd.DataFrame, state_code: str, year: int) -> pd.DataFrame:
    """
    Extract occupation wage data for a specific state.
    
    Uses STATE_NAMES mapping to convert state code to full name.
    Fails with clear error if state not found.
    """
    # Get state name from mapping
    state_name = STATE_NAMES.get(state_code.upper())
    
    if not state_name:
        logger.error(f"\n✗ Invalid state code: {state_code}")
        logger.error(f"Valid state codes: {', '.join(sorted(STATE_NAMES.keys()))}")
        raise ValueError(f"Invalid state code: {state_code}")
    
    # Filter to state
    state_df = df[df['AREA_TITLE'] == state_name].copy()
    
    if len(state_df) == 0:
        logger.error(f"\n✗ No data found for {state_name}")
        logger.error(f"This could mean:")
        logger.error(f"  - Wrong year (data not available for {year})")
        logger.error(f"  - BLS file format changed")
        raise ValueError(f"No data found for {state_name}")
    
    logger.info(f"    Found {len(state_df):,} occupation records for {state_name}")
    
    # Filter to detailed occupations (exclude summary categories ending in 0000)
    state_df = state_df[~state_df['OCC_CODE'].str.endswith('0000')].copy()
    logger.info(f"    After filtering summaries: {len(state_df):,} detailed occupations")
    
    # Keep only rows with employment and wage data
    state_df = state_df[
        (state_df['TOT_EMP'].notna()) & 
        (state_df['TOT_EMP'] > 0) &
        (state_df['A_MEDIAN'].notna())
    ].copy()
    
    logger.info(f"    With employment and wage data: {len(state_df):,} occupations")
    
    if len(state_df) == 0:
        raise ValueError(f"No occupation data found for {state_name}. Data may be incomplete.")
    
    # Create distribution table
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
    
    return occupation_dist


# =============================================================================
# DTYPE OPTIMIZATION
# =============================================================================

def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optimize data types for final distribution table to reduce SQL file size.
    Only called on small output table, not on large input data.
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
            columns.append(f"    {col} VARCHAR(200)")
        elif pd.api.types.is_integer_dtype(dtype):
            columns.append(f"    {col} INTEGER")
        elif pd.api.types.is_float_dtype(dtype):
            columns.append(f"    {col} DECIMAL(12,2)")
        else:
            columns.append(f"    {col} TEXT")
    
    lines.append(",\n".join(columns))
    lines.append(");")
    
    return "\n".join(lines)


def export_to_sql_file(occupation_dist: pd.DataFrame, state_code: str, year: int):
    """
    Export occupation distribution table to SQL file using COPY statements.
    """
    OUTPUT_DIR.mkdir(exist_ok=True)
    output_path = OUTPUT_DIR / f"bls_occupation_wages_{state_code}_{year}.sql"
    
    table_name = f"bls_occupation_wages_{state_code}_{year}"
    
    logger.info(f"  → Creating SQL file: {output_path.name}")
    
    with open(output_path, 'w') as f:
        # Write header
        f.write(f"""-- BLS OEWS Distribution Table
-- State: {state_code}
-- Year: {year}
-- Generated: {pd.Timestamp.now()}
--
-- This file contains occupation and wage distribution data from BLS OEWS
-- Import with: psql -d mydb -f {output_path.name}

BEGIN;

-- Drop existing table
DROP TABLE IF EXISTS {table_name} CASCADE;

-- Table: {table_name}
""")
        
        # CREATE TABLE
        f.write(create_table_ddl(occupation_dist, table_name))
        f.write("\n\n")
        
        # COPY data
        f.write(f"COPY {table_name} FROM stdin;\n")
        
        # Write data in tab-delimited format
        for _, row in occupation_dist.iterrows():
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

def upload_to_database(occupation_dist: pd.DataFrame, state_code: str, 
                       year: int, connection_string: str):
    """
    Upload occupation distribution table to database using COPY statements.
    All-or-nothing transaction: rollback if upload fails.
    """
    try:
        import psycopg2
    except ImportError:
        logger.error("psycopg2 not installed. Install with: pip install psycopg2-binary")
        sys.exit(1)
    
    table_name = f"bls_occupation_wages_{state_code}_{year}"
    
    logger.info(f"  → Connecting to database...")
    
    try:
        conn = psycopg2.connect(connection_string)
        cur = conn.cursor()
        
        logger.info(f"  → Connected successfully")
        logger.info(f"  → Uploading {table_name}...")
        
        # Drop and create table
        cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
        cur.execute(create_table_ddl(occupation_dist, table_name))
        
        # Use COPY for fast bulk insert
        buffer = StringIO()
        occupation_dist.to_csv(buffer, sep='\t', header=False, index=False, na_rep='\\N')
        buffer.seek(0)
        
        cur.copy_expert(f"COPY {table_name} FROM stdin", buffer)
        
        logger.info(f"    ✓ {len(occupation_dist)} rows uploaded")
        
        # Commit changes
        conn.commit()
        logger.info(f"\n✓ Table uploaded successfully")
        
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
        description='Extract BLS OEWS occupation wage distribution',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate SQL file for local import
  python extract_bls.py --state HI --year 2023 --output sql
  
  # Upload to database (using connection string)
  python extract_bls.py --state HI --year 2023 --output database --connection-string "postgresql://..."
  
  # Upload to database (using DATABASE_URL environment variable)
  export DATABASE_URL="postgresql://user:pass@host:5432/dbname"
  python extract_bls.py --state HI --year 2023 --output database
        """
    )
    
    parser.add_argument('--state', type=str, required=True,
                       help='Two-letter state code (e.g., HI, CA, TX)')
    parser.add_argument('--year', type=int, default=2023,
                       help='Year of OEWS data (default: 2023)')
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
    logger.info(f"BLS OEWS EXTRACTION: {args.state.upper()} ({args.year})")
    logger.info(f"Output mode: {args.output}")
    logger.info("="*60)
    
    try:
        # Phase 1: Download
        logger.info("\n[1/5] Downloading BLS OEWS file...")
        excel_path = download_bls_oews_file(args.year)
        
        # Phase 2: Load
        logger.info("\n[2/5] Loading data into memory...")
        oews_df = load_bls_data(excel_path)
        
        # Phase 3: Extract
        logger.info("\n[3/5] Extracting state occupation data...")
        occupation_dist = extract_state_occupations(oews_df, args.state.upper(), args.year)
        
        # Phase 4: Optimize
        logger.info("\n[4/5] Optimizing data types...")
        occupation_dist = optimize_dtypes(occupation_dist)
        logger.info("  → Data types optimized")
        
        # Phase 5: Output
        if args.output == 'sql':
            logger.info("\n[5/5] Exporting to SQL file...")
            export_to_sql_file(occupation_dist, args.state.upper(), args.year)
        else:
            logger.info("\n[5/5] Uploading to database...")
            upload_to_database(occupation_dist, args.state.upper(), args.year, conn_string)
        
        logger.info("\n" + "="*60)
        logger.info("✓ EXTRACTION COMPLETE")
        logger.info("="*60)
        logger.info(f"State: {args.state.upper()}")
        logger.info(f"Year: {args.year}")
        logger.info(f"Occupations extracted: {len(occupation_dist)}")
        
    except Exception as e:
        logger.error(f"\n✗ Extraction failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
