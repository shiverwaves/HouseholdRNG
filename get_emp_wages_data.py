#!/usr/bin/env python3
"""
OEWS State Data Download and NeonDB Storage Script

Downloads OEWS state-level data and stores in NeonDB.
Default: 2023 state data only (focused, complete state coverage)
Easy to configure for additional years as they become available.

Requirements:
- psycopg2-binary, pandas, openpyxl, requests, python-dotenv
"""

import os
import sys
import zipfile
import tempfile
import logging
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path

# CONFIGURATION - Easy to modify for different years
YEARS_TO_PROCESS = [2023]  # Default: 2023 only (complete state data including Colorado)
# To add more years: YEARS_TO_PROCESS = [2023, 2024, 2025]

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('oews_download.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class OEWSDownloader:
    def __init__(self, neon_connection_string: str):
        self.neon_connection_string = neon_connection_string
        
        # URL mapping for STATE data only - much smaller and faster
        self.urls = {
            2023: "https://www.bls.gov/oes/special-requests/oesm23st.zip",  # State data only
            # 2024: "https://www.bls.gov/oes/special-requests/oesm24st.zip",  # State data only
            # 2025: "https://www.bls.gov/oes/special-requests/oesm25st.zip",  # Add when available
        }
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
    def download_file(self, url: str, filename: str) -> bool:
        """Download file with browser headers."""
        try:
            logger.info(f"Downloading {url}")
            response = requests.get(url, headers=self.headers, stream=True, timeout=300)
            response.raise_for_status()
            
            with open(filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            size_mb = os.path.getsize(filename) / (1024 * 1024)
            logger.info(f"Downloaded {filename} ({size_mb:.1f} MB)")
            return True
            
        except Exception as e:
            logger.error(f"Download failed: {e}")
            return False
    
    def extract_files(self, zip_path: str, extract_dir: str) -> list:
        """Extract Excel files from ZIP."""
        files = []
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for file_info in zip_ref.infolist():
                    if file_info.filename.endswith(('.xlsx', '.xls')):
                        extracted = zip_ref.extract(file_info, extract_dir)
                        files.append(extracted)
                        logger.info(f"Extracted: {file_info.filename}")
        except Exception as e:
            logger.error(f"Extract failed: {e}")
        return files
    
    def create_schema(self, conn):
        """Create database schema for OEWS state data."""
        sql = """
        CREATE SCHEMA IF NOT EXISTS oews;
        DROP TABLE IF EXISTS oews.employment_wages;
        
        CREATE TABLE oews.employment_wages (
            id SERIAL PRIMARY KEY,
            year INTEGER NOT NULL,
            area_title VARCHAR(255),           -- State names (e.g., 'California', 'Texas')
            occ_code VARCHAR(20),              -- Occupation code (SOC)
            occ_title VARCHAR(500),            -- Occupation title
            tot_emp BIGINT,                    -- Total employment (BIGINT for large state numbers)
            h_mean DECIMAL(12,2),              -- Hourly mean wage
            a_mean DECIMAL(18,2),              -- Annual mean wage  
            h_median DECIMAL(12,2),            -- Hourly median wage
            a_median DECIMAL(18,2),            -- Annual median wage
            source_file VARCHAR(255),          -- Source Excel file
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX idx_oews_year_state ON oews.employment_wages(year, area_title);
        CREATE INDEX idx_oews_occupation ON oews.employment_wages(occ_code);
        CREATE INDEX idx_oews_state_occ ON oews.employment_wages(area_title, occ_code);
        """
        
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
            logger.info("Database schema created for state data")
        except Exception as e:
            logger.error(f"Schema creation failed: {e}")
            conn.rollback()
            raise
    
    def read_excel_file(self, file_path: str, year: int) -> pd.DataFrame:
        """Read and clean Excel file with robust error handling."""
        try:
            # Try different header rows to find data
            df = None
            for header_row in range(5):
                try:
                    df = pd.read_excel(file_path, header=header_row)
                    if len(df.columns) > 5 and len(df) > 10:
                        break
                except Exception as e:
                    logger.debug(f"Header row {header_row} failed: {e}")
                    continue
            
            if df is None or df.empty:
                logger.warning(f"No data found in {file_path}")
                return pd.DataFrame()
            
            # Clean column names
            df.columns = df.columns.astype(str).str.strip().str.replace('\n', ' ')
            
            # Remove completely empty rows before processing
            df = df.dropna(how='all')
            
            # Simple column mapping - look for key patterns
            column_map = {}
            for col in df.columns:
                col_lower = col.lower().strip()
                if 'area' in col_lower and 'title' in col_lower:
                    column_map['area_title'] = col
                elif 'occ_code' in col_lower or col_lower == 'occ code':
                    column_map['occ_code'] = col
                elif 'occ_title' in col_lower or 'occupation' in col_lower:
                    column_map['occ_title'] = col
                elif col_lower in ['tot_emp', 'employment']:
                    column_map['tot_emp'] = col
                elif 'h_mean' in col_lower or 'hourly mean' in col_lower:
                    column_map['h_mean'] = col
                elif 'a_mean' in col_lower or 'annual mean' in col_lower:
                    column_map['a_mean'] = col
                elif 'h_median' in col_lower or 'hourly median' in col_lower:
                    column_map['h_median'] = col
                elif 'a_median' in col_lower or 'annual median' in col_lower:
                    column_map['a_median'] = col
            
            # Create clean DataFrame with only mapped columns
            clean_df = pd.DataFrame()
            for target, source in column_map.items():
                if source in df.columns:
                    clean_df[target] = df[source]
            
            # Skip if no important columns found
            if len(clean_df.columns) < 3:
                logger.warning(f"Insufficient columns mapped in {file_path}: {list(clean_df.columns)}")
                return pd.DataFrame()
            
            # Add metadata columns
            clean_df['year'] = year
            clean_df['source_file'] = Path(file_path).name
            
            # Clean numeric data with explicit error handling
            numeric_cols = ['tot_emp', 'h_mean', 'a_mean', 'h_median', 'a_median']
            for col in numeric_cols:
                if col in clean_df.columns:
                    try:
                        # Convert to string first to handle mixed types
                        clean_df[col] = clean_df[col].astype(str)
                        # Remove non-numeric characters
                        clean_df[col] = clean_df[col].str.replace(r'[*#$,()]', '', regex=True)
                        clean_df[col] = clean_df[col].str.strip()
                        # Replace empty strings and common null indicators
                        clean_df[col] = clean_df[col].replace(['', 'nan', 'NaN', 'N/A', '-'], None)
                        # Convert to numeric
                        clean_df[col] = pd.to_numeric(clean_df[col], errors='coerce')
                        
                        # Cap extreme values to prevent database issues
                        if col == 'tot_emp':
                            clean_df[col] = clean_df[col].where(clean_df[col] <= 50_000_000, None)
                        elif 'a_' in col:  # Annual wages
                            clean_df[col] = clean_df[col].where(clean_df[col] <= 5_000_000, None)
                        elif 'h_' in col:  # Hourly wages  
                            clean_df[col] = clean_df[col].where(clean_df[col] <= 5_000, None)
                            
                    except Exception as e:
                        logger.warning(f"Error processing numeric column {col}: {e}")
                        clean_df[col] = None
            
            # Clean string data
            string_cols = ['area_title', 'occ_code', 'occ_title']
            for col in string_cols:
                if col in clean_df.columns:
                    try:
                        clean_df[col] = clean_df[col].astype(str)
                        clean_df[col] = clean_df[col].str.strip()
                        # Replace pandas string representations of null
                        clean_df[col] = clean_df[col].replace(['nan', 'None', '<NA>'], None)
                        # Replace empty strings with None
                        clean_df[col] = clean_df[col].replace('', None)
                    except Exception as e:
                        logger.warning(f"Error processing string column {col}: {e}")
            
            # Remove rows that are essentially empty (all main data columns are null)
            main_cols = ['area_title', 'occ_code', 'occ_title', 'tot_emp']
            available_main_cols = [col for col in main_cols if col in clean_df.columns]
            if available_main_cols:
                # Keep rows that have at least one non-null value in main columns
                clean_df = clean_df.dropna(subset=available_main_cols, how='all')
            
            logger.info(f"Processed {len(clean_df)} rows from {Path(file_path).name}")
            logger.debug(f"Columns found: {list(clean_df.columns)}")
            
            return clean_df
            
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")
            return pd.DataFrame()
    
    def insert_data(self, df: pd.DataFrame, conn):
        """Insert data to database with robust NaN handling."""
        if df.empty:
            return
        
        try:
            # Get columns (exclude id)
            columns = [col for col in df.columns if col != 'id']
            df_clean = df[columns].copy()
            
            # More robust NaN handling
            logger.info(f"Cleaning data for {len(df_clean)} rows...")
            
            # Replace various forms of missing data with None
            df_clean = df_clean.replace([pd.NA, pd.NaT, float('nan'), 'nan', 'NaN', ''], None)
            
            # For any remaining NaN values, replace with None explicitly
            df_clean = df_clean.where(pd.notnull(df_clean), None)
            
            # Ensure data types are correct
            for col in df_clean.columns:
                if col in ['tot_emp']:
                    # Convert employment to integer, handling NaN
                    df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
                    df_clean[col] = df_clean[col].astype('Int64')  # Nullable integer
                elif col in ['h_mean', 'a_mean', 'h_median', 'a_median']:
                    # Convert wages to float, handling NaN
                    df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
                elif col in ['area_title', 'occ_code', 'occ_title', 'source_file']:
                    # Ensure strings are properly handled
                    df_clean[col] = df_clean[col].astype(str)
                    df_clean[col] = df_clean[col].replace('nan', None)
            
            # Convert DataFrame to list of tuples for insertion
            logger.info("Converting to database format...")
            values_list = []
            for _, row in df_clean.iterrows():
                # Convert each row to tuple, handling pandas nullable types
                row_values = []
                for val in row:
                    if pd.isna(val) or val is pd.NA:
                        row_values.append(None)
                    elif isinstance(val, pd._libs.missing.NAType):
                        row_values.append(None)
                    else:
                        row_values.append(val)
                values_list.append(tuple(row_values))
            
            # Insert with execute_values
            cols_str = ', '.join(columns)
            query = f"""
                INSERT INTO oews.employment_wages ({cols_str}) 
                VALUES %s 
                ON CONFLICT DO NOTHING
            """
            
            logger.info(f"Inserting {len(values_list)} rows into database...")
            with conn.cursor() as cur:
                execute_values(cur, query, values_list, page_size=1000)
            
            conn.commit()
            logger.info(f"Successfully inserted {len(values_list):,} rows")
            
        except Exception as e:
            logger.error(f"Insert error: {e}")
            logger.error(f"DataFrame columns: {list(df.columns)}")
            logger.error(f"DataFrame shape: {df.shape}")
            logger.error(f"DataFrame dtypes:\n{df.dtypes}")
            
            # Show sample data for debugging
            logger.error("Sample DataFrame data:")
            logger.error(df.head().to_string())
            
            conn.rollback()
            raise
    
    def process_year(self, year: int, conn):
        """Download and process data for one year."""
        url = self.urls[year]
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Download
            zip_file = Path(temp_dir) / f"oews_{year}.zip"
            if not self.download_file(url, str(zip_file)):
                return False
            
            # Extract
            extract_dir = Path(temp_dir) / "extracted"
            extract_dir.mkdir()
            excel_files = self.extract_files(str(zip_file), str(extract_dir))
            
            if not excel_files:
                logger.error(f"No Excel files found for {year}")
                return False
            
            # Process each file
            total_rows = 0
            for excel_file in excel_files:
                df = self.read_excel_file(excel_file, year)
                if not df.empty:
                    self.insert_data(df, conn)
                    total_rows += len(df)
            
            logger.info(f"Year {year}: {len(excel_files)} files, {total_rows:,} rows")
            return True
    
    def run(self):
        """Main execution."""
        logger.info(f"Starting OEWS state data download for years: {YEARS_TO_PROCESS}")
        
        # Connect to database
        try:
            conn = psycopg2.connect(self.neon_connection_string)
            logger.info("Connected to database")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
        
        try:
            # Create schema
            self.create_schema(conn)
            
            # Process configured years
            for year in YEARS_TO_PROCESS:
                if year not in self.urls:
                    logger.warning(f"No URL configured for year {year}, skipping")
                    continue
                    
                logger.info(f"Processing {year} state data")
                success = self.process_year(year, conn)
                if not success:
                    logger.warning(f"Failed to process {year} state data, continuing with other years")
            
            # Summary
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT year, COUNT(*) as records, 
                           COUNT(DISTINCT area_title) as states,
                           COUNT(DISTINCT occ_code) as occupations
                    FROM oews.employment_wages 
                    WHERE year = ANY(%s)
                    GROUP BY year 
                    ORDER BY year DESC
                """, (YEARS_TO_PROCESS,))
                
                results = cur.fetchall()
                if results:
                    logger.info("=== STATE DATA SUMMARY ===")
                    for year, records, states, occs in results:
                        logger.info(f"{year}: {records:,} records, {states} states, {occs} occupations")
                else:
                    logger.warning("No state data found in database after processing")
            
            logger.info("OEWS state data processing completed!")
            return True
            
        except Exception as e:
            logger.error(f"Processing failed: {e}")
            return False
        finally:
            conn.close()

def main():
    """Main function."""
    from dotenv import load_dotenv
    load_dotenv()
    
    connection_string = os.getenv('NEON_CONNECTION_STRING')
    if not connection_string:
        logger.error("NEON_CONNECTION_STRING environment variable required")
        return
    
    downloader = OEWSDownloader(connection_string)
    success = downloader.run()
    
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()