#!/usr/bin/env python3
"""
IMPROVED BLS Download Function - Multiple Strategies to Avoid 403 Errors

This replacement function tries multiple approaches to download BLS data:
1. Requests with enhanced headers
2. Requests with Session (cookie handling)
3. wget fallback
4. curl fallback

Copy this function to replace the load_bls_data function in extract_bls_distributions.py
"""

import requests
import zipfile
import subprocess
import time
from io import BytesIO
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def load_bls_data(year: int) -> Path:
    """
    Download and cache BLS OEWS data with multiple fallback strategies.
    
    Tries multiple approaches to avoid 403 errors:
    1. requests with enhanced headers
    2. requests.Session with cookies
    3. wget command-line tool
    4. curl command-line tool
    
    Args:
        year: Year of data (e.g., 2023 for May 2023 data)
        
    Returns:
        Path to cached Excel file
    """
    CACHE_DIR.mkdir(exist_ok=True)
    
    year_short = str(year)[2:]  # 2023 -> "23"
    zip_url = f"https://www.bls.gov/oes/special.requests/oesm{year_short}st.zip"
    
    cached_excel = CACHE_DIR / f"oews_{year}_state_data.xlsx"
    
    if cached_excel.exists():
        logger.info(f"Using cached OEWS file: {cached_excel}")
        return cached_excel
    
    logger.info(f"Downloading OEWS data for {year}...")
    logger.info(f"URL: {zip_url}")
    
    # Strategy 1: requests with enhanced headers
    logger.info("Strategy 1: Trying requests with enhanced headers...")
    try:
        content = download_with_requests_enhanced(zip_url)
        if content:
            extract_and_cache(content, cached_excel)
            return cached_excel
    except Exception as e:
        logger.warning(f"Strategy 1 failed: {e}")
    
    # Strategy 2: requests.Session with cookies
    logger.info("Strategy 2: Trying requests.Session...")
    try:
        content = download_with_session(zip_url)
        if content:
            extract_and_cache(content, cached_excel)
            return cached_excel
    except Exception as e:
        logger.warning(f"Strategy 2 failed: {e}")
    
    # Strategy 3: wget fallback
    logger.info("Strategy 3: Trying wget...")
    try:
        temp_zip = CACHE_DIR / f"temp_{year}.zip"
        if download_with_wget(zip_url, temp_zip):
            with open(temp_zip, 'rb') as f:
                extract_and_cache(f.read(), cached_excel)
            temp_zip.unlink()  # Clean up temp file
            return cached_excel
    except Exception as e:
        logger.warning(f"Strategy 3 failed: {e}")
    
    # Strategy 4: curl fallback
    logger.info("Strategy 4: Trying curl...")
    try:
        temp_zip = CACHE_DIR / f"temp_{year}.zip"
        if download_with_curl(zip_url, temp_zip):
            with open(temp_zip, 'rb') as f:
                extract_and_cache(f.read(), cached_excel)
            temp_zip.unlink()  # Clean up temp file
            return cached_excel
    except Exception as e:
        logger.warning(f"Strategy 4 failed: {e}")
    
    # All strategies failed
    raise RuntimeError(
        f"All download strategies failed for BLS data year {year}.\n"
        f"URL: {zip_url}\n"
        f"This might be due to:\n"
        f"  1. BLS blocking automated requests\n"
        f"  2. Network issues\n"
        f"  3. Invalid year (data not yet available)\n"
        f"Try:\n"
        f"  - Running locally (not in GitHub Actions)\n"
        f"  - Checking if {year} data is available at BLS website\n"
        f"  - Downloading manually and placing in cache/bls_cache/"
    )


def download_with_requests_enhanced(url: str, max_retries: int = 3) -> bytes:
    """
    Download using requests with enhanced headers and retry logic.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Referer': 'https://www.bls.gov/oes/',
        'DNT': '1',
    }
    
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                wait_time = 2 ** attempt  # Exponential backoff: 2, 4, 8 seconds
                logger.info(f"Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
            
            response = requests.get(
                url, 
                headers=headers, 
                timeout=120, 
                allow_redirects=True,
                stream=True
            )
            response.raise_for_status()
            logger.info(f"Download successful (attempt {attempt + 1})")
            return response.content
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                logger.warning(f"403 Forbidden (attempt {attempt + 1}/{max_retries})")
                if attempt == max_retries - 1:
                    raise
            else:
                raise
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                raise
    
    raise RuntimeError("Max retries exceeded")


def download_with_session(url: str) -> bytes:
    """
    Download using requests.Session for proper cookie/connection handling.
    """
    session = requests.Session()
    
    # More realistic headers
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.bls.gov/',
        'Origin': 'https://www.bls.gov',
    })
    
    # First visit the main page to get cookies
    try:
        logger.info("Visiting BLS main page to establish session...")
        session.get('https://www.bls.gov/oes/', timeout=30)
        time.sleep(1)  # Brief delay
        
        # Now download the file
        logger.info("Downloading file with session...")
        response = session.get(url, timeout=120, allow_redirects=True)
        response.raise_for_status()
        return response.content
        
    except Exception as e:
        logger.error(f"Session download failed: {e}")
        raise
    finally:
        session.close()


def download_with_wget(url: str, output_path: Path) -> bool:
    """
    Download using wget command-line tool (fallback).
    """
    try:
        # Check if wget is available
        subprocess.run(['wget', '--version'], capture_output=True, check=True)
        
        cmd = [
            'wget',
            '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            '--timeout=120',
            '--tries=3',
            '--wait=2',
            '--no-check-certificate',  # Skip SSL verification if needed
            '-O', str(output_path),
            url
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0 and output_path.exists():
            logger.info("wget download successful")
            return True
        else:
            logger.warning(f"wget failed: {result.stderr}")
            return False
            
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logger.warning(f"wget not available or failed: {e}")
        return False


def download_with_curl(url: str, output_path: Path) -> bool:
    """
    Download using curl command-line tool (fallback).
    """
    try:
        # Check if curl is available
        subprocess.run(['curl', '--version'], capture_output=True, check=True)
        
        cmd = [
            'curl',
            '-L',  # Follow redirects
            '-A', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            '--connect-timeout', '120',
            '--max-time', '300',
            '--retry', '3',
            '--retry-delay', '2',
            '-k',  # Skip SSL verification if needed
            '-o', str(output_path),
            url
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0 and output_path.exists():
            logger.info("curl download successful")
            return True
        else:
            logger.warning(f"curl failed: {result.stderr}")
            return False
            
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logger.warning(f"curl not available or failed: {e}")
        return False


def extract_and_cache(zip_content: bytes, output_path: Path):
    """
    Extract Excel file from ZIP content and save to cache.
    """
    with zipfile.ZipFile(BytesIO(zip_content)) as zf:
        # Find Excel file in ZIP
        excel_files = [f for f in zf.namelist() if f.endswith(('.xlsx', '.xls'))]
        
        if not excel_files:
            raise ValueError("No Excel files found in ZIP")
        
        excel_filename = excel_files[0]
        logger.info(f"Extracting: {excel_filename}")
        
        # Extract to cache
        with zf.open(excel_filename) as ef:
            with open(output_path, 'wb') as out:
                out.write(ef.read())
    
    logger.info(f"Cached OEWS file: {output_path}")


# Example usage note:
# Replace the existing load_bls_data function in extract_bls_distributions.py
# with this improved version. No other changes needed!
