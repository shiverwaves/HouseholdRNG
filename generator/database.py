"""
Database connection and distribution table loading.

Connects to PostgreSQL and loads the distribution tables
created by extract_pums.py, extract_bls.py, and extract_derived.py.

Table Naming Conventions:
- PUMS tables: {table}_{state}_{year}  (e.g., household_patterns_hi_2023)
- BLS tables: {table}_{state}_{year}   (e.g., bls_occupation_wages_hi_2023)
- Derived tables: {table}_{state}_pums_{pums_year}_bls_{bls_year}
"""

import os
import logging
from typing import Dict, List, Optional
from functools import lru_cache

import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


class DistributionLoader:
    """
    Loads distribution tables from database.
    
    Table naming conventions:
    - PUMS: {table}_{state}_{year} (e.g., household_patterns_hi_2023)
    - BLS: {table}_{state}_{year} (e.g., bls_occupation_wages_hi_2023)
    - Derived: {table}_{state}_pums_{pums_year}_bls_{bls_year}
    """
    
    # Tables created by extract_pums.py
    PUMS_TABLES = [
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
    
    # Tables created by extract_bls.py
    BLS_TABLES = [
        'bls_occupation_wages'
    ]
    
    # Tables created by extract_derived.py
    DERIVED_TABLES = [
        'education_occupation_probabilities',
        'age_income_adjustments',
        'occupation_self_employment_probability'
    ]
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize database connection.
        
        Args:
            connection_string: PostgreSQL connection string.
                              If None, uses DATABASE_URL environment variable.
        """
        if connection_string is None:
            connection_string = os.getenv('DATABASE_URL')
            if not connection_string:
                raise ValueError(
                    "No database connection string provided. "
                    "Set DATABASE_URL environment variable or pass connection_string."
                )
        
        self.connection_string = connection_string
        self.engine = create_engine(connection_string)
        self._verify_connection()
        logger.info("Database connection established")
    
    def _verify_connection(self):
        """Verify database connection works"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception as e:
            raise RuntimeError(f"Failed to connect to database: {e}")
    
    def load_all_tables(
        self, 
        state: str, 
        pums_year: int, 
        bls_year: Optional[int] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Load all distribution tables for a state/year.
        
        Args:
            state: Two-letter state code (e.g., 'HI')
            pums_year: Year for PUMS data (e.g., 2023)
            bls_year: Year for BLS data. If None, uses pums_year
        
        Returns:
            Dictionary mapping table names to DataFrames
        """
        if bls_year is None:
            bls_year = pums_year
        
        # Use lowercase state code to match table naming convention
        state_lower = state.lower()
        
        distributions = {}
        
        # Load PUMS tables: {table}_{state}_{year}
        for table in self.PUMS_TABLES:
            full_name = f"{table}_{state_lower}_{pums_year}"
            try:
                distributions[table] = self._load_table(full_name)
                logger.debug(f"Loaded {full_name}")
            except Exception as e:
                logger.warning(f"Could not load {full_name}: {e}")
        
        # Load BLS tables: {table}_{state}_{year}
        for table in self.BLS_TABLES:
            full_name = f"{table}_{state_lower}_{bls_year}"
            try:
                distributions[table] = self._load_table(full_name)
                logger.debug(f"Loaded {full_name}")
            except Exception as e:
                logger.warning(f"Could not load {full_name}: {e}")
        
        # Load derived tables: {table}_{state}_pums_{pums_year}_bls_{bls_year}
        for table in self.DERIVED_TABLES:
            full_name = f"{table}_{state_lower}_pums_{pums_year}_bls_{bls_year}"
            try:
                distributions[table] = self._load_table(full_name)
                logger.debug(f"Loaded {full_name}")
            except Exception as e:
                logger.warning(f"Could not load {full_name}: {e}")
        
        logger.info(f"Loaded {len(distributions)} distribution tables for {state}")
        return distributions
    
    def _load_table(self, table_name: str) -> pd.DataFrame:
        """Load a single table from database"""
        return pd.read_sql_table(table_name, self.engine)
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in database"""
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names()
    
    def list_available_states(self) -> Dict[str, List[int]]:
        """
        List all available state/year combinations.
        
        Returns:
            Dict with states as keys and lists of years as values
        """
        inspector = inspect(self.engine)
        all_tables = inspector.get_table_names()
        
        states_years = {}
        
        for table in all_tables:
            if table.startswith('household_patterns_'):
                # Table format: household_patterns_{state}_{year}
                # Example: household_patterns_hi_2023
                parts = table.split('_')
                # parts = ['household', 'patterns', 'hi', '2023']
                if len(parts) >= 4:
                    state = parts[2].upper()  # Convert to uppercase for display
                    try:
                        year = int(parts[3])
                        if state not in states_years:
                            states_years[state] = []
                        if year not in states_years[state]:
                            states_years[state].append(year)
                    except ValueError:
                        continue
        
        # Sort years
        for state in states_years:
            states_years[state].sort()
        
        return states_years
    
    def get_table_count(self, state: str, year: int) -> int:
        """Count how many tables exist for a state/year"""
        inspector = inspect(self.engine)
        all_tables = inspector.get_table_names()
        
        state_lower = state.lower()
        pattern = f"_{state_lower}_{year}"
        return sum(1 for t in all_tables if pattern in t)


# Global cached loader instance
_loader_cache: Dict[str, DistributionLoader] = {}


def get_loader(connection_string: Optional[str] = None) -> DistributionLoader:
    """
    Get a cached DistributionLoader instance.
    
    Args:
        connection_string: Database connection string
    
    Returns:
        Cached DistributionLoader instance
    """
    cache_key = connection_string or os.getenv('DATABASE_URL', 'default')
    
    if cache_key not in _loader_cache:
        _loader_cache[cache_key] = DistributionLoader(connection_string)
    
    return _loader_cache[cache_key]
