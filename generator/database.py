"""
Database connection and distribution table loading.

Connects to Neon PostgreSQL and loads the 20 distribution tables
created by extract_pums.py, extract_bls.py, and extract_derived.py.
"""

import os
from typing import Dict
import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine


class DistributionLoader:
    """
    Loads distribution tables from database.
    
    Tables follow naming convention: {table}_{STATE}_{YEAR}
    Example: household_patterns_HI_2023
    """
    
    # Tables created by extract_pums.py (17 tables)
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
    
    # Table created by extract_bls.py (1 table)
    BLS_TABLES = [
        'bls_occupation_wages'
    ]
    
    # Tables created by extract_derived.py (3 tables)
    # Note: These use different naming with both PUMS and BLS years
    DERIVED_TABLES = [
        'education_occupation_probabilities',
        'age_income_adjustments',
        'occupation_self_employment_probability'
    ]
    
    def __init__(self, connection_string: str = None):
        """
        Initialize database connection.
        
        Args:
            connection_string: PostgreSQL connection string
                              If None, uses DATABASE_URL environment variable
        """
        if connection_string is None:
            connection_string = os.getenv('DATABASE_URL')
            if not connection_string:
                raise ValueError(
                    "No database connection string provided. "
                    "Set DATABASE_URL environment variable or pass connection_string."
                )
        
        self.engine = create_engine(connection_string)
        self._verify_connection()
    
    def _verify_connection(self):
        """Verify database connection works"""
        try:
            with self.engine.connect() as conn:
                conn.execute("SELECT 1")
        except Exception as e:
            raise RuntimeError(f"Failed to connect to database: {e}")
    
    def load_all_tables(self, state: str, pums_year: int, bls_year: int = None) -> Dict[str, pd.DataFrame]:
        """
        Load all distribution tables for a state/year.
        
        Args:
            state: Two-letter state code (e.g., 'HI')
            pums_year: Year for PUMS data (e.g., 2022)
            bls_year: Year for BLS data (e.g., 2023). If None, uses pums_year
        
        Returns:
            Dictionary mapping table names to DataFrames
        """
        if bls_year is None:
            bls_year = pums_year
        
        distributions = {}
        
        # Load PUMS tables
        for table in self.PUMS_TABLES:
            full_table_name = f"{table}_{state}_{pums_year}"
            distributions[table] = self._load_table(full_table_name)
        
        # Load BLS table
        for table in self.BLS_TABLES:
            full_table_name = f"{table}_{state}_{bls_year}"
            distributions[table] = self._load_table(full_table_name)
        
        # Load derived tables (special naming)
        for table in self.DERIVED_TABLES:
            full_table_name = f"{table}_{state}_pums_{pums_year}_bls_{bls_year}"
            distributions[table] = self._load_table(full_table_name)
        
        return distributions
    
    def _load_table(self, table_name: str) -> pd.DataFrame:
        """
        Load a single table from database.
        
        Args:
            table_name: Full table name (e.g., 'household_patterns_HI_2023')
        
        Returns:
            DataFrame with table contents
        """
        try:
            df = pd.read_sql_table(table_name, self.engine)
            return df
        except Exception as e:
            raise RuntimeError(
                f"Failed to load table '{table_name}': {e}\n"
                f"Make sure extraction scripts have been run for this state/year."
            )
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in database"""
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names()
    
    def list_available_states_years(self) -> Dict[str, list]:
        """
        List all available state/year combinations in database.
        
        Returns:
            Dict with states as keys and lists of years as values
        """
        inspector = inspect(self.engine)
        all_tables = inspector.get_table_names()
        
        # Parse table names to extract state/year
        states_years = {}
        
        for table in all_tables:
            # Look for household_patterns tables (one per state/year)
            if table.startswith('household_patterns_'):
                parts = table.split('_')
                if len(parts) >= 3:
                    state = parts[2]
                    year = int(parts[3]) if len(parts) > 3 else None
                    
                    if state not in states_years:
                        states_years[state] = []
                    if year and year not in states_years[state]:
                        states_years[state].append(year)
        
        return states_years


# Convenience function
def load_distributions(state: str, pums_year: int, bls_year: int = None,
                      connection_string: str = None) -> Dict[str, pd.DataFrame]:
    """
    Convenience function to load all distribution tables.
    
    Example:
        distributions = load_distributions('HI', 2022, 2023)
        patterns = distributions['household_patterns']
    """
    loader = DistributionLoader(connection_string)
    return loader.load_all_tables(state, pums_year, bls_year)
