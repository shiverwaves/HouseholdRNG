"""
Main household generation pipeline.

Orchestrates the 7-stage generation process:
1. Structure Selection
2. Adult Generation
3. Child Generation
4. Income Assignment
5. Expense Assignment
6. Tax Calculation
7. Validation & Scoring
"""

from typing import Dict, Optional
import pandas as pd

from .models import Household
from .database import DistributionLoader
from .stages.stage1_structure import select_household_structure


class HouseholdGenerator:
    """
    Main household generation pipeline.
    
    Loads distribution tables from database and generates complete
    households through a 7-stage process.
    """
    
    def __init__(self, state: str, pums_year: int, bls_year: int = None,
                 connection_string: str = None):
        """
        Initialize household generator.
        
        Args:
            state: Two-letter state code (e.g., 'HI')
            pums_year: Year for PUMS data (e.g., 2022)
            bls_year: Year for BLS data (default: same as pums_year)
            connection_string: Database connection string (default: from DATABASE_URL)
        """
        self.state = state
        self.pums_year = pums_year
        self.bls_year = bls_year or pums_year
        
        # Load all distribution tables
        loader = DistributionLoader(connection_string)
        self.distributions = loader.load_all_tables(
            state, pums_year, self.bls_year
        )
    
    def generate_stage1(self) -> Household:
        """
        Generate Stage 1: Household structure selection.
        
        Returns:
            Household with pattern selected and members list empty
        """
        household = select_household_structure(
            self.distributions,
            self.state,
            self.pums_year
        )
        return household
    
    def generate_household(self) -> Household:
        """
        Generate complete household through all 7 stages.
        
        Currently only implements Stage 1.
        Stages 2-7 will be added incrementally.
        
        Returns:
            Complete household object
        """
        # Stage 1: Structure
        household = self.generate_stage1()
        
        # TODO: Stage 2: Adults
        # household = generate_adult_members(household, self.distributions)
        
        # TODO: Stage 3: Children
        # household = generate_child_members(household, self.distributions)
        
        # TODO: Stage 4: Income
        # household = assign_occupation_and_income(household, self.distributions)
        
        # TODO: Stage 5: Expenses
        # household = assign_tax_expenses(household, self.distributions)
        
        # TODO: Stage 6: Tax calculation
        # household = calculate_tax_and_filing(household, self.distributions)
        
        # TODO: Stage 7: Validation
        # household = validate_and_score_complexity(household, self.distributions)
        
        return household
