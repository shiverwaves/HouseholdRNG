"""
Main household generation pipeline.

Orchestrates the 7-stage generation process:
1. Structure Selection ✅
2. Adult Generation ✅
3. Child Generation (TODO)
4. Income Assignment (TODO)
5. Expense Assignment (TODO)
6. Tax Calculation (TODO)
7. Validation & Scoring (TODO)
"""

import logging
from typing import Dict, List, Optional
import uuid

import numpy as np
import pandas as pd

from .models import Household, Person, RelationshipType, PATTERN_METADATA
from .database import DistributionLoader, get_loader
from .sampler import weighted_sample, set_random_seed
from .adult_generator import AdultGenerator
from .child_generator import ChildGenerator
from .income_generator import IncomeGenerator

logger = logging.getLogger(__name__)


class HouseholdGenerator:
    """
    Main household generation pipeline.
    
    Loads distribution tables from database and generates complete
    households through a 7-stage process.
    """
    
    def __init__(
        self, 
        state: str, 
        pums_year: int, 
        bls_year: Optional[int] = None,
        connection_string: Optional[str] = None
    ):
        """
        Initialize household generator.
        
        Args:
            state: Two-letter state code (e.g., 'HI')
            pums_year: Year for PUMS data (e.g., 2023)
            bls_year: Year for BLS data (default: same as pums_year)
            connection_string: Database connection string
        """
        self.state = state.upper()
        self.pums_year = pums_year
        self.bls_year = bls_year or pums_year
        
        # Load distribution tables
        loader = get_loader(connection_string)
        self.distributions = loader.load_all_tables(
            self.state, 
            self.pums_year, 
            self.bls_year
        )
        
        # Initialize sub-generators
        self.adult_generator = AdultGenerator(self.distributions)
        self.child_generator = ChildGenerator(self.distributions)
        self.income_generator = IncomeGenerator(self.distributions)
        
        logger.info(
            f"Initialized generator for {self.state} "
            f"(PUMS {self.pums_year}, BLS {self.bls_year})"
        )
        logger.info(f"Loaded {len(self.distributions)} distribution tables")
    
    # =========================================================================
    # STAGE 1: Structure Selection
    # =========================================================================
    
    def generate_stage1(
        self, 
        complexity: Optional[str] = None,
        pattern: Optional[str] = None
    ) -> Household:
        """
        Stage 1: Select household structure pattern.
        
        Args:
            complexity: Filter by complexity ('simple', 'medium', 'complex')
                       If None, samples from all patterns
            pattern: Specific pattern to use (overrides complexity and sampling)
        
        Returns:
            Household with pattern selected, members empty
        """
        patterns_df = self.distributions.get('household_patterns')
        
        if patterns_df is None or len(patterns_df) == 0:
            raise ValueError(
                f"No household patterns found for {self.state} {self.pums_year}. "
                f"Run extract_pums.py first."
            )
        
        # If specific pattern requested, use it directly
        if pattern:
            # Verify pattern exists
            available_patterns = patterns_df['pattern'].unique()
            if pattern not in available_patterns:
                raise ValueError(
                    f"Pattern '{pattern}' not found. Available: {list(available_patterns)}"
                )
            pattern_name = pattern
        else:
            # Filter by complexity if specified
            if complexity:
                patterns_df = patterns_df.copy()
                patterns_df['complexity'] = patterns_df['pattern'].map(
                    lambda p: PATTERN_METADATA.get(p, PATTERN_METADATA['other'])['complexity']
                )
                patterns_df = patterns_df[patterns_df['complexity'] == complexity]
                
                if len(patterns_df) == 0:
                    raise ValueError(f"No patterns with complexity '{complexity}'")
            
            # Sample pattern (household_patterns table uses 'weight' column)
            pattern_row = weighted_sample(patterns_df, 'weight')
            pattern_name = pattern_row['pattern']
        
        # Get metadata
        metadata = PATTERN_METADATA.get(pattern_name, PATTERN_METADATA['other'])
        
        # Handle expected_adults as int or tuple
        expected_adults = metadata['expected_adults']
        if isinstance(expected_adults, tuple):
            expected_adults = expected_adults[0]  # Use minimum
        
        # Create household
        household = Household(
            household_id=str(uuid.uuid4()),
            state=self.state,
            year=self.pums_year,
            pattern=pattern_name,
            members=[],
            expected_adults=expected_adults,
            expected_children_range=metadata['expected_children'],
            expected_complexity=metadata['complexity']
        )
        
        logger.debug(f"Stage 1: Selected pattern '{pattern_name}'")
        return household
    
    # =========================================================================
    # STAGE 2: Adult Generation
    # =========================================================================
    
    def generate_stage2(self, household: Household) -> Household:
        """
        Stage 2: Generate adult household members.
        
        Populates household.members with adult Person objects including:
        - Demographics (age, sex, race, hispanic origin)
        - Employment status
        - Education level
        - Occupation (if employed)
        - Disability status
        
        Args:
            household: Household from Stage 1 with pattern set
        
        Returns:
            Household with adult members populated
        """
        adults = self.adult_generator.generate_adults(household)
        household.members = adults
        
        logger.debug(f"Stage 2: Generated {len(adults)} adults")
        return household
    
    # =========================================================================
    # STAGE 3: Child Generation
    # =========================================================================
    
    def generate_stage3(self, household: Household) -> Household:
        """
        Stage 3: Generate child household members.
        
        Populates household.members with child Person objects based on:
        - Pattern child count expectations
        - children_by_parent_age distribution
        - child_age_distributions table
        - Parent demographics (for race inheritance)
        
        Args:
            household: Household from Stage 2 with adults
        
        Returns:
            Household with children added to members
        """
        children = self.child_generator.generate_children(household)
        household.members.extend(children)
        
        logger.debug(f"Stage 3: Generated {len(children)} children")
        return household
    
    # =========================================================================
    # STAGE 4: Income Assignment
    # =========================================================================
    
    def generate_stage4(self, household: Household) -> Household:
        """
        Stage 4: Assign income to household members.
        
        Assigns realistic income based on:
        - Employment status and occupation (wages)
        - Age (Social Security, retirement)
        - Household income level (investment income)
        - Means-testing (public assistance)
        
        Income types:
        - Wage income (employed adults, occupation-based)
        - Self-employment income (occupation probability)
        - Unemployment income (unemployed adults)
        - Social Security (62+ or disabled)
        - Retirement income (55+)
        - Interest & dividend income (age + income correlated)
        - Other income (rare)
        - Public assistance (household-level, means-tested)
        
        Args:
            household: Household from Stage 3 with all members
        
        Returns:
            Household with income fields populated
        """
        household = self.income_generator.assign_income(household)
        
        logger.debug(f"Stage 4: Assigned income, total: ${household.total_household_income():,}")
        return household
    
    # =========================================================================
    # Full Generation Pipeline
    # =========================================================================
    
    def generate_household(
        self, 
        complexity: Optional[str] = None,
        pattern: Optional[str] = None,
        seed: Optional[int] = None
    ) -> Household:
        """
        Generate a complete household through all stages.
        
        Currently implements Stages 1-4.
        Stages 5-7 will be added incrementally.
        
        Args:
            complexity: Filter by complexity level
            pattern: Specific pattern to generate (overrides complexity)
            seed: Random seed for reproducibility
        
        Returns:
            Generated household
        """
        if seed is not None:
            set_random_seed(seed)
        
        # Stage 1: Structure Selection
        household = self.generate_stage1(complexity=complexity, pattern=pattern)
        
        # Stage 2: Adult Generation
        household = self.generate_stage2(household)
        
        # Stage 3: Child Generation
        household = self.generate_stage3(household)
        
        # Stage 4: Income Assignment
        household = self.generate_stage4(household)
        
        # TODO: Stage 5: Expense Assignment
        # TODO: Stage 6: Tax Calculation
        # TODO: Stage 7: Validation & Scoring
        
        return household
    
    def generate_batch(
        self, 
        count: int, 
        complexity: Optional[str] = None,
        pattern: Optional[str] = None,
        seed: Optional[int] = None
    ) -> List[Household]:
        """
        Generate multiple households.
        
        Args:
            count: Number of households to generate
            complexity: Filter by complexity level
            pattern: Specific pattern to generate
            seed: Random seed for reproducibility
        
        Returns:
            List of generated households
        """
        if seed is not None:
            set_random_seed(seed)
        
        households = []
        for i in range(count):
            household = self.generate_household(complexity=complexity, pattern=pattern)
            households.append(household)
        
        logger.info(f"Generated {count} households")
        return households
    
    def get_available_patterns(self) -> List[Dict]:
        """
        Get available patterns with their distribution.
        
        Returns:
            List of pattern info dictionaries
        """
        patterns_df = self.distributions.get('household_patterns')
        
        if patterns_df is None:
            return []
        
        result = []
        # household_patterns table uses 'weight' column
        total = patterns_df['weight'].sum()
        
        for _, row in patterns_df.iterrows():
            pattern = row['pattern']
            metadata = PATTERN_METADATA.get(pattern, PATTERN_METADATA['other'])
            
            result.append({
                'pattern': pattern,
                'weight': int(row['weight']),
                'percentage': round(row['weight'] / total * 100, 2),
                'complexity': metadata['complexity'],
                'description': metadata['description'],
                'expected_adults': metadata['expected_adults'],
                'expected_children': metadata['expected_children'],
            })
        
        return sorted(result, key=lambda x: -x['weight'])
