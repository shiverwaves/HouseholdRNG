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

import logging
from typing import Dict, List, Optional
import uuid

import numpy as np
import pandas as pd

from .models import Household, Person, RelationshipType
from .database import DistributionLoader, get_loader
from .sampler import weighted_sample, set_random_seed

logger = logging.getLogger(__name__)


# Pattern metadata based on PUMS household classifications
# Pattern names must match those in extract_pums.py classify_household()
PATTERN_METADATA = {
    'married_couple_no_children': {
        'expected_adults': 2,
        'expected_children': (0, 0),
        'complexity': 'simple',
        'description': 'Married couple without children'
    },
    'married_couple_with_children': {
        'expected_adults': 2,
        'expected_children': (1, 5),
        'complexity': 'simple',
        'description': 'Married couple with children'
    },
    'single_parent': {
        'expected_adults': 1,
        'expected_children': (1, 4),
        'complexity': 'simple',
        'description': 'Single parent with children'
    },
    'single_adult': {
        'expected_adults': 1,
        'expected_children': (0, 0),
        'complexity': 'simple',
        'description': 'Single person living alone'
    },
    'blended_family': {
        'expected_adults': 2,
        'expected_children': (2, 5),
        'complexity': 'complex',
        'description': 'Married couple with bio and/or stepchildren'
    },
    'multigenerational': {
        'expected_adults': (2, 4),
        'expected_children': (0, 3),
        'complexity': 'complex',
        'description': '3+ generations in household'
    },
    'unmarried_partners': {
        'expected_adults': 2,
        'expected_children': (0, 3),
        'complexity': 'complex',
        'description': 'Cohabiting couple (not married)'
    },
    'other': {
        'expected_adults': (1, 5),
        'expected_children': (0, 3),
        'complexity': 'medium',
        'description': 'Other household arrangement'
    }
}


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
        
        logger.info(
            f"Initialized generator for {self.state} "
            f"(PUMS {self.pums_year}, BLS {self.bls_year})"
        )
    
    def generate_stage1(self, complexity: Optional[str] = None) -> Household:
        """
        Stage 1: Select household structure pattern.
        
        Args:
            complexity: Filter by complexity ('simple', 'medium', 'complex')
                       If None, samples from all patterns
        
        Returns:
            Household with pattern selected, members empty
        """
        patterns_df = self.distributions.get('household_patterns')
        
        if patterns_df is None or len(patterns_df) == 0:
            raise ValueError(
                f"No household patterns found for {self.state} {self.pums_year}. "
                f"Run extract_pums.py first."
            )
        
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
    
    def generate_household(
        self, 
        complexity: Optional[str] = None,
        seed: Optional[int] = None
    ) -> Household:
        """
        Generate a complete household through all stages.
        
        Currently implements Stage 1 only.
        Stages 2-7 will be added incrementally.
        
        Args:
            complexity: Filter by complexity level
            seed: Random seed for reproducibility
        
        Returns:
            Generated household
        """
        if seed is not None:
            set_random_seed(seed)
        
        # Stage 1: Structure Selection
        household = self.generate_stage1(complexity)
        
        # TODO: Stage 2: Adult Generation
        # TODO: Stage 3: Child Generation
        # TODO: Stage 4: Income Assignment
        # TODO: Stage 5: Expense Assignment
        # TODO: Stage 6: Tax Calculation
        # TODO: Stage 7: Validation & Scoring
        
        return household
    
    def generate_batch(
        self, 
        count: int, 
        complexity: Optional[str] = None,
        seed: Optional[int] = None
    ) -> List[Household]:
        """
        Generate multiple households.
        
        Args:
            count: Number of households to generate
            complexity: Filter by complexity level
            seed: Random seed for reproducibility
        
        Returns:
            List of generated households
        """
        if seed is not None:
            set_random_seed(seed)
        
        households = []
        for i in range(count):
            household = self.generate_household(complexity=complexity)
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
