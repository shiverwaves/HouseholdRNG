"""
Stage 1: Household Structure Selection

Selects a household pattern from PUMS distribution and creates
the initial Household object with pattern metadata.
"""

import uuid
from typing import Dict
import pandas as pd

import sys
sys.path.append('..')
from ..models import Household
from ..sampler import weighted_sample


# Pattern metadata based on PUMS household classifications
PATTERN_METADATA = {
    'married_couple_no_children': {
        'expected_adults': 2,
        'expected_children': 0,
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
    'single_person': {
        'expected_adults': 1,
        'expected_children': 0,
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
    'unmarried_partner': {
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


def select_household_structure(
    distributions: Dict[str, pd.DataFrame],
    state: str,
    year: int
) -> Household:
    """
    Stage 1: Select household pattern from PUMS distribution.
    
    Args:
        distributions: Dictionary of distribution DataFrames
        state: Two-letter state code
        year: Year for generation
    
    Returns:
        Household object with pattern selected and metadata set
    
    Example:
        >>> distributions = load_distributions('HI', 2023)
        >>> household = select_household_structure(distributions, 'HI', 2023)
        >>> print(household.pattern)
        'married_couple_with_children'
        >>> print(household.expected_adults)
        2
    """
    # Load household patterns distribution
    patterns_df = distributions['household_patterns']
    
    if len(patterns_df) == 0:
        raise ValueError(
            f"No household patterns found for {state} {year}. "
            f"Run extract_pums.py first to create distribution tables."
        )
    
    # Sample one pattern using weighted probabilities
    pattern_row = weighted_sample(patterns_df, 'weighted_count')
    pattern_name = pattern_row['pattern']
    
    # Get metadata for this pattern
    metadata = PATTERN_METADATA.get(pattern_name, PATTERN_METADATA['other'])
    
    # Create household with pattern and metadata
    household = Household(
        household_id=str(uuid.uuid4()),
        state=state,
        year=year,
        pattern=pattern_name,
        members=[],  # Will be populated in Stages 2-3
        expected_adults=metadata['expected_adults'],
        expected_children_range=metadata['expected_children'],
        expected_complexity=metadata['complexity']
    )
    
    return household


def select_household_structure_with_complexity(
    distributions: Dict[str, pd.DataFrame],
    state: str,
    year: int,
    complexity_preference: str = 'random'
) -> Household:
    """
    Stage 1: Select household pattern with complexity filtering.
    
    Args:
        distributions: Dictionary of distribution DataFrames
        state: Two-letter state code
        year: Year for generation
        complexity_preference: 'simple', 'medium', 'complex', or 'random'
    
    Returns:
        Household object with pattern selected
    
    Example:
        >>> # Generate only complex scenarios for training
        >>> household = select_household_structure_with_complexity(
        ...     distributions, 'HI', 2023, complexity_preference='complex'
        ... )
        >>> household.expected_complexity
        'complex'
    """
    patterns_df = distributions['household_patterns'].copy()
    
    # Add complexity column based on pattern metadata
    patterns_df['complexity'] = patterns_df['pattern'].map(
        lambda p: PATTERN_METADATA.get(p, PATTERN_METADATA['other'])['complexity']
    )
    
    # Filter by complexity if specified
    if complexity_preference != 'random':
        patterns_df = patterns_df[patterns_df['complexity'] == complexity_preference]
        
        if len(patterns_df) == 0:
            raise ValueError(
                f"No patterns found with complexity '{complexity_preference}' "
                f"for {state} {year}"
            )
    
    # Sample
    pattern_row = weighted_sample(patterns_df, 'weighted_count')
    pattern_name = pattern_row['pattern']
    
    # Get metadata
    metadata = PATTERN_METADATA.get(pattern_name, PATTERN_METADATA['other'])
    
    # Create household
    household = Household(
        household_id=str(uuid.uuid4()),
        state=state,
        year=year,
        pattern=pattern_name,
        members=[],
        expected_adults=metadata['expected_adults'],
        expected_children_range=metadata['expected_children'],
        expected_complexity=metadata['complexity']
    )
    
    return household
