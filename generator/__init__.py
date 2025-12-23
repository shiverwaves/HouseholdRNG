"""
Household Generator Package

Generates realistic synthetic households for tax preparation training.
Uses US Census PUMS and Bureau of Labor Statistics data.
"""

from .pipeline import HouseholdGenerator
from .adult_generator import AdultGenerator
from .child_generator import ChildGenerator
from .income_generator import IncomeGenerator
from .models import (
    Person, 
    Household, 
    FilingUnit,
    FilingStatus, 
    EmploymentStatus, 
    RelationshipType,
    Race,
    EducationLevel,
    PATTERN_METADATA
)
from .database import DistributionLoader, get_loader
from .sampler import (
    weighted_sample, 
    sample_from_bracket, 
    sample_age_from_bracket,
    match_age_bracket,
    get_age_bracket,
    set_random_seed
)

__version__ = "1.1.0"

__all__ = [
    # Main classes
    'HouseholdGenerator',
    'AdultGenerator',
    'ChildGenerator',
    'IncomeGenerator',
    
    # Data models
    'Person',
    'Household',
    'FilingUnit',
    
    # Enums
    'FilingStatus',
    'EmploymentStatus',
    'RelationshipType',
    'Race',
    'EducationLevel',
    
    # Constants
    'PATTERN_METADATA',
    
    # Database
    'DistributionLoader',
    'get_loader',
    
    # Sampling utilities
    'weighted_sample',
    'sample_from_bracket',
    'sample_age_from_bracket',
    'match_age_bracket',
    'get_age_bracket',
    'set_random_seed',
]
