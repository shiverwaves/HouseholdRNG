"""
Household Generator Package

Main entry point for household generation.
"""

from .models import Household, Person, FilingUnit, FilingStatus, EmploymentStatus, RelationshipType
from .database import DistributionLoader, load_distributions
from .pipeline import HouseholdGenerator
from .sampler import weighted_sample, sample_from_bracket, parse_dollar_amount

__version__ = "1.0.0"

__all__ = [
    'HouseholdGenerator',
    'Household',
    'Person',
    'FilingUnit',
    'FilingStatus',
    'EmploymentStatus',
    'RelationshipType',
    'DistributionLoader',
    'load_distributions',
    'weighted_sample',
    'sample_from_bracket',
    'parse_dollar_amount',
]
