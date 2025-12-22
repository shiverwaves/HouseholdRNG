"""
Data models for household generation.

These models represent the output of the 7-stage generation pipeline.
"""

from dataclasses import dataclass, field
from typing import List, Optional
from enum import Enum
import uuid


class FilingStatus(Enum):
    """Tax filing status options"""
    SINGLE = "single"
    MARRIED_FILING_JOINTLY = "married_filing_jointly"
    MARRIED_FILING_SEPARATELY = "married_filing_separately"
    HEAD_OF_HOUSEHOLD = "head_of_household"
    QUALIFYING_WIDOW = "qualifying_widow"


class EmploymentStatus(Enum):
    """Employment status from PUMS ESR variable"""
    EMPLOYED = "employed"                     # ESR 1-2
    UNEMPLOYED = "unemployed"                 # ESR 3
    NOT_IN_LABOR_FORCE = "not_in_labor_force" # ESR 6


class RelationshipType(Enum):
    """Relationship to householder (PUMS RELSHIPP)"""
    HOUSEHOLDER = "householder"               # RELSHIPP 20
    SPOUSE = "spouse"                         # RELSHIPP 21
    BIOLOGICAL_CHILD = "biological_child"     # RELSHIPP 22
    ADOPTED_CHILD = "adopted_child"           # RELSHIPP 23
    STEPCHILD = "stepchild"                   # RELSHIPP 24
    GRANDCHILD = "grandchild"                 # RELSHIPP 27
    PARENT = "parent"                         # RELSHIPP 26
    UNMARRIED_PARTNER = "unmarried_partner"   # RELSHIPP 33


@dataclass
class Person:
    """Represents an individual in a household"""
    person_id: str
    relationship: RelationshipType
    age: int
    sex: str  # 'M' or 'F'
    
    # Employment & Education (from Stage 2)
    employment_status: Optional[EmploymentStatus] = None
    education_level: Optional[str] = None
    occupation: Optional[str] = None  # SOC code
    
    # Income (from Stage 4)
    wage_income: int = 0
    self_employment_income: int = 0
    social_security_income: int = 0
    retirement_income: int = 0
    interest_dividend_income: int = 0
    other_income: int = 0  # Unemployment, alimony
    
    # Tax-relevant attributes
    has_disability: bool = False
    is_student: bool = False
    months_lived_in_household: int = 12
    
    # For dependency determination (Stage 6)
    can_be_claimed_as_dependent: bool = False
    provides_more_than_half_support: bool = False
    
    def total_income(self) -> int:
        """Calculate total income for this person"""
        return (
            self.wage_income +
            self.self_employment_income +
            self.social_security_income +
            self.retirement_income +
            self.interest_dividend_income +
            self.other_income
        )


@dataclass
class Household:
    """Represents a complete household for tax purposes"""
    household_id: str
    state: str
    year: int
    
    # Structure (from Stage 1)
    pattern: str  # e.g., 'married_couple_with_children'
    members: List[Person] = field(default_factory=list)
    
    # Housing (from Stage 5)
    tenure: Optional[str] = None  # 'owned_mortgage', 'owned_free', 'rented'
    property_taxes: int = 0
    mortgage_interest: int = 0
    
    # Tax filing (from Stage 6)
    filing_units: List['FilingUnit'] = field(default_factory=list)
    
    # Metadata (from Stage 7)
    complexity_score: int = 0
    complexity_factors: List[str] = field(default_factory=list)
    
    # Pattern metadata (set in Stage 1)
    expected_adults: Optional[int] = None
    expected_children_range: Optional[tuple] = None
    expected_complexity: Optional[str] = None
    
    def get_adults(self) -> List[Person]:
        """Return all adults (18+)"""
        return [p for p in self.members if p.age >= 18]
    
    def get_children(self) -> List[Person]:
        """Return all children (<18)"""
        return [p for p in self.members if p.age < 18]
    
    def get_householder(self) -> Person:
        """Return the householder"""
        return next(p for p in self.members 
                   if p.relationship == RelationshipType.HOUSEHOLDER)
    
    def total_household_income(self) -> int:
        """Calculate total household income"""
        return sum(p.total_income() for p in self.members)


@dataclass
class FilingUnit:
    """
    Represents a tax filing unit within a household.
    Some households have multiple filing units (e.g., unmarried partners).
    """
    filing_unit_id: str
    filing_status: FilingStatus
    primary_taxpayer: Person
    spouse: Optional[Person] = None
    dependents: List[Person] = field(default_factory=list)
    
    # Calculated totals (from Stage 6)
    total_wages: int = 0
    total_income: int = 0
    total_deductions: int = 0
    total_credits: int = 0
    
    # Tax forms needed
    requires_schedule_a: bool = False  # Itemized deductions
    requires_schedule_b: bool = False  # Interest/dividends
    requires_schedule_c: bool = False  # Self-employment
    requires_schedule_se: bool = False  # SE tax
    
    def calculate_agi(self) -> int:
        """Calculate Adjusted Gross Income"""
        agi = self.primary_taxpayer.total_income()
        if self.spouse:
            agi += self.spouse.total_income()
        return agi
