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
    EMPLOYED = "employed"
    UNEMPLOYED = "unemployed"
    NOT_IN_LABOR_FORCE = "not_in_labor_force"


class RelationshipType(Enum):
    """Relationship to householder (PUMS RELSHIPP)"""
    HOUSEHOLDER = "householder"
    SPOUSE = "spouse"
    BIOLOGICAL_CHILD = "biological_child"
    ADOPTED_CHILD = "adopted_child"
    STEPCHILD = "stepchild"
    GRANDCHILD = "grandchild"
    PARENT = "parent"
    UNMARRIED_PARTNER = "unmarried_partner"


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
    occupation: Optional[str] = None
    
    # Income (from Stage 4)
    wage_income: int = 0
    self_employment_income: int = 0
    social_security_income: int = 0
    retirement_income: int = 0
    interest_dividend_income: int = 0
    other_income: int = 0
    
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
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return {
            "person_id": self.person_id,
            "relationship": self.relationship.value,
            "age": self.age,
            "sex": self.sex,
            "employment_status": self.employment_status.value if self.employment_status else None,
            "education_level": self.education_level,
            "occupation": self.occupation,
            "wage_income": self.wage_income,
            "self_employment_income": self.self_employment_income,
            "social_security_income": self.social_security_income,
            "retirement_income": self.retirement_income,
            "interest_dividend_income": self.interest_dividend_income,
            "other_income": self.other_income,
            "total_income": self.total_income(),
            "has_disability": self.has_disability,
            "is_student": self.is_student,
        }


@dataclass
class Household:
    """Represents a complete household for tax purposes"""
    household_id: str
    state: str
    year: int
    
    # Structure (from Stage 1)
    pattern: str
    members: List[Person] = field(default_factory=list)
    
    # Housing (from Stage 5)
    tenure: Optional[str] = None
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
    
    def get_householder(self) -> Optional[Person]:
        """Return the householder"""
        for p in self.members:
            if p.relationship == RelationshipType.HOUSEHOLDER:
                return p
        return None
    
    def total_household_income(self) -> int:
        """Calculate total household income"""
        return sum(p.total_income() for p in self.members)
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return {
            "household_id": self.household_id,
            "state": self.state,
            "year": self.year,
            "pattern": self.pattern,
            "members": [m.to_dict() for m in self.members],
            "tenure": self.tenure,
            "property_taxes": self.property_taxes,
            "mortgage_interest": self.mortgage_interest,
            "complexity_score": self.complexity_score,
            "complexity_factors": self.complexity_factors,
            "expected_adults": self.expected_adults,
            "expected_children_range": list(self.expected_children_range) if self.expected_children_range else None,
            "expected_complexity": self.expected_complexity,
            "total_household_income": self.total_household_income(),
            "adult_count": len(self.get_adults()),
            "child_count": len(self.get_children()),
        }


@dataclass
class FilingUnit:
    """
    Represents a tax filing unit within a household.
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
    requires_schedule_a: bool = False
    requires_schedule_b: bool = False
    requires_schedule_c: bool = False
    requires_schedule_se: bool = False
    
    def calculate_agi(self) -> int:
        """Calculate Adjusted Gross Income"""
        agi = self.primary_taxpayer.total_income()
        if self.spouse:
            agi += self.spouse.total_income()
        return agi
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return {
            "filing_unit_id": self.filing_unit_id,
            "filing_status": self.filing_status.value,
            "primary_taxpayer_id": self.primary_taxpayer.person_id,
            "spouse_id": self.spouse.person_id if self.spouse else None,
            "dependent_ids": [d.person_id for d in self.dependents],
            "total_wages": self.total_wages,
            "total_income": self.total_income,
            "agi": self.calculate_agi(),
        }
