"""
Data models for household generation.

Defines the core data structures used throughout the generation pipeline.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Tuple
from enum import Enum


class FilingStatus(Enum):
    """IRS filing status options"""
    SINGLE = "single"
    MARRIED_FILING_JOINTLY = "married_filing_jointly"
    MARRIED_FILING_SEPARATELY = "married_filing_separately"
    HEAD_OF_HOUSEHOLD = "head_of_household"
    QUALIFYING_SURVIVING_SPOUSE = "qualifying_surviving_spouse"


class EmploymentStatus(Enum):
    """Employment status categories"""
    EMPLOYED = "employed"
    UNEMPLOYED = "unemployed"
    NOT_IN_LABOR_FORCE = "not_in_labor_force"


class RelationshipType(Enum):
    """Relationship to householder (matches PUMS RELSHIPP codes conceptually)"""
    HOUSEHOLDER = "householder"
    SPOUSE = "spouse"
    UNMARRIED_PARTNER = "unmarried_partner"
    BIOLOGICAL_CHILD = "biological_child"
    ADOPTED_CHILD = "adopted_child"
    STEPCHILD = "stepchild"
    GRANDCHILD = "grandchild"
    PARENT = "parent"
    SIBLING = "sibling"
    OTHER_RELATIVE = "other_relative"
    ROOMMATE = "roommate"
    OTHER_NONRELATIVE = "other_nonrelative"


class Race(Enum):
    """Race categories (matches PUMS RAC1P)"""
    WHITE = "white"
    BLACK = "black"
    AMERICAN_INDIAN = "american_indian"
    ALASKA_NATIVE = "alaska_native"
    AMERICAN_INDIAN_ALASKA_NATIVE = "american_indian_alaska_native"
    ASIAN = "asian"
    NATIVE_HAWAIIAN_PACIFIC_ISLANDER = "native_hawaiian_pacific_islander"
    OTHER = "other"
    TWO_OR_MORE = "two_or_more"


class EducationLevel(Enum):
    """Education level categories"""
    LESS_THAN_HS = "less_than_hs"
    HIGH_SCHOOL = "high_school"
    SOME_COLLEGE = "some_college"
    ASSOCIATES = "associates"
    BACHELORS = "bachelors"
    MASTERS = "masters"
    PROFESSIONAL = "professional"
    DOCTORATE = "doctorate"


@dataclass
class Person:
    """
    Represents an individual in a household.
    
    Built incrementally through stages:
    - Stage 2: Demographics (age, sex, race, employment, education, occupation)
    - Stage 4: Income (wages, self-employment, SS, retirement, etc.)
    """
    person_id: str
    relationship: RelationshipType
    
    # Demographics (Stage 2)
    age: int = 0
    sex: str = ""  # "M" or "F"
    race: str = ""  # Race category from Race enum
    hispanic_origin: bool = False
    
    # Employment & Education (Stage 2)
    employment_status: str = ""  # EmploymentStatus value
    education: str = ""  # EducationLevel value
    occupation_code: Optional[str] = None  # Full SOC code (e.g., "29-1141")
    occupation_title: Optional[str] = None  # Human-readable title
    has_disability: bool = False
    
    # Income (Stage 4)
    wage_income: int = 0
    self_employment_income: int = 0
    social_security_income: int = 0
    retirement_income: int = 0
    interest_income: int = 0
    dividend_income: int = 0
    other_income: int = 0
    public_assistance_income: int = 0
    
    # Tax-related (Stage 6)
    is_dependent: bool = False
    can_be_claimed: bool = False
    
    # Expenses (Stage 5) - person-level
    student_loan_interest: int = 0
    educator_expenses: int = 0
    ira_contributions: int = 0
    
    def total_income(self) -> int:
        """Calculate total income from all sources"""
        return (
            self.wage_income +
            self.self_employment_income +
            self.social_security_income +
            self.retirement_income +
            self.interest_income +
            self.dividend_income +
            self.other_income +
            self.public_assistance_income
        )
    
    def is_adult(self) -> bool:
        """Check if person is an adult (18+)"""
        return self.age >= 18
    
    def is_child(self) -> bool:
        """Check if person is a child (<18)"""
        return self.age < 18
    
    def is_employed(self) -> bool:
        """Check if person is employed"""
        return self.employment_status == EmploymentStatus.EMPLOYED.value
    
    def is_senior(self) -> bool:
        """Check if person is 65+"""
        return self.age >= 65
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return {
            'person_id': self.person_id,
            'relationship': self.relationship.value if isinstance(self.relationship, RelationshipType) else self.relationship,
            'age': self.age,
            'sex': self.sex,
            'race': self.race,
            'hispanic_origin': self.hispanic_origin,
            'employment_status': self.employment_status,
            'education': self.education,
            'occupation_code': self.occupation_code,
            'occupation_title': self.occupation_title,
            'has_disability': self.has_disability,
            'wage_income': self.wage_income,
            'self_employment_income': self.self_employment_income,
            'social_security_income': self.social_security_income,
            'retirement_income': self.retirement_income,
            'interest_income': self.interest_income,
            'dividend_income': self.dividend_income,
            'other_income': self.other_income,
            'public_assistance_income': self.public_assistance_income,
            'total_income': self.total_income(),
            'is_adult': self.is_adult(),
            'is_dependent': self.is_dependent,
            # Expenses (Stage 5)
            'student_loan_interest': self.student_loan_interest,
            'educator_expenses': self.educator_expenses,
            'ira_contributions': self.ira_contributions,
        }


@dataclass
class Household:
    """
    Represents a complete household with all members.
    
    Built incrementally through stages:
    - Stage 1: Pattern selection
    - Stage 2: Adult generation
    - Stage 3: Child generation
    - Stage 4: Income assignment
    - Stage 5: Expense assignment
    - Stage 6: Tax calculation
    - Stage 7: Validation
    """
    household_id: str
    state: str
    year: int
    pattern: str
    members: List[Person] = field(default_factory=list)
    
    # Pattern metadata (Stage 1)
    expected_adults: Optional[int] = None
    expected_children_range: Optional[Tuple[int, int]] = None
    expected_complexity: Optional[str] = None
    multigenerational_subpattern: Optional[str] = None  # For multigenerational households
    
    # Household-level expenses (Stage 5)
    property_taxes: int = 0
    mortgage_interest: int = 0
    state_income_tax: int = 0
    medical_expenses: int = 0
    charitable_contributions: int = 0
    
    # Above-the-line deductions (Stage 5) - household totals
    student_loan_interest: int = 0
    educator_expenses: int = 0
    ira_contributions: int = 0
    
    # Credit-related expenses (Stage 5)
    child_care_expenses: int = 0
    education_expenses: int = 0
    
    # Calculated totals (Stage 5)
    total_itemized_deductions: int = 0
    total_above_line_deductions: int = 0
    
    # Validation (Stage 7)
    validation_score: Optional[float] = None
    validation_notes: List[str] = field(default_factory=list)
    
    def get_adults(self) -> List[Person]:
        """Get all adult members (18+)"""
        return [p for p in self.members if p.is_adult()]
    
    def get_children(self) -> List[Person]:
        """Get all child members (<18)"""
        return [p for p in self.members if p.is_child()]
    
    def get_householder(self) -> Optional[Person]:
        """Get the householder"""
        for p in self.members:
            if p.relationship == RelationshipType.HOUSEHOLDER:
                return p
        return None
    
    def get_spouse(self) -> Optional[Person]:
        """Get the spouse if present"""
        for p in self.members:
            if p.relationship == RelationshipType.SPOUSE:
                return p
        return None
    
    def get_partner(self) -> Optional[Person]:
        """Get unmarried partner if present"""
        for p in self.members:
            if p.relationship == RelationshipType.UNMARRIED_PARTNER:
                return p
        return None
    
    def total_household_income(self) -> int:
        """Calculate total household income"""
        return sum(p.total_income() for p in self.members)
    
    def adult_count(self) -> int:
        """Count adults"""
        return len(self.get_adults())
    
    def child_count(self) -> int:
        """Count children"""
        return len(self.get_children())
    
    def is_married(self) -> bool:
        """Check if household has married couple"""
        return self.get_spouse() is not None
    
    def has_dependents(self) -> bool:
        """Check if household has potential dependents"""
        return len(self.get_children()) > 0
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return {
            'household_id': self.household_id,
            'state': self.state,
            'year': self.year,
            'pattern': self.pattern,
            'multigenerational_subpattern': self.multigenerational_subpattern,
            'expected_adults': self.expected_adults,
            'expected_children_range': list(self.expected_children_range) if self.expected_children_range else None,
            'expected_complexity': self.expected_complexity,
            'members': [m.to_dict() for m in self.members],
            'adult_count': self.adult_count(),
            'child_count': self.child_count(),
            'total_household_income': self.total_household_income(),
            'is_married': self.is_married(),
            # Expenses (Stage 5)
            'property_taxes': self.property_taxes,
            'mortgage_interest': self.mortgage_interest,
            'state_income_tax': self.state_income_tax,
            'medical_expenses': self.medical_expenses,
            'charitable_contributions': self.charitable_contributions,
            'student_loan_interest': self.student_loan_interest,
            'educator_expenses': self.educator_expenses,
            'ira_contributions': self.ira_contributions,
            'child_care_expenses': self.child_care_expenses,
            'education_expenses': self.education_expenses,
            'total_itemized_deductions': self.total_itemized_deductions,
            'total_above_line_deductions': self.total_above_line_deductions,
            # Validation
            'validation_score': self.validation_score
        }


@dataclass  
class FilingUnit:
    """
    Represents a tax filing unit within a household.
    
    A household may contain multiple filing units:
    - Married couple filing jointly
    - Single parent filing as HOH
    - Adult child filing separately
    """
    filing_unit_id: str
    household_id: str
    filing_status: FilingStatus
    
    # Filers
    primary_filer: Person = None
    spouse_filer: Optional[Person] = None
    
    # Dependents claimed on this return
    dependents: List[Person] = field(default_factory=list)
    
    # Calculated values (Stage 6)
    adjusted_gross_income: int = 0
    taxable_income: int = 0
    total_tax: int = 0
    refund_or_owed: int = 0
    
    def total_filer_income(self) -> int:
        """Total income of filers (not dependents)"""
        total = self.primary_filer.total_income() if self.primary_filer else 0
        if self.spouse_filer:
            total += self.spouse_filer.total_income()
        return total


# Pattern metadata for household generation
PATTERN_METADATA = {
    'married_couple_no_children': {
        'expected_adults': 2,
        'expected_children': (0, 0),
        'complexity': 'simple',
        'description': 'Married couple without children',
        'relationships': [RelationshipType.HOUSEHOLDER, RelationshipType.SPOUSE]
    },
    'married_couple_with_children': {
        'expected_adults': 2,
        'expected_children': (1, 5),
        'complexity': 'simple',
        'description': 'Married couple with children',
        'relationships': [RelationshipType.HOUSEHOLDER, RelationshipType.SPOUSE]
    },
    'single_parent': {
        'expected_adults': 1,
        'expected_children': (1, 4),
        'complexity': 'simple',
        'description': 'Single parent with children',
        'relationships': [RelationshipType.HOUSEHOLDER]
    },
    'single_adult': {
        'expected_adults': 1,
        'expected_children': (0, 0),
        'complexity': 'simple',
        'description': 'Single person living alone',
        'relationships': [RelationshipType.HOUSEHOLDER]
    },
    'blended_family': {
        'expected_adults': 2,
        'expected_children': (2, 5),
        'complexity': 'complex',
        'description': 'Married couple with bio and/or stepchildren',
        'relationships': [RelationshipType.HOUSEHOLDER, RelationshipType.SPOUSE]
    },
    'multigenerational': {
        'expected_adults': (2, 4),
        'expected_children': (0, 3),
        'complexity': 'complex',
        'description': '3+ generations in household',
        'relationships': [RelationshipType.HOUSEHOLDER]  # Others added dynamically
    },
    'unmarried_partners': {
        'expected_adults': 2,
        'expected_children': (0, 3),
        'complexity': 'complex',
        'description': 'Cohabiting couple (not married)',
        'relationships': [RelationshipType.HOUSEHOLDER, RelationshipType.UNMARRIED_PARTNER]
    },
    'other': {
        'expected_adults': (1, 5),
        'expected_children': (0, 3),
        'complexity': 'medium',
        'description': 'Other household arrangement',
        'relationships': [RelationshipType.HOUSEHOLDER]
    }
}
