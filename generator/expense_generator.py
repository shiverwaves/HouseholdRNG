"""
Expense generation logic for Stage 5.

Assigns realistic expenses to households for tax calculation:
- Itemized deductions (property taxes, mortgage interest, medical, charitable)
- Above-the-line deductions (student loan interest, educator expenses, IRA)
- Credit-related expenses (child care, education)

These expenses feed into Stage 6 tax calculation.
"""

import logging
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .models import Person, Household, EmploymentStatus
from .sampler import weighted_sample

logger = logging.getLogger(__name__)


# =============================================================================
# HAWAII STATE TAX BRACKETS (2023)
# =============================================================================

# Single filer brackets
HAWAII_TAX_BRACKETS_SINGLE = [
    (2400, 0.014),
    (4800, 0.032),
    (9600, 0.055),
    (14400, 0.064),
    (19200, 0.068),
    (24000, 0.072),
    (36000, 0.076),
    (48000, 0.079),
    (150000, 0.0825),
    (175000, 0.09),
    (200000, 0.10),
    (float('inf'), 0.11)
]

# Married filing jointly brackets
HAWAII_TAX_BRACKETS_MFJ = [
    (4800, 0.014),
    (9600, 0.032),
    (19200, 0.055),
    (28800, 0.064),
    (38400, 0.068),
    (48000, 0.072),
    (72000, 0.076),
    (96000, 0.079),
    (300000, 0.0825),
    (350000, 0.09),
    (400000, 0.10),
    (float('inf'), 0.11)
]

# Standard deductions (2023)
STANDARD_DEDUCTION = {
    'single': 13850,
    'married_filing_jointly': 27700,
    'married_filing_separately': 13850,
    'head_of_household': 20800,
}

# Expense caps
IRA_CONTRIBUTION_LIMIT = 6500
IRA_CONTRIBUTION_LIMIT_50_PLUS = 7500
STUDENT_LOAN_INTEREST_LIMIT = 2500
EDUCATOR_EXPENSE_LIMIT = 300
SALT_CAP = 10000  # State and Local Tax cap


class ExpenseGenerator:
    """
    Assigns expenses to household members for tax purposes.
    
    Uses distribution tables for:
    - Property taxes (by income bracket)
    - Mortgage interest (by income bracket)
    
    Derives other expenses from demographics and income.
    """
    
    def __init__(self, distributions: Dict[str, pd.DataFrame], state: str = 'HI'):
        """
        Initialize with loaded distribution tables.
        
        Args:
            distributions: Dictionary of DataFrames from DistributionLoader
            state: State code for tax calculations
        """
        self.distributions = distributions
        self.state = state.upper()
        self._log_available_tables()
    
    def _log_available_tables(self):
        """Log which expense tables are available"""
        expense_tables = [
            'property_taxes',
            'mortgage_interest',
            'homeownership_rates',
        ]
        
        available = [t for t in expense_tables if t in self.distributions]
        missing = [t for t in expense_tables if t not in self.distributions]
        
        logger.info(f"Expense tables available: {available}")
        if missing:
            logger.info(f"Expense tables missing (will use fallbacks): {missing}")
    
    def assign_expenses(self, household: Household) -> Household:
        """
        Assign all expense types to household.
        
        Args:
            household: Household with income from Stage 4
        
        Returns:
            Household with expense fields populated
        """
        logger.info(f"Stage 5 starting for household {household.household_id}, income=${household.total_household_income():,}")
        
        # 5.1 Housing expenses (property taxes, mortgage interest)
        self._assign_housing_expenses(household)
        logger.info(f"  Housing: property_taxes=${household.property_taxes}, mortgage=${household.mortgage_interest}")
        
        # 5.2 State income tax
        self._assign_state_income_tax(household)
        logger.info(f"  State tax: ${household.state_income_tax}")
        
        # 5.3 Medical expenses
        self._assign_medical_expenses(household)
        
        # 5.4 Charitable contributions
        self._assign_charitable_contributions(household)
        logger.info(f"  Charitable: ${household.charitable_contributions}")
        
        # 5.5 Above-the-line deductions
        self._assign_above_line_deductions(household)
        
        # 5.6 Credit-related expenses
        self._assign_credit_expenses(household)
        
        # Calculate totals
        self._calculate_totals(household)
        
        logger.info(f"Stage 5 complete: itemized=${household.total_itemized_deductions:,}, "
                    f"above-line=${household.total_above_line_deductions:,}")
        
        return household
    
    # =========================================================================
    # 5.1 HOUSING EXPENSES
    # =========================================================================
    
    def _assign_housing_expenses(self, household: Household):
        """
        Assign property taxes and mortgage interest based on homeownership.
        
        Homeownership probability based on age and income.
        """
        # Determine homeownership
        is_homeowner = self._determine_homeownership(household)
        
        if not is_homeowner:
            household.property_taxes = 0
            household.mortgage_interest = 0
            return
        
        # Assign property taxes
        household.property_taxes = self._sample_property_taxes(household)
        
        # Assign mortgage interest
        household.mortgage_interest = self._sample_mortgage_interest(household)
    
    def _determine_homeownership(self, household: Household) -> bool:
        """
        Determine if household owns home based on demographics.
        
        Uses homeownership_rates table from PUMS if available,
        otherwise falls back to estimated probabilities.
        
        Returns True if owner (with or without mortgage).
        """
        householder = household.get_householder()
        if not householder:
            return False
        
        age = householder.age
        income = household.total_household_income()
        
        # Try to use PUMS homeownership data
        homeownership_dist = self.distributions.get('homeownership_rates')
        
        if homeownership_dist is not None and len(homeownership_dist) > 0:
            return self._sample_homeownership_from_data(age, income, homeownership_dist)
        
        # Fallback to estimated probabilities if no data
        return self._estimate_homeownership_probability(age, income, household)
    
    def _sample_homeownership_from_data(self, age: int, income: int, dist: pd.DataFrame) -> bool:
        """
        Sample homeownership status from PUMS distribution data.
        """
        # Find age bracket
        age_bracket = self._get_age_bracket_for_homeownership(age)
        
        # Find income bracket  
        income_bracket = self._get_income_bracket_for_homeownership(income)
        
        # Filter to matching brackets
        filtered = dist[
            (dist['age_bracket'] == age_bracket) & 
            (dist['income_bracket'] == income_bracket)
        ]
        
        # If no exact match, try just income bracket
        if len(filtered) == 0:
            filtered = dist[dist['income_bracket'] == income_bracket]
        
        # If still no match, try just age bracket
        if len(filtered) == 0:
            filtered = dist[dist['age_bracket'] == age_bracket]
        
        # If still no match, use all data
        if len(filtered) == 0:
            filtered = dist
        
        if len(filtered) == 0:
            return self._estimate_homeownership_probability(age, income, None)
        
        # Calculate owner probability from the filtered data
        owner_rows = filtered[filtered['tenure'].isin(['owner_with_mortgage', 'owner_free_clear'])]
        total_weight = filtered['weighted_count'].sum()
        owner_weight = owner_rows['weighted_count'].sum()
        
        if total_weight == 0:
            return False
        
        owner_probability = owner_weight / total_weight
        
        return np.random.random() < owner_probability
    
    def _get_age_bracket_for_homeownership(self, age: int) -> str:
        """Map age to bracket string matching PUMS extraction"""
        if age < 25:
            return '<25'
        elif age < 35:
            return '25-34'
        elif age < 45:
            return '35-44'
        elif age < 55:
            return '45-54'
        elif age < 65:
            return '55-64'
        else:
            return '65+'
    
    def _get_income_bracket_for_homeownership(self, income: int) -> str:
        """Map income to bracket string matching PUMS extraction"""
        if income < 25000:
            return '<$25K'
        elif income < 50000:
            return '$25-50K'
        elif income < 75000:
            return '$50-75K'
        elif income < 100000:
            return '$75-100K'
        elif income < 150000:
            return '$100-150K'
        else:
            return '$150K+'
    
    def _estimate_homeownership_probability(self, age: int, income: int, household: Optional[Household]) -> bool:
        """
        Fallback estimation when PUMS data not available.
        Based on Census Bureau published statistics.
        """
        # Base probability by age (Census 2018 data)
        if age < 25:
            base_prob = 0.25
        elif age < 35:
            base_prob = 0.37
        elif age < 45:
            base_prob = 0.55
        elif age < 55:
            base_prob = 0.65
        elif age < 65:
            base_prob = 0.70
        else:
            base_prob = 0.78
        
        # Adjust for income
        if income < 25000:
            base_prob *= 0.6
        elif income < 50000:
            base_prob *= 0.8
        elif income < 100000:
            base_prob *= 1.0
        elif income < 150000:
            base_prob *= 1.1
        else:
            base_prob *= 1.15
        
        # Hawaii has lower homeownership rate (~60% vs national 66%)
        if self.state == 'HI':
            base_prob *= 0.91
        
        # Cap at reasonable maximum
        prob = min(0.90, base_prob)
        
        return np.random.random() < prob
    
    def _sample_property_taxes(self, household: Household) -> int:
        """Sample property taxes from distribution based on income bracket"""
        prop_tax_dist = self.distributions.get('property_taxes')
        income = household.total_household_income()
        
        if prop_tax_dist is None or len(prop_tax_dist) == 0:
            # Fallback: estimate based on income
            # Hawaii median property tax ~$1,800 on median home
            if income < 50000:
                return int(np.random.uniform(1000, 2500))
            elif income < 100000:
                return int(np.random.uniform(2000, 4500))
            elif income < 200000:
                return int(np.random.uniform(3500, 7000))
            else:
                return int(np.random.uniform(5000, 12000))
        
        # Find matching income bracket
        bracket = self._get_income_bracket(income, prop_tax_dist)
        
        if bracket is None:
            return int(np.random.uniform(2000, 5000))
        
        filtered = prop_tax_dist[prop_tax_dist['income_bracket'] == bracket]
        
        if len(filtered) == 0:
            return int(np.random.uniform(2000, 5000))
        
        row = filtered.iloc[0]
        mean_amount = float(row.get('mean_amount', 3000))
        
        # Add variation
        amount = int(np.random.normal(mean_amount, mean_amount * 0.25))
        return max(500, amount)  # Minimum $500
    
    def _sample_mortgage_interest(self, household: Household) -> int:
        """Sample mortgage interest from distribution"""
        mortgage_dist = self.distributions.get('mortgage_interest')
        income = household.total_household_income()
        householder = household.get_householder()
        
        # Older homeowners more likely to have paid off mortgage
        if householder and householder.age >= 65:
            if np.random.random() < 0.40:  # 40% of 65+ have no mortgage
                return 0
        
        if mortgage_dist is None or len(mortgage_dist) == 0:
            # Fallback: estimate based on income
            # Hawaii has high housing costs
            if income < 50000:
                return int(np.random.uniform(3000, 8000))
            elif income < 100000:
                return int(np.random.uniform(6000, 15000))
            elif income < 200000:
                return int(np.random.uniform(10000, 25000))
            else:
                return int(np.random.uniform(15000, 35000))
        
        # Find matching income bracket
        bracket = self._get_income_bracket(income, mortgage_dist)
        
        if bracket is None:
            return int(np.random.uniform(5000, 15000))
        
        filtered = mortgage_dist[mortgage_dist['income_bracket'] == bracket]
        
        if len(filtered) == 0:
            return int(np.random.uniform(5000, 15000))
        
        row = filtered.iloc[0]
        mean_amount = float(row.get('mean_amount', 10000))
        
        # Add variation
        amount = int(np.random.normal(mean_amount, mean_amount * 0.30))
        return max(0, amount)
    
    # =========================================================================
    # 5.2 STATE INCOME TAX
    # =========================================================================
    
    def _assign_state_income_tax(self, household: Household):
        """
        Calculate state income tax based on income and filing status.
        """
        income = household.total_household_income()
        
        # Determine likely filing status from household pattern
        if household.pattern in ['married_couple_with_children', 'married_couple_no_children']:
            brackets = HAWAII_TAX_BRACKETS_MFJ
        else:
            brackets = HAWAII_TAX_BRACKETS_SINGLE
        
        # Calculate progressive tax
        household.state_income_tax = self._calculate_progressive_tax(income, brackets)
    
    def _calculate_progressive_tax(self, income: int, brackets: List[Tuple[float, float]]) -> int:
        """Calculate tax using progressive brackets"""
        tax = 0
        prev_bracket = 0
        
        for bracket_max, rate in brackets:
            if income <= prev_bracket:
                break
            
            taxable_in_bracket = min(income, bracket_max) - prev_bracket
            tax += taxable_in_bracket * rate
            prev_bracket = bracket_max
        
        return int(tax)
    
    # =========================================================================
    # 5.3 MEDICAL EXPENSES
    # =========================================================================
    
    def _assign_medical_expenses(self, household: Household):
        """
        Assign medical expenses (only significant if likely to exceed 7.5% AGI floor).
        """
        # Check for elderly or disabled members (higher medical costs)
        has_elderly = any(m.age >= 65 for m in household.members)
        has_disabled = any(m.has_disability for m in household.members)
        member_count = len(household.members)
        
        # Probability of significant medical expenses
        prob = 0.10  # Base 10%
        if has_elderly:
            prob += 0.25
        if has_disabled:
            prob += 0.20
        if member_count >= 4:
            prob += 0.10
        
        if np.random.random() >= prob:
            household.medical_expenses = 0
            return
        
        # Calculate 7.5% AGI floor
        agi = household.total_household_income()
        floor = agi * 0.075
        
        # Generate amount that exceeds floor (to be deductible)
        # Exponential distribution for varying severity
        excess = np.random.exponential(5000)
        
        # Total medical expenses (floor + excess)
        household.medical_expenses = int(floor + excess)
    
    # =========================================================================
    # 5.4 CHARITABLE CONTRIBUTIONS
    # =========================================================================
    
    def _assign_charitable_contributions(self, household: Household):
        """
        Assign charitable contributions based on income.
        """
        income = household.total_household_income()
        
        # ~65% of households give something
        if np.random.random() >= 0.65:
            household.charitable_contributions = 0
            return
        
        # Giving rate varies by income
        if income < 30000:
            # Lower income: smaller amounts but sometimes higher % of income
            rate = np.random.uniform(0.005, 0.02)
        elif income < 75000:
            rate = np.random.uniform(0.01, 0.025)
        elif income < 150000:
            rate = np.random.uniform(0.015, 0.035)
        else:
            # Higher income: wider range, some very generous
            rate = np.random.uniform(0.02, 0.06)
        
        amount = int(income * rate)
        
        # Add some randomness for occasional larger gifts
        if np.random.random() < 0.05:  # 5% chance of larger gift
            amount = int(amount * np.random.uniform(1.5, 3.0))
        
        # Cap at 60% of AGI (IRS limit)
        max_amount = int(income * 0.60)
        household.charitable_contributions = min(amount, max_amount)
    
    # =========================================================================
    # 5.5 ABOVE-THE-LINE DEDUCTIONS
    # =========================================================================
    
    def _assign_above_line_deductions(self, household: Household):
        """Assign above-the-line deductions to eligible members"""
        
        for person in household.members:
            if not person.is_adult():
                continue
            
            # Student loan interest
            person.student_loan_interest = self._calculate_student_loan_interest(person)
            
            # Educator expenses
            person.educator_expenses = self._calculate_educator_expenses(person)
            
            # IRA contributions
            person.ira_contributions = self._calculate_ira_contributions(person)
        
        # Calculate household totals
        household.student_loan_interest = sum(
            p.student_loan_interest for p in household.members
        )
        household.educator_expenses = sum(
            p.educator_expenses for p in household.members
        )
        household.ira_contributions = sum(
            p.ira_contributions for p in household.members
        )
    
    def _calculate_student_loan_interest(self, person: Person) -> int:
        """
        Calculate student loan interest for a person.
        
        More likely for ages 22-45 with higher education.
        """
        # Age check
        if person.age < 22 or person.age > 50:
            return 0
        
        # Education check - need college education to have student loans
        college_educated = person.education in [
            'some_college', 'associates', 'bachelors', 
            'masters', 'doctorate', 'professional'
        ]
        if not college_educated:
            return 0
        
        # Higher education = higher probability and amount
        if person.education in ['masters', 'doctorate', 'professional']:
            prob = 0.50  # 50% of advanced degree holders have loans
            avg_interest = 1800
        elif person.education == 'bachelors':
            prob = 0.40  # 40% of bachelor's holders
            avg_interest = 1400
        else:
            prob = 0.25  # 25% of some college/associates
            avg_interest = 800
        
        # Younger = more likely to still have loans
        if person.age > 35:
            prob *= 0.6
        if person.age > 45:
            prob *= 0.5
        
        if np.random.random() >= prob:
            return 0
        
        # Generate interest amount
        interest = int(np.random.normal(avg_interest, avg_interest * 0.3))
        return min(max(0, interest), STUDENT_LOAN_INTEREST_LIMIT)
    
    def _calculate_educator_expenses(self, person: Person) -> int:
        """
        Calculate educator expenses for teachers.
        
        Based on SOC codes for education occupations.
        """
        if not person.occupation_code:
            return 0
        
        # Check if occupation is education-related (SOC 25-xxxx)
        soc = str(person.occupation_code).replace('-', '')
        if not soc.startswith('25'):
            return 0
        
        # ~70% of teachers claim educator expenses
        if np.random.random() >= 0.70:
            return 0
        
        # Most claim close to the max
        amount = int(np.random.uniform(150, EDUCATOR_EXPENSE_LIMIT))
        return amount
    
    def _calculate_ira_contributions(self, person: Person) -> int:
        """
        Calculate IRA contributions.
        
        More likely for employed, middle-aged, higher income.
        """
        # Must be employed and earning income
        if person.employment_status != EmploymentStatus.EMPLOYED.value:
            return 0
        
        # Age limits for traditional IRA contributions
        if person.age < 21 or person.age > 70:
            return 0
        
        # Income factor - need disposable income to contribute
        if person.wage_income < 25000:
            prob = 0.05
        elif person.wage_income < 50000:
            prob = 0.10
        elif person.wage_income < 100000:
            prob = 0.18
        else:
            prob = 0.25
        
        # Age factor - peak savings years
        if 35 <= person.age <= 55:
            prob *= 1.3
        
        if np.random.random() >= prob:
            return 0
        
        # Contribution amount
        limit = IRA_CONTRIBUTION_LIMIT_50_PLUS if person.age >= 50 else IRA_CONTRIBUTION_LIMIT
        
        # Most contribute less than max
        if np.random.random() < 0.30:
            # 30% max out
            return limit
        else:
            # Others contribute varying amounts
            return int(np.random.uniform(500, limit * 0.8))
    
    # =========================================================================
    # 5.6 CREDIT-RELATED EXPENSES
    # =========================================================================
    
    def _assign_credit_expenses(self, household: Household):
        """Assign expenses related to tax credits"""
        
        # Child care expenses
        household.child_care_expenses = self._calculate_child_care_expenses(household)
        
        # Education expenses (tuition)
        household.education_expenses = self._calculate_education_expenses(household)
    
    def _calculate_child_care_expenses(self, household: Household) -> int:
        """
        Calculate child care expenses for working parents.
        
        Required: children under 13, at least one working parent.
        """
        # Check for children under 13
        children_under_13 = [
            m for m in household.members 
            if not m.is_adult() and m.age < 13
        ]
        
        if not children_under_13:
            return 0
        
        # Check for working adults
        working_adults = [
            m for m in household.members
            if m.is_adult() and m.employment_status == EmploymentStatus.EMPLOYED.value
        ]
        
        if not working_adults:
            return 0
        
        # Single parent or dual-income household needs child care
        # Some may have family help
        if np.random.random() >= 0.65:  # 65% need paid child care
            return 0
        
        # Cost per child varies significantly
        num_children = len(children_under_13)
        
        # Hawaii child care costs: ~$12,000-15,000/year per child
        cost_per_child = int(np.random.uniform(8000, 15000))
        
        # Discount for multiple children
        if num_children >= 2:
            cost_per_child = int(cost_per_child * 0.85)
        
        total = cost_per_child * num_children
        
        # Cap at reasonable maximum ($16,000 for 2+ children for credit purposes)
        return min(total, 16000)
    
    def _calculate_education_expenses(self, household: Household) -> int:
        """
        Calculate education expenses (tuition) for credits.
        
        For: college-age members (18-24) or continuing education.
        """
        # Find eligible students
        students = []
        
        for member in household.members:
            # Traditional college age
            if 18 <= member.age <= 24:
                if member.education in ['some_college', 'associates', 'bachelors']:
                    students.append(('undergrad', member))
            # Graduate students
            elif 22 <= member.age <= 35:
                if member.education in ['masters', 'doctorate', 'professional']:
                    students.append(('graduate', member))
        
        if not students:
            return 0
        
        # Not all students have out-of-pocket tuition
        # (scholarships, employer paid, etc.)
        if np.random.random() >= 0.60:  # 60% have some tuition expense
            return 0
        
        # Calculate tuition
        total_tuition = 0
        
        for student_type, student in students:
            if student_type == 'undergrad':
                # Community college: $3k-5k, State university: $8k-15k
                tuition = int(np.random.choice([
                    np.random.uniform(3000, 5000),   # Community college
                    np.random.uniform(8000, 15000),  # State university
                ], p=[0.4, 0.6]))
            else:
                # Graduate: $10k-30k
                tuition = int(np.random.uniform(10000, 30000))
            
            total_tuition += tuition
        
        return total_tuition
    
    # =========================================================================
    # TOTALS CALCULATION
    # =========================================================================
    
    def _calculate_totals(self, household: Household):
        """Calculate total itemized and above-the-line deductions"""
        
        # Total itemized deductions (Schedule A)
        household.total_itemized_deductions = (
            min(household.property_taxes + household.state_income_tax, SALT_CAP) +  # SALT cap
            household.mortgage_interest +
            max(0, household.medical_expenses - int(household.total_household_income() * 0.075)) +  # 7.5% floor
            household.charitable_contributions
        )
        
        # Total above-the-line deductions
        household.total_above_line_deductions = (
            household.student_loan_interest +
            household.educator_expenses +
            household.ira_contributions
        )
    
    # =========================================================================
    # HELPER METHODS
    # =========================================================================
    
    def _get_income_bracket(self, income: int, dist: pd.DataFrame) -> Optional[str]:
        """Find matching income bracket in distribution table"""
        if 'income_bracket' not in dist.columns:
            return None
        
        brackets = dist['income_bracket'].unique()
        
        for bracket in brackets:
            if self._income_in_bracket(income, str(bracket)):
                return bracket
        
        # Return highest bracket if income exceeds all
        return brackets[-1] if len(brackets) > 0 else None
    
    def _income_in_bracket(self, income: int, bracket: str) -> bool:
        """Check if income falls within bracket string"""
        bracket = str(bracket).strip().lower()
        
        try:
            # Handle various formats: "$25-50K", "25000-50000", "<$25K", "$200K+"
            bracket = bracket.replace('$', '').replace(',', '').replace('k', '000')
            
            if bracket.startswith('<'):
                max_val = int(bracket[1:])
                return income < max_val
            elif bracket.endswith('+'):
                min_val = int(bracket[:-1])
                return income >= min_val
            elif '-' in bracket:
                parts = bracket.split('-')
                min_val = int(parts[0])
                max_val = int(parts[1])
                return min_val <= income < max_val
            else:
                return False
        except (ValueError, IndexError):
            return False
