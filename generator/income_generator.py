"""
Income generation logic for Stage 4.

Assigns realistic income to household members based on:
- Employment status and occupation
- Age (affects wage levels, SS eligibility, retirement)
- Household income level (affects investment income probability)
- Disability status (affects SS eligibility)

Income Types:
- Wage income (employed adults)
- Self-employment income (based on occupation probability)
- Unemployment income (unemployed adults)
- Social Security (62+ or disabled)
- Retirement income (55+)
- Interest & dividend income (age + income correlated)
- Other income (rare)
- Public assistance (household-level, means-tested)
"""

import logging
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .models import Person, Household, EmploymentStatus
from .sampler import weighted_sample

logger = logging.getLogger(__name__)


# =============================================================================
# INCOME CAPS (to keep scenarios realistic)
# =============================================================================

INCOME_CAPS = {
    'wage': 500_000,
    'self_employment': 250_000,
    'unemployment': 30_000,
    'social_security': 50_000,
    'retirement': 200_000,
    'interest': 100_000,
    'dividend': 100_000,
    'other': 50_000,
    'public_assistance': 15_000,
}

# =============================================================================
# AGE MULTIPLIERS (fallback if age_income_adjustments table missing)
# =============================================================================

DEFAULT_AGE_MULTIPLIERS = {
    '18-24': 0.60,
    '25-34': 0.85,
    '35-44': 1.00,
    '45-54': 1.10,
    '55-64': 1.05,
    '65+': 0.90,
}

# =============================================================================
# SELF-EMPLOYMENT PROBABILITIES BY SOC MAJOR GROUP (fallback)
# =============================================================================

DEFAULT_SE_PROBABILITY = {
    '11': 0.15,  # Management
    '13': 0.10,  # Business/Financial
    '15': 0.12,  # Computer/Math
    '17': 0.08,  # Architecture/Engineering
    '19': 0.05,  # Life/Physical/Social Science
    '21': 0.08,  # Community/Social Service
    '23': 0.25,  # Legal (high self-employment)
    '25': 0.05,  # Education
    '27': 0.30,  # Arts/Design/Entertainment (high)
    '29': 0.15,  # Healthcare Practitioners
    '31': 0.05,  # Healthcare Support
    '33': 0.02,  # Protective Service
    '35': 0.08,  # Food Preparation
    '37': 0.15,  # Building/Grounds Maintenance
    '39': 0.20,  # Personal Care
    '41': 0.10,  # Sales
    '43': 0.03,  # Office/Admin
    '45': 0.25,  # Farming/Fishing/Forestry (high)
    '47': 0.25,  # Construction (high)
    '49': 0.15,  # Installation/Maintenance
    '51': 0.05,  # Production
    '53': 0.12,  # Transportation
}


class IncomeGenerator:
    """
    Assigns income to household members based on demographics.
    
    Uses distribution tables for:
    - BLS occupation wages
    - Social Security amounts
    - Retirement income
    - Interest and dividend income
    - Public assistance
    """
    
    def __init__(self, distributions: Dict[str, pd.DataFrame]):
        """
        Initialize with loaded distribution tables.
        
        Args:
            distributions: Dictionary of DataFrames from DistributionLoader
        """
        self.distributions = distributions
        self._log_available_tables()
    
    def _log_available_tables(self):
        """Log which income tables are available"""
        income_tables = [
            'bls_occupation_wages',
            'social_security',
            'retirement_income',
            'interest_and_dividend_income',
            'other_income_by_employment_status',
            'public_assistance_income',
            'age_income_adjustments',
            'occupation_self_employment_probability',
        ]
        
        available = [t for t in income_tables if t in self.distributions]
        missing = [t for t in income_tables if t not in self.distributions]
        
        logger.info(f"Income tables available: {available}")
        if missing:
            logger.info(f"Income tables missing (will use fallbacks): {missing}")
        
        # Log BLS table info if available
        bls = self.distributions.get('bls_occupation_wages')
        if bls is not None:
            logger.info(f"BLS table: {len(bls)} rows, columns: {list(bls.columns)}")
        else:
            logger.warning("BLS occupation wages table not loaded!")
    
    def assign_income(self, household: Household) -> Household:
        """
        Assign all income types to household members.
        
        Args:
            household: Household with members from Stages 2-3
        
        Returns:
            Household with income fields populated
        """
        # Process each member
        for person in household.members:
            if person.is_adult():
                self._assign_adult_income(person)
            else:
                self._assign_child_income(person)
        
        # Household-level: Public assistance (means-tested)
        self._assign_public_assistance(household)
        
        logger.debug(f"Assigned income to {len(household.members)} members, "
                    f"total household income: ${household.total_household_income():,}")
        
        return household
    
    def _assign_adult_income(self, person: Person):
        """Assign all income types to an adult"""
        
        # 4.1 Wage Income (employed only)
        if person.employment_status == EmploymentStatus.EMPLOYED.value:
            person.wage_income = self._calculate_wage_income(person)
            
            # 4.2 Self-Employment Income (employed only)
            person.self_employment_income = self._calculate_self_employment_income(person)
        
        # 4.3 Unemployment Income (unemployed only)
        if person.employment_status == EmploymentStatus.UNEMPLOYED.value:
            person.wage_income = self._calculate_unemployment_income(person)
            # Note: storing in wage_income field for simplicity
            # Could add separate unemployment_income field if needed
        
        # 4.4 Social Security (62+ or disabled)
        if person.age >= 62 or person.has_disability:
            person.social_security_income = self._calculate_social_security(person)
        
        # 4.5 Retirement Income (55+)
        if person.age >= 55:
            person.retirement_income = self._calculate_retirement_income(person)
        
        # 4.6 Interest & Dividend Income (correlated with age + income)
        current_income = (person.wage_income + person.self_employment_income + 
                         person.social_security_income + person.retirement_income)
        
        interest, dividend = self._calculate_investment_income(person, current_income)
        person.interest_income = interest
        person.dividend_income = dividend
        
        # 4.7 Other Income (rare)
        person.other_income = self._calculate_other_income(person)
    
    def _assign_child_income(self, person: Person):
        """Assign income to employed children (16-17)"""
        
        if (person.employment_status == EmploymentStatus.EMPLOYED.value and 
            16 <= person.age <= 17):
            # Part-time teen job: $5k-$15k annually
            person.wage_income = int(np.random.uniform(5000, 15000))
    
    # =========================================================================
    # 4.1 WAGE INCOME
    # =========================================================================
    
    def _calculate_wage_income(self, person: Person) -> int:
        """
        Calculate wage income based on occupation and age.
        
        Uses BLS occupation wage data with:
        - Percentile variation (not everyone earns median)
        - Age adjustment (younger earn less, peak at 45-54)
        - 65+ part-time consideration
        """
        bls_wages = self.distributions.get('bls_occupation_wages')
        
        # Fallback if no BLS data or no occupation
        if bls_wages is None or len(bls_wages) == 0 or person.occupation_code is None:
            base_wage = 45000  # Rough median US wage
            age_mult = self._get_age_multiplier(person.age)
            return min(int(base_wage * age_mult), INCOME_CAPS['wage'])
        
        # Verify required column exists
        if 'soc_code' not in bls_wages.columns:
            logger.warning(f"BLS table missing 'soc_code' column. Columns: {list(bls_wages.columns)}")
            base_wage = 45000
            age_mult = self._get_age_multiplier(person.age)
            return min(int(base_wage * age_mult), INCOME_CAPS['wage'])
        
        try:
            # Look up occupation wage data
            occ_row = bls_wages[bls_wages['soc_code'] == person.occupation_code]
            
            if len(occ_row) == 0:
                # Try matching by major group (first 2 digits)
                if person.occupation_code:
                    major_group = person.occupation_code.replace('-', '')[:2]
                    occ_row = bls_wages[
                        bls_wages['soc_code'].astype(str).str.replace('-', '').str[:2] == major_group
                    ]
            
            if len(occ_row) == 0:
                # Still no match, use fallback
                base_wage = 45000
            else:
                # Sample from wage distribution (realistic variation)
                occ_data = occ_row.iloc[0]
                
                # Choose percentile: most people cluster around median
                percentile = np.random.choice(
                    ['p10', 'p25', 'median', 'p75', 'p90'],
                    p=[0.10, 0.20, 0.40, 0.20, 0.10]
                )
                
                if percentile == 'median':
                    col_name = 'median_annual_wage'
                else:
                    col_name = f'{percentile}_annual_wage'
                
                if col_name in occ_data.index:
                    base_wage = occ_data[col_name]
                else:
                    base_wage = occ_data.get('median_annual_wage', 45000)
                
                # Handle missing/null values
                if pd.isna(base_wage) or base_wage <= 0:
                    base_wage = 45000
        
        except Exception as e:
            logger.warning(f"Error looking up wage for {person.occupation_code}: {e}")
            base_wage = 45000
        
        # Apply age adjustment
        age_mult = self._get_age_multiplier(person.age)
        wage = float(base_wage) * age_mult
        
        # 65+ worker adjustment (part-time vs full-time vs senior)
        if person.age >= 65:
            roll = np.random.random()
            if roll < 0.55:
                # 55% part-time
                wage *= 0.5
            elif roll < 0.90:
                # 35% full-time (no change)
                pass
            else:
                # 10% senior executive/professional (premium)
                wage *= 1.1
        
        return min(int(wage), INCOME_CAPS['wage'])
    
    def _get_age_multiplier(self, age: int) -> float:
        """Get wage multiplier based on age"""
        age_adjustments = self.distributions.get('age_income_adjustments')
        
        if age_adjustments is not None and len(age_adjustments) > 0:
            # Find matching bracket
            for _, row in age_adjustments.iterrows():
                bracket = str(row.get('age_bracket', ''))
                if self._age_in_bracket(age, bracket):
                    return float(row.get('multiplier', 1.0))
        
        # Fallback to defaults
        if age < 25:
            return DEFAULT_AGE_MULTIPLIERS['18-24']
        elif age < 35:
            return DEFAULT_AGE_MULTIPLIERS['25-34']
        elif age < 45:
            return DEFAULT_AGE_MULTIPLIERS['35-44']
        elif age < 55:
            return DEFAULT_AGE_MULTIPLIERS['45-54']
        elif age < 65:
            return DEFAULT_AGE_MULTIPLIERS['55-64']
        else:
            return DEFAULT_AGE_MULTIPLIERS['65+']
    
    # =========================================================================
    # 4.2 SELF-EMPLOYMENT INCOME
    # =========================================================================
    
    def _calculate_self_employment_income(self, person: Person) -> int:
        """
        Calculate self-employment income based on occupation.
        
        Not everyone with a job has SE income - depends on occupation.
        """
        se_prob_table = self.distributions.get('occupation_self_employment_probability')
        
        # Get SE probability for this occupation
        if (se_prob_table is not None and 
            len(se_prob_table) > 0 and 
            person.occupation_code and
            'soc_code' in se_prob_table.columns):
            try:
                occ_row = se_prob_table[
                    se_prob_table['soc_code'] == person.occupation_code
                ]
                if len(occ_row) > 0:
                    se_prob = float(occ_row.iloc[0].get('probability', 0.1))
                else:
                    se_prob = self._get_default_se_probability(person.occupation_code)
            except Exception:
                se_prob = self._get_default_se_probability(person.occupation_code)
        else:
            se_prob = self._get_default_se_probability(person.occupation_code)
        
        # Roll for SE income
        if np.random.random() >= se_prob:
            return 0
        
        # SE income typically 20-80% of wage income
        if person.wage_income > 0:
            se_ratio = np.random.uniform(0.2, 0.8)
            se_income = int(person.wage_income * se_ratio)
        else:
            # Primary self-employment (no wage job)
            se_income = int(np.random.uniform(20000, 100000))
        
        return min(se_income, INCOME_CAPS['self_employment'])
    
    def _get_default_se_probability(self, occupation_code: Optional[str]) -> float:
        """Get default self-employment probability by SOC major group"""
        if not occupation_code:
            return 0.10  # Default 10%
        
        major_group = occupation_code.replace('-', '')[:2]
        return DEFAULT_SE_PROBABILITY.get(major_group, 0.10)
    
    # =========================================================================
    # 4.3 UNEMPLOYMENT INCOME
    # =========================================================================
    
    def _calculate_unemployment_income(self, person: Person) -> int:
        """
        Calculate unemployment income for unemployed workers.
        
        Not all unemployed people receive UI:
        - Must have worked recently
        - Must have filed for benefits
        - Benefits run out after ~26 weeks
        """
        # ~40% of unemployed collect UI
        if np.random.random() >= 0.40:
            return 0
        
        # Weekly benefit varies by state and prior wages
        # Hawaii range: ~$200-762/week
        weekly_benefit = np.random.uniform(250, 650)
        
        # Duration: 10-26 weeks typically
        weeks_collected = np.random.randint(10, 27)
        
        ui_income = int(weekly_benefit * weeks_collected)
        
        return min(ui_income, INCOME_CAPS['unemployment'])
    
    # =========================================================================
    # 4.4 SOCIAL SECURITY INCOME
    # =========================================================================
    
    def _calculate_social_security(self, person: Person) -> int:
        """
        Calculate Social Security income.
        
        Eligible if:
        - Age 62+ (can claim early, reduced benefits)
        - OR has disability
        
        Amount depends on age bracket.
        """
        ss_dist = self.distributions.get('social_security')
        
        if ss_dist is None or len(ss_dist) == 0:
            # Fallback: rough SS estimates
            if person.has_disability:
                return int(np.random.uniform(12000, 24000))
            elif person.age >= 67:  # Full retirement age
                return int(np.random.uniform(18000, 36000))
            elif person.age >= 62:
                return int(np.random.uniform(12000, 28000))
            else:
                return 0
        
        # Get age bracket
        bracket = self._get_ss_age_bracket(person.age, ss_dist)
        
        if bracket is None:
            return 0
        
        # Filter to bracket
        bracket_data = ss_dist[ss_dist['age_bracket'] == bracket]
        
        if len(bracket_data) == 0:
            return 0
        
        # Sample from distribution
        row = bracket_data.iloc[0]
        mean_amount = float(row.get('mean_amount', 20000))
        
        # Add variation (+/- 20%)
        ss_income = int(np.random.normal(mean_amount, mean_amount * 0.20))
        ss_income = max(0, ss_income)  # No negative
        
        # Disabled get lower amounts typically
        if person.has_disability and person.age < 62:
            ss_income = int(ss_income * 0.7)
        
        return min(ss_income, INCOME_CAPS['social_security'])
    
    def _get_ss_age_bracket(self, age: int, ss_dist: pd.DataFrame) -> Optional[str]:
        """Find matching age bracket in SS distribution"""
        brackets = ss_dist['age_bracket'].unique()
        
        for bracket in brackets:
            if self._age_in_bracket(age, str(bracket)):
                return bracket
        
        return None
    
    # =========================================================================
    # 4.5 RETIREMENT INCOME
    # =========================================================================
    
    def _calculate_retirement_income(self, person: Person) -> int:
        """
        Calculate retirement income (pension, 401k, IRA distributions).
        
        More likely with age. Not everyone has retirement savings.
        """
        # Probability increases with age
        if person.age < 55:
            return 0
        
        # Base probability: increases with age
        prob = min(0.80, (person.age - 55) * 0.04 + 0.10)
        
        # Higher if employed in professional occupations
        if person.occupation_code:
            major_group = person.occupation_code.replace('-', '')[:2]
            if major_group in ['11', '13', '15', '17', '23', '29']:  # Professional
                prob += 0.15
        
        if np.random.random() >= prob:
            return 0
        
        # Get retirement income distribution
        ret_dist = self.distributions.get('retirement_income')
        
        if ret_dist is None or len(ret_dist) == 0:
            # Fallback estimates
            if person.age >= 70:
                return int(np.random.uniform(15000, 60000))
            else:
                return int(np.random.uniform(5000, 40000))
        
        # Find age bracket
        bracket = self._get_retirement_age_bracket(person.age, ret_dist)
        
        if bracket is None:
            return int(np.random.uniform(10000, 40000))
        
        bracket_data = ret_dist[ret_dist['age_bracket'] == bracket]
        
        if len(bracket_data) == 0:
            return int(np.random.uniform(10000, 40000))
        
        row = bracket_data.iloc[0]
        mean_amount = float(row.get('mean_amount', 25000))
        
        # Add variation
        ret_income = int(np.random.normal(mean_amount, mean_amount * 0.25))
        ret_income = max(0, ret_income)
        
        return min(ret_income, INCOME_CAPS['retirement'])
    
    def _get_retirement_age_bracket(self, age: int, ret_dist: pd.DataFrame) -> Optional[str]:
        """Find matching age bracket in retirement distribution"""
        brackets = ret_dist['age_bracket'].unique()
        
        for bracket in brackets:
            if self._age_in_bracket(age, str(bracket)):
                return bracket
        
        return None
    
    # =========================================================================
    # 4.6 INTEREST & DIVIDEND INCOME
    # =========================================================================
    
    def _calculate_investment_income(
        self, 
        person: Person, 
        current_income: int
    ) -> Tuple[int, int]:
        """
        Calculate interest and dividend income.
        
        Probability correlates with:
        - Age (older = more likely to have investments)
        - Income (higher earners have more to invest)
        """
        # Calculate probability
        prob = self._get_investment_probability(person.age, current_income)
        
        if np.random.random() >= prob:
            return 0, 0
        
        # Get distribution
        inv_dist = self.distributions.get('interest_and_dividend_income')
        
        if inv_dist is None or len(inv_dist) == 0:
            # Fallback: simple estimates based on income
            if current_income > 100000:
                interest = int(np.random.uniform(2000, 15000))
                dividend = int(np.random.uniform(2000, 20000))
            elif current_income > 50000:
                interest = int(np.random.uniform(500, 5000))
                dividend = int(np.random.uniform(500, 8000))
            else:
                interest = int(np.random.uniform(100, 2000))
                dividend = int(np.random.uniform(100, 3000))
            
            return (min(interest, INCOME_CAPS['interest']), 
                    min(dividend, INCOME_CAPS['dividend']))
        
        # Sample from distribution
        row = weighted_sample(inv_dist, 'weight')
        bracket = row.get('income_bracket', '1-999')
        
        # Parse income bracket and sample within it
        amount = self._sample_from_income_bracket(str(bracket))
        
        # Split between interest and dividend (roughly 40/60)
        split = np.random.uniform(0.3, 0.5)
        interest = int(amount * split)
        dividend = int(amount * (1 - split))
        
        return (min(interest, INCOME_CAPS['interest']), 
                min(dividend, INCOME_CAPS['dividend']))
    
    def _get_investment_probability(self, age: int, income: int) -> float:
        """Calculate probability of having investment income"""
        base_prob = 0.10
        
        # Age factor
        if age >= 45:
            base_prob += 0.10
        if age >= 55:
            base_prob += 0.10
        if age >= 65:
            base_prob += 0.15
        
        # Income factor
        if income >= 50000:
            base_prob += 0.10
        if income >= 100000:
            base_prob += 0.15
        if income >= 150000:
            base_prob += 0.15
        
        return min(0.80, base_prob)
    
    # =========================================================================
    # 4.7 OTHER INCOME
    # =========================================================================
    
    def _calculate_other_income(self, person: Person) -> int:
        """
        Calculate other income (alimony, royalties, etc.).
        
        Relatively rare.
        """
        other_dist = self.distributions.get('other_income_by_employment_status')
        
        # Base probability: 5-10%
        if np.random.random() >= 0.08:
            return 0
        
        if other_dist is None or len(other_dist) == 0:
            return int(np.random.uniform(1000, 10000))
        
        # Filter by employment status if possible
        emp_status = person.employment_status
        filtered = other_dist[other_dist['employment_status'] == emp_status]
        
        if len(filtered) == 0:
            filtered = other_dist
        
        if len(filtered) == 0:
            return int(np.random.uniform(1000, 10000))
        
        # Get mean amount
        row = filtered.iloc[0]
        mean_amount = float(row.get('mean_amount', 5000))
        
        # Add variation
        other_income = int(np.random.normal(mean_amount, mean_amount * 0.30))
        other_income = max(0, other_income)
        
        return min(other_income, INCOME_CAPS['other'])
    
    # =========================================================================
    # 4.8 PUBLIC ASSISTANCE (Household-Level)
    # =========================================================================
    
    def _assign_public_assistance(self, household: Household):
        """
        Assign public assistance to qualifying households.
        
        Means-tested: only low-income households qualify.
        Assigned to householder.
        """
        # Calculate current household income (before PA)
        total_income = household.total_household_income()
        household_size = len(household.members)
        
        # Rough poverty threshold: base + per-person
        # 2023 federal poverty level: ~$14,580 + $5,140 per additional person
        poverty_threshold = 14580 + (household_size - 1) * 5140
        
        # PA eligibility typically ~130-185% of poverty level
        eligibility_threshold = poverty_threshold * 1.5
        
        if total_income >= eligibility_threshold:
            return
        
        # Get PA distribution
        pa_dist = self.distributions.get('public_assistance_income')
        
        if pa_dist is None or len(pa_dist) == 0:
            # Fallback
            if total_income < poverty_threshold:
                pa_amount = int(np.random.uniform(3000, 8000))
            else:
                pa_amount = int(np.random.uniform(1000, 4000))
        else:
            # Sample from distribution
            row = weighted_sample(pa_dist, 'weighted_count')
            mean_amount = float(row.get('mean_amount', 4000))
            pa_amount = int(np.random.normal(mean_amount, mean_amount * 0.20))
            pa_amount = max(0, pa_amount)
        
        pa_amount = min(pa_amount, INCOME_CAPS['public_assistance'])
        
        # Assign to householder
        householder = household.get_householder()
        if householder:
            householder.public_assistance_income = pa_amount
    
    # =========================================================================
    # HELPER METHODS
    # =========================================================================
    
    def _age_in_bracket(self, age: int, bracket: str) -> bool:
        """Check if age falls within bracket like '25-34' or '65+'"""
        bracket = str(bracket).strip()
        
        try:
            if '-' in bracket:
                parts = bracket.split('-')
                min_age = int(parts[0])
                max_age = int(parts[1].replace('+', ''))
                return min_age <= age <= max_age
            elif bracket.endswith('+'):
                min_age = int(bracket[:-1])
                return age >= min_age
            else:
                return age == int(bracket)
        except (ValueError, IndexError):
            return False
    
    def _sample_from_income_bracket(self, bracket: str) -> int:
        """Sample a specific amount from income bracket like '1000-4999'"""
        bracket = str(bracket).strip()
        
        try:
            if '-' in bracket:
                parts = bracket.replace(',', '').replace('$', '').split('-')
                min_val = int(float(parts[0]))
                max_val = int(float(parts[1]))
                return int(np.random.uniform(min_val, max_val))
            elif bracket.endswith('+'):
                min_val = int(bracket.replace('+', '').replace(',', '').replace('$', ''))
                return int(np.random.exponential(min_val * 0.5) + min_val)
            else:
                return int(float(bracket.replace(',', '').replace('$', '')))
        except (ValueError, IndexError):
            return int(np.random.uniform(1000, 10000))
