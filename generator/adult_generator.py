"""
Adult generation logic for Stage 2.

Generates realistic adult household members based on:
- Household pattern from Stage 1
- PUMS demographic distributions
- BLS occupation data
"""

import logging
import uuid
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .models import (
    Person, Household, RelationshipType, EmploymentStatus, 
    EducationLevel, Race, PATTERN_METADATA
)
from .sampler import (
    weighted_sample, sample_age_from_bracket, get_age_bracket,
    match_age_bracket, set_random_seed
)

logger = logging.getLogger(__name__)


class AdultGenerator:
    """
    Generates adult household members with realistic demographics.
    
    Uses PUMS distributions for:
    - Age by employment status
    - Sex by pattern
    - Race by age
    - Hispanic origin by age
    - Employment by age/sex
    - Education by age
    - Disability by age
    
    Uses BLS data for:
    - Occupation by education level
    """
    
    def __init__(self, distributions: Dict[str, pd.DataFrame]):
        """
        Initialize with loaded distribution tables.
        
        Args:
            distributions: Dictionary of DataFrames from DistributionLoader
        """
        self.distributions = distributions
        self._validate_required_tables()
    
    def _validate_required_tables(self):
        """Check that required distribution tables are available"""
        required = [
            'employment_by_age',
            'education_by_age',
            'disability_by_age'
        ]
        
        optional = [
            'race_distribution',
            'race_by_age',
            'hispanic_origin_by_age',
            'spousal_age_gaps',
            'couple_sex_patterns',
            'education_occupation_probabilities',
            'bls_occupation_wages'
        ]
        
        missing_required = [t for t in required if t not in self.distributions]
        if missing_required:
            logger.warning(f"Missing required tables: {missing_required}")
        
        missing_optional = [t for t in optional if t not in self.distributions]
        if missing_optional:
            logger.debug(f"Missing optional tables (will use defaults): {missing_optional}")
    
    def generate_adults(self, household: Household) -> List[Person]:
        """
        Generate all adults for a household based on its pattern.
        
        Args:
            household: Household with pattern set from Stage 1
        
        Returns:
            List of Person objects (adults only)
        """
        pattern = household.pattern
        metadata = PATTERN_METADATA.get(pattern, PATTERN_METADATA['other'])
        
        # Step 2.1: Determine number of adults
        num_adults = self._determine_adult_count(pattern, metadata)
        
        # Step 2.2: Assign relationships
        relationships = self._assign_relationships(pattern, num_adults, household)
        
        # Step 2.3-2.7: Generate each adult
        adults = []
        for i, relationship in enumerate(relationships):
            adult = self._generate_single_adult(
                relationship=relationship,
                pattern=pattern,
                existing_adults=adults,
                household=household
            )
            adults.append(adult)
        
        logger.debug(f"Generated {len(adults)} adults for pattern '{pattern}'")
        return adults
    
    def _determine_adult_count(self, pattern: str, metadata: dict) -> int:
        """Determine number of adults based on pattern"""
        expected = metadata.get('expected_adults', 1)
        
        if isinstance(expected, tuple):
            # Range like (2, 4) for multigenerational
            return np.random.randint(expected[0], expected[1] + 1)
        
        return expected
    
    def _assign_relationships(self, pattern: str, num_adults: int, household: Household) -> List[RelationshipType]:
        """Assign relationship types to adults based on pattern"""
        
        if pattern in ['single_adult', 'single_parent']:
            return [RelationshipType.HOUSEHOLDER]
        
        elif pattern in ['married_couple_no_children', 'married_couple_with_children', 'blended_family']:
            return [RelationshipType.HOUSEHOLDER, RelationshipType.SPOUSE]
        
        elif pattern == 'unmarried_partners':
            return [RelationshipType.HOUSEHOLDER, RelationshipType.UNMARRIED_PARTNER]
        
        elif pattern == 'multigenerational':
            # Sample from multigenerational_patterns to determine structure
            relationships = [RelationshipType.HOUSEHOLDER]
            
            multi_patterns = self.distributions.get('multigenerational_patterns')
            if multi_patterns is not None and len(multi_patterns) > 0:
                # Sample a sub-pattern (uses weighted_count column)
                sub_pattern = weighted_sample(multi_patterns, 'weighted_count')['pattern']
                
                # Store on household for Stage 3 to use
                household.multigenerational_subpattern = sub_pattern
                
                if sub_pattern == 'grandparent_with_grandchildren':
                    # Grandparent is householder, may have spouse
                    # Grandchildren will be added in Stage 3
                    if num_adults >= 2:
                        relationships.append(RelationshipType.SPOUSE)
                
                elif sub_pattern == 'adult_with_parent':
                    # Adult child as householder, with parent
                    relationships.append(RelationshipType.PARENT)
                    if num_adults >= 3:
                        relationships.append(RelationshipType.SPOUSE)
                
                elif sub_pattern == 'four_generations':
                    # Complex: householder + parent + maybe spouse
                    relationships.append(RelationshipType.PARENT)
                    if num_adults >= 3:
                        relationships.append(RelationshipType.SPOUSE)
            else:
                # Default: add parents for extra adults
                household.multigenerational_subpattern = 'adult_with_parent'
                for _ in range(num_adults - 1):
                    relationships.append(RelationshipType.PARENT)
            
            return relationships[:num_adults]
        
        else:  # 'other' pattern
            relationships = [RelationshipType.HOUSEHOLDER]
            for _ in range(num_adults - 1):
                relationships.append(RelationshipType.OTHER_RELATIVE)
            return relationships
    
    def _generate_single_adult(
        self,
        relationship: RelationshipType,
        pattern: str,
        existing_adults: List[Person],
        household: Household
    ) -> Person:
        """Generate a single adult with all demographics"""
        
        person_id = str(uuid.uuid4())
        
        # Generate in order (dependencies matter)
        age = self._sample_age(relationship, pattern, existing_adults)
        sex = self._sample_sex(relationship, pattern, existing_adults)
        race = self._sample_race(age)
        hispanic = self._sample_hispanic_origin(age)
        employment = self._sample_employment_status(age, sex)
        education = self._sample_education(age)
        has_disability = self._sample_disability(age)
        
        # Occupation only for employed
        occupation_code = None
        occupation_title = None
        if employment == EmploymentStatus.EMPLOYED.value:
            occupation_code, occupation_title = self._sample_occupation(education)
        
        return Person(
            person_id=person_id,
            relationship=relationship,
            age=int(age),
            sex=str(sex),
            race=str(race),
            hispanic_origin=bool(hispanic),
            employment_status=str(employment),
            education=str(education),
            occupation_code=str(occupation_code) if occupation_code else None,
            occupation_title=str(occupation_title) if occupation_title else None,
            has_disability=bool(has_disability)
        )
    
    def _sample_age(
        self, 
        relationship: RelationshipType, 
        pattern: str,
        existing_adults: List[Person]
    ) -> int:
        """
        Sample age based on relationship and pattern constraints.
        """
        # Get householder for reference if this is spouse/partner
        householder = None
        for adult in existing_adults:
            if adult.relationship == RelationshipType.HOUSEHOLDER:
                householder = adult
                break
        
        if relationship == RelationshipType.HOUSEHOLDER:
            return self._sample_householder_age(pattern)
        
        elif relationship == RelationshipType.SPOUSE:
            return self._sample_spouse_age(householder)
        
        elif relationship == RelationshipType.UNMARRIED_PARTNER:
            return self._sample_partner_age(householder)
        
        elif relationship == RelationshipType.PARENT:
            return self._sample_parent_age(householder)
        
        else:
            # Other relatives: sample from general adult distribution
            return self._sample_general_adult_age()
    
    def _sample_householder_age(self, pattern: str) -> int:
        """Sample householder age with pattern-specific constraints"""
        
        # Age constraints by pattern
        if pattern == 'single_parent':
            min_age, max_age = 20, 65  # Must be old enough for children
        elif pattern in ['married_couple_with_children', 'blended_family']:
            min_age, max_age = 22, 55  # Child-bearing/raising ages
        elif pattern == 'multigenerational':
            min_age, max_age = 30, 75  # Wide range
        else:
            min_age, max_age = 18, 85
        
        # Sample from employment_by_age distribution (good proxy for adult age dist)
        emp_dist = self.distributions.get('employment_by_age')
        
        if emp_dist is not None and len(emp_dist) > 0:
            # Get age brackets that are in our valid range
            valid_brackets = []
            for bracket in emp_dist['age_bracket'].unique():
                # Parse bracket to check if in range
                if self._bracket_overlaps_range(str(bracket), min_age, max_age):
                    valid_brackets.append(bracket)
            
            if valid_brackets:
                # Filter to valid brackets and sample
                filtered = emp_dist[emp_dist['age_bracket'].isin(valid_brackets)]
                bracket_weights = filtered.groupby('age_bracket', observed=True)['weight'].sum()
                
                # Sample a bracket
                probs = bracket_weights / bracket_weights.sum()
                chosen_bracket = np.random.choice(bracket_weights.index, p=probs.values)
                
                # Sample age within bracket
                age = sample_age_from_bracket(str(chosen_bracket))
                return max(min_age, min(max_age, age))
        
        # Fallback: uniform within range
        return np.random.randint(min_age, max_age + 1)
    
    def _sample_spouse_age(self, householder: Optional[Person]) -> int:
        """Sample spouse age based on spousal age gap distribution"""
        
        if householder is None:
            return self._sample_general_adult_age()
        
        # Try to use spousal_age_gaps table
        gaps_dist = self.distributions.get('spousal_age_gaps')
        
        if gaps_dist is not None and len(gaps_dist) > 0:
            gap_row = weighted_sample(gaps_dist, 'weight')
            gap_bracket = str(gap_row['age_gap_bracket'])
            
            # Parse gap bracket and sample
            gap = self._sample_from_gap_bracket(gap_bracket)
            spouse_age = householder.age - gap  # gap is (householder - spouse)
        else:
            # Fallback: spouse within ±5 years
            gap = np.random.randint(-5, 6)
            spouse_age = householder.age - gap
        
        # Clamp to valid adult age
        return max(18, min(85, spouse_age))
    
    def _sample_partner_age(self, householder: Optional[Person]) -> int:
        """Sample unmarried partner age (similar to spouse but can be more varied)"""
        
        if householder is None:
            return self._sample_general_adult_age()
        
        # Partners can have wider age gaps than spouses
        # Use spousal gaps if available, otherwise ±8 years
        gaps_dist = self.distributions.get('spousal_age_gaps')
        
        if gaps_dist is not None and len(gaps_dist) > 0:
            gap_row = weighted_sample(gaps_dist, 'weight')
            gap_bracket = str(gap_row['age_gap_bracket'])
            gap = self._sample_from_gap_bracket(gap_bracket)
            partner_age = householder.age - gap
        else:
            gap = np.random.randint(-8, 9)
            partner_age = householder.age - gap
        
        return max(18, min(85, partner_age))
    
    def _sample_parent_age(self, householder: Optional[Person]) -> int:
        """Sample parent age (must be older than householder)"""
        
        if householder is None:
            return np.random.randint(55, 85)
        
        # Parent is 18-40 years older than householder
        age_diff = np.random.randint(18, 40)
        parent_age = householder.age + age_diff
        
        return min(95, parent_age)  # Cap at 95
    
    def _sample_general_adult_age(self) -> int:
        """Sample age from general adult distribution"""
        emp_dist = self.distributions.get('employment_by_age')
        
        if emp_dist is not None and len(emp_dist) > 0:
            bracket_weights = emp_dist.groupby('age_bracket', observed=True)['weight'].sum()
            probs = bracket_weights / bracket_weights.sum()
            chosen_bracket = np.random.choice(bracket_weights.index, p=probs.values)
            return sample_age_from_bracket(str(chosen_bracket))
        
        return np.random.randint(18, 70)
    
    def _sample_sex(
        self,
        relationship: RelationshipType,
        pattern: str,
        existing_adults: List[Person]
    ) -> str:
        """Sample sex based on relationship and couple patterns"""
        
        householder = None
        for adult in existing_adults:
            if adult.relationship == RelationshipType.HOUSEHOLDER:
                householder = adult
                break
        
        if relationship == RelationshipType.HOUSEHOLDER:
            # Sample from couple_sex_patterns or 50/50
            couple_patterns = self.distributions.get('couple_sex_patterns')
            
            if pattern in ['married_couple_no_children', 'married_couple_with_children', 
                          'blended_family', 'unmarried_partners']:
                if couple_patterns is not None and len(couple_patterns) > 0:
                    # Filter by couple type
                    couple_type = 'married' if 'married' in pattern else 'unmarried'
                    filtered = couple_patterns[couple_patterns['couple_type'] == couple_type]
                    
                    if len(filtered) > 0:
                        pattern_row = weighted_sample(filtered, 'weight')
                        sex_pattern = pattern_row['sex_pattern']  # e.g., "M_F", "F_M", "M_M", "F_F"
                        return sex_pattern.split('_')[0]  # Householder sex
            
            # Default: 50/50
            return np.random.choice(['M', 'F'])
        
        elif relationship in [RelationshipType.SPOUSE, RelationshipType.UNMARRIED_PARTNER]:
            # Use couple patterns if available
            couple_patterns = self.distributions.get('couple_sex_patterns')
            
            if couple_patterns is not None and householder is not None:
                couple_type = 'married' if relationship == RelationshipType.SPOUSE else 'unmarried'
                filtered = couple_patterns[couple_patterns['couple_type'] == couple_type]
                
                # Filter to patterns starting with householder's sex
                h_sex = householder.sex
                filtered = filtered[filtered['sex_pattern'].str.startswith(h_sex)]
                
                if len(filtered) > 0:
                    pattern_row = weighted_sample(filtered, 'weight')
                    sex_pattern = pattern_row['sex_pattern']
                    return sex_pattern.split('_')[1]  # Partner sex
            
            # Fallback: opposite sex (traditional default)
            if householder:
                return 'F' if householder.sex == 'M' else 'M'
            return np.random.choice(['M', 'F'])
        
        else:
            # Other relationships: 50/50
            return np.random.choice(['M', 'F'])
    
    def _sample_race(self, age: int) -> str:
        """Sample race based on age bracket distribution"""
        
        race_by_age = self.distributions.get('race_by_age')
        
        if race_by_age is not None and len(race_by_age) > 0:
            # Find matching age bracket
            age_bracket = self._get_matching_age_bracket(age, race_by_age['age_bracket'].unique())
            
            if age_bracket:
                filtered = race_by_age[race_by_age['age_bracket'] == age_bracket]
                if len(filtered) > 0:
                    race_row = weighted_sample(filtered, 'weight')
                    return race_row['race']
        
        # Try overall race distribution
        race_dist = self.distributions.get('race_distribution')
        if race_dist is not None and len(race_dist) > 0:
            race_row = weighted_sample(race_dist, 'weight')
            return race_row['race']
        
        # Fallback default
        return 'white'
    
    def _sample_hispanic_origin(self, age: int) -> bool:
        """Sample Hispanic origin based on age bracket"""
        
        hisp_dist = self.distributions.get('hispanic_origin_by_age')
        
        if hisp_dist is not None and len(hisp_dist) > 0:
            age_bracket = self._get_matching_age_bracket(age, hisp_dist['age_bracket'].unique())
            
            if age_bracket:
                filtered = hisp_dist[hisp_dist['age_bracket'] == age_bracket]
                if len(filtered) > 0:
                    hisp_row = weighted_sample(filtered, 'weight')
                    return hisp_row['hispanic_origin'] == 'hispanic'
        
        # Fallback: ~18% Hispanic (US average)
        return np.random.random() < 0.18
    
    def _sample_employment_status(self, age: int, sex: str) -> str:
        """Sample employment status based on age and sex"""
        
        emp_dist = self.distributions.get('employment_by_age')
        
        if emp_dist is not None and len(emp_dist) > 0:
            # Find matching age bracket
            age_bracket = self._get_matching_age_bracket(age, emp_dist['age_bracket'].unique())
            
            # Map sex to match table format
            sex_value = 'male' if sex == 'M' else 'female'
            
            if age_bracket:
                filtered = emp_dist[
                    (emp_dist['age_bracket'] == age_bracket) & 
                    (emp_dist['sex'] == sex_value)
                ]
                
                if len(filtered) > 0:
                    emp_row = weighted_sample(filtered, 'weight')
                    return emp_row['employment_status']
        
        # Fallback based on age
        if age >= 65:
            return np.random.choice(
                [EmploymentStatus.EMPLOYED.value, EmploymentStatus.NOT_IN_LABOR_FORCE.value],
                p=[0.25, 0.75]
            )
        elif age < 22:
            return np.random.choice(
                [EmploymentStatus.EMPLOYED.value, EmploymentStatus.NOT_IN_LABOR_FORCE.value, 
                 EmploymentStatus.UNEMPLOYED.value],
                p=[0.50, 0.40, 0.10]
            )
        else:
            return np.random.choice(
                [EmploymentStatus.EMPLOYED.value, EmploymentStatus.NOT_IN_LABOR_FORCE.value,
                 EmploymentStatus.UNEMPLOYED.value],
                p=[0.75, 0.20, 0.05]
            )
    
    def _sample_education(self, age: int) -> str:
        """Sample education level based on age"""
        
        edu_dist = self.distributions.get('education_by_age')
        
        if edu_dist is not None and len(edu_dist) > 0:
            age_bracket = self._get_matching_age_bracket(age, edu_dist['age_bracket'].unique())
            
            if age_bracket:
                filtered = edu_dist[edu_dist['age_bracket'] == age_bracket]
                
                if len(filtered) > 0:
                    # education_by_age uses 'weighted_count' column and 'education_level' field
                    edu_row = weighted_sample(filtered, 'weighted_count')
                    return edu_row['education_level']
        
        # Fallback: basic distribution
        if age < 22:
            return np.random.choice(
                ['high_school', 'some_college'],
                p=[0.6, 0.4]
            )
        else:
            return np.random.choice(
                ['high_school', 'some_college', 'bachelors', 'associates', 'masters'],
                p=[0.30, 0.25, 0.25, 0.10, 0.10]
            )
    
    def _sample_disability(self, age: int) -> bool:
        """Sample disability status based on age"""
        
        dis_dist = self.distributions.get('disability_by_age')
        
        if dis_dist is not None and len(dis_dist) > 0:
            age_bracket = self._get_matching_age_bracket(age, dis_dist['age_bracket'].unique())
            
            if age_bracket:
                # Get disability rate for this age bracket
                # Table has 'disability_percentage' column (already a percentage 0-100)
                filtered = dis_dist[dis_dist['age_bracket'] == age_bracket]
                
                if len(filtered) > 0:
                    disability_rate = filtered['disability_percentage'].values[0] / 100
                    return np.random.random() < disability_rate
        
        # Fallback: age-based rates
        if age < 35:
            return np.random.random() < 0.05
        elif age < 55:
            return np.random.random() < 0.10
        elif age < 65:
            return np.random.random() < 0.20
        else:
            return np.random.random() < 0.35
    
    def _sample_occupation(self, education: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Sample occupation based on education level.
        
        Returns:
            Tuple of (soc_code, occupation_title)
        """
        edu_occ = self.distributions.get('education_occupation_probabilities')
        bls_wages = self.distributions.get('bls_occupation_wages')
        
        # Need at least BLS data to assign occupation
        if bls_wages is None or len(bls_wages) == 0:
            return None, None
        
        # Verify BLS has required columns
        if 'soc_code' not in bls_wages.columns or 'occupation_title' not in bls_wages.columns:
            logger.warning(f"BLS table missing required columns. Has: {list(bls_wages.columns)}")
            return None, None
        
        try:
            # If we have education->occupation mapping, use it
            if edu_occ is not None and len(edu_occ) > 0:
                # Map education to table format (matches extract_derived education_map)
                edu_mapping = {
                    'less_than_hs': 'no_hs_diploma',
                    'no_hs_diploma': 'no_hs_diploma',
                    'high_school': 'hs_graduate',
                    'hs_graduate': 'hs_graduate',
                    'some_college': 'some_college',
                    'associates': 'associates',
                    'bachelors': 'bachelors',
                    'masters': 'masters',
                    'professional': 'professional_doctorate',
                    'professional_doctorate': 'professional_doctorate',
                    'doctorate': 'professional_doctorate'
                }
                edu_key = edu_mapping.get(education, 'hs_graduate')
                
                # Get SOC major group based on education
                filtered = edu_occ[edu_occ['education_level'] == edu_key]
                
                if len(filtered) > 0:
                    occ_row = weighted_sample(filtered, 'weighted_count')
                    soc_major = str(occ_row['soc_major_group'])  # Already 2-digit string
                    
                    # Get specific occupation from BLS data within this major group
                    # BLS soc_code format: "11-1021" so we match the prefix
                    bls_filtered = bls_wages[
                        bls_wages['soc_code'].astype(str).str.replace('-', '').str[:2] == soc_major
                    ]
                    
                    if len(bls_filtered) > 0:
                        occ = weighted_sample(bls_filtered, 'employment_count')
                        return str(occ['soc_code']), str(occ['occupation_title'])
            
            # Fallback: sample directly from BLS data (weighted by employment)
            # This ignores education but still gives realistic occupation distribution
            occ = weighted_sample(bls_wages, 'employment_count')
            return str(occ['soc_code']), str(occ['occupation_title'])
        
        except Exception as e:
            logger.warning(f"Error sampling occupation for education '{education}': {e}")
            return None, None
    
    # =========================================================================
    # Helper methods
    # =========================================================================
    
    def _bracket_overlaps_range(self, bracket: str, min_val: int, max_val: int) -> bool:
        """Check if age bracket overlaps with a range"""
        bracket = str(bracket).strip()
        
        try:
            if '-' in bracket:
                parts = bracket.split('-')
                b_min = int(parts[0])
                b_max = int(parts[1].replace('+', ''))
                return b_min <= max_val and b_max >= min_val
            elif bracket.endswith('+'):
                b_min = int(bracket[:-1])
                return b_min <= max_val
            else:
                b_val = int(bracket)
                return min_val <= b_val <= max_val
        except (ValueError, IndexError):
            return True  # Include if can't parse
    
    def _get_matching_age_bracket(self, age: int, brackets) -> Optional[str]:
        """Find the bracket that contains the given age"""
        for bracket in brackets:
            if match_age_bracket(age, str(bracket)):
                return bracket
        return None
    
    def _sample_from_gap_bracket(self, gap_bracket: str) -> int:
        """Sample an age gap from a bracket like '-5_to_-3' or '6_to_10'"""
        gap_bracket = str(gap_bracket).strip()
        
        try:
            if gap_bracket == '0':
                return 0
            elif '_or_less' in gap_bracket:
                max_gap = int(gap_bracket.split('_')[0])
                return np.random.randint(max_gap - 5, max_gap + 1)
            elif '_or_more' in gap_bracket:
                min_gap = int(gap_bracket.split('_')[0])
                return min_gap + int(np.random.exponential(3))
            elif '_to_' in gap_bracket:
                parts = gap_bracket.replace('_to_', ',').split(',')
                min_gap = int(parts[0])
                max_gap = int(parts[1])
                return np.random.randint(min_gap, max_gap + 1)
            else:
                return int(gap_bracket)
        except (ValueError, IndexError):
            return 0
