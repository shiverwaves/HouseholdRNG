"""
Child generation logic for Stage 3.

Generates realistic child household members based on:
- Household pattern from Stage 1
- Parent demographics from Stage 2
- PUMS child age distributions
"""

import logging
import uuid
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .models import (
    Person, Household, RelationshipType, EmploymentStatus,
    PATTERN_METADATA
)
from .sampler import weighted_sample, set_random_seed

logger = logging.getLogger(__name__)


# Patterns that can have children
PATTERNS_WITH_CHILDREN = {
    'married_couple_with_children',
    'single_parent',
    'blended_family',
    'multigenerational',
    'unmarried_partners',
}

# Patterns that MUST have children (at least 1)
PATTERNS_REQUIRING_CHILDREN = {
    'married_couple_with_children',
    'single_parent',
    'blended_family',
}

# Teen employment rate (16-17 year olds)
TEEN_EMPLOYMENT_RATE = 0.35

# Common teen occupations (simplified)
TEEN_OCCUPATIONS = [
    ('35-3023', 'Fast Food and Counter Workers'),
    ('41-2011', 'Cashiers'),
    ('35-3031', 'Waiters and Waitresses'),
    ('41-2031', 'Retail Salespersons'),
    ('37-2011', 'Janitors and Cleaners'),
    ('53-7065', 'Stockers and Order Fillers'),
]


class ChildGenerator:
    """
    Generates child household members with realistic demographics.
    
    Uses PUMS distributions for:
    - Number of children by parent age
    - Child age distributions by parent age
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
            'children_by_parent_age',
            'child_age_distributions',
        ]
        
        optional = [
            'stepchild_patterns',
        ]
        
        missing_required = [t for t in required if t not in self.distributions]
        if missing_required:
            logger.warning(f"Missing required child tables: {missing_required}")
        
        missing_optional = [t for t in optional if t not in self.distributions]
        if missing_optional:
            logger.debug(f"Missing optional child tables: {missing_optional}")
    
    def generate_children(self, household: Household) -> List[Person]:
        """
        Generate all children for a household based on pattern and parent ages.
        
        Args:
            household: Household with adults from Stage 2
        
        Returns:
            List of Person objects (children only)
        """
        pattern = household.pattern
        
        # 3.1 Check if pattern expects children
        if not self._pattern_has_children(pattern):
            logger.debug(f"Pattern '{pattern}' does not have children")
            return []
        
        # Get adults for reference
        adults = household.get_adults()
        if not adults:
            logger.warning("No adults in household, cannot generate children")
            return []
        
        # 3.2 Determine number of children
        num_children = self._determine_child_count(household, adults)
        
        if num_children == 0:
            return []
        
        # 3.3 Determine relationships for each child
        relationships = self._assign_child_relationships(
            household.pattern, 
            num_children,
            household
        )
        
        # 3.4 Generate each child
        children = []
        for relationship in relationships:
            child = self._generate_single_child(
                household=household,
                adults=adults,
                relationship=relationship,
                existing_children=children
            )
            children.append(child)
        
        logger.debug(f"Generated {len(children)} children for pattern '{pattern}'")
        return children
    
    def _pattern_has_children(self, pattern: str) -> bool:
        """Check if pattern can have children"""
        return pattern in PATTERNS_WITH_CHILDREN
    
    def _determine_child_count(self, household: Household, adults: List[Person]) -> int:
        """
        Determine number of children based on parent age and pattern.
        """
        pattern = household.pattern
        metadata = PATTERN_METADATA.get(pattern, PATTERN_METADATA['other'])
        expected_range = metadata.get('expected_children', (0, 0))
        
        # Get reference parent (youngest adult, as they set the constraint)
        parent = min(adults, key=lambda a: a.age)
        parent_bracket = self._get_parent_age_bracket(parent.age)
        
        # Sample from children_by_parent_age distribution
        children_dist = self.distributions.get('children_by_parent_age')
        
        if children_dist is not None and len(children_dist) > 0:
            # Filter to parent's age bracket
            filtered = children_dist[children_dist['parent_age_bracket'] == parent_bracket]
            
            if len(filtered) > 0:
                row = weighted_sample(filtered, 'weight')
                num_children = int(row['num_children'])
            else:
                # Fallback: random within expected range
                num_children = np.random.randint(
                    expected_range[0], 
                    expected_range[1] + 1
                )
        else:
            # Fallback: random within expected range
            num_children = np.random.randint(
                expected_range[0], 
                expected_range[1] + 1
            )
        
        # Clamp to pattern's expected range
        min_children, max_children = expected_range
        
        # Patterns requiring children must have at least 1
        if pattern in PATTERNS_REQUIRING_CHILDREN:
            min_children = max(1, min_children)
        
        # Multigenerational sub-pattern specific rules
        if pattern == 'multigenerational':
            sub_pattern = household.multigenerational_subpattern
            if sub_pattern == 'grandparent_with_grandchildren':
                # Must have at least 1 grandchild to be "grandparent with grandchildren"
                min_children = max(1, min_children)
            elif sub_pattern == 'four_generations':
                # Four generations implies children exist
                min_children = max(1, min_children)
            # adult_with_parent doesn't require children (the "multi" comes from parent)
        
        num_children = max(min_children, min(max_children, num_children))
        
        return num_children
    
    def _assign_child_relationships(
        self, 
        pattern: str, 
        num_children: int,
        household: Household
    ) -> List[RelationshipType]:
        """
        Assign relationship types to children based on pattern.
        """
        if num_children == 0:
            return []
        
        if pattern == 'blended_family':
            # Sample from stepchild_patterns to determine mix
            return self._assign_blended_family_relationships(num_children)
        
        elif pattern == 'multigenerational':
            # Determine based on multigenerational sub-pattern
            return self._assign_multigenerational_child_relationships(
                num_children, household
            )
        
        else:
            # Default: all biological children
            return [RelationshipType.BIOLOGICAL_CHILD] * num_children
    
    def _assign_blended_family_relationships(
        self, 
        num_children: int
    ) -> List[RelationshipType]:
        """
        Assign relationships for blended family using stepchild_patterns.
        """
        stepchild_patterns = self.distributions.get('stepchild_patterns')
        
        if stepchild_patterns is not None and len(stepchild_patterns) > 0:
            # Sample a pattern
            pattern_row = weighted_sample(stepchild_patterns, 'weighted_count')
            pattern_name = pattern_row['pattern']
            
            # Parse pattern to determine split
            # Patterns like: 'bio_only', 'step_only', 'bio_and_step'
            if 'step_only' in pattern_name:
                return [RelationshipType.STEPCHILD] * num_children
            elif 'bio_only' in pattern_name:
                return [RelationshipType.BIOLOGICAL_CHILD] * num_children
            else:
                # Mix: half bio, half step (at least 1 of each)
                num_step = max(1, num_children // 2)
                num_bio = num_children - num_step
                relationships = (
                    [RelationshipType.BIOLOGICAL_CHILD] * num_bio +
                    [RelationshipType.STEPCHILD] * num_step
                )
                np.random.shuffle(relationships)
                return list(relationships)
        
        # Fallback: 50/50 split
        num_step = max(1, num_children // 2)
        num_bio = num_children - num_step
        relationships = (
            [RelationshipType.BIOLOGICAL_CHILD] * num_bio +
            [RelationshipType.STEPCHILD] * num_step
        )
        np.random.shuffle(relationships)
        return list(relationships)
    
    def _assign_multigenerational_child_relationships(
        self, 
        num_children: int,
        household: Household
    ) -> List[RelationshipType]:
        """
        Assign child relationships for multigenerational households.
        
        Uses the sub-pattern stored on household during Stage 2.
        """
        # Use stored sub-pattern from Stage 2 (consistent with adult generation)
        sub_pattern = household.multigenerational_subpattern
        
        # Fallback if not set (shouldn't happen)
        if not sub_pattern:
            multi_patterns = self.distributions.get('multigenerational_patterns')
            if multi_patterns is not None and len(multi_patterns) > 0:
                row = weighted_sample(multi_patterns, 'weighted_count')
                sub_pattern = row['pattern']
            else:
                sub_pattern = 'adult_with_parent'
        
        if sub_pattern == 'grandparent_with_grandchildren':
            # Householder is grandparent, children are grandchildren
            return [RelationshipType.GRANDCHILD] * num_children
        
        elif sub_pattern == 'adult_with_parent':
            # Householder is middle generation, has own children
            return [RelationshipType.BIOLOGICAL_CHILD] * num_children
        
        elif sub_pattern == 'four_generations':
            # Mix of children and grandchildren
            if num_children >= 2:
                num_grandchildren = max(1, num_children // 2)
                num_children_bio = num_children - num_grandchildren
                relationships = (
                    [RelationshipType.BIOLOGICAL_CHILD] * num_children_bio +
                    [RelationshipType.GRANDCHILD] * num_grandchildren
                )
                np.random.shuffle(relationships)
                return list(relationships)
            else:
                # Single child - randomly bio or grandchild
                return [np.random.choice([
                    RelationshipType.BIOLOGICAL_CHILD,
                    RelationshipType.GRANDCHILD
                ])]
        
        # Default: biological children
        return [RelationshipType.BIOLOGICAL_CHILD] * num_children
    
    def _generate_single_child(
        self,
        household: Household,
        adults: List[Person],
        relationship: RelationshipType,
        existing_children: List[Person]
    ) -> Person:
        """Generate a single child with all demographics"""
        
        person_id = str(uuid.uuid4())
        
        # Get reference parent for age constraints
        if relationship == RelationshipType.GRANDCHILD:
            # For grandchildren, use oldest adult as reference
            reference_adult = max(adults, key=lambda a: a.age)
            min_age_gap = 28  # Grandparent must be at least 28 years older
        else:
            # For bio/step children, use youngest adult
            reference_adult = min(adults, key=lambda a: a.age)
            min_age_gap = 14  # Parent must be at least 14 years older
        
        # Generate child attributes
        age = self._sample_child_age(reference_adult, min_age_gap, existing_children)
        sex = self._sample_sex()
        race = self._determine_child_race(adults)
        hispanic = self._determine_child_hispanic(adults)
        education = self._determine_child_education(age)
        employment, occupation_code, occupation_title = self._determine_child_employment(age)
        
        return Person(
            person_id=person_id,
            relationship=relationship,
            age=int(age),
            sex=str(sex),
            race=str(race),
            hispanic_origin=bool(hispanic),
            employment_status=str(employment),
            education=str(education),
            occupation_code=occupation_code,
            occupation_title=occupation_title,
            has_disability=False  # Simplified for children
        )
    
    def _sample_child_age(
        self, 
        reference_adult: Person,
        min_age_gap: int,
        existing_children: List[Person]
    ) -> int:
        """
        Sample child age based on parent age bracket.
        """
        parent_age = reference_adult.age
        parent_bracket = self._get_parent_age_bracket(parent_age)
        
        # Maximum child age based on parent age
        max_child_age = min(17, parent_age - min_age_gap)
        
        if max_child_age < 0:
            # Parent too young for children, use minimum
            return 0
        
        # Sample from child_age_distributions
        child_age_dist = self.distributions.get('child_age_distributions')
        
        if child_age_dist is not None and len(child_age_dist) > 0:
            filtered = child_age_dist[
                child_age_dist['parent_age_bracket'] == parent_bracket
            ]
            
            if len(filtered) > 0:
                row = weighted_sample(filtered, 'weight')
                age_group = row['child_age_group']
                
                # Sample specific age within group
                age = self._sample_age_from_child_group(age_group)
                
                # Clamp to valid range
                return max(0, min(max_child_age, age))
        
        # Fallback: uniform distribution 0 to max_child_age
        return np.random.randint(0, max_child_age + 1)
    
    def _sample_age_from_child_group(self, age_group: str) -> int:
        """Sample specific age from a child age group like '0-5' or '6-12'"""
        age_group = str(age_group).strip()
        
        try:
            if '-' in age_group:
                parts = age_group.split('-')
                min_age = int(parts[0])
                max_age = int(parts[1])
                return np.random.randint(min_age, max_age + 1)
            elif age_group.endswith('+'):
                min_age = int(age_group[:-1])
                return min_age + int(np.random.exponential(2))
            else:
                return int(age_group)
        except (ValueError, IndexError):
            return np.random.randint(0, 18)
    
    def _sample_sex(self) -> str:
        """Sample sex for child (50/50)"""
        return np.random.choice(['M', 'F'])
    
    def _determine_child_race(self, adults: List[Person]) -> str:
        """
        Determine child's race based on parents.
        
        Rules:
        - If both parents same race: child inherits that race
        - If parents different races: child is 'two_or_more' (mixed)
        - Single parent: inherit from that parent
        """
        parent_races = [a.race for a in adults if a.race]
        
        if not parent_races:
            return 'two_or_more'  # Default fallback
        
        unique_races = set(parent_races)
        
        if len(unique_races) == 1:
            # Both parents same race
            return parent_races[0]
        else:
            # Mixed race parents - child is mixed
            # Could also randomly inherit one parent's race
            if np.random.random() < 0.7:
                return 'two_or_more'
            else:
                # 30% chance to inherit one parent's race
                return np.random.choice(parent_races)
    
    def _determine_child_hispanic(self, adults: List[Person]) -> bool:
        """
        Determine child's Hispanic origin based on parents.
        
        If either parent is Hispanic, child has high chance of being Hispanic.
        """
        parent_hispanic = [a.hispanic_origin for a in adults]
        
        if any(parent_hispanic):
            # At least one Hispanic parent
            return np.random.random() < 0.9  # 90% chance Hispanic
        else:
            return False
    
    def _determine_child_education(self, age: int) -> str:
        """
        Determine child's education based on age.
        """
        if age < 5:
            return 'none'
        elif age < 6:
            return 'preschool'
        elif age < 14:
            return 'elementary_middle'
        elif age < 18:
            return 'high_school'
        else:
            return 'high_school'  # Shouldn't happen for children
    
    def _determine_child_employment(
        self, 
        age: int
    ) -> Tuple[str, Optional[str], Optional[str]]:
        """
        Determine child's employment status.
        
        Returns:
            Tuple of (employment_status, occupation_code, occupation_title)
        """
        if age < 14:
            # Too young to work
            return EmploymentStatus.NOT_IN_LABOR_FORCE.value, None, None
        
        elif age < 16:
            # 14-15: very limited work (paper routes, etc.)
            if np.random.random() < 0.1:  # 10% chance
                occ = TEEN_OCCUPATIONS[np.random.randint(len(TEEN_OCCUPATIONS))]
                return EmploymentStatus.EMPLOYED.value, occ[0], occ[1]
            else:
                return EmploymentStatus.NOT_IN_LABOR_FORCE.value, None, None
        
        else:
            # 16-17: typical teen employment
            if np.random.random() < TEEN_EMPLOYMENT_RATE:
                occ = TEEN_OCCUPATIONS[np.random.randint(len(TEEN_OCCUPATIONS))]
                return EmploymentStatus.EMPLOYED.value, occ[0], occ[1]
            else:
                return EmploymentStatus.NOT_IN_LABOR_FORCE.value, None, None
    
    def _get_parent_age_bracket(self, age: int) -> str:
        """Convert age to parent age bracket matching the distribution table"""
        # Check what brackets exist in the table
        children_dist = self.distributions.get('children_by_parent_age')
        
        if children_dist is not None and len(children_dist) > 0:
            brackets = children_dist['parent_age_bracket'].unique()
            
            # Find matching bracket
            for bracket in brackets:
                if self._age_in_bracket(age, str(bracket)):
                    return bracket
        
        # Fallback brackets
        if age < 25:
            return '18-24'
        elif age < 35:
            return '25-34'
        elif age < 45:
            return '35-44'
        elif age < 55:
            return '45-54'
        else:
            return '55+'
    
    def _age_in_bracket(self, age: int, bracket: str) -> bool:
        """Check if age falls within bracket like '25-34' or '55+'"""
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
