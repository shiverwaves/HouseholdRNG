#!/usr/bin/env python3
"""
Tax Household Generator

Generates realistic US households with demographic data, employment, occupations, and income
for tax preparation practice scenarios. Combines family generation with occupation/income data.

Features:
- Census-based demographic family generation
- Occupation-based income generation for employed members
- Tax-relevant household data (dependents, filing status, income types)
- Export to JSON for tax software practice

Usage:
    python tax_household_generator.py                           # Generate 5 sample households
    python tax_household_generator.py --count 20                # Generate 20 households
    python tax_household_generator.py --state CA                # Generate from California only
    python tax_household_generator.py --export tax_scenarios.json  # Export to JSON
    python tax_household_generator.py --tax-year 2023           # Generate for specific tax year
"""

import psycopg2
import random
import json
import sys
import os
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, date
import math

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# State cost of living adjustments for income calculation
STATE_ADJUSTMENTS = {
    "New York": 1.35, "California": 1.35, "Hawaii": 1.30, "Massachusetts": 1.25,
    "Connecticut": 1.20, "New Jersey": 1.18, "Washington": 1.15, "Maryland": 1.15,
    "Colorado": 1.10, "Rhode Island": 1.10, "New Hampshire": 1.08, "Virginia": 1.08,
    "Oregon": 1.08, "Pennsylvania": 1.05, "Delaware": 1.05, "Nevada": 1.05,
    "Florida": 1.02, "Minnesota": 1.02, "Maine": 1.02, "Utah": 1.02,
    "Vermont": 1.03, "Illinois": 1.05, "Arizona": 0.98, "Texas": 0.98,
    "Wyoming": 0.98, "North Carolina": 0.95, "Georgia": 0.95, "Wisconsin": 0.95,
    "Montana": 0.95, "North Dakota": 0.95, "Michigan": 0.92, "Idaho": 0.92,
    "Ohio": 0.90, "Tennessee": 0.90, "New Mexico": 0.90, "South Carolina": 0.90,
    "Indiana": 0.88, "Iowa": 0.88, "Kansas": 0.88, "Missouri": 0.88,
    "Kentucky": 0.88, "Louisiana": 0.88, "Oklahoma": 0.88, "Nebraska": 0.90,
    "South Dakota": 0.88, "Alabama": 0.85, "Arkansas": 0.85, "West Virginia": 0.85,
    "Mississippi": 0.82
}

class TaxHouseholdGenerator:
    """
    Generate complete tax households with demographic data, employment, and income
    """
    
    def __init__(self, tax_year: int = None):
        self.tax_year = tax_year or datetime.now().year - 1  # Default to previous year
        self.connection_string = os.getenv('NEON_CONNECTION_STRING')
        if not self.connection_string:
            raise ValueError("NEON_CONNECTION_STRING environment variable is required")
        
        self.conn = psycopg2.connect(self.connection_string)
        self.cursor = self.conn.cursor()
        
        # Cache demographic and occupation data
        self._demographic_cache = {}
        self._occupation_cache = {}
        self._load_demographic_cache()
        self._load_occupation_cache()
    
    def _load_demographic_cache(self):
        """Load Census demographic data for family generation"""
        print("Loading demographic data...")
        
        # Load state demographics
        self.cursor.execute("""
            SELECT sd.state_code, sd.state_name, r.region_name, 
                   sd.total_population, sd.population_weight
            FROM state_demographics sd
            JOIN regions r ON sd.region_id = r.id
            WHERE sd.total_population > 0
            ORDER BY sd.population_weight DESC
        """)
        
        self._demographic_cache['states'] = {}
        self._demographic_cache['state_weights'] = {}
        
        for row in self.cursor.fetchall():
            state_code, state_name, region_name, total_pop, pop_weight = row
            self._demographic_cache['states'][state_code] = {
                'name': state_name,
                'region': region_name,
                'population': total_pop,
                'weight': float(pop_weight) if pop_weight else 0
            }
            self._demographic_cache['state_weights'][state_code] = float(pop_weight) if pop_weight else 0
        
        # Load race/ethnicity distributions
        self.cursor.execute("""
            SELECT sd.state_code, re.race_key, re.race_name, sre.population_percent
            FROM state_race_ethnicity sre
            JOIN state_demographics sd ON sre.state_id = sd.id
            JOIN race_ethnicity re ON sre.race_id = re.id
            ORDER BY sd.state_code, sre.population_percent DESC
        """)
        
        self._demographic_cache['state_race_data'] = {}
        for row in self.cursor.fetchall():
            state_code, race_key, race_name, race_percent = row
            if state_code not in self._demographic_cache['state_race_data']:
                self._demographic_cache['state_race_data'][state_code] = {}
            
            self._demographic_cache['state_race_data'][state_code][race_key] = {
                'name': race_name,
                'percent': float(race_percent)
            }
        
        # Load family structure distributions
        self.cursor.execute("""
            SELECT sd.state_code, fs.structure_key, fs.structure_name, sfs.probability_percent
            FROM state_family_structures sfs
            JOIN state_demographics sd ON sfs.state_id = sd.id
            JOIN family_structures fs ON sfs.structure_id = fs.id
            ORDER BY sd.state_code, sfs.probability_percent DESC
        """)
        
        self._demographic_cache['state_family_structures'] = {}
        for row in self.cursor.fetchall():
            state_code, structure_key, structure_name, structure_percent = row
            if state_code not in self._demographic_cache['state_family_structures']:
                self._demographic_cache['state_family_structures'][state_code] = {}
            
            self._demographic_cache['state_family_structures'][state_code][structure_key] = {
                'name': structure_name,
                'percent': float(structure_percent)
            }
        
        print(f"  Loaded demographic data for {len(self._demographic_cache['states'])} states")
    
    def _load_occupation_cache(self):
        """Load occupation and wage data for income generation"""
        print("Loading occupation data...")
        
        try:
            # Load occupation data from OEWS schema
            self.cursor.execute("""
                SELECT occ_title, area_title, a_median, a_mean, tot_emp
                FROM oews.employment_wages 
                WHERE a_median IS NOT NULL AND tot_emp > 0
                ORDER BY tot_emp DESC
            """)
            
            self._occupation_cache['occupations'] = []
            self._occupation_cache['state_occupations'] = {}
            
            for row in self.cursor.fetchall():
                occ_title, area_title, a_median, a_mean, tot_emp = row
                
                occupation_data = {
                    'title': occ_title,
                    'area': area_title,
                    'median_wage': float(a_median) if a_median else None,
                    'mean_wage': float(a_mean) if a_mean else None,
                    'employment': int(tot_emp) if tot_emp else 0
                }
                
                self._occupation_cache['occupations'].append(occupation_data)
                
                # Group by state for targeted selection
                if area_title not in self._occupation_cache['state_occupations']:
                    self._occupation_cache['state_occupations'][area_title] = []
                self._occupation_cache['state_occupations'][area_title].append(occupation_data)
            
            print(f"  Loaded {len(self._occupation_cache['occupations'])} occupation records")
            
        except Exception as e:
            print(f"  Warning: Could not load occupation data: {e}")
            self._occupation_cache['occupations'] = []
            self._occupation_cache['state_occupations'] = {}
    
    def _weighted_random_selection(self, items: Dict[str, Any], weight_key: str = 'percent') -> str:
        """Select item based on weighted probabilities"""
        if not items:
            return None
        
        # Calculate cumulative weights
        total_weight = 0
        cumulative_weights = []
        item_keys = list(items.keys())
        
        for key in item_keys:
            # Handle both dict and direct float values
            if isinstance(items[key], dict):
                weight = items[key].get(weight_key, items[key].get('weight', 1))
            else:
                # Direct float/numeric value (like in state_weights)
                weight = float(items[key]) if items[key] is not None else 1
            
            total_weight += weight
            cumulative_weights.append(total_weight)
        
        if total_weight == 0:
            return random.choice(item_keys)
        
        rand_val = random.uniform(0, total_weight)
        for i, cum_weight in enumerate(cumulative_weights):
            if rand_val <= cum_weight:
                return item_keys[i]
        
        return item_keys[0]
    
    def _select_state(self, target_state: str = None) -> str:
        """Select state based on population weights or target"""
        if target_state:
            if target_state.upper() in self._demographic_cache['states']:
                return target_state.upper()
            else:
                for state_code, state_data in self._demographic_cache['states'].items():
                    if state_data['name'].upper() == target_state.upper():
                        return state_code
                raise ValueError(f"State '{target_state}' not found")
        
        return self._weighted_random_selection(self._demographic_cache['state_weights'], 'weight')
    
    def _get_random_occupation(self, state_name: str = None) -> Optional[Dict]:
        """Get random occupation, optionally filtered by state"""
        occupations = self._occupation_cache.get('occupations', [])
        if not occupations:
            return None
        
        if state_name and state_name in self._occupation_cache['state_occupations']:
            state_occupations = self._occupation_cache['state_occupations'][state_name]
            if state_occupations:
                # Weight by employment numbers
                weights = [occ['employment'] for occ in state_occupations]
                return random.choices(state_occupations, weights=weights)[0]
        
        # Fallback to national data
        weights = [occ['employment'] for occ in occupations]
        return random.choices(occupations, weights=weights)[0]
    
    def _generate_income(self, occupation_title: str, age: int, state_name: str, 
                        employment_status: str) -> Dict[str, Any]:
        """Generate realistic income based on occupation and demographics"""
        if employment_status not in ["Employed", "Self-Employed"]:
            return {'annual_income': 0, 'income_type': 'No Income'}
        
        # Find occupation data
        occupation_data = None
        for occ in self._occupation_cache.get('occupations', []):
            if occ['title'].lower() == occupation_title.lower() and (
                occ['area'] == state_name or occ['area'] == 'National'
            ):
                occupation_data = occ
                break
        
        if not occupation_data:
            # Fallback to similar occupation or random
            base_wage = 45000  # National median fallback
        else:
            base_wage = occupation_data['median_wage']
        
        # Apply age/experience multiplier
        if age < 25:
            exp_multiplier = random.uniform(0.70, 0.85)
        elif age < 35:
            exp_multiplier = random.uniform(0.85, 1.10)
        elif age < 45:
            exp_multiplier = random.uniform(0.95, 1.25)
        elif age < 55:
            exp_multiplier = random.uniform(1.05, 1.40)
        elif age < 65:
            exp_multiplier = random.uniform(1.00, 1.35)
        else:
            exp_multiplier = random.uniform(0.90, 1.20)
        
        # Apply geographic multiplier
        geo_multiplier = STATE_ADJUSTMENTS.get(state_name, 1.0)
        
        # Apply employment status multiplier
        if employment_status == "Self-Employed":
            emp_multiplier = random.uniform(0.6, 1.8)
            income_type = "1099-MISC"
        else:
            emp_multiplier = 1.0
            income_type = "W-2"
        
        # Add random variation
        variation = random.uniform(0.8, 1.2)
        
        # Calculate final income
        final_income = base_wage * exp_multiplier * geo_multiplier * emp_multiplier * variation
        annual_income = max(int(round(final_income, -2)), 15080)  # Round to nearest $100, min wage
        
        return {
            'annual_income': annual_income,
            'base_wage': int(base_wage),
            'income_type': income_type,
            'occupation': occupation_title
        }
    
    def _generate_family_member(self, role: str, age: int, gender: str, race_name: str, 
                               state_code: str, state_name: str) -> Dict[str, Any]:
        """Generate a complete family member with employment and income if applicable"""
        
        # Determine employment status based on age and role
        employment_status = self._determine_employment_status(age, role)
        
        member = {
            "role": role,
            "age": age,
            "gender": gender,
            "race": race_name,
            "employment_status": employment_status,
            "is_dependent": self._is_dependent(age, role),
            "tax_year": self.tax_year
        }
        
        # Generate occupation and income for employed members
        if employment_status in ["Employed", "Self-Employed"]:
            occupation_data = self._get_random_occupation(state_name)
            if occupation_data:
                income_data = self._generate_income(
                    occupation_data['title'], age, state_name, employment_status
                )
                member.update({
                    "occupation": occupation_data['title'],
                    "annual_income": income_data['annual_income'],
                    "income_type": income_data['income_type'],
                    "base_median_wage": income_data['base_wage']
                })
                
                # TODO: Add multiple income sources for some members
                # TODO: Add investment income for higher earners
                # TODO: Add unemployment compensation for some
                
            else:
                # Fallback income estimation
                member.update({
                    "occupation": "General Worker",
                    "annual_income": random.randint(25000, 60000),
                    "income_type": "W-2" if employment_status == "Employed" else "1099-MISC",
                    "base_median_wage": 40000
                })
        else:
            member.update({
                "occupation": None,
                "annual_income": 0,
                "income_type": "No Income",
                "base_median_wage": None
            })
        
        # TODO: Add tax-specific fields
        # member["tax_withholding"] = self._calculate_withholding(member)
        # member["retirement_contributions"] = self._generate_retirement_contrib(member)
        # member["health_insurance_premiums"] = self._generate_health_premiums(member)
        
        return member
    
    def _determine_employment_status(self, age: int, role: str) -> str:
        """Determine employment status based on age and family role"""
        if age < 16:
            return "Minor (Not Working)"
        elif age >= 67:
            return "Retired" if random.random() < 0.75 else "Employed"
        elif 16 <= age <= 24 and role == 'Child':
            return random.choices(
                ["Student", "Student (Part-time)", "Employed", "Unemployed"],
                weights=[40, 30, 25, 5]
            )[0]
        else:
            return random.choices(
                ["Employed", "Self-Employed", "Unemployed", "Not in Labor Force"],
                weights=[70, 12, 8, 10]
            )[0]
    
    def _is_dependent(self, age: int, role: str) -> bool:
        """Determine if person qualifies as a tax dependent"""
        if role == 'Child' and age < 19:
            return True
        elif role == 'Child' and age < 24 and random.random() < 0.6:  # Student dependency
            return True
        elif age >= 65 and role == 'Parent':  # Elderly parent dependency
            return random.random() < 0.3
        return False
    
    def _generate_age(self, role: str, context: Dict = None) -> int:
        """Generate realistic age based on family role"""
        context = context or {}
        
        if role == 'Head of Household':
            if context.get('has_children'):
                return random.randint(25, 55)
            else:
                return random.randint(25, 75)
        elif role == 'Spouse':
            head_age = context.get('head_age', 40)
            age_diff = random.randint(-8, 8)
            return max(18, min(80, head_age + age_diff))
        elif role == 'Child':
            parent_age = context.get('parent_age', 35)
            child_number = context.get('child_number', 1)
            if child_number == 1:
                max_age = min(25, parent_age - 16)  # Extended for college students
                return random.randint(0, max_age)
            else:
                previous_age = context.get('previous_child_age', 10)
                return max(0, previous_age - random.randint(1, 4))
        else:
            return random.randint(18, 80)
    
    def _generate_gender(self, role: str, context: Dict = None) -> str:
        """Generate gender with realistic distribution"""
        context = context or {}
        
        if role == 'Spouse':
            head_gender = context.get('head_gender', 'Male')
            # Allow same-sex marriages (about 5% of marriages)
            if random.random() < 0.05:
                return head_gender
            return 'Female' if head_gender == 'Male' else 'Male'
        else:
            return 'Female' if random.random() < 0.51 else 'Male'
    
    def generate_household(self, target_state: str = None) -> Dict[str, Any]:
        """Generate a complete tax household"""
        
        # Select state and basic demographics
        state_code = self._select_state(target_state)
        state_info = self._demographic_cache['states'][state_code]
        state_name = state_info['name']
        
        # Select race/ethnicity
        race_data = self._demographic_cache['state_race_data'].get(state_code, {})
        race_key = self._weighted_random_selection(race_data, 'percent') if race_data else 'WHITE_NON_HISPANIC'
        race_name = race_data.get(race_key, {}).get('name', 'White Non-Hispanic')
        
        # Select family structure
        structure_data = self._demographic_cache['state_family_structures'].get(state_code, {})
        structure_key = self._weighted_random_selection(structure_data, 'percent') if structure_data else 'MARRIED_COUPLE'
        
        # Generate household members based on structure
        members = []
        filing_status = "Single"
        
        if structure_key == 'MARRIED_COUPLE':
            members, filing_status = self._generate_married_couple_household(
                race_name, state_code, state_name
            )
        elif structure_key == 'SINGLE_PERSON':
            members, filing_status = self._generate_single_person_household(
                race_name, state_code, state_name
            )
        elif structure_key in ['SINGLE_PARENT_FEMALE', 'SINGLE_PARENT_MALE']:
            parent_gender = 'Female' if structure_key == 'SINGLE_PARENT_FEMALE' else 'Male'
            members, filing_status = self._generate_single_parent_household(
                race_name, state_code, state_name, parent_gender
            )
        else:
            # Default to married couple
            members, filing_status = self._generate_married_couple_household(
                race_name, state_code, state_name
            )
        
        # Calculate household totals
        total_income = sum(member.get('annual_income', 0) for member in members)
        dependents = [member for member in members if member.get('is_dependent', False)]
        
        household = {
            "household_id": f"TAX_{self.tax_year}_{random.randint(100000, 999999)}",
            "tax_year": self.tax_year,
            "state_code": state_code,
            "state_name": state_name,
            "region": state_info['region'],
            "primary_race": race_name,
            "filing_status": filing_status,
            "household_size": len(members),
            "number_of_dependents": len(dependents),
            "total_household_income": total_income,
            "primary_taxpayer": self._identify_primary_taxpayer(members),
            "generation_date": datetime.now().isoformat(),
            "members": members
        }
        
        # TODO: Add additional tax scenario elements
        # household["itemized_deductions"] = self._generate_deductions(household)
        # household["tax_credits"] = self._calculate_eligible_credits(household)
        # household["estimated_tax_liability"] = self._estimate_tax_liability(household)
        # household["complexity_score"] = self._calculate_complexity_score(household)
        
        return household
    
    def _generate_married_couple_household(self, race_name: str, state_code: str, 
                                          state_name: str) -> Tuple[List[Dict], str]:
        """Generate married couple household"""
        members = []
        
        # Head of household
        head_age = self._generate_age('Head of Household', {'has_children': random.random() < 0.6})
        head_gender = self._generate_gender('Head of Household')
        
        head = self._generate_family_member(
            'Head of Household', head_age, head_gender, race_name, state_code, state_name
        )
        members.append(head)
        
        # Spouse
        spouse_age = self._generate_age('Spouse', {'head_age': head_age})
        spouse_gender = self._generate_gender('Spouse', {'head_gender': head_gender})
        
        spouse = self._generate_family_member(
            'Spouse', spouse_age, spouse_gender, race_name, state_code, state_name
        )
        members.append(spouse)
        
        # Children (60% chance of having children)
        if random.random() < 0.6:
            child_count = random.choices([1, 2, 3, 4], weights=[35, 40, 20, 5])[0]
            oldest_parent_age = max(head_age, spouse_age)
            
            previous_child_age = None
            for i in range(child_count):
                child_age = self._generate_age('Child', {
                    'parent_age': oldest_parent_age,
                    'child_number': i + 1,
                    'previous_child_age': previous_child_age
                })
                child_gender = self._generate_gender('Child')
                
                child = self._generate_family_member(
                    'Child', child_age, child_gender, race_name, state_code, state_name
                )
                members.append(child)
                previous_child_age = child_age
        
        filing_status = "Married Filing Jointly"  # Could add "Married Filing Separately" logic
        return members, filing_status
    
    def _generate_single_person_household(self, race_name: str, state_code: str, 
                                         state_name: str) -> Tuple[List[Dict], str]:
        """Generate single person household"""
        age = self._generate_age('Head of Household', {'has_children': False})
        gender = self._generate_gender('Head of Household')
        
        member = self._generate_family_member(
            'Head of Household', age, gender, race_name, state_code, state_name
        )
        
        return [member], "Single"
    
    def _generate_single_parent_household(self, race_name: str, state_code: str, 
                                         state_name: str, parent_gender: str) -> Tuple[List[Dict], str]:
        """Generate single parent household"""
        members = []
        
        # Single parent
        parent_age = random.randint(20, 50)
        parent = self._generate_family_member(
            'Head of Household', parent_age, parent_gender, race_name, state_code, state_name
        )
        members.append(parent)
        
        # Children
        child_count = random.choices([1, 2, 3], weights=[50, 35, 15])[0]
        
        previous_child_age = None
        for i in range(child_count):
            child_age = self._generate_age('Child', {
                'parent_age': parent_age,
                'child_number': i + 1,
                'previous_child_age': previous_child_age
            })
            child_gender = self._generate_gender('Child')
            
            child = self._generate_family_member(
                'Child', child_age, child_gender, race_name, state_code, state_name
            )
            members.append(child)
            previous_child_age = child_age
        
        filing_status = "Head of Household"
        return members, filing_status
    
    def _identify_primary_taxpayer(self, members: List[Dict]) -> Dict[str, Any]:
        """Identify the primary taxpayer (highest earner or head of household)"""
        primary = None
        highest_income = 0
        
        for member in members:
            if member['role'] == 'Head of Household':
                primary = member
                break
            elif member.get('annual_income', 0) > highest_income:
                highest_income = member.get('annual_income', 0)
                primary = member
        
        return {
            "name": f"{primary['gender']} {primary['role']}",
            "age": primary['age'],
            "income": primary.get('annual_income', 0),
            "occupation": primary.get('occupation', 'Unemployed')
        }
    
    def generate_households(self, count: int = 5, target_state: str = None) -> List[Dict[str, Any]]:
        """Generate multiple tax households"""
        households = []
        
        print(f"Generating {count} tax households for {self.tax_year}...")
        if target_state:
            print(f"Targeting state: {target_state}")
        
        for i in range(count):
            try:
                household = self.generate_household(target_state)
                households.append(household)
                
                if (i + 1) % 10 == 0 or i == count - 1:
                    print(f"  Generated {i + 1}/{count} households")
                    
            except Exception as e:
                print(f"  Warning: Failed to generate household {i + 1}: {e}")
                continue
        
        return households
    
    def export_households(self, households: List[Dict], filename: str):
        """Export households to JSON for tax practice scenarios"""
        export_data = {
            "metadata": {
                "total_households": len(households),
                "tax_year": self.tax_year,
                "generation_date": datetime.now().isoformat(),
                "data_sources": [
                    "US Census Bureau ACS 2022",
                    "Bureau of Labor Statistics OEWS",
                    "Neon Database"
                ],
                "purpose": "Tax preparation practice scenarios"
            },
            "households": households
        }
        
        try:
            with open(filename, 'w') as f:
                json.dump(export_data, f, indent=2)
            print(f"✅ Exported {len(households)} households to {filename}")
        except Exception as e:
            print(f"❌ Error exporting households: {e}")
    
    def print_household_summary(self, households: List[Dict]):
        """Print summary of generated households"""
        if not households:
            print("No households to summarize")
            return
        
        print(f"\n=== TAX HOUSEHOLD SUMMARY ===")
        print(f"Tax Year: {self.tax_year}")
        print(f"Total households: {len(households)}")
        
        # Filing status distribution
        filing_counts = {}
        for household in households:
            status = household.get('filing_status', 'Unknown')
            filing_counts[status] = filing_counts.get(status, 0) + 1
        
        print(f"\nFiling Status Distribution:")
        for status, count in sorted(filing_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {status}: {count}")
        
        # Income distribution
        incomes = [h['total_household_income'] for h in households if h['total_household_income']]
        if incomes:
            print(f"\nIncome Statistics:")
            print(f"  Average household income: ${sum(incomes)/len(incomes):,.0f}")
            print(f"  Median household income: ${sorted(incomes)[len(incomes)//2]:,.0f}")
            print(f"  Highest income: ${max(incomes):,.0f}")
            print(f"  Lowest income: ${min(incomes):,.0f}")
        
        # Dependency analysis
        total_dependents = sum(h['number_of_dependents'] for h in households)
        households_with_dependents = sum(1 for h in households if h['number_of_dependents'] > 0)
        
        print(f"\nDependent Analysis:")
        print(f"  Households with dependents: {households_with_dependents}")
        print(f"  Total dependents: {total_dependents}")
        print(f"  Average dependents per household: {total_dependents/len(households):.1f}")
    
    def close(self):
        """Close database connections"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

def format_currency(amount: int) -> str:
    """Format currency for display"""
    if amount >= 1000000:
        return f"${amount/1000000:.1f}M"
    elif amount >= 1000:
        return f"${amount/1000:.0f}K"
    else:
        return f"${amount:,}"

def main():
    """Main execution function"""
    
    print("💰 TAX HOUSEHOLD GENERATOR")
    print("="*50)
    
    # Parse command line arguments
    count = 5
    target_state = None
    export_file = None
    tax_year = datetime.now().year - 1
    
    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == '--count' and i + 1 < len(sys.argv):
            count = int(sys.argv[i + 1])
            i += 2
        elif arg == '--state' and i + 1 < len(sys.argv):
            target_state = sys.argv[i + 1]
            i += 2
        elif arg == '--export' and i + 1 < len(sys.argv):
            export_file = sys.argv[i + 1]
            i += 2
        elif arg == '--tax-year' and i + 1 < len(sys.argv):
            tax_year = int(sys.argv[i + 1])
            i += 2
        else:
            i += 1
    
    try:
        # Initialize generator
        generator = TaxHouseholdGenerator(tax_year)
        
        # Generate households
        households = generator.generate_households(count, target_state)
        
        # Display sample households
        print(f"\n=== SAMPLE TAX HOUSEHOLDS ===")
        for i, household in enumerate(households[:3]):
            print(f"\nHousehold {i + 1} (ID: {household['household_id']}):")
            print(f"  Location: {household['state_name']}")
            print(f"  Filing Status: {household['filing_status']}")
            print(f"  Total Income: {format_currency(household['total_household_income'])}")
            print(f"  Dependents: {household['number_of_dependents']}")
            print(f"  Primary Taxpayer: {household['primary_taxpayer']['occupation']} ({household['primary_taxpayer']['age']} years old)")
            
            print("  Members:")
            for member in household['members']:
                income_info = f", {format_currency(member.get('annual_income', 0))}" if member.get('annual_income', 0) > 0 else ""
                job_info = f" ({member.get('occupation', 'No Job')})" if member.get('occupation') else ""
                print(f"    {member['role']}: {member['gender']}, Age {member['age']}, {member['employment_status']}{job_info}{income_info}")
        
        # Print summary statistics
        generator.print_household_summary(households)
        
        # Export if requested
        if export_file:
            generator.export_households(households, export_file)
        
        print(f"\n✅ Tax household generation completed!")
        print(f"💡 Use these households to practice tax preparation scenarios")
        
        # TODO: Add suggestions for tax scenarios
        # print(f"\n📋 Suggested Practice Scenarios:")
        # print(f"  - Standard vs. Itemized deductions")
        # print(f"  - Child Tax Credit calculations")
        # print(f"  - Earned Income Credit eligibility")
        # print(f"  - Multiple income sources (W-2 + 1099)")
        # print(f"  - Self-employment tax calculations")
        # print(f"  - Student loan interest deduction")
        # print(f"  - Retirement contribution limits")
        
    except Exception as e:
        print(f"\n❌ Generation failed: {e}")
        return False
    finally:
        try:
            generator.close()
        except:
            pass
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

# TODO: Future Enhancements for Tax Practice Scenarios
"""
1. INCOME DIVERSIFICATION:
   - Multiple W-2s for job changes
   - 1099-INT for interest income
   - 1099-DIV for dividend income
   - 1099-R for retirement distributions
   - 1099-G for unemployment compensation
   - Schedule K-1 for partnership income
   - Capital gains/losses from investments

2. DEDUCTION SCENARIOS:
   - Mortgage interest (Schedule A)
   - State and local tax deductions
   - Charitable contributions
   - Medical expenses
   - Business expenses (Schedule C)
   - Home office deduction
   - Vehicle expenses for business use
   - Educational expenses

3. TAX CREDIT SCENARIOS:
   - Child Tax Credit calculations
   - Earned Income Credit
   - American Opportunity Credit
   - Lifetime Learning Credit
   - Child and Dependent Care Credit
   - Premium Tax Credit (ACA)
   - Residential Energy Credit

4. COMPLEX SITUATIONS:
   - Estimated tax payments
   - Alternative Minimum Tax (AMT)
   - Net Investment Income Tax
   - Self-employment tax
   - Retirement plan contributions (401k, IRA, Roth)
   - Health Savings Account (HSA) contributions
   - Flexible Spending Account (FSA) deductions

5. LIFE EVENT SCENARIOS:
   - Marriage/divorce during tax year
   - Birth/adoption of children
   - Job loss and unemployment
   - Home purchase/sale
   - Moving for work
   - Starting a business
   - Retirement
   - Death of spouse

6. STATE TAX COMPLICATIONS:
   - Multi-state income situations
   - State-specific deductions
   - Reciprocity agreements
   - Remote work across state lines

7. COMPLIANCE FEATURES:
   - Form generation templates
   - Error checking capabilities
   - Audit trail documentation
   - Prior year comparisons
   - Amended return scenarios

8. EDUCATIONAL FEATURES:
   - Step-by-step calculation breakdowns
   - Common mistake identification
   - Tax law explanations
   - IRS publication references
   - Practice quiz generation
"""
