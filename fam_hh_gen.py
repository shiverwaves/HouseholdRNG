#!/usr/bin/env python3
"""
Census-Based Family Generator

Generates random US families using state-level demographic data from the US Census Bureau
stored in Neon database. Works with the schema created by combined_census_importer.py

Usage:
    python census_family_generator.py                    # Generate 5 sample families
    python census_family_generator.py --count 20         # Generate 20 families
    python census_family_generator.py --state CA         # Generate families from California only
    python census_family_generator.py --export families.json  # Export families to JSON
"""

import psycopg2
import random
import json
import sys
import os
from typing import Dict, List, Any, Optional
from datetime import datetime

class CensusFamilyGenerator:
    """
    Generate random families using state-level Census demographic data
    """
    
    def __init__(self):
        self.connection_string = os.getenv('NEON_CONNECTION_STRING')
        if not self.connection_string:
            raise ValueError("NEON_CONNECTION_STRING environment variable is required")
        
        self.conn = psycopg2.connect(self.connection_string)
        self.cursor = self.conn.cursor()
        
        # Cache data for performance
        self._cache = {}
        self._load_cache()
    
    def _load_cache(self):
        """Load demographic data into memory for efficient family generation"""
        
        print("Loading demographic data...")
        
        # Load state demographics with region info
        self.cursor.execute("""
            SELECT sd.state_code, sd.state_name, r.region_name, 
                   sd.total_population, sd.population_weight
            FROM state_demographics sd
            JOIN regions r ON sd.region_id = r.id
            WHERE sd.total_population > 0
            ORDER BY sd.population_weight DESC
        """)
        
        self._cache['states'] = {}
        self._cache['state_weights'] = {}
        
        for row in self.cursor.fetchall():
            state_code, state_name, region_name, total_pop, pop_weight = row
            self._cache['states'][state_code] = {
                'name': state_name,
                'region': region_name,
                'population': total_pop,
                'weight': float(pop_weight) if pop_weight else 0
            }
            self._cache['state_weights'][state_code] = float(pop_weight) if pop_weight else 0
        
        # Load race/ethnicity data by state
        self.cursor.execute("""
            SELECT sd.state_code, re.race_key, re.race_name, sre.population_percent
            FROM state_race_ethnicity sre
            JOIN state_demographics sd ON sre.state_id = sd.id
            JOIN race_ethnicity re ON sre.race_id = re.id
            ORDER BY sd.state_code, sre.population_percent DESC
        """)
        
        self._cache['state_race_data'] = {}
        for row in self.cursor.fetchall():
            state_code, race_key, race_name, race_percent = row
            if state_code not in self._cache['state_race_data']:
                self._cache['state_race_data'][state_code] = {}
            
            self._cache['state_race_data'][state_code][race_key] = {
                'name': race_name,
                'percent': float(race_percent)
            }
        
        # Load family structure data by state
        self.cursor.execute("""
            SELECT sd.state_code, fs.structure_key, fs.structure_name, sfs.probability_percent
            FROM state_family_structures sfs
            JOIN state_demographics sd ON sfs.state_id = sd.id
            JOIN family_structures fs ON sfs.structure_id = fs.id
            ORDER BY sd.state_code, sfs.probability_percent DESC
        """)
        
        self._cache['state_family_structures'] = {}
        for row in self.cursor.fetchall():
            state_code, structure_key, structure_name, structure_percent = row
            if state_code not in self._cache['state_family_structures']:
                self._cache['state_family_structures'][state_code] = {}
            
            self._cache['state_family_structures'][state_code][structure_key] = {
                'name': structure_name,
                'percent': float(structure_percent)
            }
        
        # Load employment data by state
        self.cursor.execute("""
            SELECT sd.state_code, ses.employment_rate, ses.unemployment_rate, 
                   ses.labor_force_participation_rate
            FROM state_employment_stats ses
            JOIN state_demographics sd ON ses.state_id = sd.id
        """)
        
        self._cache['state_employment'] = {}
        for row in self.cursor.fetchall():
            state_code, emp_rate, unemp_rate, lfp_rate = row
            self._cache['state_employment'][state_code] = {
                'employment_rate': float(emp_rate) if emp_rate else 65.0,
                'unemployment_rate': float(unemp_rate) if unemp_rate else 5.0,
                'labor_force_participation_rate': float(lfp_rate) if lfp_rate else 70.0
            }
        
        # Load race-based employment rates for more realistic generation
        self.cursor.execute("""
            SELECT re.race_key, er.employed_rate, er.unemployed_rate, er.not_in_labor_force_rate
            FROM employment_rates er
            JOIN race_ethnicity re ON er.race_id = re.id
            WHERE er.age_group = 'All'
        """)
        
        self._cache['race_employment'] = {}
        for row in self.cursor.fetchall():
            race_key, emp_rate, unemp_rate, not_in_lf_rate = row
            self._cache['race_employment'][race_key] = {
                'employed_rate': float(emp_rate) if emp_rate else 60.0,
                'unemployed_rate': float(unemp_rate) if unemp_rate else 5.0,
                'not_in_labor_force_rate': float(not_in_lf_rate) if not_in_lf_rate else 35.0
            }
        
        print(f"  Loaded data for {len(self._cache['states'])} states")
        print(f"  Loaded race data for {len(self._cache['state_race_data'])} states")
        print(f"  Loaded family structure data for {len(self._cache['state_family_structures'])} states")
        print(f"  Loaded employment data for {len(self._cache['state_employment'])} states")
    
    def _weighted_random_selection(self, items: Dict[str, Any], weight_key: str = 'percent') -> str:
        """Select item based on weighted probabilities"""
        if not items:
            return None
        
        # Calculate cumulative weights
        total_weight = 0
        cumulative_weights = []
        item_keys = list(items.keys())
        
        for key in item_keys:
            weight = items[key].get(weight_key, items[key].get('weight', 1))
            total_weight += weight
            cumulative_weights.append(total_weight)
        
        # Select based on random value
        if total_weight == 0:
            return random.choice(item_keys)
        
        rand_val = random.uniform(0, total_weight)
        for i, cum_weight in enumerate(cumulative_weights):
            if rand_val <= cum_weight:
                return item_keys[i]
        
        return item_keys[0]
    
    def _select_state(self, target_state: str = None) -> str:
        """Select a state based on population weights or specific target"""
        if target_state:
            if target_state.upper() in self._cache['states']:
                return target_state.upper()
            else:
                # Try to find by state name
                for state_code, state_data in self._cache['states'].items():
                    if state_data['name'].upper() == target_state.upper():
                        return state_code
                raise ValueError(f"State '{target_state}' not found in database")
        
        return self._weighted_random_selection(self._cache['state_weights'], 'weight')
    
    def _select_race_ethnicity(self, state_code: str) -> Dict[str, str]:
        """Select race/ethnicity based on state demographics"""
        state_races = self._cache['state_race_data'].get(state_code, {})
        
        if not state_races:
            # Fallback to national averages if state data missing
            race_key = 'WHITE_NON_HISPANIC'  # Most common fallback
        else:
            race_key = self._weighted_random_selection(state_races, 'percent')
        
        race_name = state_races.get(race_key, {}).get('name', 'White Non-Hispanic')
        
        return {
            'race_key': race_key,
            'race_name': race_name
        }
    
    def _select_family_structure(self, state_code: str) -> Dict[str, str]:
        """Select family structure based on state demographics"""
        state_structures = self._cache['state_family_structures'].get(state_code, {})
        
        if not state_structures:
            # Fallback structure distribution
            structure_key = random.choice(['MARRIED_COUPLE', 'SINGLE_PERSON', 'SINGLE_PARENT_FEMALE'])
        else:
            structure_key = self._weighted_random_selection(state_structures, 'percent')
        
        structure_name = state_structures.get(structure_key, {}).get('name', structure_key.replace('_', ' ').title())
        
        return {
            'structure_key': structure_key,
            'structure_name': structure_name
        }
    
    def _get_employment_status(self, age: int, race_key: str, state_code: str, role: str) -> str:
        """Determine employment status based on age, race, state, and role"""
        
        # Children under 16 are not in labor force
        if age < 16:
            return "Child (Under 16)"
        
        # Retirement logic (simple)
        if age >= 67:
            return "Retired" if random.random() < 0.8 else "Employed"
        
        # Students (18-24) have different employment patterns
        if 18 <= age <= 24 and role == 'CHILD':
            if random.random() < 0.6:  # 60% are students
                return "Student" if random.random() < 0.7 else "Student (Employed)"
        
        # Use race-based employment rates if available
        race_employment = self._cache['race_employment'].get(race_key)
        if race_employment:
            rand_val = random.uniform(0, 100)
            if rand_val < race_employment['employed_rate']:
                return "Employed"
            elif rand_val < race_employment['employed_rate'] + race_employment['unemployed_rate']:
                return "Unemployed"
            else:
                return "Not in Labor Force"
        
        # Fallback to state employment rates
        state_employment = self._cache['state_employment'].get(state_code, {})
        emp_rate = state_employment.get('employment_rate', 65.0)
        unemp_rate = state_employment.get('unemployment_rate', 5.0)
        
        rand_val = random.uniform(0, 100)
        if rand_val < emp_rate:
            return "Employed"
        elif rand_val < emp_rate + unemp_rate:
            return "Unemployed"
        else:
            return "Not in Labor Force"
    
    def _generate_age(self, role: str, context: Dict = None) -> int:
        """Generate realistic age based on role and context"""
        context = context or {}
        
        if role == 'HEAD':
            # Head of household typically 25-75
            if context.get('has_children'):
                return random.randint(25, 55)  # Parents are younger
            else:
                return random.randint(25, 75)
        
        elif role == 'SPOUSE':
            head_age = context.get('head_age', 40)
            # Spouse typically within 10 years of head
            age_diff = random.randint(-8, 8)
            return max(18, min(80, head_age + age_diff))
        
        elif role == 'CHILD':
            parent_age = context.get('parent_age', 35)
            child_number = context.get('child_number', 1)
            
            # First child: 0-17, subsequent children are younger
            if child_number == 1:
                max_age = min(17, parent_age - 18)
                return random.randint(0, max_age)
            else:
                previous_child_age = context.get('previous_child_age', 10)
                return max(0, previous_child_age - random.randint(1, 4))
        
        elif role == 'SINGLE_PARENT':
            return random.randint(18, 55)
        
        else:
            return random.randint(18, 80)
    
    def _generate_gender(self, role: str, context: Dict = None) -> str:
        """Generate gender with realistic probabilities"""
        context = context or {}
        
        if role == 'SPOUSE':
            head_gender = context.get('head_gender', 'Male')
            return 'Female' if head_gender == 'Male' else 'Male'
        
        elif role == 'SINGLE_PARENT_FEMALE':
            return 'Female'
        
        elif role == 'SINGLE_PARENT_MALE':
            return 'Male'
        
        else:
            # Roughly 50/50 with slight female bias for general population
            return 'Female' if random.random() < 0.51 else 'Male'
    
    def _create_family_member(self, role: str, age: int, gender: str, employment_status: str, race_name: str) -> Dict[str, Any]:
        """Create a family member dictionary"""
        return {
            "role": role,
            "age": age,
            "gender": gender,
            "employment_status": employment_status,
            "race": race_name
        }
    
    def _generate_married_couple_family(self, race_info: Dict, state_code: str, has_children: bool = True) -> List[Dict]:
        """Generate married couple family with or without children"""
        members = []
        
        # Generate head of household
        head_age = self._generate_age('HEAD', {'has_children': has_children})
        head_gender = self._generate_gender('HEAD')
        head_employment = self._get_employment_status(head_age, race_info['race_key'], state_code, 'HEAD')
        
        members.append(self._create_family_member(
            'Head of Household', head_age, head_gender, head_employment, race_info['race_name']
        ))
        
        # Generate spouse
        spouse_age = self._generate_age('SPOUSE', {'head_age': head_age})
        spouse_gender = self._generate_gender('SPOUSE', {'head_gender': head_gender})
        spouse_employment = self._get_employment_status(spouse_age, race_info['race_key'], state_code, 'SPOUSE')
        
        members.append(self._create_family_member(
            'Spouse', spouse_age, spouse_gender, spouse_employment, race_info['race_name']
        ))
        
        # Generate children if applicable
        if has_children:
            # Determine number of children (weighted toward 1-3)
            child_count = random.choices([1, 2, 3, 4], weights=[30, 40, 20, 10])[0]
            oldest_parent_age = max(head_age, spouse_age)
            
            previous_child_age = None
            for i in range(child_count):
                child_age = self._generate_age('CHILD', {
                    'parent_age': oldest_parent_age,
                    'child_number': i + 1,
                    'previous_child_age': previous_child_age
                })
                
                child_gender = self._generate_gender('CHILD')
                child_employment = self._get_employment_status(child_age, race_info['race_key'], state_code, 'CHILD')
                
                members.append(self._create_family_member(
                    'Child', child_age, child_gender, child_employment, race_info['race_name']
                ))
                
                previous_child_age = child_age
        
        return members
    
    def _generate_single_parent_family(self, race_info: Dict, state_code: str, parent_gender: str) -> List[Dict]:
        """Generate single parent family"""
        members = []
        
        # Generate single parent
        parent_role = f'SINGLE_PARENT_{parent_gender.upper()}'
        parent_age = self._generate_age('SINGLE_PARENT')
        parent_employment = self._get_employment_status(parent_age, race_info['race_key'], state_code, 'HEAD')
        
        members.append(self._create_family_member(
            'Head of Household', parent_age, parent_gender, parent_employment, race_info['race_name']
        ))
        
        # Generate children (single parents typically have 1-2 children)
        child_count = random.choices([1, 2, 3], weights=[50, 35, 15])[0]
        
        previous_child_age = None
        for i in range(child_count):
            child_age = self._generate_age('CHILD', {
                'parent_age': parent_age,
                'child_number': i + 1,
                'previous_child_age': previous_child_age
            })
            
            child_gender = self._generate_gender('CHILD')
            child_employment = self._get_employment_status(child_age, race_info['race_key'], state_code, 'CHILD')
            
            members.append(self._create_family_member(
                'Child', child_age, child_gender, child_employment, race_info['race_name']
            ))
            
            previous_child_age = child_age
        
        return members
    
    def _generate_single_person_household(self, race_info: Dict, state_code: str) -> List[Dict]:
        """Generate single person household"""
        age = self._generate_age('HEAD', {'has_children': False})
        gender = self._generate_gender('HEAD')
        employment = self._get_employment_status(age, race_info['race_key'], state_code, 'HEAD')
        
        return [self._create_family_member(
            'Head of Household', age, gender, employment, race_info['race_name']
        )]
    
    def generate_family(self, target_state: str = None) -> Dict[str, Any]:
        """Generate a single random family"""
        
        # Select state
        state_code = self._select_state(target_state)
        state_info = self._cache['states'][state_code]
        
        # Select race/ethnicity
        race_info = self._select_race_ethnicity(state_code)
        
        # Select family structure
        structure_info = self._select_family_structure(state_code)
        structure_key = structure_info['structure_key']
        
        # Generate family members based on structure
        if structure_key == 'MARRIED_COUPLE':
            members = self._generate_married_couple_family(race_info, state_code, has_children=True)
            family_type = "Married Couple with Children"
        
        elif structure_key == 'SINGLE_PERSON':
            members = self._generate_single_person_household(race_info, state_code)
            family_type = "Single Person Household"
        
        elif structure_key == 'SINGLE_PARENT_FEMALE':
            members = self._generate_single_parent_family(race_info, state_code, 'Female')
            family_type = "Single Mother Household"
        
        elif structure_key == 'SINGLE_PARENT_MALE':
            members = self._generate_single_parent_family(race_info, state_code, 'Male')
            family_type = "Single Father Household"
        
        else:
            # Default to married couple
            members = self._generate_married_couple_family(race_info, state_code, has_children=True)
            family_type = "Married Couple with Children"
        
        return {
            "family_id": f"FAM_{random.randint(100000, 999999)}",
            "state_code": state_code,
            "state_name": state_info['name'],
            "region": state_info['region'],
            "race": race_info['race_name'],
            "race_key": race_info['race_key'],
            "family_type": family_type,
            "family_structure": structure_info['structure_name'],
            "family_size": len(members),
            "total_household_income": self._estimate_household_income(members, state_code),
            "generation_date": datetime.now().isoformat(),
            "members": members
        }
    
    def _estimate_household_income(self, members: List[Dict], state_code: str) -> Optional[int]:
        """Estimate household income based on employment status and number of earners"""
        employed_count = sum(1 for member in members if member['employment_status'] == 'Employed')
        
        if employed_count == 0:
            return 25000  # Basic assistance/retirement income
        
        # Very rough income estimation based on number of earners
        base_income_per_earner = {
            1: 50000,
            2: 45000,  # Slightly lower per person for dual earners
            3: 35000,  # Part-time for additional earners
        }
        
        income_per_earner = base_income_per_earner.get(employed_count, 30000)
        estimated_income = employed_count * income_per_earner
        
        # Add some randomness
        variance = random.uniform(0.7, 1.4)
        return int(estimated_income * variance)
    
    def generate_families(self, count: int = 5, target_state: str = None) -> List[Dict[str, Any]]:
        """Generate multiple families"""
        families = []
        
        print(f"Generating {count} families...")
        if target_state:
            print(f"Targeting state: {target_state}")
        
        for i in range(count):
            try:
                family = self.generate_family(target_state)
                families.append(family)
                
                if (i + 1) % 10 == 0 or i == count - 1:
                    print(f"  Generated {i + 1}/{count} families")
                    
            except Exception as e:
                print(f"  Warning: Failed to generate family {i + 1}: {e}")
                continue
        
        return families
    
    def export_families(self, families: List[Dict], filename: str):
        """Export families to JSON file"""
        export_data = {
            "generation_metadata": {
                "total_families": len(families),
                "generation_date": datetime.now().isoformat(),
                "data_source": "US Census Bureau ACS 2022 via Neon Database"
            },
            "families": families
        }
        
        try:
            with open(filename, 'w') as f:
                json.dump(export_data, f, indent=2)
            print(f"✅ Exported {len(families)} families to {filename}")
        except Exception as e:
            print(f"❌ Error exporting families: {e}")
    
    def print_family_summary(self, families: List[Dict]):
        """Print summary statistics of generated families"""
        if not families:
            print("No families to summarize")
            return
        
        print(f"\n=== FAMILY GENERATION SUMMARY ===")
        print(f"Total families generated: {len(families)}")
        
        # State distribution
        state_counts = {}
        for family in families:
            state = family['state_name']
            state_counts[state] = state_counts.get(state, 0) + 1
        
        print(f"\nState distribution:")
        for state, count in sorted(state_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {state}: {count}")
        
        # Race distribution
        race_counts = {}
        for family in families:
            race = family['race']
            race_counts[race] = race_counts.get(race, 0) + 1
        
        print(f"\nRace/Ethnicity distribution:")
        for race, count in sorted(race_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {race}: {count}")
        
        # Family type distribution
        type_counts = {}
        for family in families:
            family_type = family['family_type']
            type_counts[family_type] = type_counts.get(family_type, 0) + 1
        
        print(f"\nFamily type distribution:")
        for family_type, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {family_type}: {count}")
        
        # Family size distribution
        size_counts = {}
        for family in families:
            size = family['family_size']
            size_counts[size] = size_counts.get(size, 0) + 1
        
        print(f"\nFamily size distribution:")
        for size in sorted(size_counts.keys()):
            print(f"  {size} members: {size_counts[size]}")
        
        # Average income
        incomes = [f['total_household_income'] for f in families if f['total_household_income']]
        if incomes:
            avg_income = sum(incomes) / len(incomes)
            print(f"\nAverage household income: ${avg_income:,.0f}")
    
    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

def main():
    """Main execution function"""
    
    print("🏠 CENSUS-BASED FAMILY GENERATOR")
    print("="*50)
    
    # Parse command line arguments
    count = 5
    target_state = None
    export_file = None
    
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
        else:
            i += 1
    
    try:
        # Initialize generator
        generator = CensusFamilyGenerator()
        
        # Generate families
        families = generator.generate_families(count, target_state)
        
        # Print sample families
        print(f"\n=== SAMPLE FAMILIES ===")
        for i, family in enumerate(families[:3]):  # Show first 3 families
            print(f"\nFamily {i + 1}:")
            print(f"  ID: {family['family_id']}")
            print(f"  Location: {family['state_name']}, {family['region']}")
            print(f"  Race: {family['race']}")
            print(f"  Type: {family['family_type']}")
            print(f"  Size: {family['family_size']} members")
            print(f"  Income: ${family['total_household_income']:,}")
            
            print("  Members:")
            for member in family['members']:
                print(f"    {member['role']}: {member['gender']}, Age {member['age']}, {member['employment_status']}")
        
        # Print summary
        generator.print_family_summary(families)
        
        # Export if requested
        if export_file:
            generator.export_families(families, export_file)
        
        print(f"\n✅ Family generation completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Family generation failed: {e}")
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
