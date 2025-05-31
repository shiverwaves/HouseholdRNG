#!/usr/bin/env python3
"""
Enhanced Family Generator - Realistic Family Member Creation

Generates realistic families with individual family members using real 
demographic and employment data stored in the Neon database.

Usage:
    from enhanced_family_generator import EnhancedFamilyGenerator
    
    generator = EnhancedFamilyGenerator()
    
    # Generate a single family
    family = generator.create_family()
    
    # Generate multiple families
    families = generator.generate_multiple_families(20)
    
    # Generate families from specific area
    ca_family = generator.create_family(state_code='06')
    south_families = generator.generate_multiple_families(10, region='South')
    
    generator.close()
"""

import psycopg2
import random
import os
import json
from typing import Dict, List, Any, Optional
from datetime import datetime

class EnhancedFamilyGenerator:
    """
    Generate realistic families with individual members using real demographic data
    """
    
    def __init__(self):
        self.connection_string = os.getenv('NEON_CONNECTION_STRING')
        if not self.connection_string:
            raise ValueError("NEON_CONNECTION_STRING environment variable is required")
        
        self.conn = psycopg2.connect(self.connection_string)
        self.cursor = self.conn.cursor()
        
        # Cache for demographic data
        self._cache = {}
        self._load_demographic_cache()
        
        # Generation statistics
        self.generation_stats = {
            'families_created': 0,
            'individuals_created': 0,
            'states_used': set(),
            'races_used': set(),
            'family_types_used': set()
        }
    
    def _load_demographic_cache(self):
        """Load all demographic data into memory for fast family generation"""
        
        print("Loading demographic data for family generation...")
        
        try:
            # Verify database has data
            self.cursor.execute("SELECT COUNT(*) FROM state_demographics")
            state_count = self.cursor.fetchone()[0]
            
            if state_count == 0:
                raise ValueError("No state demographic data found. Run neon_data_import.py first.")
            
            # Load all necessary data
            self._load_state_data()
            self._load_race_data()
            self._load_family_structure_data()
            self._load_employment_data()
            self._load_reference_data()
            
            print(f"✓ Loaded data for realistic family generation ({len(self._cache['states'])} states)")
            
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            raise
    
    def _load_state_data(self):
        """Load state information and population weights"""
        
        self.cursor.execute("""
            SELECT sd.state_code, sd.state_name, r.region_name, 
                   sd.population_weight, sd.total_population
            FROM state_demographics sd
            JOIN regions r ON sd.region_id = r.id
            ORDER BY sd.population_weight DESC
        """)
        
        self._cache['states'] = {}
        self._cache['state_weights'] = {}
        
        for row in self.cursor.fetchall():
            state_code, state_name, region_name, pop_weight, total_pop = row
            
            self._cache['states'][state_code] = {
                'name': state_name,
                'region': region_name,
                'population_weight': float(pop_weight) if pop_weight else 0,
                'total_population': total_pop or 0
            }
            
            self._cache['state_weights'][state_code] = float(pop_weight) if pop_weight else 0
    
    def _load_race_data(self):
        """Load race/ethnicity distributions by state"""
        
        self.cursor.execute("""
            SELECT sd.state_code, re.race_key, sre.population_percent
            FROM state_demographics sd
            JOIN state_race_ethnicity sre ON sd.id = sre.state_id
            JOIN race_ethnicity re ON sre.race_id = re.id
        """)
        
        self._cache['state_race'] = {}
        for row in self.cursor.fetchall():
            state_code, race_key, percentage = row
            
            if state_code not in self._cache['state_race']:
                self._cache['state_race'][state_code] = {}
            
            self._cache['state_race'][state_code][race_key] = float(percentage) if percentage else 0
    
    def _load_family_structure_data(self):
        """Load family structure distributions by state"""
        
        self.cursor.execute("""
            SELECT sd.state_code, fs.structure_key, sfs.probability_percent
            FROM state_demographics sd
            JOIN state_family_structures sfs ON sd.id = sfs.state_id
            JOIN family_structures fs ON sfs.structure_id = fs.id
        """)
        
        self._cache['state_family_structures'] = {}
        for row in self.cursor.fetchall():
            state_code, structure_key, percentage = row
            
            if state_code not in self._cache['state_family_structures']:
                self._cache['state_family_structures'][state_code] = {}
            
            self._cache['state_family_structures'][state_code][structure_key] = float(percentage) if percentage else 0
    
    def _load_employment_data(self):
        """Load real employment statistics by state"""
        
        self.cursor.execute("""
            SELECT sd.state_code, ses.employment_rate, ses.unemployment_rate,
                   ses.labor_force_participation_rate
            FROM state_demographics sd
            JOIN state_employment_stats ses ON sd.id = ses.state_id
        """)
        
        self._cache['state_employment'] = {}
        for row in self.cursor.fetchall():
            state_code, emp_rate, unemp_rate, lf_part_rate = row
            
            self._cache['state_employment'][state_code] = {
                'employment_rate': float(emp_rate) if emp_rate else 95.0,
                'unemployment_rate': float(unemp_rate) if unemp_rate else 5.0,
                'labor_force_participation_rate': float(lf_part_rate) if lf_part_rate else 63.0
            }
        
        # Load employment rates by race for individual calculations
        self.cursor.execute("""
            SELECT re.race_key, er.employed_rate, er.unemployed_rate, 
                   er.not_in_labor_force_rate, er.self_employment_rate
            FROM employment_rates er
            JOIN race_ethnicity re ON er.race_id = re.id
            WHERE er.age_group = 'All'
        """)
        
        self._cache['race_employment_rates'] = {}
        for row in self.cursor.fetchall():
            race_key, employed, unemployed, not_in_lf, self_emp = row
            
            self._cache['race_employment_rates'][race_key] = {
                'employed': float(employed) if employed else 60.0,
                'unemployed': float(unemployed) if unemployed else 4.0,
                'not_in_labor_force': float(not_in_lf) if not_in_lf else 36.0,
                'self_employment': float(self_emp) if self_emp else 10.0
            }
    
    def _load_reference_data(self):
        """Load reference data for family generation"""
        
        # Load household roles
        self.cursor.execute("SELECT role_key, role_name FROM household_roles")
        self._cache['roles'] = {row[0]: row[1] for row in self.cursor.fetchall()}
        
        # Load race names
        self.cursor.execute("SELECT race_key, race_name FROM race_ethnicity")
        self._cache['race_names'] = {row[0]: row[1] for row in self.cursor.fetchall()}
        
        # Load gender probabilities
        self.cursor.execute("""
            SELECT hr.role_key, gp.gender, gp.probability
            FROM gender_probabilities gp
            JOIN household_roles hr ON gp.role_id = hr.id
        """)
        
        self._cache['gender_probs'] = {}
        for row in self.cursor.fetchall():
            role_key, gender, prob = row
            if role_key not in self._cache['gender_probs']:
                self._cache['gender_probs'][role_key] = {}
            self._cache['gender_probs'][role_key][gender] = float(prob)
        
        # Load generation parameters
        self.cursor.execute("""
            SELECT parameter_name, parameter_value, data_type
            FROM generation_parameters
        """)
        
        self._cache['parameters'] = {}
        for row in self.cursor.fetchall():
            param_name, param_value, data_type = row
            if data_type == 'integer':
                self._cache['parameters'][param_name] = int(param_value)
            elif data_type == 'decimal':
                self._cache['parameters'][param_name] = float(param_value)
            else:
                self._cache['parameters'][param_name] = param_value
    
    def _select_state(self, preferred_state: str = None, preferred_region: str = None) -> str:
        """Select a state based on population weights or user preference"""
        
        if preferred_state and preferred_state in self._cache['states']:
            return preferred_state
        
        if preferred_region:
            region_states = [
                state_code for state_code, data in self._cache['states'].items()
                if data['region'].lower() == preferred_region.lower()
            ]
            
            if region_states:
                total_weight = sum(self._cache['state_weights'][state] for state in region_states)
                
                if total_weight > 0:
                    rand_val = random.random() * total_weight
                    cumulative = 0
                    
                    for state_code in sorted(region_states, key=lambda x: self._cache['state_weights'][x], reverse=True):
                        cumulative += self._cache['state_weights'][state_code]
                        if rand_val <= cumulative:
                            return state_code
        
        # Population-weighted random selection
        total_weight = sum(self._cache['state_weights'].values())
        
        if total_weight > 0:
            rand_val = random.random() * total_weight
            cumulative = 0
            
            for state_code in sorted(self._cache['state_weights'].keys(), 
                                   key=lambda x: self._cache['state_weights'][x], reverse=True):
                cumulative += self._cache['state_weights'][state_code]
                if rand_val <= cumulative:
                    return state_code
        
        return '06'  # Fallback to California
    
    def _weighted_random_selection(self, items: Dict[str, float]) -> Optional[str]:
        """Select an item based on weighted probabilities"""
        
        if not items:
            return None
        
        valid_items = {k: v for k, v in items.items() if v > 0}
        if not valid_items:
            return random.choice(list(items.keys()))
        
        total_weight = sum(valid_items.values())
        if total_weight <= 0:
            return random.choice(list(valid_items.keys()))
        
        rand_val = random.random() * total_weight
        cumulative = 0
        
        for key, weight in valid_items.items():
            cumulative += weight
            if rand_val <= cumulative:
                return key
        
        return list(valid_items.keys())[0]
    
    def _determine_gender(self, role: str, existing_spouse_gender: Optional[str] = None) -> str:
        """Determine gender based on household role and existing family members"""
        
        if role == 'SPOUSE' and existing_spouse_gender:
            return 'Female' if existing_spouse_gender == 'Male' else 'Male'
        
        gender_probs = self._cache['gender_probs'].get(role, {'Male': 0.5, 'Female': 0.5})
        return 'Male' if random.random() < gender_probs.get('Male', 0.5) else 'Female'
    
    def _calculate_employment_status(self, race_key: str, age: int, role: str, state_code: str) -> str:
        """
        Calculate employment status for an individual using real demographic data
        """
        
        # Children under 16 cannot work
        if age < 16:
            return "Child (Under 16)"
        
        # Students (16-18) have limited employment
        if 16 <= age <= 18:
            if random.random() < 0.3:  # 30% of teens work part-time
                return "Part-time Student Worker"
            else:
                return "Student (Not Employed)"
        
        # College age (19-25) may be students or working
        if 19 <= age <= 25:
            if random.random() < 0.4:  # 40% are students
                if random.random() < 0.6:  # 60% of students also work
                    return "Part-time Student Worker"
                else:
                    return "Student (Not Employed)"
        
        # Retirement considerations
        retirement_age = self._cache['parameters'].get('retirement_age_threshold', 65)
        if age >= retirement_age:
            retirement_prob = self._cache['parameters'].get('retirement_probability', 0.7)
            
            if random.random() < retirement_prob:
                # Heads of household may remain economically active longer
                if role == 'HEAD' and random.random() < 0.3:
                    return "Semi-Retired (Part-time)"
                else:
                    return "Retired"
        
        # Get employment probabilities based on race and state data
        race_rates = self._cache['race_employment_rates'].get(race_key, {
            'employed': 60.0, 'unemployed': 4.0, 'not_in_labor_force': 36.0, 'self_employment': 10.0
        })
        
        state_employment = self._cache['state_employment'].get(state_code, {})
        
        # Adjust employment probabilities using real state data
        if state_employment:
            state_emp_rate = state_employment['employment_rate']
            state_unemp_rate = state_employment['unemployment_rate']
            
            # Use state rates to adjust individual probabilities
            adjustment_factor = state_emp_rate / 95.0  # Normalize to ~95% national average
            
            adjusted_employed = race_rates['employed'] * adjustment_factor
            adjusted_unemployed = race_rates['unemployed'] * (state_unemp_rate / 4.0)  # Normalize to ~4%
            adjusted_not_in_lf = 100 - adjusted_employed - adjusted_unemployed
        else:
            adjusted_employed = race_rates['employed']
            adjusted_unemployed = race_rates['unemployed']
            adjusted_not_in_lf = race_rates['not_in_labor_force']
        
        # Role-based adjustments (heads of household more likely to be employed)
        if role == 'HEAD':
            adjusted_employed = min(98.0, adjusted_employed * 1.4)
            adjusted_unemployed = max(0.5, adjusted_unemployed * 0.6)
            adjusted_not_in_lf = max(1.0, 100 - adjusted_employed - adjusted_unemployed)
        
        # Determine employment status
        rand_val = random.random() * 100
        
        if rand_val < adjusted_employed:
            # Determine if self-employed
            self_emp_rate = race_rates['self_employment'] / 100.0
            if role == 'HEAD':
                self_emp_rate = self._cache['parameters'].get('head_self_employment_rate', 0.15)
            
            if random.random() < self_emp_rate:
                return "Self-Employed"
            else:
                return "Employed"
        elif rand_val < adjusted_employed + adjusted_unemployed:
            return "Unemployed"
        else:
            return "Not in Labor Force"
    
    def _create_family_member(self, role: str, age: int, gender: str, employment_status: str, race_name: str) -> Dict[str, Any]:
        """Create an individual family member with realistic characteristics"""
        
        # Increment individual counter
        self.generation_stats['individuals_created'] += 1
        
        return {
            "member_id": self.generation_stats['individuals_created'],
            "role": self._cache['roles'].get(role, role),
            "age": age,
            "gender": gender,
            "employment_status": employment_status,
            "race": race_name,
            "can_work": age >= 16,
            "is_adult": age >= 18,
            "is_senior": age >= 65
        }
    
    def _generate_family_members(self, structure_key: str, race_key: str, race_name: str, state_code: str) -> List[Dict]:
        """Generate family members based on family structure"""
        
        members = []
        
        if structure_key == "SINGLE_PERSON":
            # Single person household
            age = random.randint(22, 85)
            gender = self._determine_gender('HEAD')
            employment = self._calculate_employment_status(race_key, age, 'HEAD', state_code)
            members.append(self._create_family_member('HEAD', age, gender, employment, race_name))
            
        elif structure_key == "MARRIED_COUPLE":
            # Married couple without children
            head_age = random.randint(22, 85)
            head_gender = self._determine_gender('HEAD')
            head_employment = self._calculate_employment_status(race_key, head_age, 'HEAD', state_code)
            members.append(self._create_family_member('HEAD', head_age, head_gender, head_employment, race_name))
            
            spouse_age = max(18, head_age + random.randint(-8, 8))
            spouse_gender = self._determine_gender('SPOUSE', head_gender)
            spouse_employment = self._calculate_employment_status(race_key, spouse_age, 'SPOUSE', state_code)
            members.append(self._create_family_member('SPOUSE', spouse_age, spouse_gender, spouse_employment, race_name))
            
        elif structure_key == "MARRIED_WITH_CHILDREN":
            # Married couple with children
            head_age = random.randint(25, 55)
            head_gender = self._determine_gender('HEAD')
            head_employment = self._calculate_employment_status(race_key, head_age, 'HEAD', state_code)
            members.append(self._create_family_member('HEAD', head_age, head_gender, head_employment, race_name))
            
            spouse_age = max(18, head_age + random.randint(-5, 5))
            spouse_gender = self._determine_gender('SPOUSE', head_gender)
            spouse_employment = self._calculate_employment_status(race_key, spouse_age, 'SPOUSE', state_code)
            members.append(self._create_family_member('SPOUSE', spouse_age, spouse_gender, spouse_employment, race_name))
            
            # Generate children
            num_children = random.randint(1, min(4, self._cache['parameters'].get('max_children_per_family', 4)))
            
            for i in range(num_children):
                # Children's ages should make sense relative to parents
                max_child_age = min(17, head_age - 18, spouse_age - 18)
                child_age = random.randint(0, max(0, max_child_age))
                
                child_gender = self._determine_gender('CHILD')
                child_employment = self._calculate_employment_status(race_key, child_age, 'CHILD', state_code)
                members.append(self._create_family_member('CHILD', child_age, child_gender, child_employment, race_name))
        
        elif structure_key in ["SINGLE_PARENT_MALE", "SINGLE_PARENT_FEMALE"]:
            # Single parent household
            parent_age = random.randint(22, 55)
            parent_gender = 'Male' if structure_key == "SINGLE_PARENT_MALE" else 'Female'
            parent_employment = self._calculate_employment_status(race_key, parent_age, 'HEAD', state_code)
            members.append(self._create_family_member('HEAD', parent_age, parent_gender, parent_employment, race_name))
            
            # Generate children
            num_children = random.randint(1, 3)
            for i in range(num_children):
                max_child_age = min(17, parent_age - 18)
                child_age = random.randint(0, max(0, max_child_age))
                
                child_gender = self._determine_gender('CHILD')
                child_employment = self._calculate_employment_status(race_key, child_age, 'CHILD', state_code)
                members.append(self._create_family_member('CHILD', child_age, child_gender, child_employment, race_name))
        
        else:
            # Default to single person
            age = random.randint(22, 85)
            gender = self._determine_gender('HEAD')
            employment = self._calculate_employment_status(race_key, age, 'HEAD', state_code)
            members.append(self._create_family_member('HEAD', age, gender, employment, race_name))
        
        return members
    
    def create_family(self, state_code: str = None, region: str = None) -> Dict[str, Any]:
        """
        Generate a realistic family with individual members using real demographic data
        
        Args:
            state_code: Specific state code (e.g., '06' for California)
            region: Preferred region ('Northeast', 'Midwest', 'South', 'West')
        
        Returns:
            Dictionary containing family information with detailed member data
        """
        
        # Select state based on realistic distribution
        selected_state = self._select_state(state_code, region)
        state_info = self._cache['states'][selected_state]
        
        # Select race/ethnicity based on state demographics
        state_race_data = self._cache['state_race'].get(selected_state, {})
        fallback_race_data = {
            'WHITE_NON_HISPANIC': 59.3, 'BLACK': 13.6, 'HISPANIC': 18.9, 
            'ASIAN': 6.1, 'NATIVE': 1.3, 'PACIFIC_ISLANDER': 0.2, 'TWO_OR_MORE': 2.9
        }
        
        race_key = self._weighted_random_selection(state_race_data or fallback_race_data)
        if not race_key:
            race_key = 'WHITE_NON_HISPANIC'
        
        race_name = self._cache['race_names'].get(race_key, "Unknown")
        
        # Select family structure based on state data
        state_family_structures = self._cache['state_family_structures'].get(selected_state, {})
        fallback_family_data = {
            'SINGLE_PERSON': 28.0, 'MARRIED_COUPLE': 22.0, 'MARRIED_WITH_CHILDREN': 20.0,
            'SINGLE_PARENT_FEMALE': 12.0, 'SINGLE_PARENT_MALE': 4.0
        }
        
        structure_key = self._weighted_random_selection(state_family_structures or fallback_family_data)
        if not structure_key:
            structure_key = 'SINGLE_PERSON'
        
        # Generate family members with realistic characteristics
        members = self._generate_family_members(structure_key, race_key, race_name, selected_state)
        
        # Calculate family statistics
        adults = [m for m in members if m['is_adult']]
        working_age = [m for m in members if m['can_work'] and not m['is_senior']]
        employed = [m for m in members if m['employment_status'] in ['Employed', 'Self-Employed']]
        children = [m for m in members if not m['is_adult']]
        
        # Update generation statistics
        self.generation_stats['families_created'] += 1
        self.generation_stats['states_used'].add(selected_state)
        self.generation_stats['races_used'].add(race_key)
        self.generation_stats['family_types_used'].add(structure_key)
        
        # Create family data structure
        family_data = {
            "family_id": self.generation_stats['families_created'],
            "location": {
                "state": state_info['name'],
                "state_code": selected_state,
                "region": state_info['region']
            },
            "demographics": {
                "race": race_name,
                "race_key": race_key,
                "family_type": structure_key.replace('_', ' ').title()
            },
            "composition": {
                "total_members": len(members),
                "adults": len(adults),
                "children": len(children),
                "working_age_members": len(working_age)
            },
            "employment": {
                "employed_members": len(employed),
                "family_employment_rate": round((len(employed) / len(adults) * 100) if adults else 0, 1),
                "primary_earners": len([m for m in employed if m['role'] in ['Head of Household', 'Spouse']])
            },
            "members": members
        }
        
        return family_data
    
    def generate_multiple_families(self, count: int = 10, state_code: str = None, region: str = None) -> List[Dict]:
        """Generate multiple families"""
        
        families = []
        for i in range(count):
            family = self.create_family(state_code=state_code, region=region)
            families.append(family)
        
        return families
    
    def get_generation_statistics(self) -> Dict[str, Any]:
        """Get statistics about generated families and individuals"""
        
        return {
            'families_generated': self.generation_stats['families_created'],
            'individuals_generated': self.generation_stats['individuals_created'],
            'unique_states_used': len(self.generation_stats['states_used']),
            'unique_races_used': len(self.generation_stats['races_used']),
            'unique_family_types_used': len(self.generation_stats['family_types_used']),
            'states_represented': list(self.generation_stats['states_used']),
            'races_represented': list(self.generation_stats['races_used']),
            'family_types_generated': list(self.generation_stats['family_types_used'])
        }
    
    def export_families_to_json(self, families: List[Dict], filename: str = None) -> str:
        """Export generated families to JSON file"""
        
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'generated_families_{timestamp}.json'
        
        export_data = {
            'generation_metadata': {
                'generated_at': datetime.now().isoformat(),
                'total_families': len(families),
                'total_individuals': sum(f['composition']['total_members'] for f in families),
                'generation_statistics': self.get_generation_statistics()
            },
            'families': families
        }
        
        with open(filename, 'w') as f:
            json.dump(export_data, f, indent=2)
        
        return filename
    
    def close(self):
        """Close database connections"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

def demonstrate_family_generation():
    """Demonstrate realistic family member creation using database data"""
    
    print("👨‍👩‍👧‍👦 ENHANCED FAMILY MEMBER GENERATION DEMONSTRATION")
    print("="*70)
    print("Generating realistic families with individual members using real demographic data")
    print()
    
    try:
        generator = EnhancedFamilyGenerator()
        
        # Demo : Show variety of family structures
        print("1. FAMILY STRUCTURE VARIETY")
        print("-" * 30)
        
        for i in range(5):
            family = generator.create_family()
            
            print(f"\nFamily {family['family_id']} - {family['demographics']['family_type']}")
            print(f"📍 Location: {family['location']['state']}, {family['location']['region']}")
            print(f"👥 Demographics: {family['demographics']['race']}")
            print(f"🏠 Composition: {family['composition']['total_members']} members ({family['composition']['adults']} adults, {family['composition']['children']} children)")
            print(f"💼 Employment: {family['employment']['employed_members']}/{family['composition']['adults']} adults employed ({family['employment']['family_employment_rate']}%)")
            
            print("   Members:")
            for member in family['members']:
                age_group = "Child" if member['age'] < 18 else ("Senior" if member['age'] >= 65 else "Adult")
                work_status = f", {member['employment_status']}" if member['can_work'] else ""
                print(f"     • {member['role']}: {member['gender']}, Age {member['age']} ({age_group}){work_status}")
        
        print(f"\n✅ Family generation demonstration completed!")
        print("The system successfully created realistic families with individual members")
        print("using real demographic and employment data from the Census Bureau.")
        
    except Exception as e:
        print(f"❌ Demonstration failed: {e}")
        print("Make sure you've run neon_data_import.py to populate the database first.")
    finally:
        try:
            generator.close()
        except:
            pass

if __name__ == "__main__":
    demonstrate_family_generation()