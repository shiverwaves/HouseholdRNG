import psycopg2
import random
import os
from typing import Dict, List, Any, Optional

class DatabaseFamilyGenerator:
    """
    Family generator that queries Neon database for demographic data
    """
    
    def __init__(self):
        self.conn = self._get_db_connection()
        self.cursor = self.conn.cursor()
        
        # Cache frequently accessed data
        self._cache = {}
        self._load_cache()
    
    def _get_db_connection(self):
        """Connect to Neon cloud database using connection string"""
        NEON_CONNECTION_STRING = os.getenv('NEON_CONNECTION_STRING')
        if not NEON_CONNECTION_STRING:
            raise ValueError("NEON_CONNECTION_STRING environment variable is required")
        
        return psycopg2.connect(NEON_CONNECTION_STRING)
    
    def _load_cache(self):
        """Load frequently used data into memory to avoid repeated queries"""
        
        # Cache regions with states
        self.cursor.execute("""
        SELECT r.region_code, r.region_name, r.population_percent,
               array_agg(s.state_name) as states
        FROM regions r
        LEFT JOIN states s ON r.id = s.region_id
        GROUP BY r.id, r.region_code, r.region_name, r.population_percent
        """)
        
        self._cache['regions'] = {}
        for row in self.cursor.fetchall():
            region_code, region_name, pop_percent, states = row
            self._cache['regions'][region_code] = {
                'name': region_name,
                'population_percent': float(pop_percent),
                'states': states
            }
        
        # Cache race/ethnicity data
        self.cursor.execute("""
        SELECT race_key, race_name, population_percent
        FROM race_ethnicity
        """)
        
        self._cache['race_ethnicity'] = {}
        for row in self.cursor.fetchall():
            race_key, race_name, pop_percent = row
            self._cache['race_ethnicity'][race_key] = {
                'name': race_name,
                'percent': float(pop_percent)
            }
        
        # Cache employment rates
        self.cursor.execute("""
        SELECT re.race_key, er.employed_rate, er.unemployed_rate, er.not_in_labor_force_rate
        FROM employment_rates er
        JOIN race_ethnicity re ON er.race_id = re.id
        """)
        
        self._cache['employment_rates'] = {}
        for row in self.cursor.fetchall():
            race_key, employed, unemployed, not_in_labor = row
            self._cache['employment_rates'][race_key] = {
                'employed': float(employed),
                'unemployed': float(unemployed),
                'not_in_labor_force': float(not_in_labor)
            }
        
        # Cache family structures
        self.cursor.execute("""
        SELECT structure_key, structure_name, probability_percent, avg_children
        FROM family_structures
        """)
        
        self._cache['family_structures'] = {}
        for row in self.cursor.fetchall():
            structure_key, structure_name, prob_percent, avg_children = row
            self._cache['family_structures'][structure_key] = {
                'name': structure_name,
                'percent': float(prob_percent),
                'avg_children': float(avg_children) if avg_children else None
            }
        
        # Cache family size by race
        self.cursor.execute("""
        SELECT re.race_key, fsr.avg_family_size
        FROM family_size_by_race fsr
        JOIN race_ethnicity re ON fsr.race_id = re.id
        """)
        
        self._cache['family_size_by_race'] = {}
        for row in self.cursor.fetchall():
            race_key, avg_size = row
            self._cache['family_size_by_race'][race_key] = float(avg_size)
        
        # Cache household roles
        self.cursor.execute("""
        SELECT role_key, role_name
        FROM household_roles
        """)
        
        self._cache['roles'] = {}
        for row in self.cursor.fetchall():
            role_key, role_name = row
            self._cache['roles'][role_key] = role_name
        
        # Cache gender probabilities
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
        
        # Cache generation parameters
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
    
    def _weighted_random(self, items: Dict) -> str:
        """Select an item from a dictionary with weighted probabilities"""
        weights = []
        item_keys = list(items.keys())
        
        total_weight = 0
        for key in item_keys:
            weight = items[key].get('percent', items[key].get('population_percent', items[key]))
            total_weight += weight
            weights.append(total_weight)
        
        rand_val = random.random() * total_weight
        for i, weight in enumerate(weights):
            if rand_val < weight:
                return item_keys[i]
        
        return item_keys[0]
    
    def _get_gender(self, role: str, spouse_gender: Optional[str] = None) -> str:
        """Determine gender based on household role"""
        if role == 'SPOUSE' and spouse_gender:
            return 'Female' if spouse_gender == 'Male' else 'Male'
        
        gender_probs = self._cache['gender_probs'][role]
        return 'Male' if random.random() < gender_probs['Male'] else 'Female'
    
    def _get_employment_status(self, race_key: str, age: int, role: str) -> str:
        """Select employment status based on race/ethnicity, age, and role"""
        
        # Children under 16 are not in the labor force
        if age < 16:
            return "Child (Under 16)"
        
        # Retirement logic
        retirement_age = self._cache['parameters']['retirement_age_threshold']
        retirement_prob = self._cache['parameters']['retirement_probability']
        
        if age >= retirement_age and random.random() < retirement_prob:
            if role == 'HEAD':
                # Heads of household still have chance to work
                if random.random() < 0.3:
                    return "Retired"
                else:
                    self_emp_rate = self._cache['parameters']['head_self_employment_rate']
                    return "Self-Employed" if random.random() < self_emp_rate else "Employed"
            return "Retired"
        
        # Head of household employment logic
        if role == 'HEAD':
            self_emp_rate = self._cache['parameters']['head_self_employment_rate']
            return "Self-Employed" if random.random() < self_emp_rate else "Employed"
        
        # Dependent employment logic
        if role == 'DEPENDENT':
            return "Retired" if age >= retirement_age else "Not in Labor Force"
        
        # Student employment logic
        if role == 'DEPENDENT_STUDENT':
            student_emp_rate = self._cache['parameters']['part_time_student_employment']
            return "Employed" if random.random() < student_emp_rate else "Not in Labor Force"
        
        # Use race-based employment rates
        rates = self._cache['employment_rates'][race_key]
        rand_val = random.random() * 100
        
        if rand_val < rates['employed']:
            self_emp_rate = self._cache['parameters']['self_employment_rate']
            return "Self-Employed" if random.random() < self_emp_rate else "Employed"
        elif rand_val < rates['employed'] + rates['unemployed']:
            return "Unemployed"
        else:
            return "Not in Labor Force"
    
    def _get_age_constraints(self, role: str, context: str = 'default') -> Dict[str, int]:
        """Get age constraints for a role and context"""
        self.cursor.execute("""
        SELECT min_age, max_age
        FROM age_constraints ac
        JOIN household_roles hr ON ac.role_id = hr.id
        WHERE hr.role_key = %s AND ac.context_type = %s
        """, (role, context))
        
        result = self.cursor.fetchone()
        if result:
            return {'min_age': result[0], 'max_age': result[1]}
        else:
            # Fallback to default
            self.cursor.execute("""
            SELECT min_age, max_age
            FROM age_constraints ac
            JOIN household_roles hr ON ac.role_id = hr.id
            WHERE hr.role_key = %s AND ac.context_type = 'default'
            """, (role,))
            result = self.cursor.fetchone()
            return {'min_age': result[0], 'max_age': result[1]} if result else {'min_age': 18, 'max_age': 80}
    
    def _get_random_age(self, role: str, context: Dict = None) -> int:
        """Generate random age based on role and context"""
        context = context or {}
        
        if role in ['HEAD', 'SPOUSE', 'PARENT']:
            if context.get('has_young_children'):
                age_context = 'with_young_children' if context.get('youngest_child_age', 18) < 5 else 'default'
                constraints = self._get_age_constraints(role, age_context)
                
                min_age = constraints['min_age']
                max_age = constraints['max_age']
                
                if context.get('gender') == 'Female' and context.get('youngest_child_age', 18) < 5:
                    max_age = min(max_age, self._cache['parameters']['female_max_childbearing_age'])
                
                return random.randint(min_age, max_age)
            else:
                constraints = self._get_age_constraints(role)
                return random.randint(constraints['min_age'], constraints['max_age'])
        
        elif role == 'CHILD':
            if context.get('parent_age'):
                max_child_age = min(18, context['parent_age'] - 20)
                
                if context.get('child_index', 0) > 0:
                    max_age = context['previous_child_age'] - 1
                    min_age = max(0, max_age - 4)
                    return random.randint(min_age, max_age)
                
                return random.randint(0, max_child_age)
            return random.randint(0, 17)
        
        elif role == 'GRANDPARENT':
            if context.get('parent_age'):
                min_age = context['parent_age'] + 20
                max_age = min(context['parent_age'] + 40, 90)
                return random.randint(min_age, max_age)
            constraints = self._get_age_constraints(role)
            return random.randint(constraints['min_age'], constraints['max_age'])
        
        else:
            constraints = self._get_age_constraints(role)
            return random.randint(constraints['min_age'], constraints['max_age'])
    
    def _create_member(self, role: str, age: int, gender: str, employment_status: str, race: str) -> Dict[str, Any]:
        """Create a family member dictionary"""
        return {
            "role": self._cache['roles'][role],
            "age": age,
            "gender": gender,
            "employment_status": employment_status,
            "race": race
        }
    
    def create_family(self) -> Dict[str, Any]:
        """Generate one random US family using database data"""
        
        # Select race/ethnicity
        race_key = self._weighted_random(self._cache['race_ethnicity'])
        race = self._cache['race_ethnicity'][race_key]['name']
        
        # Select region and state
        region_key = self._weighted_random(self._cache['regions'])
        region = self._cache['regions'][region_key]['name']
        state = random.choice(self._cache['regions'][region_key]['states'])
        
        # Select family structure
        structure_key = self._weighted_random(self._cache['family_structures'])
        
        # Create family members array
        members = []
        
        # Determine if there's a grandparent in the household
        grandparent_prob = self._cache['parameters']['grandparent_probability_extended']
        has_grandparent = structure_key == "EXTENDED_FAMILY" and random.random() < grandparent_prob
        
        # Handle different family structures
        if structure_key == "MARRIED_WITH_CHILDREN":
            members = self._create_married_with_children_family(race_key, race, has_grandparent)
            
        elif structure_key == "MARRIED_NO_CHILDREN":
            members = self._create_married_no_children_family(race_key, race, has_grandparent)
            
        elif structure_key in ["SINGLE_PARENT_FEMALE", "SINGLE_PARENT_MALE"]:
            members = self._create_single_parent_family(structure_key, race_key, race, has_grandparent)
            
        elif structure_key == "EXTENDED_FAMILY":
            members = self._create_extended_family(race_key, race)
        
        # Return complete family structure
        family_type = structure_key.replace('_', ' ').lower()
        return {
            "race": race,
            "race_key": race_key,
            "region": region,
            "state": state,
            "family_type": family_type,
            "family_size": len(members),
            "members": members
        }
    
    def _create_married_with_children_family(self, race_key: str, race: str, has_grandparent: bool) -> List[Dict]:
        """Create married couple with children"""
        members = []
        
        # Determine children count and ages
        avg_children = self._cache['family_structures']["MARRIED_WITH_CHILDREN"]['avg_children']
        base_child_count = int(avg_children)
        extra_child = random.random() < (avg_children - base_child_count)
        child_count = base_child_count + (1 if extra_child else 0)
        
        # Adjust based on racial factors
        racial_factor = self._cache['family_size_by_race'][race_key] / 3.15
        adjusted_child_count = round(child_count * racial_factor)
        
        # Generate children ages
        children_ages = []
        for i in range(adjusted_child_count):
            if i == 0:
                child_age = random.randint(0, 17)
            else:
                child_age = max(0, children_ages[i-1] - 1 - random.randint(0, 3))
            children_ages.append(child_age)
        
        youngest_child_age = min(children_ages) if children_ages else None
        
        # Add head of household
        head_gender = self._get_gender('HEAD')
        head_age = self._get_random_age('HEAD', {
            'has_young_children': bool(children_ages),
            'youngest_child_age': youngest_child_age,
            'gender': head_gender
        })
        head_employment = self._get_employment_status(race_key, head_age, 'HEAD')
        
        members.append(self._create_member('HEAD', head_age, head_gender, head_employment, race))
        
        # Add spouse
        spouse_gender = 'Female' if head_gender == 'Male' else 'Male'
        max_age_diff = self._cache['parameters']['max_age_difference_spouses']
        age_difference = random.randint(-max_age_diff, max_age_diff)
        
        if spouse_gender == 'Female' and youngest_child_age is not None and youngest_child_age < 5:
            max_female_age = self._cache['parameters']['female_max_childbearing_age']
            spouse_age = max(18, min(head_age + age_difference, max_female_age))
        else:
            spouse_age = max(18, head_age + age_difference)
        
        spouse_employment = self._get_employment_status(race_key, spouse_age, 'SPOUSE')
        members.append(self._create_member('SPOUSE', spouse_age, spouse_gender, spouse_employment, race))
        
        # Add children
        for child_age in children_ages:
            child_gender = self._get_gender('CHILD')
            child_employment = self._get_employment_status(race_key, child_age, 'CHILD')
            members.append(self._create_member('CHILD', child_age, child_gender, child_employment, race))
        
        # Add grandparent if needed
        if has_grandparent:
            grandparent_age = self._get_random_age('GRANDPARENT', {'parent_age': max(head_age, spouse_age)})
            grandparent_gender = self._get_gender('GRANDPARENT')
            grandparent_employment = self._get_employment_status(race_key, grandparent_age, 'GRANDPARENT')
            members.append(self._create_member('GRANDPARENT', grandparent_age, grandparent_gender, grandparent_employment, race))
        
        return members
    
    def _create_married_no_children_family(self, race_key: str, race: str, has_grandparent: bool) -> List[Dict]:
        """Create married couple without children"""
        members = []
        
        # Add head and spouse
        head_age = self._get_random_age('HEAD')
        head_gender = self._get_gender('HEAD')
        head_employment = self._get_employment_status(race_key, head_age, 'HEAD')
        members.append(self._create_member('HEAD', head_age, head_gender, head_employment, race))
        
        max_age_diff = self._cache['parameters']['max_age_difference_spouses']
        spouse_age = max(18, head_age + random.randint(-max_age_diff, max_age_diff))
        spouse_gender = 'Female' if head_gender == 'Male' else 'Male'
        spouse_employment = self._get_employment_status(race_key, spouse_age, 'SPOUSE')
        members.append(self._create_member('SPOUSE', spouse_age, spouse_gender, spouse_employment, race))
        
        # Add grandparent if needed
        if has_grandparent:
            grandparent_age = self._get_random_age('GRANDPARENT')
            grandparent_gender = self._get_gender('GRANDPARENT')
            grandparent_employment = self._get_employment_status(race_key, grandparent_age, 'GRANDPARENT')
            members.append(self._create_member('GRANDPARENT', grandparent_age, grandparent_gender, grandparent_employment, race))
        
        # Possibly add dependents
        dependent_prob = self._cache['parameters']['dependent_probability_married']
        if random.random() < dependent_prob:
            student_ratio = self._cache['parameters']['student_vs_dependent_ratio']
            if random.random() < student_ratio:
                student_age = self._get_random_age('DEPENDENT_STUDENT')
                student_gender = self._get_gender('DEPENDENT_STUDENT')
                student_employment = self._get_employment_status(race_key, student_age, 'DEPENDENT_STUDENT')
                members.append(self._create_member('DEPENDENT_STUDENT', student_age, student_gender, student_employment, race))
            else:
                dependent_age = self._get_random_age('DEPENDENT')
                dependent_gender = self._get_gender('DEPENDENT')
                dependent_employment = self._get_employment_status(race_key, dependent_age, 'DEPENDENT')
                members.append(self._create_member('DEPENDENT', dependent_age, dependent_gender, dependent_employment, race))
        
        return members
    
    def _create_single_parent_family(self, structure_key: str, race_key: str, race: str, has_grandparent: bool) -> List[Dict]:
        """Create single parent family"""
        members = []
        
        # Determine children count and ages
        avg_children = self._cache['family_structures'][structure_key]['avg_children']
        base_child_count = int(avg_children)
        extra_child = random.random() < (avg_children - base_child_count)
        child_count = base_child_count + (1 if extra_child else 0)
        
        # Generate children ages
        children_ages = []
        for i in range(child_count):
            if i == 0:
                child_age = random.randint(0, 17)
            else:
                child_age = max(0, children_ages[i-1] - 1 - random.randint(0, 3))
            children_ages.append(child_age)
        
        youngest_child_age = min(children_ages) if children_ages else None
        
        # Add parent
        parent_gender = 'Female' if structure_key == "SINGLE_PARENT_FEMALE" else 'Male'
        parent_age = self._get_random_age('PARENT', {
            'has_young_children': bool(children_ages),
            'youngest_child_age': youngest_child_age,
            'gender': parent_gender
        })
        parent_employment = self._get_employment_status(race_key, parent_age, 'HEAD')
        members.append(self._create_member('HEAD', parent_age, parent_gender, parent_employment, race))
        
        # Add children
        for child_age in children_ages:
            child_gender = self._get_gender('CHILD')
            child_employment = self._get_employment_status(race_key, child_age, 'CHILD')
            members.append(self._create_member('CHILD', child_age, child_gender, child_employment, race))
        
        # Add grandparent if needed
        if has_grandparent:
            grandparent_age = self._get_random_age('GRANDPARENT', {'parent_age': parent_age})
            grandparent_gender = self._get_gender('GRANDPARENT')
            grandparent_employment = self._get_employment_status(race_key, grandparent_age, 'GRANDPARENT')
            members.append(self._create_member('GRANDPARENT', grandparent_age, grandparent_gender, grandparent_employment, race))
        
        return members
    
    def _create_extended_family(self, race_key: str, race: str) -> List[Dict]:
        """Create extended family"""
        members = []
        
        # Create primary family unit
        child_count = random.randint(1, 3)
        children_ages = []
        for i in range(child_count):
            if i == 0:
                child_age = random.randint(0, 17)
            else:
                child_age = max(0, children_ages[i-1] - 1 - random.randint(0, 3))
            children_ages.append(child_age)
        
        youngest_child_age = min(children_ages) if children_ages else None
        
        # Add head
        head_gender = self._get_gender('HEAD')
        head_age = self._get_random_age('HEAD', {
            'has_young_children': bool(children_ages),
            'youngest_child_age': youngest_child_age,
            'gender': head_gender
        })
        head_employment = self._get_employment_status(race_key, head_age, 'HEAD')
        members.append(self._create_member('HEAD', head_age, head_gender, head_employment, race))
        
        # Maybe add spouse
        spouse_prob = self._cache['parameters']['spouse_probability_extended']
        spouse_age = None
        if random.random() < spouse_prob:
            spouse_gender = 'Female' if head_gender == 'Male' else 'Male'
            max_age_diff = self._cache['parameters']['max_age_difference_spouses']
            age_difference = random.randint(-max_age_diff, max_age_diff)
            
            if spouse_gender == 'Female' and youngest_child_age is not None and youngest_child_age < 5:
                max_female_age = self._cache['parameters']['female_max_childbearing_age']
                spouse_age = max(18, min(head_age + age_difference, max_female_age))
            else:
                spouse_age = max(18, head_age + age_difference)
            
            spouse_employment = self._get_employment_status(race_key, spouse_age, 'SPOUSE')
            members.append(self._create_member('SPOUSE', spouse_age, spouse_gender, spouse_employment, race))
        
        # Add children
        for child_age in children_ages:
            child_gender = self._get_gender('CHILD')
            child_employment = self._get_employment_status(race_key, child_age, 'CHILD')
            members.append(self._create_member('CHILD', child_age, child_gender, child_employment, race))
        
        # Add grandparents (1-2)
        grandparent_count = random.randint(1, 2)
        oldest_parent_age = max(head_age, spouse_age) if spouse_age else head_age
        
        # First grandparent
        first_grandparent_gender = self._get_gender('GRANDPARENT')
        first_grandparent_age = self._get_random_age('GRANDPARENT', {'parent_age': oldest_parent_age})
        first_grandparent_employment = self._get_employment_status(race_key, first_grandparent_age, 'GRANDPARENT')
        members.append(self._create_member('GRANDPARENT', first_grandparent_age, first_grandparent_gender, first_grandparent_employment, race))
        
        # Second grandparent if needed
        if grandparent_count > 1:
            age_diff = random.randint(-5, 5)
            second_grandparent_age = max(60, first_grandparent_age + age_diff)
            second_grandparent_gender = 'Female' if first_grandparent_gender == 'Male' else 'Male'
            second_grandparent_employment = self._get_employment_status(race_key, second_grandparent_age, 'GRANDPARENT')
            members.append(self._create_member('GRANDPARENT', second_grandparent_age, second_grandparent_gender, second_grandparent_employment, race))
        
        # Possibly add other relatives
        other_relative_prob = self._cache['parameters']['other_relative_probability']
        if random.random() < other_relative_prob:
            relative_age = self._get_random_age('OTHER_RELATIVE')
            relative_gender = self._get_gender('OTHER_RELATIVE')
            relative_employment = self._get_employment_status(race_key, relative_age, 'OTHER_RELATIVE')
            members.append(self._create_member('OTHER_RELATIVE', relative_age, relative_gender, relative_employment, race))
        
        return members
    
    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

def test_db_family_generation():
    """Test the database-driven family generation"""
    print("=== Database-Driven Family Generation Test ===")
    
    generator = DatabaseFamilyGenerator()
    
    try:
        for i in range(3):
            print(f"\n--- Family {i+1} ---")
            family = generator.create_family()
            
            print(f"Family: {family['family_type']}")
            print(f"Location: {family['state']}, {family['region']}")
            print(f"Race: {family['race']}")
            print(f"Size: {family['family_size']} members")
            
            print("\nMembers:")
            for member in family['members']:
                print(f"  {member['role']}: {member['gender']}, {member['age']}, {member['employment_status']}")
    
    finally:
        generator.close()

if __name__ == "__main__":
    # Make sure to set environment variable before running:
    # export NEON_CONNECTION_STRINGL="postgresql://user:password@host:5432/database?sslmode=require"
    
    print("Starting database-driven family generation...")
    test_db_family_generation()