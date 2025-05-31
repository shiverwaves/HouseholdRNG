import requests
import pandas as pd
import json
import os
from typing import Dict, List, Any
from datetime import datetime

class EnhancedCensusDataExtractor:
    """
    Extract state-level demographic data from US Census Bureau APIs
    Provides much more granular data than regional aggregates
    """
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv('CENSUS_API_KEY')
        self.base_url = "https://api.census.gov/data"
        self.session = requests.Session()
        
        # Data year - use most recent available
        self.acs_year = "2022"
        
        # State information
        self.state_info = self._load_state_info()
        
    def _load_state_info(self) -> Dict[str, Dict]:
        """
        Load state codes, names, and regional mappings
        """
        return {
            '01': {'name': 'Alabama', 'region': 'South'},
            '02': {'name': 'Alaska', 'region': 'West'},
            '04': {'name': 'Arizona', 'region': 'West'},
            '05': {'name': 'Arkansas', 'region': 'South'},
            '06': {'name': 'California', 'region': 'West'},
            '08': {'name': 'Colorado', 'region': 'West'},
            '09': {'name': 'Connecticut', 'region': 'Northeast'},
            '10': {'name': 'Delaware', 'region': 'South'},
            '11': {'name': 'District of Columbia', 'region': 'South'},
            '12': {'name': 'Florida', 'region': 'South'},
            '13': {'name': 'Georgia', 'region': 'South'},
            '15': {'name': 'Hawaii', 'region': 'West'},
            '16': {'name': 'Idaho', 'region': 'West'},
            '17': {'name': 'Illinois', 'region': 'Midwest'},
            '18': {'name': 'Indiana', 'region': 'Midwest'},
            '19': {'name': 'Iowa', 'region': 'Midwest'},
            '20': {'name': 'Kansas', 'region': 'Midwest'},
            '21': {'name': 'Kentucky', 'region': 'South'},
            '22': {'name': 'Louisiana', 'region': 'South'},
            '23': {'name': 'Maine', 'region': 'Northeast'},
            '24': {'name': 'Maryland', 'region': 'South'},
            '25': {'name': 'Massachusetts', 'region': 'Northeast'},
            '26': {'name': 'Michigan', 'region': 'Midwest'},
            '27': {'name': 'Minnesota', 'region': 'Midwest'},
            '28': {'name': 'Mississippi', 'region': 'South'},
            '29': {'name': 'Missouri', 'region': 'Midwest'},
            '30': {'name': 'Montana', 'region': 'West'},
            '31': {'name': 'Nebraska', 'region': 'Midwest'},
            '32': {'name': 'Nevada', 'region': 'West'},
            '33': {'name': 'New Hampshire', 'region': 'Northeast'},
            '34': {'name': 'New Jersey', 'region': 'Northeast'},
            '35': {'name': 'New Mexico', 'region': 'West'},
            '36': {'name': 'New York', 'region': 'Northeast'},
            '37': {'name': 'North Carolina', 'region': 'South'},
            '38': {'name': 'North Dakota', 'region': 'Midwest'},
            '39': {'name': 'Ohio', 'region': 'Midwest'},
            '40': {'name': 'Oklahoma', 'region': 'South'},
            '41': {'name': 'Oregon', 'region': 'West'},
            '42': {'name': 'Pennsylvania', 'region': 'Northeast'},
            '44': {'name': 'Rhode Island', 'region': 'Northeast'},
            '45': {'name': 'South Carolina', 'region': 'South'},
            '46': {'name': 'South Dakota', 'region': 'Midwest'},
            '47': {'name': 'Tennessee', 'region': 'South'},
            '48': {'name': 'Texas', 'region': 'South'},
            '49': {'name': 'Utah', 'region': 'West'},
            '50': {'name': 'Vermont', 'region': 'Northeast'},
            '51': {'name': 'Virginia', 'region': 'South'},
            '53': {'name': 'Washington', 'region': 'West'},
            '54': {'name': 'West Virginia', 'region': 'South'},
            '55': {'name': 'Wisconsin', 'region': 'Midwest'},
            '56': {'name': 'Wyoming', 'region': 'West'},
        }
    
    def get_state_level_race_data(self) -> Dict[str, Dict[str, float]]:
        """
        Extract race/ethnicity distribution for each state
        """
        
        print("  - Extracting state-level race/ethnicity data...")
        
        url = f"{self.base_url}/{self.acs_year}/acs/acs1"
        
        # Race variables from B03002 table
        variables = {
            'B03002_001E': 'total_population',
            'B03002_003E': 'white_non_hispanic',
            'B03002_004E': 'black',
            'B03002_006E': 'asian',
            'B03002_005E': 'native_american', 
            'B03002_007E': 'pacific_islander',
            'B03002_009E': 'two_or_more_races',
            'B03002_012E': 'hispanic_total'
        }
        
        params = {
            'get': ','.join(variables.keys()),
            'for': 'state:*'
        }
        
        if self.api_key:
            params['key'] = self.api_key
            
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            state_race_data = {}
            
            # Parse each state's data
            for row in data[1:]:  # Skip header
                values = row[:-1]  # All except state code
                state_code = row[-1]
                
                if state_code not in self.state_info:
                    continue
                    
                # Create data dictionary for this state
                race_data = {}
                for i, var_code in enumerate(variables.keys()):
                    try:
                        race_data[variables[var_code]] = int(values[i]) if values[i] else 0
                    except (ValueError, TypeError):
                        race_data[variables[var_code]] = 0
                
                # Calculate percentages
                total = race_data['total_population']
                if total > 0:
                    state_percentages = {
                        'WHITE_NON_HISPANIC': (race_data['white_non_hispanic'] / total) * 100,
                        'BLACK': (race_data['black'] / total) * 100,
                        'HISPANIC': (race_data['hispanic_total'] / total) * 100,
                        'ASIAN': (race_data['asian'] / total) * 100,
                        'NATIVE': (race_data['native_american'] / total) * 100,
                        'PACIFIC_ISLANDER': (race_data['pacific_islander'] / total) * 100,
                        'TWO_OR_MORE': (race_data['two_or_more_races'] / total) * 100,
                        'total_population': total
                    }
                    
                    state_name = self.state_info[state_code]['name']
                    state_race_data[state_code] = {
                        'state_name': state_name,
                        'region': self.state_info[state_code]['region'],
                        'demographics': state_percentages
                    }
            
            print(f"    Successfully extracted data for {len(state_race_data)} states")
            return state_race_data
            
        except Exception as e:
            print(f"    Error extracting state race data: {e}")
            return {}
    
    def get_state_level_household_data(self) -> Dict[str, Dict[str, float]]:
        """
        Extract household composition data for each state
        """
        
        print("  - Extracting state-level household composition data...")
        
        url = f"{self.base_url}/{self.acs_year}/acs/acs1"
        
        # Household variables from B11001 and B25010
        variables = {
            'B11001_001E': 'total_households',
            'B11001_002E': 'family_households',
            'B11001_003E': 'married_couple_families',
            'B11001_005E': 'male_householder_no_wife',
            'B11001_006E': 'female_householder_no_husband',
            'B11001_009E': 'living_alone',
            'B25010_001E': 'avg_household_size'
        }
        
        params = {
            'get': ','.join(variables.keys()),
            'for': 'state:*'
        }
        
        if self.api_key:
            params['key'] = self.api_key
            
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            state_household_data = {}
            
            for row in data[1:]:  # Skip header
                values = row[:-1]
                state_code = row[-1]
                
                if state_code not in self.state_info:
                    continue
                
                # Parse household data
                household_data = {}
                for i, var_code in enumerate(variables.keys()):
                    try:
                        if var_code == 'B25010_001E':  # Average household size
                            household_data[variables[var_code]] = float(values[i]) if values[i] else 0
                        else:
                            household_data[variables[var_code]] = int(values[i]) if values[i] else 0
                    except (ValueError, TypeError):
                        household_data[variables[var_code]] = 0
                
                # Calculate family structure percentages
                total = household_data['total_households']
                if total > 0:
                    family_structure = {
                        'SINGLE_PERSON': (household_data['living_alone'] / total) * 100,
                        'MARRIED_COUPLE': (household_data['married_couple_families'] / total) * 100,
                        'SINGLE_PARENT_MALE': (household_data['male_householder_no_wife'] / total) * 100,
                        'SINGLE_PARENT_FEMALE': (household_data['female_householder_no_husband'] / total) * 100,
                        'avg_household_size': household_data['avg_household_size']
                    }
                    
                    state_name = self.state_info[state_code]['name']
                    state_household_data[state_code] = {
                        'state_name': state_name,
                        'region': self.state_info[state_code]['region'],
                        'family_structures': family_structure
                    }
            
            print(f"    Successfully extracted household data for {len(state_household_data)} states")
            return state_household_data
            
        except Exception as e:
            print(f"    Error extracting state household data: {e}")
            return {}
    
    def get_state_level_education_data(self) -> Dict[str, Dict[str, float]]:
        """
        Extract education enrollment and attainment data by state
        """
        
        print("  - Extracting state-level education data...")
        
        url = f"{self.base_url}/{self.acs_year}/acs/acs1"
        
        # Education variables
        variables = {
            'B14001_002E': 'enrolled_nursery_preschool',
            'B14001_005E': 'enrolled_high_school',
            'B14001_006E': 'enrolled_college_undergraduate',
            'B14001_007E': 'enrolled_graduate_professional',
            'B14001_008E': 'not_enrolled',
            'B15003_022E': 'bachelors_degree',
            'B15003_023E': 'masters_degree',
            'B15003_024E': 'professional_degree',
            'B15003_025E': 'doctorate_degree'
        }
        
        params = {
            'get': ','.join(variables.keys()),
            'for': 'state:*'
        }
        
        if self.api_key:
            params['key'] = self.api_key
            
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            state_education_data = {}
            
            for row in data[1:]:  # Skip header
                values = row[:-1]
                state_code = row[-1]
                
                if state_code not in self.state_info:
                    continue
                
                # Parse education data
                education_data = {}
                for i, var_code in enumerate(variables.keys()):
                    try:
                        education_data[variables[var_code]] = int(values[i]) if values[i] else 0
                    except (ValueError, TypeError):
                        education_data[variables[var_code]] = 0
                
                # Calculate education statistics
                total_enrollment_age = sum([
                    education_data['enrolled_nursery_preschool'],
                    education_data['enrolled_high_school'],
                    education_data['enrolled_college_undergraduate'],
                    education_data['enrolled_graduate_professional'],
                    education_data['not_enrolled']
                ])
                
                total_adults = sum([
                    education_data['bachelors_degree'],
                    education_data['masters_degree'],
                    education_data['professional_degree'],
                    education_data['doctorate_degree']
                ])
                
                if total_enrollment_age > 0 and total_adults > 0:
                    education_stats = {
                        'high_school_enrollment_rate': (education_data['enrolled_high_school'] / total_enrollment_age) * 100,
                        'college_enrollment_rate': (education_data['enrolled_college_undergraduate'] / total_enrollment_age) * 100,
                        'graduate_enrollment_rate': (education_data['enrolled_graduate_professional'] / total_enrollment_age) * 100,
                        'college_graduation_rate': ((education_data['bachelors_degree'] + education_data['masters_degree'] + 
                                                   education_data['professional_degree'] + education_data['doctorate_degree']) / total_adults) * 100
                    }
                    
                    state_name = self.state_info[state_code]['name']
                    state_education_data[state_code] = {
                        'state_name': state_name,
                        'region': self.state_info[state_code]['region'],
                        'education_stats': education_stats
                    }
            
            print(f"    Successfully extracted education data for {len(state_education_data)} states")
            return state_education_data
            
        except Exception as e:
            print(f"    Error extracting state education data: {e}")
            return {}
    
    def get_state_employment_rate_data(self, state_code: str = None) -> Dict[str, Any]:
        """
        Extract employment rate data for a specific state or all states (omitting race breakdowns)
        
        Args:
            state_code: Two-digit state FIPS code (e.g., '06' for California)
                       If None, returns data for all states
        """
        
        print(f"  - Extracting employment rate data for state {state_code or 'all states'}...")
        
        url = f"{self.base_url}/{self.acs_year}/acs/acs1"
        
        # Employment variables from B23025 table (overall employment status)
        variables = {
            'B23025_001E': 'total_pop_16_over',        # Total population 16 years and over
            'B23025_002E': 'in_labor_force',           # In labor force
            'B23025_003E': 'civilian_labor_force',     # Civilian labor force
            'B23025_004E': 'employed_civilian',        # Employed (civilian)
            'B23025_005E': 'unemployed_civilian',      # Unemployed (civilian)
            'B23025_006E': 'armed_forces',             # Armed Forces
            'B23025_007E': 'not_in_labor_force'        # Not in labor force
        }
        
        # Set up API parameters
        params = {
            'get': ','.join(variables.keys()),
            'for': f'state:{state_code}' if state_code else 'state:*'
        }
        
        if self.api_key:
            params['key'] = self.api_key
            
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            employment_data = {}
            
            # Parse employment data for each state returned
            for row in data[1:]:  # Skip header
                values = row[:-1]  # All except state code
                current_state_code = row[-1]
                
                if current_state_code not in self.state_info:
                    continue
                    
                # Parse raw employment numbers
                employment_raw = {}
                for i, var_code in enumerate(variables.keys()):
                    try:
                        employment_raw[variables[var_code]] = int(values[i]) if values[i] else 0
                    except (ValueError, TypeError):
                        employment_raw[variables[var_code]] = 0
                
                # Calculate employment rates and statistics
                total_pop_16_over = employment_raw['total_pop_16_over']
                labor_force = employment_raw['in_labor_force']
                employed = employment_raw['employed_civilian'] + employment_raw['armed_forces']
                unemployed = employment_raw['unemployed_civilian']
                not_in_labor_force = employment_raw['not_in_labor_force']
                
                if total_pop_16_over > 0 and labor_force > 0:
                    employment_stats = {
                        # Core employment metrics
                        'employment_rate': (employed / labor_force) * 100,                    # Employed / Labor Force
                        'unemployment_rate': (unemployed / labor_force) * 100,                # Unemployed / Labor Force  
                        'labor_force_participation_rate': (labor_force / total_pop_16_over) * 100,  # Labor Force / Total Pop 16+
                        
                        # Additional useful metrics
                        'employment_to_population_ratio': (employed / total_pop_16_over) * 100,     # Employed / Total Pop 16+
                        'not_in_labor_force_rate': (not_in_labor_force / total_pop_16_over) * 100, # Not in LF / Total Pop 16+
                        
                        # Raw numbers for context
                        'total_population_16_over': total_pop_16_over,
                        'total_labor_force': labor_force,
                        'total_employed': employed,
                        'total_unemployed': unemployed,
                        'total_not_in_labor_force': not_in_labor_force,
                        
                        # Civilian vs military breakdown
                        'civilian_employed': employment_raw['employed_civilian'],
                        'armed_forces': employment_raw['armed_forces']
                    }
                    
                    state_name = self.state_info[current_state_code]['name']
                    employment_data[current_state_code] = {
                        'state_name': state_name,
                        'region': self.state_info[current_state_code]['region'],
                        'employment_statistics': employment_stats
                    }
            
            print(f"    Successfully extracted employment data for {len(employment_data)} state(s)")
            return employment_data
            
        except Exception as e:
            print(f"    Error extracting employment data: {e}")
            return {}

    def get_state_employment_by_age_groups(self, state_code: str) -> Dict[str, Any]:
        """
        Get employment rates broken down by age groups for a specific state
        (still omitting race)
        """
        
        print(f"  - Extracting age-specific employment data for state {state_code}...")
        
        url = f"{self.base_url}/{self.acs_year}/acs/acs1"
        
        # Age-specific employment variables from B23001 table
        age_employment_vars = {
            'B23001_001E': 'total_pop_16_over',
            'B23001_007E': 'male_16_19_in_labor_force',
            'B23001_014E': 'male_20_21_in_labor_force', 
            'B23001_021E': 'male_22_24_in_labor_force',
            'B23001_028E': 'male_25_29_in_labor_force',
            'B23001_035E': 'male_30_34_in_labor_force',
            'B23001_042E': 'male_35_44_in_labor_force',
            'B23001_049E': 'male_45_54_in_labor_force',
            'B23001_056E': 'male_55_59_in_labor_force',
            'B23001_063E': 'male_60_61_in_labor_force',
            'B23001_070E': 'male_62_64_in_labor_force',
            'B23001_075E': 'male_65_69_in_labor_force',
            'B23001_080E': 'male_70_74_in_labor_force',
            'B23001_085E': 'male_75_over_in_labor_force',
            # Female equivalents (B23001_093E onwards)
            'B23001_093E': 'female_16_19_in_labor_force',
            'B23001_100E': 'female_20_21_in_labor_force',
            'B23001_107E': 'female_22_24_in_labor_force',
            'B23001_114E': 'female_25_29_in_labor_force',
            'B23001_121E': 'female_30_34_in_labor_force',
            'B23001_128E': 'female_35_44_in_labor_force',
            'B23001_135E': 'female_45_54_in_labor_force',
            'B23001_142E': 'female_55_59_in_labor_force',
            'B23001_149E': 'female_60_61_in_labor_force',
            'B23001_156E': 'female_62_64_in_labor_force',
            'B23001_161E': 'female_65_69_in_labor_force',
            'B23001_166E': 'female_70_74_in_labor_force',
            'B23001_171E': 'female_75_over_in_labor_force'
        }
        
        params = {
            'get': ','.join(age_employment_vars.keys()),
            'for': f'state:{state_code}'
        }
        
        if self.api_key:
            params['key'] = self.api_key
            
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if len(data) > 1:  # Has data beyond header
                values = data[1][:-1]  # Exclude state code
                
                # Parse age-specific data
                age_data = {}
                for i, var_code in enumerate(age_employment_vars.keys()):
                    try:
                        age_data[age_employment_vars[var_code]] = int(values[i]) if values[i] else 0
                    except (ValueError, TypeError):
                        age_data[age_employment_vars[var_code]] = 0
                
                # Calculate age group employment rates
                age_employment_rates = {
                    'youth_16_24': {
                        'male_labor_force': age_data['male_16_19_in_labor_force'] + age_data['male_20_21_in_labor_force'] + age_data['male_22_24_in_labor_force'],
                        'female_labor_force': age_data['female_16_19_in_labor_force'] + age_data['female_20_21_in_labor_force'] + age_data['female_22_24_in_labor_force']
                    },
                    'prime_working_25_54': {
                        'male_labor_force': age_data['male_25_29_in_labor_force'] + age_data['male_30_34_in_labor_force'] + age_data['male_35_44_in_labor_force'] + age_data['male_45_54_in_labor_force'],
                        'female_labor_force': age_data['female_25_29_in_labor_force'] + age_data['female_30_34_in_labor_force'] + age_data['female_35_44_in_labor_force'] + age_data['female_45_54_in_labor_force']
                    },
                    'older_workers_55_plus': {
                        'male_labor_force': age_data['male_55_59_in_labor_force'] + age_data['male_60_61_in_labor_force'] + age_data['male_62_64_in_labor_force'] + age_data['male_65_69_in_labor_force'] + age_data['male_70_74_in_labor_force'] + age_data['male_75_over_in_labor_force'],
                        'female_labor_force': age_data['female_55_59_in_labor_force'] + age_data['female_60_61_in_labor_force'] + age_data['female_62_64_in_labor_force'] + age_data['female_65_69_in_labor_force'] + age_data['female_70_74_in_labor_force'] + age_data['female_75_over_in_labor_force']
                    }
                }
                
                return {
                    'state_name': self.state_info[state_code]['name'],
                    'age_employment_breakdown': age_employment_rates,
                    'total_population_16_over': age_data['total_pop_16_over']
                }
                
        except Exception as e:
            print(f"    Error extracting age-specific employment data: {e}")
            return {}

    def extract_state_employment_data(self, target_state_code: str) -> Dict[str, Any]:
        """
        Extract comprehensive employment data for a specific state
        """
        
        print(f"Extracting employment data for {self.state_info[target_state_code]['name']}...")
        
        employment_data = {
            'extraction_date': datetime.now().isoformat(),
            'data_year': self.acs_year,
            'source': 'US Census Bureau API - Employment Statistics',
            'target_state': {
                'code': target_state_code,
                'name': self.state_info[target_state_code]['name'],
                'region': self.state_info[target_state_code]['region']
            }
        }
        
        # Get overall employment rates
        overall_employment = self.get_state_employment_rate_data(target_state_code)
        if target_state_code in overall_employment:
            employment_data['overall_employment'] = overall_employment[target_state_code]['employment_statistics']
        
        # Get age-specific employment data
        age_employment = self.get_state_employment_by_age_groups(target_state_code)
        if age_employment:
            employment_data['age_specific_employment'] = age_employment
        
        return employment_data
    
    def calculate_state_weights(self, state_race_data: Dict) -> Dict[str, float]:
        """
        Calculate population-based weights for realistic state selection
        """
        
        total_us_population = sum(
            data['demographics']['total_population'] 
            for data in state_race_data.values()
        )
        
        state_weights = {}
        for state_code, data in state_race_data.items():
            state_pop = data['demographics']['total_population']
            weight = (state_pop / total_us_population) * 100
            state_weights[state_code] = weight
        
        return state_weights
    
    def extract_all_state_data(self, include_employment: bool = True) -> Dict[str, Any]:
        """
        Extract comprehensive state-level demographic data including employment
        
        Args:
            include_employment: Whether to include employment data extraction
        """
        
        print("Extracting state-level demographic data from Census Bureau...")
        
        all_data = {
            'extraction_date': datetime.now().isoformat(),
            'data_year': self.acs_year,
            'source': 'US Census Bureau API - State Level',
            'data_granularity': 'state_level'
        }
        
        # Extract state-level data
        state_race_data = self.get_state_level_race_data()
        state_household_data = self.get_state_level_household_data()
        state_education_data = self.get_state_level_education_data()
        
        # Extract employment data for all states if requested
        state_employment_data = {}
        if include_employment:
            state_employment_data = self.get_state_employment_rate_data()
        
        # Calculate state population weights
        if state_race_data:
            state_weights = self.calculate_state_weights(state_race_data)
            all_data['state_weights'] = state_weights
        
        # Combine all state data
        all_data['state_demographics'] = {}
        
        for state_code in self.state_info.keys():
            if state_code in state_race_data:
                combined_state_data = {
                    'state_name': self.state_info[state_code]['name'],
                    'region': self.state_info[state_code]['region'],
                    'population_weight': state_weights.get(state_code, 0),
                    'race_ethnicity': state_race_data[state_code]['demographics'],
                    'family_structures': state_household_data.get(state_code, {}).get('family_structures', {}),
                    'education_stats': state_education_data.get(state_code, {}).get('education_stats', {})
                }
                
                # Add employment data if available
                if state_code in state_employment_data:
                    combined_state_data['employment_stats'] = state_employment_data[state_code]['employment_statistics']
                
                all_data['state_demographics'][state_code] = combined_state_data
        
        # Also calculate regional aggregates for comparison
        all_data['regional_aggregates'] = self._calculate_regional_aggregates(all_data['state_demographics'])
        
        data_count = len(all_data['state_demographics'])
        employment_note = " (including employment data)" if include_employment else ""
        print(f"State-level data extraction complete! Extracted data for {data_count} states{employment_note}")
        return all_data
    
    def _calculate_regional_aggregates(self, state_data: Dict) -> Dict[str, Any]:
        """
        Calculate population-weighted regional averages from state data
        """
        
        regions = ['Northeast', 'Midwest', 'South', 'West']
        regional_data = {}
        
        for region in regions:
            region_states = [
                state_code for state_code, data in state_data.items()
                if data['region'] == region
            ]
            
            if not region_states:
                continue
            
            # Calculate population-weighted averages
            total_weight = sum(state_data[state]['population_weight'] for state in region_states)
            
            if total_weight > 0:
                # Weighted average race demographics
                weighted_race = {}
                race_keys = ['WHITE_NON_HISPANIC', 'BLACK', 'HISPANIC', 'ASIAN', 'NATIVE', 'PACIFIC_ISLANDER', 'TWO_OR_MORE']
                
                for race_key in race_keys:
                    weighted_sum = sum(
                        state_data[state]['race_ethnicity'].get(race_key, 0) * state_data[state]['population_weight']
                        for state in region_states
                    )
                    weighted_race[race_key] = weighted_sum / total_weight

                # Weighted average employment stats if available
                weighted_employment = {}
                employment_keys = ['employment_rate', 'unemployment_rate', 'labor_force_participation_rate']
                
                states_with_employment = [
                    state for state in region_states 
                    if 'employment_stats' in state_data[state] and state_data[state]['employment_stats']
                ]
                
                if states_with_employment:
                    employment_weight = sum(state_data[state]['population_weight'] for state in states_with_employment)
                    
                    for emp_key in employment_keys:
                        weighted_sum = sum(
                            state_data[state]['employment_stats'].get(emp_key, 0) * state_data[state]['population_weight']
                            for state in states_with_employment
                            if emp_key in state_data[state]['employment_stats']
                        )
                        if employment_weight > 0:
                            weighted_employment[emp_key] = weighted_sum / employment_weight
                
                regional_data[region] = {
                    'population_percent': total_weight,
                    'race_ethnicity': weighted_race,
                    'employment_stats': weighted_employment,
                    'states_included': [state_data[state]['state_name'] for state in region_states]
                }
        
        return regional_data

def main():
    """
    Example usage of enhanced state-level extractor with employment data
    """
    
    extractor = EnhancedCensusDataExtractor()
    
    # Extract all state-level data including employment
    state_data = extractor.extract_all_state_data(include_employment=True)
    
    # Save comprehensive data
    with open('complete_state_demographic_data.json', 'w') as f:
        json.dump(state_data, f, indent=2)
    
    # Print interesting state variations
    print("\n=== STATE-LEVEL DEMOGRAPHIC AND EMPLOYMENT VARIATIONS ===")
    
    if 'state_demographics' in state_data:
        
        # Find states with highest/lowest Hispanic populations
        hispanic_by_state = []
        for state_code, data in state_data['state_demographics'].items():
            hispanic_pct = data['race_ethnicity'].get('HISPANIC', 0)
            hispanic_by_state.append((data['state_name'], hispanic_pct))
        
        hispanic_by_state.sort(key=lambda x: x[1], reverse=True)
        
        print("\nHispanic Population by State:")
        print("Highest:")
        for state, pct in hispanic_by_state[:5]:
            print(f"  {state}: {pct:.1f}%")
        print("Lowest:")
        for state, pct in hispanic_by_state[-5:]:
            print(f"  {state}: {pct:.1f}%")
        
        # Find states with highest single-person household rates
        single_person_by_state = []
        for state_code, data in state_data['state_demographics'].items():
            single_pct = data['family_structures'].get('SINGLE_PERSON', 0)
            if single_pct > 0:
                single_person_by_state.append((data['state_name'], single_pct))
        
        if single_person_by_state:
            single_person_by_state.sort(key=lambda x: x[1], reverse=True)
            
            print("\nSingle-Person Households by State:")
            print("Highest:")
            for state, pct in single_person_by_state[:5]:
                print(f"  {state}: {pct:.1f}%")

        # Find states with highest/lowest employment rates
        employment_by_state = []
        for state_code, data in state_data['state_demographics'].items():
            if 'employment_stats' in data and data['employment_stats']:
                employment_rate = data['employment_stats'].get('employment_rate', 0)
                employment_by_state.append((data['state_name'], employment_rate))
        
        if employment_by_state:
            employment_by_state.sort(key=lambda x: x[1], reverse=True)
            
            print("\nEmployment Rates by State:")
            print("Highest:")
            for state, rate in employment_by_state[:5]:
                print(f"  {state}: {rate:.1f}%")
            print("Lowest:")
            for state, rate in employment_by_state[-5:]:
                print(f"  {state}: {rate:.1f}%")

        # Find states with highest unemployment rates
        unemployment_by_state = []
        for state_code, data in state_data['state_demographics'].items():
            if 'employment_stats' in data and data['employment_stats']:
                unemployment_rate = data['employment_stats'].get('unemployment_rate', 0)
                unemployment_by_state.append((data['state_name'], unemployment_rate))
        
        if unemployment_by_state:
            unemployment_by_state.sort(key=lambda x: x[1], reverse=True)
            
            print("\nUnemployment Rates by State:")
            print("Highest:")
            for state, rate in unemployment_by_state[:5]:
                print(f"  {state}: {rate:.1f}%")

    # Example: Extract detailed employment data for a specific state (California)
    print("\n=== DETAILED CALIFORNIA EMPLOYMENT DATA ===")
    ca_employment = extractor.extract_state_employment_data('06')
    
    if 'overall_employment' in ca_employment:
        emp_stats = ca_employment['overall_employment']
        print(f"California Employment Statistics:")
        print(f"  Employment Rate: {emp_stats.get('employment_rate', 0):.1f}%")
        print(f"  Unemployment Rate: {emp_stats.get('unemployment_rate', 0):.1f}%")
        print(f"  Labor Force Participation: {emp_stats.get('labor_force_participation_rate', 0):.1f}%")
        print(f"  Total Labor Force: {emp_stats.get('total_labor_force', 0):,}")

if __name__ == "__main__":
    main()