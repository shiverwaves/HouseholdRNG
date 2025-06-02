#!/usr/bin/env python3
"""
Combined Census Data Extractor and Neon Database Importer

Extracts demographic data from US Census Bureau API and stores it directly in Neon database.
No intermediate JSON files are created. Database schema is automatically created if it doesn't exist.

Usage:
    python combined_census_importer.py                    # Extract and import all data (auto-creates schema)
    python combined_census_importer.py --setup-schema     # Setup database schema only
    python combined_census_importer.py --employment-only  # Import only employment data
"""

import psycopg2
import requests
import pandas as pd
import os
import sys
from typing import Dict, List, Any
from datetime import datetime

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # dotenv not available, use system environment variables only
    pass

class EnhancedCensusDataExtractor:
    """
    Extract state-level demographic data from US Census Bureau APIs
    """
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv('CENSUS_API_KEY')
        self.base_url = "https://api.census.gov/data"
        self.session = requests.Session()
        self.acs_year = "2022"
        self.state_info = self._load_state_info()
        
    def _load_state_info(self) -> Dict[str, Dict]:
        """Load state codes, names, and regional mappings"""
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
        """Extract race/ethnicity distribution for each state"""
        print("  - Extracting state-level race/ethnicity data...")
        
        url = f"{self.base_url}/{self.acs_year}/acs/acs1"
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
            
            for row in data[1:]:  # Skip header
                values = row[:-1]
                state_code = row[-1]
                
                if state_code not in self.state_info:
                    continue
                    
                race_data = {}
                for i, var_code in enumerate(variables.keys()):
                    try:
                        race_data[variables[var_code]] = int(values[i]) if values[i] else 0
                    except (ValueError, TypeError):
                        race_data[variables[var_code]] = 0
                
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
        """Extract household composition data for each state"""
        print("  - Extracting state-level household composition data...")
        
        url = f"{self.base_url}/{self.acs_year}/acs/acs1"
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
            
            for row in data[1:]:
                values = row[:-1]
                state_code = row[-1]
                
                if state_code not in self.state_info:
                    continue
                
                household_data = {}
                for i, var_code in enumerate(variables.keys()):
                    try:
                        if var_code == 'B25010_001E':
                            household_data[variables[var_code]] = float(values[i]) if values[i] else 0
                        else:
                            household_data[variables[var_code]] = int(values[i]) if values[i] else 0
                    except (ValueError, TypeError):
                        household_data[variables[var_code]] = 0
                
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
        """Extract education enrollment and attainment data by state"""
        print("  - Extracting state-level education data...")
        
        url = f"{self.base_url}/{self.acs_year}/acs/acs1"
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
            
            for row in data[1:]:
                values = row[:-1]
                state_code = row[-1]
                
                if state_code not in self.state_info:
                    continue
                
                education_data = {}
                for i, var_code in enumerate(variables.keys()):
                    try:
                        education_data[variables[var_code]] = int(values[i]) if values[i] else 0
                    except (ValueError, TypeError):
                        education_data[variables[var_code]] = 0
                
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
        """Extract employment rate data for a specific state or all states"""
        print(f"  - Extracting employment rate data for state {state_code or 'all states'}...")
        
        url = f"{self.base_url}/{self.acs_year}/acs/acs1"
        variables = {
            'B23025_001E': 'total_pop_16_over',
            'B23025_002E': 'in_labor_force',
            'B23025_003E': 'civilian_labor_force',
            'B23025_004E': 'employed_civilian',
            'B23025_005E': 'unemployed_civilian',
            'B23025_006E': 'armed_forces',
            'B23025_007E': 'not_in_labor_force'
        }
        
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
            
            for row in data[1:]:
                values = row[:-1]
                current_state_code = row[-1]
                
                if current_state_code not in self.state_info:
                    continue
                    
                employment_raw = {}
                for i, var_code in enumerate(variables.keys()):
                    try:
                        employment_raw[variables[var_code]] = int(values[i]) if values[i] else 0
                    except (ValueError, TypeError):
                        employment_raw[variables[var_code]] = 0
                
                total_pop_16_over = employment_raw['total_pop_16_over']
                labor_force = employment_raw['in_labor_force']
                employed = employment_raw['employed_civilian'] + employment_raw['armed_forces']
                unemployed = employment_raw['unemployed_civilian']
                not_in_labor_force = employment_raw['not_in_labor_force']
                
                if total_pop_16_over > 0 and labor_force > 0:
                    employment_stats = {
                        'employment_rate': (employed / labor_force) * 100,
                        'unemployment_rate': (unemployed / labor_force) * 100,
                        'labor_force_participation_rate': (labor_force / total_pop_16_over) * 100,
                        'employment_to_population_ratio': (employed / total_pop_16_over) * 100,
                        'not_in_labor_force_rate': (not_in_labor_force / total_pop_16_over) * 100,
                        'total_population_16_over': total_pop_16_over,
                        'total_labor_force': labor_force,
                        'total_employed': employed,
                        'total_unemployed': unemployed,
                        'total_not_in_labor_force': not_in_labor_force,
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

    def calculate_state_weights(self, state_race_data: Dict) -> Dict[str, float]:
        """Calculate population-based weights for realistic state selection"""
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

class NeonDataImporter:
    """Import Census demographic data directly into Neon database"""
    
    def __init__(self):
        self.connection_string = os.getenv('NEON_CONNECTION_STRING')
        if not self.connection_string:
            raise ValueError("NEON_CONNECTION_STRING environment variable is required")
        
        self.conn = psycopg2.connect(self.connection_string)
        self.cursor = self.conn.cursor()
        
        self.import_stats = {
            'states_imported': 0,
            'race_entries': 0,
            'family_structure_entries': 0,
            'employment_entries': 0,
            'education_entries': 0
        }
    
    def setup_database_schema(self):
        """Create database schema if it doesn't exist"""
        print("Setting up database schema...")
        
        # Check if schema already exists
        self.cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = 'state_demographics'
        """)
        
        if self.cursor.fetchone():
            print("  ✓ Database schema already exists")
            return
        
        try:
            print("  Creating database schema...")
            
            # Create regions table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS regions (
                    id SERIAL PRIMARY KEY,
                    region_name VARCHAR(50) UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Insert standard US regions
            self.cursor.execute("""
                INSERT INTO regions (region_name) VALUES 
                ('Northeast'), ('Midwest'), ('South'), ('West')
                ON CONFLICT (region_name) DO NOTHING
            """)
            
            # Create race_ethnicity table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS race_ethnicity (
                    id SERIAL PRIMARY KEY,
                    race_key VARCHAR(50) UNIQUE NOT NULL,
                    race_name VARCHAR(100) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Insert race/ethnicity categories
            race_categories = [
                ('WHITE_NON_HISPANIC', 'White Non-Hispanic'),
                ('BLACK', 'Black or African American'),
                ('HISPANIC', 'Hispanic or Latino'),
                ('ASIAN', 'Asian'),
                ('NATIVE', 'American Indian and Alaska Native'),
                ('PACIFIC_ISLANDER', 'Native Hawaiian and Other Pacific Islander'),
                ('TWO_OR_MORE', 'Two or More Races')
            ]
            
            for race_key, race_name in race_categories:
                self.cursor.execute("""
                    INSERT INTO race_ethnicity (race_key, race_name) VALUES (%s, %s)
                    ON CONFLICT (race_key) DO NOTHING
                """, (race_key, race_name))
            
            # Create family_structures table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS family_structures (
                    id SERIAL PRIMARY KEY,
                    structure_key VARCHAR(50) UNIQUE NOT NULL,
                    structure_name VARCHAR(100) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Insert family structure types
            family_structures = [
                ('SINGLE_PERSON', 'Single Person Household'),
                ('MARRIED_COUPLE', 'Married Couple Family'),
                ('SINGLE_PARENT_MALE', 'Single Parent - Male Householder'),
                ('SINGLE_PARENT_FEMALE', 'Single Parent - Female Householder')
            ]
            
            for structure_key, structure_name in family_structures:
                self.cursor.execute("""
                    INSERT INTO family_structures (structure_key, structure_name) VALUES (%s, %s)
                    ON CONFLICT (structure_key) DO NOTHING
                """, (structure_key, structure_name))
            
            # Create state_demographics table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS state_demographics (
                    id SERIAL PRIMARY KEY,
                    state_code VARCHAR(2) UNIQUE NOT NULL,
                    state_name VARCHAR(100) NOT NULL,
                    region_id INTEGER REFERENCES regions(id),
                    total_population BIGINT,
                    population_weight DECIMAL(10,6),
                    data_year INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create state_race_ethnicity table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS state_race_ethnicity (
                    id SERIAL PRIMARY KEY,
                    state_id INTEGER REFERENCES state_demographics(id) ON DELETE CASCADE,
                    race_id INTEGER REFERENCES race_ethnicity(id),
                    population_count BIGINT,
                    population_percent DECIMAL(8,4),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(state_id, race_id)
                )
            """)
            
            # Create state_family_structures table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS state_family_structures (
                    id SERIAL PRIMARY KEY,
                    state_id INTEGER REFERENCES state_demographics(id) ON DELETE CASCADE,
                    structure_id INTEGER REFERENCES family_structures(id),
                    household_count BIGINT,
                    probability_percent DECIMAL(8,4),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(state_id, structure_id)
                )
            """)
            
            # Create state_employment_stats table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS state_employment_stats (
                    id SERIAL PRIMARY KEY,
                    state_id INTEGER REFERENCES state_demographics(id) ON DELETE CASCADE,
                    employment_rate DECIMAL(8,4),
                    unemployment_rate DECIMAL(8,4),
                    labor_force_participation_rate DECIMAL(8,4),
                    employment_to_population_ratio DECIMAL(8,4),
                    total_labor_force BIGINT,
                    total_employed BIGINT,
                    total_unemployed BIGINT,
                    civilian_employed BIGINT,
                    armed_forces BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(state_id)
                )
            """)
            
            # Create state_education_stats table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS state_education_stats (
                    id SERIAL PRIMARY KEY,
                    state_id INTEGER REFERENCES state_demographics(id) ON DELETE CASCADE,
                    high_school_enrollment_rate DECIMAL(8,4),
                    college_enrollment_rate DECIMAL(8,4),
                    graduate_enrollment_rate DECIMAL(8,4),
                    college_graduation_rate DECIMAL(8,4),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(state_id)
                )
            """)
            
            # Create employment_rates table for family generation
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS employment_rates (
                    id SERIAL PRIMARY KEY,
                    race_id INTEGER REFERENCES race_ethnicity(id),
                    age_group VARCHAR(20) DEFAULT 'All',
                    employed_rate DECIMAL(8,4),
                    unemployed_rate DECIMAL(8,4),
                    not_in_labor_force_rate DECIMAL(8,4),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(race_id, age_group)
                )
            """)
            
            # Initialize employment_rates with default values for all races
            self.cursor.execute("""
                INSERT INTO employment_rates (race_id, age_group, employed_rate, unemployed_rate, not_in_labor_force_rate)
                SELECT id, 'All', 60.0, 5.0, 35.0
                FROM race_ethnicity
                ON CONFLICT (race_id, age_group) DO NOTHING
            """)
            
            # Create indexes for better performance
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_state_demographics_state_code ON state_demographics(state_code)",
                "CREATE INDEX IF NOT EXISTS idx_state_race_ethnicity_state_id ON state_race_ethnicity(state_id)",
                "CREATE INDEX IF NOT EXISTS idx_state_race_ethnicity_race_id ON state_race_ethnicity(race_id)",
                "CREATE INDEX IF NOT EXISTS idx_state_family_structures_state_id ON state_family_structures(state_id)",
                "CREATE INDEX IF NOT EXISTS idx_state_employment_stats_state_id ON state_employment_stats(state_id)",
                "CREATE INDEX IF NOT EXISTS idx_state_education_stats_state_id ON state_education_stats(state_id)",
                "CREATE INDEX IF NOT EXISTS idx_employment_rates_race_id ON employment_rates(race_id)"
            ]
            
            for index_sql in indexes:
                self.cursor.execute(index_sql)
            
            # Commit all schema changes
            self.conn.commit()
            print("  ✓ Database schema created successfully")
            
        except Exception as e:
            self.conn.rollback()
            print(f"❌ Error creating database schema: {e}")
            raise
    
    def store_state_data(self, state_code: str, state_data: Dict[str, Any], data_year: int):
        """Store data for a single state"""
        state_name = state_data['state_name']
        region = state_data['region']
        population_weight = state_data.get('population_weight', 0)
        
        race_data = state_data.get('race_ethnicity', {})
        total_population = race_data.get('total_population', 0)
        
        try:
            # Get region ID
            self.cursor.execute("SELECT id FROM regions WHERE region_name = %s", (region,))
            region_result = self.cursor.fetchone()
            region_id = region_result[0] if region_result else None
            
            if not region_id:
                print(f"  ⚠️  Warning: Region '{region}' not found for state {state_name}")
            
            # Insert/update state demographics
            self.cursor.execute("""
                INSERT INTO state_demographics (state_code, state_name, region_id, total_population, population_weight, data_year)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (state_code) DO UPDATE SET
                    state_name = EXCLUDED.state_name,
                    region_id = EXCLUDED.region_id,
                    total_population = EXCLUDED.total_population,
                    population_weight = EXCLUDED.population_weight,
                    data_year = EXCLUDED.data_year,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id
            """, (state_code, state_name, region_id, total_population, population_weight, data_year))
            
            state_id = self.cursor.fetchone()[0]
            
            # Store race/ethnicity data
            self._store_race_data(state_id, race_data)
            
            # Store family structure data
            family_data = state_data.get('family_structures', {})
            self._store_family_structure_data(state_id, family_data)
            
            # Store employment data if available
            employment_data = state_data.get('employment_stats', {})
            if employment_data:
                self._store_employment_data(state_id, employment_data)
            
            # Store education data if available
            education_data = state_data.get('education_stats', {})
            if education_data:
                self._store_education_data(state_id, education_data)
            
            self.conn.commit()
            self.import_stats['states_imported'] += 1
            
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Failed to store data for {state_name}: {e}")
    
    def _store_race_data(self, state_id: int, race_data: Dict[str, Any]):
        """Store race/ethnicity breakdown for a state"""
        self.cursor.execute("DELETE FROM state_race_ethnicity WHERE state_id = %s", (state_id,))
        
        total_population = race_data.get('total_population', 0)
        race_entries = 0
        
        for race_key, percentage in race_data.items():
            if race_key == 'total_population':
                continue
                
            self.cursor.execute("SELECT id FROM race_ethnicity WHERE race_key = %s", (race_key,))
            race_result = self.cursor.fetchone()
            
            if race_result:
                race_id = race_result[0]
                population_count = int((percentage / 100) * total_population) if total_population > 0 else 0
                
                self.cursor.execute("""
                    INSERT INTO state_race_ethnicity (state_id, race_id, population_count, population_percent)
                    VALUES (%s, %s, %s, %s)
                """, (state_id, race_id, population_count, percentage))
                
                race_entries += 1
        
        self.import_stats['race_entries'] += race_entries
    
    def _store_family_structure_data(self, state_id: int, family_data: Dict[str, Any]):
        """Store family structure breakdown for a state"""
        self.cursor.execute("DELETE FROM state_family_structures WHERE state_id = %s", (state_id,))
        
        structure_mapping = {
            'SINGLE_PERSON': 'SINGLE_PERSON',
            'MARRIED_COUPLE': 'MARRIED_COUPLE', 
            'SINGLE_PARENT_MALE': 'SINGLE_PARENT_MALE',
            'SINGLE_PARENT_FEMALE': 'SINGLE_PARENT_FEMALE',
            'avg_household_size': None
        }
        
        family_entries = 0
        
        for census_key, percentage in family_data.items():
            if census_key == 'avg_household_size':
                continue
                
            structure_key = structure_mapping.get(census_key, census_key)
            if not structure_key:
                continue
            
            self.cursor.execute("SELECT id FROM family_structures WHERE structure_key = %s", (structure_key,))
            structure_result = self.cursor.fetchone()
            
            if structure_result:
                structure_id = structure_result[0]
                household_count = 0
                
                self.cursor.execute("""
                    INSERT INTO state_family_structures (state_id, structure_id, household_count, probability_percent)
                    VALUES (%s, %s, %s, %s)
                """, (state_id, structure_id, household_count, percentage))
                
                family_entries += 1
        
        self.import_stats['family_structure_entries'] += family_entries
    
    def _store_employment_data(self, state_id: int, employment_data: Dict[str, Any]):
        """Store employment statistics for a state"""
        self.cursor.execute("DELETE FROM state_employment_stats WHERE state_id = %s", (state_id,))
        
        self.cursor.execute("""
            INSERT INTO state_employment_stats (
                state_id, employment_rate, unemployment_rate, labor_force_participation_rate,
                employment_to_population_ratio, total_labor_force, total_employed, total_unemployed,
                civilian_employed, armed_forces
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            state_id,
            employment_data.get('employment_rate'),
            employment_data.get('unemployment_rate'),
            employment_data.get('labor_force_participation_rate'),
            employment_data.get('employment_to_population_ratio'),
            employment_data.get('total_labor_force'),
            employment_data.get('total_employed'),
            employment_data.get('total_unemployed'),
            employment_data.get('civilian_employed'),
            employment_data.get('armed_forces')
        ))
        
        self.import_stats['employment_entries'] += 1
    
    def _store_education_data(self, state_id: int, education_data: Dict[str, Any]):
        """Store education statistics for a state"""
        self.cursor.execute("DELETE FROM state_education_stats WHERE state_id = %s", (state_id,))
        
        self.cursor.execute("""
            INSERT INTO state_education_stats (
                state_id, high_school_enrollment_rate, college_enrollment_rate,
                graduate_enrollment_rate, college_graduation_rate
            ) VALUES (%s, %s, %s, %s, %s)
        """, (
            state_id,
            education_data.get('high_school_enrollment_rate'),
            education_data.get('college_enrollment_rate'),
            education_data.get('graduate_enrollment_rate'),
            education_data.get('college_graduation_rate')
        ))
        
        self.import_stats['education_entries'] += 1
    
    def update_employment_rates(self):
        """Update employment rates table with state-based data for family generation"""
        print("  Updating employment rates for family generation...")
        
        self.cursor.execute("""
            WITH race_employment AS (
                SELECT 
                    re.race_key,
                    re.id as race_id,
                    SUM(ses.employment_rate * sd.population_weight) / SUM(sd.population_weight) as weighted_employment_rate,
                    SUM(ses.unemployment_rate * sd.population_weight) / SUM(sd.population_weight) as weighted_unemployment_rate,
                    SUM((100 - ses.labor_force_participation_rate) * sd.population_weight) / SUM(sd.population_weight) as weighted_not_in_lf_rate
                FROM race_ethnicity re
                JOIN state_race_ethnicity sre ON re.id = sre.race_id
                JOIN state_demographics sd ON sre.state_id = sd.id
                JOIN state_employment_stats ses ON sd.id = ses.state_id
                WHERE ses.employment_rate IS NOT NULL
                  AND sd.population_weight IS NOT NULL
                  AND sd.population_weight > 0
                GROUP BY re.race_key, re.id
            )
            UPDATE employment_rates 
            SET 
                employed_rate = race_employment.weighted_employment_rate,
                unemployed_rate = race_employment.weighted_unemployment_rate,
                not_in_labor_force_rate = race_employment.weighted_not_in_lf_rate
            FROM race_employment
            WHERE employment_rates.race_id = race_employment.race_id
            AND employment_rates.age_group = 'All'
        """)
        
        rows_updated = self.cursor.rowcount
        self.conn.commit()
        print(f"    ✓ Updated employment rates for {rows_updated} race categories")
    
    def get_import_summary(self):
        """Display summary of imported data"""
        print("\n" + "="*60)
        print("DATABASE IMPORT SUMMARY")
        print("="*60)
        
        print("Import Statistics:")
        print(f"  States imported: {self.import_stats['states_imported']}")
        print(f"  Race/ethnicity entries: {self.import_stats['race_entries']}")
        print(f"  Family structure entries: {self.import_stats['family_structure_entries']}")
        print(f"  Employment statistics: {self.import_stats['employment_entries']}")
        print(f"  Education statistics: {self.import_stats['education_entries']}")
        
        try:
            self.cursor.execute("SELECT COUNT(*) FROM state_demographics")
            state_count = self.cursor.fetchone()[0]
            
            self.cursor.execute("SELECT COUNT(*) FROM state_race_ethnicity")
            race_entries = self.cursor.fetchone()[0]
            
            self.cursor.execute("SELECT COUNT(*) FROM state_employment_stats")
            employment_entries = self.cursor.fetchone()[0]
            
            print(f"\nDatabase Verification:")
            print(f"  Total states in database: {state_count}")
            print(f"  Total race/ethnicity records: {race_entries}")
            print(f"  States with employment data: {employment_entries}")
            
            print(f"\nSample Data (Top 5 States by Population):")
            self.cursor.execute("""
                SELECT sd.state_name, sd.total_population, r.region_name,
                       ses.employment_rate, ses.unemployment_rate
                FROM state_demographics sd
                JOIN regions r ON sd.region_id = r.id
                LEFT JOIN state_employment_stats ses ON sd.id = ses.state_id
                ORDER BY sd.total_population DESC NULLS LAST
                LIMIT 5
            """)
            
            for row in self.cursor.fetchall():
                state_name, pop, region, emp_rate, unemp_rate = row
                pop_str = f"{pop:,}" if pop else "Unknown"
                emp_str = f", Employment: {emp_rate:.1f}%" if emp_rate else ""
                unemp_str = f", Unemployment: {unemp_rate:.1f}%" if unemp_rate else ""
                print(f"  {state_name} ({region}): Pop {pop_str}{emp_str}{unemp_str}")
                
        except Exception as e:
            print(f"  ⚠️  Could not verify database contents: {e}")
    
    def close(self):
        """Close database connections"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

class CombinedCensusDataPipeline:
    """Combined pipeline for extracting and importing Census data"""
    
    def __init__(self):
        self.extractor = EnhancedCensusDataExtractor()
        self.importer = NeonDataImporter()
    
    def run_full_import(self, include_employment: bool = True):
        """Extract Census data and import directly to database"""
        print("🏛️  COMBINED CENSUS DATA EXTRACTION AND IMPORT")
        print("="*60)
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # Automatically setup schema if it doesn't exist
            self.importer.setup_database_schema()
            # Extract state-level data
            print("\n📊 Extracting Census data...")
            state_race_data = self.extractor.get_state_level_race_data()
            state_household_data = self.extractor.get_state_level_household_data()
            state_education_data = self.extractor.get_state_level_education_data()
            
            state_employment_data = {}
            if include_employment:
                state_employment_data = self.extractor.get_state_employment_rate_data()
            
            # Calculate state weights
            if state_race_data:
                state_weights = self.extractor.calculate_state_weights(state_race_data)
            
            # Import data directly to database
            print("\n💾 Importing data to Neon database...")
            total_states = len(state_race_data)
            processed_states = 0
            
            for state_code in state_race_data.keys():
                if state_code in state_race_data:
                    combined_state_data = {
                        'state_name': self.extractor.state_info[state_code]['name'],
                        'region': self.extractor.state_info[state_code]['region'],
                        'population_weight': state_weights.get(state_code, 0),
                        'race_ethnicity': state_race_data[state_code]['demographics'],
                        'family_structures': state_household_data.get(state_code, {}).get('family_structures', {}),
                        'education_stats': state_education_data.get(state_code, {}).get('education_stats', {})
                    }
                    
                    if state_code in state_employment_data:
                        combined_state_data['employment_stats'] = state_employment_data[state_code]['employment_statistics']
                    
                    try:
                        self.importer.store_state_data(state_code, combined_state_data, 2022)
                        processed_states += 1
                        
                        if processed_states % 10 == 0 or processed_states == total_states:
                            print(f"  Progress: {processed_states}/{total_states} states imported")
                            
                    except Exception as e:
                        print(f"  ⚠️  Warning: Failed to import state {state_code}: {e}")
                        continue
            
            # Update employment rates for family generation
            if include_employment and state_employment_data:
                self.importer.update_employment_rates()
            
            # Show summary
            self.importer.get_import_summary()
            
            print(f"\n✅ IMPORT COMPLETED SUCCESSFULLY!")
            print("The database is now ready for family generation.")
            
            return True
            
        except Exception as e:
            print(f"\n❌ IMPORT FAILED: {e}")
            return False
        finally:
            print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    def close(self):
        """Close all connections"""
        self.importer.close()

def main():
    """Main execution function"""
    
    # Parse command line arguments
    setup_schema_only = False
    employment_only = False
    
    for arg in sys.argv[1:]:
        if arg == '--setup-schema':
            setup_schema_only = True
        elif arg == '--employment-only':
            employment_only = True
    
    try:
        pipeline = CombinedCensusDataPipeline()
        
        # Setup schema only if requested
        if setup_schema_only:
            pipeline.importer.setup_database_schema()
            print("✅ Database schema setup completed!")
            return True
        
        # Run the import (schema will be auto-created if needed)
        if employment_only:
            print("Running employment data only import...")
            success = pipeline.run_full_import(include_employment=True)
        else:
            print("Running full demographic data import...")
            success = pipeline.run_full_import(include_employment=True)
        
        return success
        
    except Exception as e:
        print(f"\n❌ PIPELINE FAILED: {e}")
        return False
    finally:
        try:
            pipeline.close()
        except:
            pass

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
