#!/usr/bin/env python3
"""
Neon Database Data Import Script

Extracts demographic data from US Census Bureau API and stores it in Neon database.
This script handles the complete data pipeline from Census API to database storage.

Usage:
    python neon_data_import.py                    # Extract live from Census API
    python neon_data_import.py data.json          # Load from existing JSON file
    python neon_data_import.py --setup-schema     # Setup database schema first
"""

import psycopg2
import json
import os
import sys
from typing import Dict, Any
from datetime import datetime

# Import the Census data extractor
try:
    from get_demographics import EnhancedCensusDataExtractor
except ImportError:
    print("❌ Error: Could not import get_demographics.py")
    print("Make sure get_demographics.py is in the same directory")
    sys.exit(1)

class NeonDataImporter:
    """
    Import Census demographic data into Neon database
    """
    
    def __init__(self):
        self.connection_string = os.getenv('NEON_CONNECTION_STRING')
        if not self.connection_string:
            raise ValueError("NEON_CONNECTION_STRING environment variable is required")
        
        self.conn = psycopg2.connect(self.connection_string)
        self.cursor = self.conn.cursor()
        
        # Track import statistics
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
        
        # Check if tables already exist
        self.cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = 'state_demographics'
        """)
        
        if self.cursor.fetchone():
            print("  ✓ Database schema already exists")
            return
        
        # Import and run schema setup
        try:
            from neon_db_schema_setup import NeonDemographicsSetup
            setup = NeonDemographicsSetup()
            setup.setup_complete_database()
            print("  ✓ Database schema created successfully")
        except ImportError:
            print("❌ Error: Could not import neon_db_schema_setup.py")
            print("Make sure neon_db_schema_setup.py is in the same directory")
            raise
    
    def extract_census_data(self) -> Dict[str, Any]:
        """Extract data from Census Bureau API"""
        
        print("Extracting data from US Census Bureau API...")
        print("This may take a few minutes...")
        
        try:
            extractor = EnhancedCensusDataExtractor()
            census_data = extractor.extract_all_state_data(include_employment=True)
            
            if not census_data or 'state_demographics' not in census_data:
                raise ValueError("No valid data extracted from Census API")
            
            print(f"  ✓ Successfully extracted data for {len(census_data['state_demographics'])} states")
            return census_data
            
        except Exception as e:
            print(f"❌ Error extracting Census data: {e}")
            raise
    
    def load_census_data_from_file(self, file_path: str) -> Dict[str, Any]:
        """Load Census data from a JSON file"""
        
        print(f"Loading Census data from {file_path}...")
        
        try:
            with open(file_path, 'r') as f:
                census_data = json.load(f)
            
            if not census_data or 'state_demographics' not in census_data:
                raise ValueError("Invalid data format in JSON file")
            
            print(f"  ✓ Successfully loaded data for {len(census_data['state_demographics'])} states")
            return census_data
            
        except FileNotFoundError:
            print(f"❌ Error: File {file_path} not found")
            raise
        except json.JSONDecodeError as e:
            print(f"❌ Error: Invalid JSON format in {file_path}: {e}")
            raise
    
    def store_census_data(self, census_data: Dict[str, Any]):
        """Store Census data in Neon database"""
        
        print("Storing Census data in Neon database...")
        
        if 'state_demographics' not in census_data:
            raise ValueError("No state demographics data found in input")
        
        total_states = len(census_data['state_demographics'])
        processed_states = 0
        
        # Store each state's data
        for state_code, state_data in census_data['state_demographics'].items():
            try:
                self._store_state_data(state_code, state_data, census_data.get('data_year', 2022))
                processed_states += 1
                self.import_stats['states_imported'] += 1
                
                # Progress indicator
                if processed_states % 10 == 0 or processed_states == total_states:
                    print(f"  Progress: {processed_states}/{total_states} states processed")
                    
            except Exception as e:
                print(f"  ⚠️  Warning: Failed to store data for state {state_code}: {e}")
                self.conn.rollback()
                continue
        
        # Update employment rates for family generation
        self._update_employment_rates()
        
        print(f"  ✓ Successfully stored data for {self.import_stats['states_imported']} states")
    
    def _store_state_data(self, state_code: str, state_data: Dict[str, Any], data_year: int):
        """Store data for a single state"""
        
        state_name = state_data['state_name']
        region = state_data['region']
        population_weight = state_data.get('population_weight', 0)
        
        # Get total population from race data
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
            
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Failed to store data for {state_name}: {e}")
    
    def _store_race_data(self, state_id: int, race_data: Dict[str, Any]):
        """Store race/ethnicity breakdown for a state"""
        
        # Clear existing race data for this state
        self.cursor.execute("DELETE FROM state_race_ethnicity WHERE state_id = %s", (state_id,))
        
        total_population = race_data.get('total_population', 0)
        race_entries = 0
        
        for race_key, percentage in race_data.items():
            if race_key == 'total_population':
                continue
                
            # Get race ID
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
        
        # Clear existing family structure data for this state
        self.cursor.execute("DELETE FROM state_family_structures WHERE state_id = %s", (state_id,))
        
        # Map Census API keys to database keys
        structure_mapping = {
            'SINGLE_PERSON': 'SINGLE_PERSON',
            'MARRIED_COUPLE': 'MARRIED_COUPLE', 
            'SINGLE_PARENT_MALE': 'SINGLE_PARENT_MALE',
            'SINGLE_PARENT_FEMALE': 'SINGLE_PARENT_FEMALE',
            'avg_household_size': None  # Skip this field
        }
        
        family_entries = 0
        
        for census_key, percentage in family_data.items():
            if census_key == 'avg_household_size':
                continue
                
            structure_key = structure_mapping.get(census_key, census_key)
            if not structure_key:
                continue
            
            # Get structure ID
            self.cursor.execute("SELECT id FROM family_structures WHERE structure_key = %s", (structure_key,))
            structure_result = self.cursor.fetchone()
            
            if structure_result:
                structure_id = structure_result[0]
                household_count = 0  # We don't have absolute counts from Census API
                
                self.cursor.execute("""
                    INSERT INTO state_family_structures (state_id, structure_id, household_count, probability_percent)
                    VALUES (%s, %s, %s, %s)
                """, (state_id, structure_id, household_count, percentage))
                
                family_entries += 1
        
        self.import_stats['family_structure_entries'] += family_entries
    
    def _store_employment_data(self, state_id: int, employment_data: Dict[str, Any]):
        """Store employment statistics for a state"""
        
        # Clear existing employment data for this state
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
        
        # Clear existing education data for this state
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
    
    def _update_employment_rates(self):
        """Update employment rates table with state-based data for family generation"""
        
        print("  Updating employment rates for family generation...")
        
        # Calculate population-weighted national employment averages by race from state data
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
    
    def save_data_to_json(self, census_data: Dict[str, Any], filename: str = None):
        """Save extracted Census data to JSON file for backup"""
        
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'census_data_{timestamp}.json'
        
        try:
            with open(filename, 'w') as f:
                json.dump(census_data, f, indent=2)
            print(f"  ✓ Data saved to {filename}")
        except Exception as e:
            print(f"  ⚠️  Warning: Could not save to JSON file: {e}")
    
    def get_import_summary(self):
        """Display summary of imported data"""
        
        print("\n" + "="*60)
        print("DATABASE IMPORT SUMMARY")
        print("="*60)
        
        # Import statistics
        print("Import Statistics:")
        print(f"  States imported: {self.import_stats['states_imported']}")
        print(f"  Race/ethnicity entries: {self.import_stats['race_entries']}")
        print(f"  Family structure entries: {self.import_stats['family_structure_entries']}")
        print(f"  Employment statistics: {self.import_stats['employment_entries']}")
        print(f"  Education statistics: {self.import_stats['education_entries']}")
        
        # Database verification
        try:
            # Count total records
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
            
            # Show sample data for verification
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

def main():
    """Main execution function"""
    
    print("🏛️  NEON DATABASE DATA IMPORT")
    print("="*60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Parse command line arguments
    setup_schema = False
    json_file = None
    
    for arg in sys.argv[1:]:
        if arg == '--setup-schema':
            setup_schema = True
        elif arg.endswith('.json'):
            json_file = arg
        elif not arg.startswith('-'):
            json_file = arg
    
    try:
        # Initialize importer
        importer = NeonDataImporter()
        
        # Setup schema if requested
        if setup_schema:
            importer.setup_database_schema()
        
        # Get Census data
        if json_file:
            # Load from JSON file
            census_data = importer.load_census_data_from_file(json_file)
        else:
            # Extract from Census API
            census_data = importer.extract_census_data()
            
            # Save to JSON for backup
            importer.save_data_to_json(census_data)
        
        # Store in database
        importer.store_census_data(census_data)
        
        # Show summary
        importer.get_import_summary()
        
        print(f"\n✅ DATA IMPORT COMPLETED SUCCESSFULLY!")
        print("The database is now ready for family generation.")
        
    except Exception as e:
        print(f"\n❌ DATA IMPORT FAILED: {e}")
        return False
    finally:
        try:
            importer.close()
        except:
            pass
    
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)