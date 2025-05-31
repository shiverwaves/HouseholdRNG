import psycopg2
import random
import os
from typing import Dict, Optional, Tuple

# State cost of living adjustments
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

class IncomeGenerator:
    def __init__(self):
        """Initialize with database connection using NEON_CONNECTION_STRING."""
        self.connection = None
        self._connect()
    
    def _connect(self):
        """Connect to database using connection string."""
        connection_string = os.getenv('NEON_CONNECTION_STRING')
        if not connection_string:
            print("❌ NEON_CONNECTION_STRING not found in environment variables")
            return
            
        try:
            self.connection = psycopg2.connect(connection_string)
            print("✅ Connected to database successfully")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
    
    def get_random_occupation(self, state: Optional[str] = None) -> Optional[Tuple]:
        """Get random occupation from database."""
        if not self.connection:
            return None
            
        try:
            cursor = self.connection.cursor()
            query = """
                SELECT occ_title, area_title, a_median, a_mean
                FROM oews.employment_wages 
                WHERE a_median IS NOT NULL AND tot_emp > 0
            """
            params = []
            
            if state:
                query += " AND area_title = %s"
                params.append(state)
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            cursor.close()
            
            return random.choice(results) if results else None
            
        except Exception as e:
            print(f"Error fetching occupation: {e}")
            return None
    
    def get_wage_data(self, occupation: str, state: Optional[str] = None) -> Optional[float]:
        """Get median wage for specific occupation."""
        if not self.connection:
            return None
            
        try:
            cursor = self.connection.cursor()
            query = """
                SELECT a_median
                FROM oews.employment_wages 
                WHERE LOWER(occ_title) LIKE LOWER(%s)
                AND a_median IS NOT NULL
            """
            params = [f"%{occupation}%"]
            
            if state:
                query += " AND area_title = %s"
                params.append(state)
            
            query += " ORDER BY tot_emp DESC LIMIT 1"
            
            cursor.execute(query, params)
            result = cursor.fetchone()
            cursor.close()
            
            return float(result[0]) if result else None
            
        except Exception as e:
            print(f"Error fetching wage data: {e}")
            return None
    
    def generate_income(self, occupation: str, age: int, state: str, 
                       employment_status: str = "Employed") -> Optional[int]:
        """Generate realistic income based on occupation and demographics."""
        if employment_status not in ["Employed", "Self-Employed"]:
            return None
        
        # Get base wage from database
        base_wage = self.get_wage_data(occupation, state)
        if not base_wage:
            base_wage = self.get_wage_data(occupation)  # Try without state
        
        if not base_wage:
            return None
        
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
        geo_multiplier = STATE_ADJUSTMENTS.get(state, 1.0)
        
        # Apply employment status multiplier
        if employment_status == "Self-Employed":
            emp_multiplier = random.uniform(0.6, 1.8)
            if random.random() < 0.6:  # 60% earn less
                emp_multiplier *= 0.85
        else:
            emp_multiplier = 1.0
        
        # Add some random variation around median
        variation = random.uniform(0.8, 1.2)
        
        # Calculate final income
        final_income = base_wage * exp_multiplier * geo_multiplier * emp_multiplier * variation
        
        # Round to nearest $1000, minimum $15,080
        return max(int(round(final_income, -3)), 15080)
    
    def generate_person(self, state: Optional[str] = None) -> Dict:
        """Generate a complete person with realistic income."""
        # Get random occupation
        occ_data = self.get_random_occupation(state)
        if not occ_data:
            return {"error": "No occupation data available"}
        
        occupation, area, median_wage, mean_wage = occ_data
        
        # Generate demographics
        age = random.randint(18, 65)
        employment_status = random.choices(
            ["Employed", "Self-Employed", "Unemployed", "Retired"],
            weights=[75, 15, 5, 5]
        )[0]
        
        # Generate income
        income = self.generate_income(occupation, age, area, employment_status)
        
        return {
            "occupation": occupation,
            "age": age,
            "state": area,
            "employment_status": employment_status,
            "annual_income": income,
            "base_median_wage": float(median_wage) if median_wage else None
        }
    
    def close(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()

def format_income(income: Optional[int]) -> str:
    """Format income for display."""
    if income is None:
        return "N/A"
    if income >= 1000000:
        return f"${income/1000000:.1f}M"
    elif income >= 1000:
        return f"${income/1000:.0f}K"
    else:
        return f"${income:,}"

def test_generator():
    """Test the income generator."""
    print("=== Income Generator Test ===")
    
    generator = IncomeGenerator()
    if not generator.connection:
        print("❌ Cannot test without database connection")
        return
    
    print("\n👥 Generated People:")
    print(f"{'Occupation':30} | {'Age':3} | {'State':12} | {'Status':12} | {'Income':>8}")
    print("-" * 75)
    
    for _ in range(8):
        person = generator.generate_person()
        if "error" not in person:
            print(f"{person['occupation'][:29]:30} | {person['age']:3} | "
                  f"{person['state'][:11]:12} | {person['employment_status']:12} | "
                  f"{format_income(person['annual_income']):>8}")
    
    # Test specific occupation
    print(f"\n🔍 Software Developer in California:")
    income = generator.generate_income("Software Developer", 30, "California")
    print(f"   Generated income: {format_income(income)}")
    
    generator.close()
    print("\n✅ Test completed")

if __name__ == "__main__":
    test_generator()
