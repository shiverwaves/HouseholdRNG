"""
Pydantic models for API requests and responses.

These models define the structure of data sent to and from the API.
"""

from pydantic import BaseModel, Field, validator
from typing import List, Optional
from datetime import datetime


# ============================================================================
# REQUEST MODELS
# ============================================================================

class GenerateHouseholdsRequest(BaseModel):
    """Request model for generating households"""
    
    state: str = Field(
        ...,
        description="Two-letter state code (e.g., HI, CA, TX)",
        min_length=2,
        max_length=2,
        examples=["HI", "CA", "TX"]
    )
    pums_year: int = Field(
        2023,
        description="Year for PUMS data",
        ge=2010,
        le=2030,
        examples=[2022, 2023]
    )
    bls_year: Optional[int] = Field(
        None,
        description="Year for BLS data (defaults to pums_year)",
        ge=2010,
        le=2030
    )
    complexity: Optional[str] = Field(
        "random",
        description="Complexity filter: simple, medium, complex, or random",
        examples=["simple", "medium", "complex", "random"]
    )
    count: int = Field(
        1,
        description="Number of households to generate",
        ge=1,
        le=100,
        examples=[1, 5, 10]
    )
    seed: Optional[int] = Field(
        None,
        description="Random seed for reproducibility",
        examples=[42, 12345]
    )
    
    @validator('state')
    def validate_state_uppercase(cls, v):
        """Ensure state code is uppercase"""
        return v.upper()
    
    @validator('complexity')
    def validate_complexity(cls, v):
        """Validate complexity level"""
        allowed = ['simple', 'medium', 'complex', 'random']
        if v not in allowed:
            raise ValueError(f"Complexity must be one of: {allowed}")
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "state": "HI",
                "pums_year": 2023,
                "complexity": "medium",
                "count": 5
            }
        }


# ============================================================================
# RESPONSE MODELS
# ============================================================================

class HouseholdSummary(BaseModel):
    """Summary of a generated household (Stage 1 only)"""
    
    household_id: str = Field(..., description="Unique household identifier")
    pattern: str = Field(..., description="Household pattern type")
    state: str = Field(..., description="State code")
    year: int = Field(..., description="Data year")
    expected_adults: Optional[int] = Field(None, description="Expected number of adults")
    expected_children_range: Optional[tuple] = Field(None, description="Expected children range")
    expected_complexity: Optional[str] = Field(None, description="Expected complexity level")
    
    class Config:
        json_schema_extra = {
            "example": {
                "household_id": "123e4567-e89b-12d3-a456-426614174000",
                "pattern": "married_couple_with_children",
                "state": "HI",
                "year": 2023,
                "expected_adults": 2,
                "expected_children_range": [1, 5],
                "expected_complexity": "simple"
            }
        }


class GenerateHouseholdsResponse(BaseModel):
    """Response model for household generation"""
    
    households: List[HouseholdSummary] = Field(..., description="Generated households")
    count: int = Field(..., description="Number of households generated")
    state: str = Field(..., description="State code")
    year: int = Field(..., description="Data year")
    complexity_filter: Optional[str] = Field(None, description="Complexity filter applied")
    seed: Optional[int] = Field(None, description="Random seed used")
    generated_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="Generation timestamp (UTC)"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "households": [
                    {
                        "household_id": "123e4567-e89b-12d3-a456-426614174000",
                        "pattern": "married_couple_with_children",
                        "state": "HI",
                        "year": 2023,
                        "expected_adults": 2,
                        "expected_children_range": [1, 5],
                        "expected_complexity": "simple"
                    }
                ],
                "count": 1,
                "state": "HI",
                "year": 2023,
                "complexity_filter": "random",
                "seed": None,
                "generated_at": "2024-12-21T12:00:00Z"
            }
        }


class HealthResponse(BaseModel):
    """Health check response"""
    
    status: str = Field(..., description="Health status: healthy or unhealthy")
    version: str = Field(..., description="API version")
    database: str = Field(..., description="Database connection status")
    timestamp: datetime = Field(
        default_factory=datetime.utcnow,
        description="Check timestamp (UTC)"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "status": "healthy",
                "version": "1.0.0",
                "database": "connected",
                "timestamp": "2024-12-21T12:00:00Z"
            }
        }


class AvailableStatesResponse(BaseModel):
    """Available states and years response"""
    
    states: dict[str, List[int]] = Field(
        ...,
        description="Dictionary mapping state codes to available years"
    )
    total_states: int = Field(..., description="Total number of states available")
    
    class Config:
        json_schema_extra = {
            "example": {
                "states": {
                    "HI": [2022, 2023],
                    "CA": [2023]
                },
                "total_states": 2
            }
        }


class ErrorResponse(BaseModel):
    """Error response model"""
    
    detail: str = Field(..., description="Error message")
    status_code: int = Field(..., description="HTTP status code")
    timestamp: datetime = Field(
        default_factory=datetime.utcnow,
        description="Error timestamp (UTC)"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "detail": "No data found for state XX year 2023",
                "status_code": 404,
                "timestamp": "2024-12-21T12:00:00Z"
            }
        }