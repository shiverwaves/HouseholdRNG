"""
Household generation endpoints.
"""

from typing import Annotated
import numpy as np
from fastapi import APIRouter, Depends, HTTPException

from ..models import GenerateHouseholdsRequest, GenerateHouseholdsResponse, HouseholdSummary
from ..dependencies import get_distribution_loader, get_settings
from ..config import Settings
from generator.database import DistributionLoader
from generator.pipeline import HouseholdGenerator


router = APIRouter(
    prefix="/api/v1/households",
    tags=["households"]
)


@router.post("/generate", response_model=GenerateHouseholdsResponse)
async def generate_households(
    request: GenerateHouseholdsRequest,
    settings: Annotated[Settings, Depends(get_settings)],
    loader: Annotated[DistributionLoader, Depends(get_distribution_loader)]
):
    """
    Generate one or more households.
    
    This endpoint generates households based on PUMS/BLS distributions for a given state and year.
    
    **Currently implements Stage 1 only** - household structure selection.
    Additional stages (adults, children, income, etc.) will be added incrementally.
    
    ## Request Parameters
    
    - **state**: Two-letter state code (e.g., HI, CA, TX)
    - **pums_year**: Year for PUMS demographic data (default: 2023)
    - **bls_year**: Year for BLS wage data (default: same as pums_year)
    - **complexity**: Filter by complexity level (simple, medium, complex, random)
    - **count**: Number of households to generate (1-100)
    - **seed**: Random seed for reproducibility (optional)
    
    ## Response
    
    Returns a list of generated households with:
    - Unique household ID
    - Selected pattern (e.g., "married_couple_with_children")
    - Expected composition (adults, children)
    - Expected complexity level
    
    ## Example Request
```json
    {
      "state": "HI",
      "pums_year": 2023,
      "complexity": "medium",
      "count": 5
    }
```
    """
    # Validate count
    if request.count > settings.max_households_per_request:
        raise HTTPException(
            status_code=400,
            detail=f"Count exceeds maximum of {settings.max_households_per_request} per request"
        )
    
    try:
        # Initialize generator
        generator = HouseholdGenerator(
            state=request.state,
            pums_year=request.pums_year,
            bls_year=request.bls_year or request.pums_year,
            connection_string=loader.engine.url.render_as_string(hide_password=False)
        )
        
        # Generate households
        households = []
        for i in range(request.count):
            # Set seed if provided
            if request.seed is not None:
                np.random.seed(request.seed + i)
            
            # Generate household (Stage 1 only for now)
            household = generator.generate_stage1()
            
            # Convert to response model
            households.append(HouseholdSummary(
                household_id=household.household_id,
                pattern=household.pattern,
                state=household.state,
                year=household.year,
                expected_adults=household.expected_adults,
                expected_children_range=household.expected_children_range,
                expected_complexity=household.expected_complexity
            ))
        
        return GenerateHouseholdsResponse(
            households=households,
            count=len(households),
            state=request.state,
            year=request.pums_year,
            complexity_filter=request.complexity,
            seed=request.seed
        )
        
    except ValueError as e:
        # Handle data not found errors
        raise HTTPException(
            status_code=404,
            detail=f"No data found: {str(e)}"
        )
    except Exception as e:
        # Handle other errors
        raise HTTPException(
            status_code=500,
            detail=f"Generation failed: {str(e)}"
        )