"""
Generator Worker Server

FastAPI application that exposes household generation endpoints.
This is an internal service called by the API server.
"""

import os
import logging
from typing import List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from .pipeline import HouseholdGenerator
from .database import get_loader

# Configure logging
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================
# Pydantic Models
# ============================================

class GenerateRequest(BaseModel):
    """Request to generate households"""
    state: str = Field(..., description="Two-letter state code", min_length=2, max_length=2)
    pums_year: int = Field(..., description="PUMS data year", ge=2018, le=2025)
    bls_year: Optional[int] = Field(None, description="BLS data year (default: same as pums_year)")
    count: int = Field(1, description="Number of households to generate", ge=1, le=100)
    complexity: Optional[str] = Field(None, description="Filter by complexity: simple, medium, complex")
    pattern: Optional[str] = Field(None, description="Specific pattern to generate (e.g., 'single_parent', 'married_couple_with_children')")
    seed: Optional[int] = Field(None, description="Random seed for reproducibility")


class HouseholdResponse(BaseModel):
    """Single household response"""
    household_id: str
    state: str
    year: int
    pattern: str
    multigenerational_subpattern: Optional[str] = None
    expected_adults: Optional[int]
    expected_children_range: Optional[List[int]]
    expected_complexity: Optional[str]
    members: List[dict] = []
    total_household_income: int = 0
    adult_count: int = 0
    child_count: int = 0
    is_married: bool = False
    
    # Expenses (Stage 5)
    property_taxes: int = 0
    mortgage_interest: int = 0
    state_income_tax: int = 0
    medical_expenses: int = 0
    charitable_contributions: int = 0
    student_loan_interest: int = 0
    educator_expenses: int = 0
    ira_contributions: int = 0
    child_care_expenses: int = 0
    education_expenses: int = 0
    total_itemized_deductions: int = 0
    total_above_line_deductions: int = 0
    
    # Validation
    validation_score: Optional[float] = None


class GenerateResponse(BaseModel):
    """Response from generation endpoint"""
    success: bool
    count: int
    state: str
    year: int
    seed: Optional[int]
    households: List[HouseholdResponse]


class PatternInfo(BaseModel):
    """Information about a household pattern"""
    pattern: str
    weight: int
    percentage: float
    complexity: str
    description: str
    expected_adults: int | tuple
    expected_children: tuple


class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    service: str
    database_connected: bool
    available_states: dict


# ============================================
# Application Lifespan
# ============================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown"""
    logger.info("Starting Generator Worker Service")
    
    # Verify database connection on startup
    try:
        loader = get_loader()
        states = loader.list_available_states()
        logger.info(f"Database connected. Available states: {list(states.keys())}")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
    
    yield
    
    logger.info("Shutting down Generator Worker Service")


# ============================================
# FastAPI Application
# ============================================

app = FastAPI(
    title="Household Generator Worker",
    description="Internal service for generating synthetic households",
    version="1.0.0",
    lifespan=lifespan
)


# ============================================
# Endpoints
# ============================================

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    try:
        loader = get_loader()
        states = loader.list_available_states()
        db_connected = True
    except Exception:
        states = {}
        db_connected = False
    
    return HealthResponse(
        status="healthy" if db_connected else "degraded",
        service="generator-worker",
        database_connected=db_connected,
        available_states=states
    )


@app.get("/states")
async def list_states():
    """List available states and years"""
    try:
        loader = get_loader()
        states = loader.list_available_states()
        return {
            "success": True,
            "states": states,
            "count": len(states)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/patterns/{state}/{year}")
async def get_patterns(state: str, year: int):
    """Get available patterns for a state/year"""
    try:
        generator = HouseholdGenerator(
            state=state.upper(),
            pums_year=year
        )
        patterns = generator.get_available_patterns()
        return {
            "success": True,
            "state": state.upper(),
            "year": year,
            "patterns": patterns,
            "count": len(patterns)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/generate", response_model=GenerateResponse)
async def generate_households(request: GenerateRequest):
    """
    Generate synthetic households.
    
    This is the main generation endpoint called by the API server.
    """
    try:
        # Initialize generator
        generator = HouseholdGenerator(
            state=request.state.upper(),
            pums_year=request.pums_year,
            bls_year=request.bls_year
        )
        
        # Generate households
        households = generator.generate_batch(
            count=request.count,
            complexity=request.complexity,
            pattern=request.pattern,
            seed=request.seed
        )
        
        # Convert to response format
        household_responses = []
        for h in households:
            household_responses.append(HouseholdResponse(
                household_id=h.household_id,
                state=h.state,
                year=h.year,
                pattern=h.pattern,
                multigenerational_subpattern=h.multigenerational_subpattern,
                expected_adults=h.expected_adults,
                expected_children_range=list(h.expected_children_range) if h.expected_children_range else None,
                expected_complexity=h.expected_complexity,
                members=[m.to_dict() for m in h.members],
                total_household_income=h.total_household_income(),
                adult_count=len(h.get_adults()),
                child_count=len(h.get_children()),
                is_married=h.is_married(),
                # Expenses (Stage 5)
                property_taxes=h.property_taxes,
                mortgage_interest=h.mortgage_interest,
                state_income_tax=h.state_income_tax,
                medical_expenses=h.medical_expenses,
                charitable_contributions=h.charitable_contributions,
                student_loan_interest=h.student_loan_interest,
                educator_expenses=h.educator_expenses,
                ira_contributions=h.ira_contributions,
                child_care_expenses=h.child_care_expenses,
                education_expenses=h.education_expenses,
                total_itemized_deductions=h.total_itemized_deductions,
                total_above_line_deductions=h.total_above_line_deductions,
                validation_score=h.validation_score
            ))
        
        logger.info(f"Generated {len(households)} households for {request.state}")
        
        return GenerateResponse(
            success=True,
            count=len(households),
            state=request.state.upper(),
            year=request.pums_year,
            seed=request.seed,
            households=household_responses
        )
    
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Generation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/generate/single")
async def generate_single(
    state: str = Query(..., description="Two-letter state code"),
    year: int = Query(..., description="PUMS data year"),
    complexity: Optional[str] = Query(None, description="Complexity filter"),
    pattern: Optional[str] = Query(None, description="Specific pattern to generate"),
    seed: Optional[int] = Query(None, description="Random seed")
):
    """Generate a single household (GET endpoint for simple testing)"""
    request = GenerateRequest(
        state=state,
        pums_year=year,
        count=1,
        complexity=complexity,
        pattern=pattern,
        seed=seed
    )
    return await generate_households(request)


# ============================================
# Run with: uvicorn generator.server:app --port 8001
# ============================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
