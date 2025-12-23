"""
Household Generation API Server

Public-facing REST API that proxies requests to the generator worker.
"""

import os
import logging
from typing import List, Optional
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import httpx

# Configure logging
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Worker service URL
WORKER_URL = os.getenv('WORKER_URL', 'http://worker:8001')


# ============================================
# Pydantic Models
# ============================================

class GenerateRequest(BaseModel):
    """Request to generate households"""
    state: str = Field(..., description="Two-letter state code (e.g., 'HI')", min_length=2, max_length=2, examples=["HI"])
    pums_year: int = Field(..., description="PUMS data year", ge=2018, le=2025, examples=[2023])
    bls_year: Optional[int] = Field(None, description="BLS data year (default: same as pums_year)")
    count: int = Field(1, description="Number of households to generate (1-100)", ge=1, le=100)
    complexity: Optional[str] = Field(None, description="Filter by complexity: simple, medium, complex", examples=["simple"])
    pattern: Optional[str] = Field(None, description="Specific pattern to generate (e.g., 'single_parent', 'married_couple_with_children')", examples=["married_couple_with_children"])
    seed: Optional[int] = Field(None, description="Random seed for reproducibility")


class HouseholdSummary(BaseModel):
    """Summary of a generated household"""
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


class GenerateResponse(BaseModel):
    """Response from generation endpoint"""
    success: bool
    message: str
    count: int
    state: str
    year: int
    seed: Optional[int]
    generated_at: datetime
    households: List[HouseholdSummary]


class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    api_version: str
    worker_status: str
    worker_url: str
    timestamp: datetime


class StateInfo(BaseModel):
    """Available state information"""
    state: str
    years: List[int]


class StatesResponse(BaseModel):
    """Response listing available states"""
    success: bool
    states: List[StateInfo]
    count: int


# ============================================
# Application Lifespan
# ============================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown"""
    logger.info("Starting Household Generation API")
    logger.info(f"Worker URL: {WORKER_URL}")
    
    yield
    
    logger.info("Shutting down Household Generation API")


# ============================================
# FastAPI Application
# ============================================

app = FastAPI(
    title="Household Generation API",
    description="""
    Generate realistic synthetic households for tax preparation training.
    
    ## Overview
    
    This API generates synthetic household data based on US Census PUMS 
    (Public Use Microdata Sample) and Bureau of Labor Statistics data.
    
    ## Endpoints
    
    - **POST /api/v1/households/generate** - Generate multiple households
    - **GET /api/v1/states** - List available states and years
    - **GET /api/v1/patterns/{state}/{year}** - Get pattern distribution for a state
    - **GET /health** - Health check
    
    ## Usage Example
    
    ```python
    import requests
    
    response = requests.post(
        'http://localhost:8000/api/v1/households/generate',
        json={
            'state': 'HI',
            'pums_year': 2023,
            'count': 5,
            'complexity': 'simple'
        }
    )
    households = response.json()['households']
    ```
    """,
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv('CORS_ORIGINS', 'http://localhost:3000').split(','),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================
# Helper Functions
# ============================================

async def call_worker(endpoint: str, method: str = "GET", json_data: dict = None) -> dict:
    """Call the worker service"""
    url = f"{WORKER_URL}{endpoint}"
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            if method == "POST":
                response = await client.post(url, json=json_data)
            else:
                response = await client.get(url)
            
            response.raise_for_status()
            return response.json()
        
        except httpx.ConnectError:
            raise HTTPException(
                status_code=503,
                detail="Generator worker service unavailable"
            )
        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=e.response.status_code,
                detail=e.response.json().get('detail', str(e))
            )
        except Exception as e:
            logger.error(f"Worker call failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))


# ============================================
# Health Endpoints
# ============================================

@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """
    Health check endpoint.
    
    Returns API status and worker service connectivity.
    """
    worker_status = "unknown"
    
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(f"{WORKER_URL}/health")
            if response.status_code == 200:
                worker_data = response.json()
                worker_status = worker_data.get('status', 'healthy')
            else:
                worker_status = "unhealthy"
    except Exception:
        worker_status = "unavailable"
    
    return HealthResponse(
        status="healthy" if worker_status in ["healthy", "unknown"] else "degraded",
        api_version="1.0.0",
        worker_status=worker_status,
        worker_url=WORKER_URL,
        timestamp=datetime.utcnow()
    )


@app.get("/", tags=["Health"])
async def root():
    """API information"""
    return {
        "name": "Household Generation API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
        "endpoints": {
            "generate": "POST /api/v1/households/generate",
            "states": "GET /api/v1/states",
            "patterns": "GET /api/v1/patterns/{state}/{year}"
        }
    }


# ============================================
# State & Pattern Endpoints
# ============================================

@app.get("/api/v1/states", response_model=StatesResponse, tags=["Configuration"])
async def list_available_states():
    """
    List available states and years.
    
    Returns all state/year combinations that have data loaded.
    """
    data = await call_worker("/states")
    
    states_list = [
        StateInfo(state=state, years=years)
        for state, years in data.get('states', {}).items()
    ]
    
    return StatesResponse(
        success=True,
        states=sorted(states_list, key=lambda x: x.state),
        count=len(states_list)
    )


@app.get("/api/v1/patterns/{state}/{year}", tags=["Configuration"])
async def get_patterns(state: str, year: int):
    """
    Get household patterns for a state/year.
    
    Returns distribution of household patterns with probabilities.
    """
    return await call_worker(f"/patterns/{state.upper()}/{year}")


# ============================================
# Generation Endpoints
# ============================================

@app.post("/api/v1/households/generate", response_model=GenerateResponse, tags=["Generation"])
async def generate_households(request: GenerateRequest):
    """
    Generate synthetic households.
    
    ## Parameters
    
    - **state**: Two-letter state code (e.g., 'HI', 'CA', 'TX')
    - **pums_year**: Year of PUMS data to use (2018-2025)
    - **bls_year**: Year of BLS data (optional, defaults to pums_year)
    - **count**: Number of households to generate (1-100)
    - **complexity**: Filter by complexity level ('simple', 'medium', 'complex')
    - **pattern**: Specific household pattern to generate (overrides complexity)
    - **seed**: Random seed for reproducible results
    
    ## Available Patterns
    
    - single_adult
    - married_couple_no_children
    - married_couple_with_children
    - single_parent
    - blended_family
    - multigenerational
    - unmarried_partners
    
    ## Response
    
    Returns a list of generated households with:
    - Household pattern (e.g., 'married_couple_with_children')
    - Expected composition (adults, children)
    - Complexity level
    
    ## Example
    
    ```json
    {
        "state": "HI",
        "pums_year": 2023,
        "count": 5,
        "pattern": "single_parent"
    }
    ```
    """
    # Call worker service
    worker_response = await call_worker(
        "/generate",
        method="POST",
        json_data=request.model_dump()
    )
    
    # Transform response
    households = [
        HouseholdSummary(**h) for h in worker_response.get('households', [])
    ]
    
    return GenerateResponse(
        success=True,
        message=f"Generated {len(households)} households",
        count=len(households),
        state=request.state.upper(),
        year=request.pums_year,
        seed=request.seed,
        generated_at=datetime.utcnow(),
        households=households
    )


@app.get("/api/v1/households/generate", tags=["Generation"])
async def generate_households_get(
    state: str = Query(..., description="Two-letter state code", examples=["HI"]),
    year: int = Query(..., description="PUMS data year", examples=[2023]),
    count: int = Query(1, description="Number of households", ge=1, le=100),
    complexity: Optional[str] = Query(None, description="Complexity filter"),
    pattern: Optional[str] = Query(None, description="Specific pattern to generate (e.g., 'single_parent', 'married_couple_with_children')"),
    seed: Optional[int] = Query(None, description="Random seed")
):
    """
    Generate households (GET endpoint).
    
    Simpler GET version for quick testing.
    """
    request = GenerateRequest(
        state=state,
        pums_year=year,
        count=count,
        complexity=complexity,
        pattern=pattern,
        seed=seed
    )
    return await generate_households(request)


# ============================================
# Run with: uvicorn api.main:app --port 8000
# ============================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
