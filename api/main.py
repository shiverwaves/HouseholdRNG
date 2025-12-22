"""
FastAPI Main Application

REST API for household generation system.
"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import logging

from .config import get_settings
from .routers import health, households
from .models import ErrorResponse


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for startup and shutdown events.
    """
    # Startup
    settings = get_settings()
    logger.info(f"Starting {settings.app_name} v{settings.app_version}")
    logger.info(f"Debug mode: {settings.debug}")
    
    # Verify database connection
    try:
        from .dependencies import get_distribution_loader
        loader = get_distribution_loader(settings)
        states = loader.list_available_states_years()
        logger.info(f"Database connected - {len(states)} states available")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down application")


# Create FastAPI application
settings = get_settings()
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="""
    REST API for generating realistic household tax scenarios.
    
    ## Features
    
    - Generate households based on US Census PUMS distributions
    - Filter by complexity level (simple, medium, complex)
    - Support for all 50+ US states
    - Reproducible generation with random seeds
    
    ## Current Status
    
    **Stage 1 Complete**: Household structure selection
    
    Upcoming stages:
    - Stage 2: Adult member generation
    - Stage 3: Child member generation  
    - Stage 4: Occupation & income assignment
    - Stage 5: Tax-relevant expenses
    - Stage 6: Tax calculation & filing units
    - Stage 7: Validation & complexity scoring
    
    ## Usage
    
    See the `/docs` endpoint for interactive API documentation.
    """,
    lifespan=lifespan,
    debug=settings.debug
)


# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Custom exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler for uncaught exceptions"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            detail=f"Internal server error: {str(exc)}",
            status_code=500
        ).dict()
    )


# Include routers
app.include_router(health.router)
app.include_router(households.router)


# Root endpoint
@app.get("/", tags=["root"])
async def root():
    """
    Root endpoint - API information.
    """
    return {
        "name": settings.app_name,
        "version": settings.app_version,
        "docs": "/docs",
        "redoc": "/redoc",
        "health": "/api/v1/health",
        "available_states": "/api/v1/available-states",
        "generate": "/api/v1/households/generate"
    }


# Health check at root level (for load balancers)
@app.get("/health", include_in_schema=False)
async def root_health():
    """Simple health check for load balancers"""
    return {"status": "ok"}