"""
Health check and system status endpoints.
"""

from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException

from ..models import HealthResponse, AvailableStatesResponse
from ..dependencies import get_distribution_loader, get_settings
from ..config import Settings
from generator.database import DistributionLoader


router = APIRouter(
    prefix="/api/v1",
    tags=["health"]
)


@router.get("/health", response_model=HealthResponse)
async def health_check(
    settings: Annotated[Settings, Depends(get_settings)],
    loader: Annotated[DistributionLoader, Depends(get_distribution_loader)]
):
    """
    Health check endpoint.
    
    Returns the health status of the API and database connection.
    """
    try:
        # Test database connection
        loader._verify_connection()
        
        return HealthResponse(
            status="healthy",
            version=settings.app_version,
            database="connected"
        )
    except Exception as e:
        return HealthResponse(
            status="unhealthy",
            version=settings.app_version,
            database=f"disconnected: {str(e)}"
        )


@router.get("/available-states", response_model=AvailableStatesResponse)
async def list_available_states(
    loader: Annotated[DistributionLoader, Depends(get_distribution_loader)]
):
    """
    List available state/year combinations.
    
    Returns all states that have distribution tables loaded in the database.
    """
    try:
        states_years = loader.list_available_states_years()
        
        return AvailableStatesResponse(
            states=states_years,
            total_states=len(states_years)
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list available states: {str(e)}"
        )