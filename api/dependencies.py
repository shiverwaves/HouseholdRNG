"""
FastAPI Dependencies

Shared dependencies for dependency injection.
"""

from functools import lru_cache
from typing import Annotated
from fastapi import Depends, HTTPException

from generator.database import DistributionLoader
from generator.pipeline import HouseholdGenerator
from .config import Settings, get_settings


@lru_cache()
def get_distribution_loader(
    settings: Annotated[Settings, Depends(get_settings)]
) -> DistributionLoader:
    """
    Get cached DistributionLoader instance.
    
    This is created once and reused for all requests.
    """
    try:
        return DistributionLoader(settings.database_url)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to connect to database: {str(e)}"
        )


def get_household_generator(
    state: str,
    pums_year: int,
    bls_year: int = None,
    loader: Annotated[DistributionLoader, Depends(get_distribution_loader)] = None
) -> HouseholdGenerator:
    """
    Create HouseholdGenerator for specific state/year.
    
    This creates a new generator for each request with the specified parameters.
    """
    try:
        # Use the existing loader's engine
        return HouseholdGenerator(
            state=state,
            pums_year=pums_year,
            bls_year=bls_year,
            connection_string=loader.engine.url.render_as_string(hide_password=False)
        )
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to initialize generator: {str(e)}"
        )