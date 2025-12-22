"""
Pytest fixtures for API testing.
"""

import pytest
from fastapi.testclient import TestClient
import pandas as pd
from sqlalchemy import create_engine

from api.main import app
from api.config import Settings


# Override settings for testing
def get_settings_override():
    return Settings(
        database_url="sqlite:///:memory:",
        debug=True,
        max_households_per_request=10
    )


@pytest.fixture
def client():
    """
    FastAPI test client.
    """
    # Override settings
    from api.main import app
    from api.dependencies import get_settings
    
    app.dependency_overrides[get_settings] = get_settings_override
    
    with TestClient(app) as test_client:
        yield test_client
    
    app.dependency_overrides.clear()


@pytest.fixture
def mock_distributions():
    """
    Mock distribution tables for testing.
    """
    return {
        'household_patterns': pd.DataFrame({
            'pattern': ['married_couple_with_children', 'single_parent', 'single_person'],
            'weighted_count': [1000, 500, 300],
            'percentage': [55.6, 27.8, 16.7],
            'avg_household_size': [4.2, 2.8, 1.0]
        })
    }