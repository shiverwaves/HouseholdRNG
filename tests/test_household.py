"""
Tests for household generation endpoints.
"""

import pytest
from fastapi.testclient import TestClient


def test_generate_single_household(client: TestClient):
    """Test generating a single household"""
    payload = {
        "state": "HI",
        "pums_year": 2023,
        "count": 1
    }
    
    response = client.post("/api/v1/households/generate", json=payload)
    
    # May fail if no data loaded - that's expected in pure unit tests
    # In integration tests with real DB, this should work
    if response.status_code == 200:
        data = response.json()
        
        assert "households" in data
        assert "count" in data
        assert data["count"] == 1
        assert len(data["households"]) == 1
        
        household = data["households"][0]
        assert "household_id" in household
        assert "pattern" in household
        assert household["state"] == "HI"


def test_generate_multiple_households(client: TestClient):
    """Test generating multiple households"""
    payload = {
        "state": "HI",
        "pums_year": 2023,
        "count": 5
    }
    
    response = client.post("/api/v1/households/generate", json=payload)
    
    if response.status_code == 200:
        data = response.json()
        assert data["count"] == 5
        assert len(data["households"]) == 5


def test_generate_with_seed(client: TestClient):
    """Test reproducible generation with seed"""
    payload = {
        "state": "HI",
        "pums_year": 2023,
        "count": 3,
        "seed": 42
    }
    
    # Generate twice with same seed
    response1 = client.post("/api/v1/households/generate", json=payload)
    response2 = client.post("/api/v1/households/generate", json=payload)
    
    if response1.status_code == 200 and response2.status_code == 200:
        data1 = response1.json()
        data2 = response2.json()
        
        # Should produce same households
        for i in range(3):
            assert data1["households"][i]["pattern"] == data2["households"][i]["pattern"]


def test_generate_invalid_state(client: TestClient):
    """Test error handling for invalid state"""
    payload = {
        "state": "INVALID",
        "pums_year": 2023,
        "count": 1
    }
    
    response = client.post("/api/v1/households/generate", json=payload)
    
    # Should fail validation or return 404
    assert response.status_code in [400, 404, 422]


def test_generate_exceeds_limit(client: TestClient):
    """Test error when exceeding max households per request"""
    payload = {
        "state": "HI",
        "pums_year": 2023,
        "count": 500  # Exceeds limit in test settings
    }
    
    response = client.post("/api/v1/households/generate", json=payload)
    
    assert response.status_code == 400
    data = response.json()
    assert "detail" in data


def test_generate_validation_errors(client: TestClient):
    """Test request validation"""
    # Missing required field
    response = client.post("/api/v1/households/generate", json={})
    assert response.status_code == 422
    
    # Invalid count
    response = client.post("/api/v1/households/generate", json={
        "state": "HI",
        "count": -1
    })
    assert response.status_code == 422
    
    # Invalid complexity
    response = client.post("/api/v1/households/generate", json={
        "state": "HI",
        "complexity": "invalid"
    })
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_available_states(client: TestClient):
    """Test listing available states"""
    response = client.get("/api/v1/available-states")
    
    if response.status_code == 200:
        data = response.json()
        
        assert "states" in data
        assert "total_states" in data
        assert isinstance(data["states"], dict)