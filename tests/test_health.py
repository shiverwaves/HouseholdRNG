"""
Tests for health check endpoints.
"""

import pytest
from fastapi.testclient import TestClient


def test_health_check(client: TestClient):
    """Test /api/v1/health endpoint"""
    response = client.get("/api/v1/health")
    
    assert response.status_code == 200
    data = response.json()
    
    assert "status" in data
    assert "version" in data
    assert "database" in data
    assert "timestamp" in data


def test_root_health(client: TestClient):
    """Test /health endpoint (for load balancers)"""
    response = client.get("/health")
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"


def test_root_endpoint(client: TestClient):
    """Test root / endpoint"""
    response = client.get("/")
    
    assert response.status_code == 200
    data = response.json()
    
    assert "name" in data
    assert "version" in data
    assert "docs" in data