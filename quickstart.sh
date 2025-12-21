#!/bin/bash
# Quick Start Script for Tax Training Data Pipeline
# Run this script to test the pipeline locally

set -e  # Exit on error

echo "=================================="
echo "Tax Training Data Pipeline"
echo "Quick Start Script"
echo "=================================="
echo ""

# Check Python version
echo "Checking Python version..."
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not found"
    exit 1
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1-2)
echo "✓ Found Python $PYTHON_VERSION"
echo ""

# Create virtual environment
echo "Creating virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "✓ Virtual environment created"
else
    echo "✓ Virtual environment already exists"
fi
echo ""

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate
echo "✓ Virtual environment activated"
echo ""

# Install dependencies
echo "Installing dependencies..."
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt
echo "✓ Dependencies installed"
echo ""

# Run test extraction
echo "=================================="
echo "Running Test Extraction (Hawaii)"
echo "=================================="
echo ""

STATE="HI"
PUMS_YEAR="2022"
BLS_YEAR="2023"

echo "Stage 1: Extracting PUMS data..."
python scripts/extract_pums.py --state $STATE --year $PUMS_YEAR --output sql
echo ""

echo "Stage 2: Extracting BLS data..."
python scripts/extract_bls.py --state $STATE --year $BLS_YEAR --output sql
echo ""

echo "Stage 3: Extracting derived tables..."
python scripts/extract_derived.py --state $STATE --pums-year $PUMS_YEAR --bls-year $BLS_YEAR --output sql
echo ""

# Summary
echo "=================================="
echo "✓ EXTRACTION COMPLETE"
echo "=================================="
echo ""
echo "SQL files generated in: output/"
ls -lh output/*.sql 2>/dev/null || echo "No SQL files found"
echo ""
echo "To import to PostgreSQL:"
echo "  psql -d mydb -f output/pums_distributions_${STATE}_${PUMS_YEAR}.sql"
echo "  psql -d mydb -f output/bls_occupation_wages_${STATE}_${BLS_YEAR}.sql"
echo "  psql -d mydb -f output/derived_distributions_${STATE}_pums_${PUMS_YEAR}_bls_${BLS_YEAR}.sql"
echo ""
echo "To deactivate virtual environment:"
echo "  deactivate"
echo ""
