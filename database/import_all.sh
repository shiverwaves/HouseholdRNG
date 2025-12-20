#!/bin/bash
# Import all distribution SQL files to PostgreSQL

set -e  # Exit on error

if [ -z "$1" ]; then
    echo "Usage: ./database/import_all.sh STATE_CODE [DATABASE_NAME]"
    echo ""
    echo "Examples:"
    echo "  ./database/import_all.sh HI"
    echo "  ./database/import_all.sh CA taxapp_production"
    echo ""
    exit 1
fi

STATE=$1
DATABASE=${2:-taxapp_dev}  # Default to taxapp_dev
PUMS_YEAR=${PUMS_YEAR:-2022}
BLS_YEAR=${BLS_YEAR:-2023}

echo "========================================"
echo "Importing distributions for $STATE"
echo "Database: $DATABASE"
echo "========================================"

# Check if database exists
if ! psql -lqt | cut -d \| -f 1 | grep -qw $DATABASE; then
    echo "❌ Database '$DATABASE' not found"
    echo "Create it with: createdb $DATABASE"
    exit 1
fi

# Check if SQL files exist
PUMS_FILE="output/pums_distributions_${STATE}_${PUMS_YEAR}.sql"
BLS_FILE="output/bls_occupation_wages_${STATE}_${BLS_YEAR}.sql"
DERIVED_FILE="output/derived_probabilities_${STATE}_${BLS_YEAR}.sql"

if [ ! -f "$PUMS_FILE" ]; then
    echo "❌ PUMS file not found: $PUMS_FILE"
    echo "Run: python scripts/extract_pums_distributions.py --state $STATE"
    exit 1
fi

if [ ! -f "$BLS_FILE" ]; then
    echo "❌ BLS file not found: $BLS_FILE"
    echo "Run: python scripts/extract_bls_distributions.py --state $STATE"
    exit 1
fi

if [ ! -f "$DERIVED_FILE" ]; then
    echo "❌ Derived file not found: $DERIVED_FILE"
    echo "Run: python scripts/extract_derived_distributions.py --state $STATE"
    exit 1
fi

# Import files
echo ""
echo "Importing PUMS distributions..."
psql -d $DATABASE -f $PUMS_FILE

echo ""
echo "Importing BLS occupation wages..."
psql -d $DATABASE -f $BLS_FILE

echo ""
echo "Importing derived probabilities..."
psql -d $DATABASE -f $DERIVED_FILE

echo ""
echo "========================================"
echo "✅ Import complete for $STATE"
echo "========================================"
echo ""
echo "Verify import:"
echo "  psql -d $DATABASE -c \"\\dt\""
echo ""
