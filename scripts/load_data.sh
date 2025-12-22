#/bin/bash
STATES="HI"
YEAR=2023
OUTPUT=database

for STATE in $STATES; do
  echo "Loading $STATE..."
  python scripts/extract_pums.py --state $STATE --year $YEAR --output $OUTPUT
  python scripts/extract_bls.py --state $STATE --year $YEAR --output $OUTPUT
  python scripts/extract_derived.py --state $STATE --pums-year $YEAR --bls-year $YEAR --output $OUTPUT
done
