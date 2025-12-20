# Documentation

Welcome to the Tax Prep Data Extraction documentation!

## Quick Links

- **[QUICKSTART.md](QUICKSTART.md)** - 5-minute getting started guide
- **[PIPELINE_GUIDE.md](PIPELINE_GUIDE.md)** - Complete pipeline explanation
- **[EXTRACTION_LOGIC.md](EXTRACTION_LOGIC.md)** - Detailed extraction logic
- **[PUMS_EXTRACTION.md](PUMS_EXTRACTION.md)** - PUMS extraction details
- **[BLS_EXTRACTION.md](BLS_EXTRACTION.md)** - BLS extraction details

## Overview

This project extracts distribution tables from government data sources:

1. **Census PUMS** (Public Use Microdata Sample)
   - Household patterns
   - Employment statistics
   - Income sources
   - Demographics

2. **BLS OEWS** (Occupational Employment and Wage Statistics)
   - Occupation codes and titles
   - State-specific wages
   - Employment counts

3. **Derived Tables** (Combining PUMS + BLS)
   - Education → Occupation probabilities
   - Age → Income adjustments
   - Occupation → Self-employment probabilities

## Output

13 distribution tables total:
- 9 PUMS tables
- 1 BLS table
- 3 Derived tables

All exported as PostgreSQL-compatible SQL files.

## Getting Help

- Check [QUICKSTART.md](QUICKSTART.md) for common tasks
- Read [PIPELINE_GUIDE.md](PIPELINE_GUIDE.md) for troubleshooting
- Open an issue on GitHub for bugs or questions
