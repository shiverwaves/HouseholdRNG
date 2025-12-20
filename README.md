# Tax Prep Data Extraction

Extract demographic distribution tables from Census PUMS and BLS OEWS data for tax preparation household generation.

## Overview

This repository contains three extraction scripts that download and process government data to create probability distribution tables:

1. **PUMS Extraction** - 9 tables from Census microdata (household patterns, employment, income sources)
2. **BLS Extraction** - 1 table of occupation wages by state
3. **Derived Extraction** - 3 combined probability tables (education-occupation, age-income, self-employment)

**Result:** 13 distribution tables ready for synthetic household generation.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Extract data for Hawaii
python scripts/extract_pums_distributions.py --state HI --year 2022
python scripts/extract_bls_distributions.py --state HI --year 2023
python scripts/extract_derived_distributions.py --state HI

# Import to PostgreSQL
./database/import_all.sh HI
```

## Documentation

- [Quick Start Guide](docs/QUICKSTART.md) - 5-minute getting started
- [Complete Pipeline Guide](docs/PIPELINE_GUIDE.md) - How all scripts work together
- [Extraction Logic](docs/EXTRACTION_LOGIC.md) - Detailed extraction explanations

## Repository Structure

```
taxprep-data-extraction/
├── .github/workflows/     # GitHub Actions CI/CD
├── scripts/               # Three extraction scripts
├── docs/                  # Documentation
├── database/              # Schema and import helpers
├── tests/                 # Test suite
├── cache/                 # Downloaded data (gitignored)
└── output/                # Generated SQL (gitignored)
```

## GitHub Actions

Run extraction in GitHub Actions:
1. Go to "Actions" tab
2. Select "Extract Distribution Tables"
3. Click "Run workflow"
4. Enter state code (e.g., HI)
5. Download SQL files from artifacts

## Output Files

Three SQL files generated per state:
- `pums_distributions_[STATE]_2022.sql` - 9 PUMS distribution tables
- `bls_occupation_wages_[STATE]_2023.sql` - 1 BLS occupation wage table
- `derived_probabilities_[STATE]_2023.sql` - 3 derived probability tables

## Data Sources

- **Census PUMS** - American Community Survey Public Use Microdata Sample
- **BLS OEWS** - Bureau of Labor Statistics Occupational Employment and Wage Statistics

## License

MIT License - see [LICENSE](LICENSE)

## Contributing

Issues and pull requests welcome!
