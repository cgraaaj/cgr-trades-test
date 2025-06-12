# NSE Stock Options Data Processing

This project is designed for collecting, processing, and analyzing NSE (National Stock Exchange) stock options data. It provides tools for data collection, processing, and analysis of options trading data.

## Project Structure

```
cgr-trades/
├── src/                    # Source code
│   ├── data_collection/    # Scripts for collecting NSE data
│   │   ├── opt-stk-data-to-db-upstox.py
│   │   └── instrument-downloader.py
│   ├── data_processing/    # Scripts for processing raw data
│   │   └── option_db_test.py
│   ├── analysis/          # Analysis and ranking scripts
│   │   ├── option_analyze.py
│   │   ├── opt-analyze.py
│   │   └── option_ranking.py
│   └── database/          # Database related files
│       ├── db-sql-upstox
│       └── dbsql-smartapi
├── data/                   # Data storage
│   ├── raw/               # Raw data dumps
│   │   ├── NSE.json
│   │   └── instrument_test.csv
│   ├── processed/         # Processed data
│   │   ├── analyzed_stocks_data.pickle
│   │   └── filtered_data.json
│   └── models/            # Saved models and predictions
│       └── prediction.pickle
├── notebooks/             # Jupyter notebooks for analysis
├── tests/                 # Test files
├── requirements/          # Project dependencies
│   └── requirements.txt
└── docs/                  # Additional documentation
```

## Directory Descriptions

### Source Code (`src/`)

- **data_collection/**: Contains scripts for gathering data from NSE
  - `opt-stk-data-to-db-upstox.py`: Script for collecting options data using Upstox API
  - `instrument-downloader.py`: Tool for downloading instrument data

- **data_processing/**: Contains scripts for processing raw data
  - `option_db_test.py`: Database operations and data processing utilities

- **analysis/**: Contains scripts for analyzing options data
  - `option_analyze.py`: Main analysis script for options data
  - `opt-analyze.py`: Additional analysis utilities
  - `option_ranking.py`: Script for ranking and scoring options

- **database/**: Contains SQL files for database operations
  - `db-sql-upstox`: SQL queries for Upstox database
  - `dbsql-smartapi`: SQL queries for SmartAPI database

### Data Storage (`data/`)

- **raw/**: Contains unprocessed data files
  - `NSE.json`: Raw NSE data dumps
  - `instrument_test.csv`: Test instrument data

- **processed/**: Contains processed and cleaned data
  - `analyzed_stocks_data.pickle`: Processed stock analysis data
  - `filtered_data.json`: Filtered and processed data

- **models/**: Contains saved models and predictions
  - `prediction.pickle`: Saved prediction models

### Other Directories

- **notebooks/**: Jupyter notebooks for interactive analysis and visualization
- **tests/**: Test files for ensuring code quality
- **requirements/**: Project dependencies and requirements
- **docs/**: Additional documentation and guides

## Getting Started

1. Install dependencies:
   ```bash
   pip install -r requirements/requirements.txt
   ```

2. Set up your database credentials in the appropriate configuration files

3. Start with data collection:
   ```bash
   python src/data_collection/instrument-downloader.py
   ```

4. Process the collected data:
   ```bash
   python src/data_processing/option_db_test.py
   ```

5. Run analysis:
   ```bash
   python src/analysis/option_analyze.py
   ```

## Data Flow

1. Data Collection: Raw data is collected from NSE using the scripts in `data_collection/`
2. Data Processing: Raw data is processed and cleaned using scripts in `data_processing/`
3. Analysis: Processed data is analyzed using scripts in `analysis/`
4. Storage: Results are stored in appropriate directories under `data/`

## Contributing

When adding new files:
- Place data collection scripts in `src/data_collection/`
- Place data processing scripts in `src/data_processing/`
- Place analysis scripts in `src/analysis/`
- Place raw data in `data/raw/`
- Place processed data in `data/processed/`
- Place models and predictions in `data/models/`

## Notes

- Keep raw data files in their original format in `data/raw/`
- Processed data should be in a format ready for analysis
- Use appropriate file extensions (.py for Python scripts, .json for JSON data, etc.)
- Document any changes to the data structure or processing pipeline

## Configuration

All sensitive and environment-specific settings (like database URLs and API endpoints) are managed using a `.env` file at the project root. This file is **not** committed to version control for security reasons.

- Copy `.env.example` to `.env` and fill in your actual values.
- The project uses [python-dotenv](https://pypi.org/project/python-dotenv/) to load these variables automatically.
- All scripts should import configuration variables from `config.py`, which loads from `.env`.

Example usage in your scripts:
```python
import config
print(config.DATABASE_URL)
```

**Never commit your `.env` file to version control.** 