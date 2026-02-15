# LinkedIn Jobs Data Engineering Pipeline

A scalable data engineering project that ingests, transforms, and analyzes LinkedIn job postings data using modern data lakehouse architecture on Databricks.

## Project Overview

This project processes the [LinkedIn Job Postings dataset](https://www.kaggle.com/datasets/arshkon/linkedin-job-postings) from Kaggle, implementing a medallion architecture (Bronze -> Silver -> Gold) to extract insights about job market trends, company hiring patterns, and skill demands.

### Key Features
- **Config-Driven Pipeline**: Centralized configuration for easy maintenance and extensibility
- **Medallion Architecture**: Structured data layers for progressive data refinement
- **Delta Lake**: ACID transactions and time travel capabilities
- **Scalable Design**: Built on Databricks for distributed processing

## Architecture
```
┌─────────────┐
│   Kaggle    │
│  Data Source│
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────┐
│         Bronze Layer                │
│  (Raw data ingestion)               │
│  - DimCompanies                     │
│  - DimJobPostings                   │
│  - DimBenefits                      │
│  - DimSkills, Industries, etc.      │
└──────┬──────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│         Silver Layer                │
│  (Cleaned & validated data)         │
│  - TBD                              │
└──────┬──────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│         Gold Layer                  │
│  (Business-ready aggregations)      │
│  - TBD                              │
└──────┬──────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│         Dashboard / Viz             │
│  - TBD                              │
└─────────────────────────────────────┘
```

## Data Sources

The pipeline processes the following data files from the LinkedIn dataset:

### Companies
- `companies.csv` - Company profiles
- `company_industries.csv` - Company industry classifications
- `company_specialities.csv` - Company specializations
- `employee_counts.csv` - Employee count ranges by company

### Jobs
- `postings.csv` - Job posting details (main fact table)
- `benefits.csv` - Benefits offered per job
- `job_industries.csv` - Industry tags per job
- `job_skills.csv` - Required skills per job
- `salaries.csv` - Salary information

### Reference/Mappings
- `industries.csv` - Industry lookup table
- `skills.csv` - Skills lookup table

## Tech Stack

- **Platform**: Databricks Free
- **Storage**: Delta Lake
- **Data Format**: Parquet (via Delta)
- **Orchestration**: Databricks Workflows
- **Language**: Python (PySpark)
- **Data Source**: Kaggle API via `kagglehub`

## Project Structure
```
linkedin-jobs/
├── notebooks/
│   ├── 00_Setup/
│   │   ├── create_schemas.py         # Initial catalog/schema setup
│   │   └── config_setup.py           # Configuration table setup
│   ├── 01_Bronze/
│   │   └── ingest_raw_data.py        # Generic ingestion notebook
│   ├── 02_Silver/
│   │   └── # TBD
│   ├── 03_Gold/
│   │   └── # TBD
│   └── 04_Analysis/
│       └── # TBD
├── config/
│   └── extract_files_config.py       # File extraction configuration
└── README.md
```

## Setup & Installation

### Prerequisites
- Databricks account
- Kaggle account

### Installation Steps

1. **Clone the repository**
```bash
   git clone https://github.com/KamilKolanowski/linkedin-jobs.git
```

TBD


## Data Model

### Bronze Layer (Raw Data)
All tables stored in `linkedin.bronze` schema with original structure from source files.

### Silver Layer (Cleaned Data)
**TBD** - Will include:
- Data quality checks
- Standardization
- Deduplication
- Type casting and validation

### Gold Layer (Business Metrics)
**TBD** - Will include aggregated metrics such as:
- Job postings by industry/location
- Skill demand trends
- Salary ranges by role/company
- etc.

## Dashboard & Visualizations

**TBD** - Planned visualizations:
---

## Pipeline Execution

The pipeline is designed to be config-driven. To add new data sources:

1. Update `config/extract_files_config.py` with new file entries
2. Run the config update notebook
3. Execute the bronze ingestion (automatically processes all configured files)
4. TBD

## Configuration Management

Configuration is stored in Delta table: `linkedin.cfg.ExtractFiles`

| Column | Description |
|--------|-------------|
| Id | Auto-generated unique identifier |
| Directory | Source directory in Kaggle dataset |
| FileName | File name without extension |
| FileExtension | File extension (csv, json, etc.) |
| Catalog | Target Databricks catalog |
| Schema | Target schema (bronze/silver/gold) |
| TableName | Target Delta table name |

## Contributing

This is a personal learning project, but suggestions and feedback are welcome! Feel free to open an issue or submit a pull request.

## License

This project is open source and available under the [Apache License 2.0](LICENSE).

## Acknowledgments

- Dataset provided by [Arsh Kon](https://www.kaggle.com/arshkon) on Kaggle
- Built using Databricks Free Edition

## Contact

Kamil Kolanowski - [GitHub Profile](https://github.com/KamilKolanowski)

---

**Status**: Work in Progress - Bronze layer implementation complete, Silver/Gold layers in development
