# Olympics Source Dataset


## Data Origin
This dataset is sourced from the public Olympics data repository by Keith Galli.
The original repository contains multiple folders with pre-labeled data, including
folders named `athletes` and `clean-data`.


## Important Note on Data Freshness and Quality
Although some source files are located in a folder named `clean-data`, **all datasets
in this project are treated as raw source data**.

No assumptions are made regarding:
-  Data cleanliness
-  Standardization
-  Completeness
-  Consistency across files

All data quality rules, transformations, and standardizations are re-applied within
the data warehouse pipeline (Sillver Layer).


## Datasets Used in This Project

The following CSV files are used as input sources:


### athletes/
-  `bios.csv`        - Athlete demographic and biographical information


### clean_data/
- `bios_locs.csv`     - Athlete birth and death location details.
- `noc_regions.csv`   - National Olympic Committee (NOC) reference data.
- `populations.csv`   - Population reference data by country.
- `results.csv`       - Olympic event results by athlete, sport, and games.

Files not listed above are not ingested into the data warehouse.


## Data Handling Strategy
All CSV files in this directory are:
-  Ingested into the Bronze layer without transformation
-  Cleaned and standardized in the Silver layer
-  Modeled into analytics-ready structures in the Gold layer

This approach ensures full control over data quality, lineage, and reproducibility.
