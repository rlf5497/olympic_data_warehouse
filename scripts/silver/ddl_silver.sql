/*
==========================================================================================
Script: ddl_silver.sql
Layer: Silver (Curated & Standardized Layer)
==========================================================================================
Purpose:
	Defines all Silver-layer tables for the Olympics Data Warehouse. The Silver layer
	stores cleaned, standardized, and enriched data derived from the Bronze layer.
	These tables apply strict data quality rules while preserving analytical flexibility
	for downstream Gold-layer consumption.

Contents:
	- silver.olympics_bios
	- silver.olympics_bios_locs
	- silver.olympics_noc_regions
	- silver.olympics_populations
	- silver.olympics_results

Notes:
	* Silver tables are recreated to enforce consistent schemas and data types.
	* Data cleansing and standardization occur in this layer (e.g., parsed dates,
	  normalized locations, numeric casting).
	* Ligth derived attributes are introduced where applicable (e.g., parsed finishing positions,
	  tie indicators).
	* No aggregation or business metrics are created in Silver; these are handled
	  in the Gold Layer
==========================================================================================
*/



/*===============================================================================
	TABLE: silver.olympics_bios
	Description:
		Curated athlete biographical data derived from bronze.olympics_bios.
		Applies standardized date parsing, normalized location attributes, and
		cleaned physical measurements.
=================================================================================*/

DROP TABLE IF EXISTS silver.olympics_bios;

CREATE TABLE silver.olympics_bios (
	sex						VARCHAR(10),
	used_name				VARCHAR(55),
	born_date				DATE,
	born_city				VARCHAR(55),
	born_region				VARCHAR(40),
	born_country_code		VARCHAR(10),
	died_date				DATE,
	died_city				VARCHAR(55),
	died_region				VARCHAR(40),
	died_country_code		VARCHAR(10),
	noc						VARCHAR(80),
	athlete_id				INT,
	height_cm				NUMERIC,
	weight_kg				NUMERIC,
	dwh_create_date			TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



/*===============================================================================
	TABLE: silver.olympics_bios_locs
	Description:
		Enriched athlete biographical data with geographic coordinates.
=================================================================================*/

DROP TABLE IF EXISTS silver.olympics_bios_locs;

CREATE TABLE silver.olympics_bios_locs (
	athlete_id				INT,
	name					VARCHAR(55),
	born_date				DATE,
	born_city				VARCHAR(55),
	born_region				VARCHAR(40),
	noc						VARCHAR(80),
	height_cm				NUMERIC,
	weight_kg				NUMERIC,
	died_date				DATE,
	lat						DOUBLE PRECISION,
	long					DOUBLE PRECISION,
	dwh_create_date			TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



/*===============================================================================
	TABLE: silver.olympics_noc_regions
	Description:
		Standardized National Olympic Committee (NOC) to region mappings.
=================================================================================*/

DROP TABLE IF EXISTS silver.olympics_noc_regions;

CREATE TABLE silver.olympics_noc_regions (
	noc						VARCHAR(5),
	region					VARCHAR(35),
	notes					VARCHAR(30),
	dwh_create_date			TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



/*===============================================================================
	TABLE: silver.olympics_populations
	Description:
		Country population reference data used for analytical enrichment
		and contextual population-based comparisons.
=================================================================================*/

DROP TABLE IF EXISTS silver.olympics_populations;

CREATE TABLE silver.olympics_populations (
	country_name			TEXT,
	country_code			TEXT,
	year					INT,
	population				NUMERIC,
	dwh_create_date			TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



/*===============================================================================
	TABLE: silver.olympics_results
	Description:
		Cleaned and standardized Olympic competition results.
		Includes parsed finishing positions, tie indicators, and standardized
		medal outcomes.
=================================================================================*/

DROP TABLE IF EXISTS silver.olympics_results;

CREATE TABLE silver.olympics_results (
	olympic_year			INT,
	game_type				TEXT,
	sport_event				VARCHAR(160),
	team					VARCHAR(60),
	pos						INT,
	is_tied					BOOLEAN,
	medal					VARCHAR(10),
	as_name					VARCHAR(50),
	athlete_id				INT,
	noc						TEXT,
	discipline				VARCHAR(35),
	dwh_create_date			TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
