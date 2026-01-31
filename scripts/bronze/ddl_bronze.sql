/*
==========================================================================================
Script: ddl_bronze.sql
Layer: Bronze (Raw Ingestion Layer)

Purpose:
	Defines all Bronze-layer tables for the Olympic Data Warehouse.
	The Bronze layer stores raw, uncleaned, untransformed data exactly as ingested
	from source systems. Tables are recreated on each load to ensure structural
	consistency with the source files.

Design Principles:
	- Schema-on-read
	- No transformations or business logic
	- Columns closely mirror source CSV structure
	- Downstream cleansing and modeling occur in Silver and Gold layers

Contents:
	- bronze.olympics_bios
	- bronze.olympics_bios_locs
	- bronze.olympics_noc_regions
	- bronze.olympics_populations
	- bronze.olympics_results
==========================================================================================


/*===============================================================================
	TABLE: bronze.olympics_bios
	Description:
		Raw athlete biography data scraped from the Olympedia website.

	Grain:
		One row per athlete record as represented in the source file.
=================================================================================*/

DROP TABLE IF EXISTS bronze.olympics_bios;

CREATE TABLE bronze.olympics_bios (
	roles							VARCHAR(125),
	sex								VARCHAR(10),
	full_name						VARCHAR(110),
	used_name						VARCHAR(55),
	born							VARCHAR(110),
	died							VARCHAR(110),
	noc								VARCHAR(75),
	athlete_id						INT,
	measurements					VARCHAR(20),
	affiliations					VARCHAR(175),
	nick_petnames					VARCHAR(245),
	titles							VARCHAR(1555),
	other_names						VARCHAR(255),
	nationality						VARCHAR(40),
	original_name					VARCHAR(120),
	name_order						VARCHAR(10)
);


/*===============================================================================
	TABLE: bronze.olympics_bios_locs
	Description: Raw athlete birth/death, locations, and physical attributes.
=================================================================================*/

DROP TABLE IF EXISTS bronze.olympics_bios_locs;

CREATE TABLE bronze.olympics_bios_locs (
	athlete_id 						INT,
	name 							VARCHAR(50),
	born_date 						VARCHAR(10),
	born_city 						VARCHAR(55),
	born_region 					VARCHAR(40),
	born_country 					VARCHAR(15),
	noc 							VARCHAR(75),
	height_cm						NUMERIC,
	weight_kg						NUMERIC,
	died_date						VARCHAR(10),
	lat								FLOAT,
	long							FLOAT
);


/*===============================================================================
	TABLE: bronze.olympics_noc_regions
	Description: Raw mapping of NOC (National Olympic Committees) to regions.
=================================================================================*/

DROP TABLE IF EXISTS bronze.olympics_noc_regions;

CREATE TABLE bronze.olympics_noc_regions (
	noc								VARCHAR(5),
	region							VARCHAR(35),
	notes							VARCHAR(30)
);


/* ============================================================================
	TABLE: bronze.olympics_populations
	Description: Raw population data by country and year (1960-2023).
	Note:
		- Pivoted format preserved exactly as source
		- Will be unpivoted in Silver -> year, population
============================================================================*/

DROP TABLE IF EXISTS bronze.olympics_populations;

CREATE TABLE bronze.olympics_populations (
	country_name					VARCHAR(55),
	country_code					VARCHAR(5),
	"1960"							NUMERIC,
	"1961"							NUMERIC,
	"1962"							NUMERIC,
	"1963"							NUMERIC,
	"1964"							NUMERIC,	
	"1965"							NUMERIC,
	"1966"							NUMERIC,
	"1967"							NUMERIC,
	"1968"							NUMERIC,
	"1969"							NUMERIC,
	"1970"							NUMERIC,
	"1971"							NUMERIC,
	"1972"							NUMERIC,
	"1973"							NUMERIC,
	"1974"							NUMERIC,
	"1975"							NUMERIC,
	"1976"							NUMERIC,
	"1977"							NUMERIC,
	"1978"							NUMERIC,
	"1979"							NUMERIC,
	"1980"							NUMERIC,
	"1981"							NUMERIC,
	"1982"							NUMERIC,
	"1983"							NUMERIC,
	"1984"							NUMERIC,
	"1985"							NUMERIC,
	"1986"							NUMERIC,
	"1987"							NUMERIC,
	"1988"							NUMERIC,
	"1989"							NUMERIC,
	"1990"							NUMERIC,
	"1991"							NUMERIC,
	"1992"							NUMERIC,
	"1993"							NUMERIC,																					
	"1994"							NUMERIC,
	"1995"							NUMERIC,
	"1996"							NUMERIC,
	"1997"							NUMERIC,	
	"1998"							NUMERIC,
	"1999"							NUMERIC,	
	"2000"							NUMERIC,
	"2001"							NUMERIC,
	"2002"							NUMERIC,
	"2003"							NUMERIC,
	"2004"							NUMERIC,
	"2005"							NUMERIC,
	"2006"							NUMERIC,
	"2007"							NUMERIC,	
	"2008"							NUMERIC,
	"2009"							NUMERIC,
	"2010"							NUMERIC,
	"2011"							NUMERIC,
	"2012"							NUMERIC,
	"2013"							NUMERIC,	
	"2014"							NUMERIC,
	"2015"							NUMERIC,
	"2016"							NUMERIC,
	"2017"							NUMERIC,	
	"2018"							NUMERIC,
	"2019"							NUMERIC,
	"2020"							NUMERIC,		
	"2021"							NUMERIC,
	"2022"							NUMERIC,
	"2023"							NUMERIC
);


/*===============================================================================
	TABLE: bronze.olympics_results
	Description: Raw athlete competition results & medal outcomes.
===============================================================================*/

DROP TABLE IF EXISTS bronze.olympics_results;

CREATE TABLE bronze.olympics_results (
	games							VARCHAR(30),
	sport_event						VARCHAR(160),
	team							VARCHAR(60),
	pos								VARCHAR(50),
	medal							VARCHAR(10),
	as_name							VARCHAR(50),
	athlete_id						INT,
	noc								VARCHAR(5),
	discipline						VARCHAR(35),
	nationality						VARCHAR(15),
	unnamed_7						VARCHAR(10)
);
