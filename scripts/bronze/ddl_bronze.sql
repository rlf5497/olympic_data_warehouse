/*
==========================================================================================
Script: ddl_bronze.sql
Layer: Bronze (Raw Ingestion Layer)
==========================================================================================
Purpose:
	Defines all Bronze-layer tables for the Olympic Data Warehouse. The Bronze layer stores
	raw, uncleaned, untransformed data exactly as ingested from the source systems. These tables
	are recreated on each load to ensure a consistent raw structure.

Contents:
	- bronze.olympics_bios
	- bronze.olympics_bios_locs
	- bronze.olympics_noc_regions
	- bronze.olympics_populations
	- bronze.olympics_results

Notes:
	* No transformations are done in Bronze.
	* Column names and datatypes follow the original raw files as closely as possible.
	* Cleaning, standardization, and relational modeling happen in the Silver & Gold Layer.
==========================================================================================
*/


/*===============================================================================
	TABLE: bronze.olympics_bios
	Description: Raw athlete biography data scraped from the Olympedia website.
=================================================================================*/

DROP TABLE IF EXISTS bronze.olympics_bios;

CREATE TABLE bronze.olympics_bios (
	roles							    VARCHAR(125),
	sex								    VARCHAR(10),
	full_name						  VARCHAR(110),
	used_name						  VARCHAR(55),
	born							    VARCHAR(110),
	died							    VARCHAR(110),
	noc								    VARCHAR(75),
	athlete_id						INT,
	measurements					VARCHAR(20),
	affiliations					VARCHAR(175),
	nick_petnames					VARCHAR(245),
	titles							  VARCHAR(1555),
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
	name 							    VARCHAR(50),
	born_date 						VARCHAR(10),
	born_city 						VARCHAR(55),
	born_region 					VARCHAR(40),
	born_country 					VARCHAR(15),
	noc 							    VARCHAR(75),
	height_cm						  INT,
	weight_kg						  INT,
	died_date						  VARCHAR(10),
	lat								    FLOAT,
	long							    FLOAT
);


/*===============================================================================
	TABLE: bronze.olympics_noc_regions
	Description: Raw mapping of NOC (National Olympic Committees) to regions.
=================================================================================*/

DROP TABLE IF EXISTS bronze.olympics_noc_regions;

CREATE TABLE bronze.olympics_noc_regions (
	noc								  VARCHAR(5),
	region							VARCHAR(35),
	notes							  VARCHAR(30)
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
	"1960"							  BIGINT,
	"1961"							  BIGINT,
	"1962"							  BIGINT,
	"1963"							  BIGINT,
	"1964"							  BIGINT,	
	"1965"							  BIGINT,
	"1966"							  BIGINT,
	"1967"							  BIGINT,
	"1968"							  BIGINT,
	"1969"							  BIGINT,
	"1970"							  BIGINT,
	"1971"							  BIGINT,
	"1972"							  BIGINT,
	"1973"							  BIGINT,
	"1974"							  BIGINT,
	"1975"							  BIGINT,
	"1976"							  BIGINT,
	"1977"							  BIGINT,
	"1978"							  BIGINT,
	"1979"							  BIGINT,
	"1980"							  BIGINT,
	"1981"							  BIGINT,
	"1982"							  BIGINT,
	"1983"							  BIGINT,
	"1984"							  BIGINT,
	"1985"							  BIGINT,
	"1986"							  BIGINT,
	"1987"							  BIGINT,
	"1988"							  BIGINT,
	"1989"							  BIGINT,
	"1990"							  BIGINT,
	"1991"							  BIGINT,
	"1992"							  BIGINT,
	"1993"							  BIGINT,																					
	"1994"							  BIGINT,
	"1995"							  BIGINT,
	"1996"							  BIGINT,
	"1997"							  BIGINT,	
	"1998"							  BIGINT,
	"1999"							  BIGINT,	
	"2000"							  BIGINT,
	"2001"							  BIGINT,
	"2002"							  BIGINT,
	"2003"							  BIGINT,
	"2004"							  BIGINT,
	"2005"							  BIGINT,
	"2006"							  BIGINT,
	"2007"							  BIGINT,	
	"2008"							  BIGINT,
	"2009"							  BIGINT,
	"2010"							  BIGINT,
	"2011"							  BIGINT,
	"2012"							  BIGINT,
	"2013"							  BIGINT,	
	"2014"							  BIGINT,
	"2015"							  BIGINT,
	"2016"							  BIGINT,
	"2017"							  BIGINT,	
	"2018"							  BIGINT,
	"2019"							  BIGINT,
	"2020"							  BIGINT,		
	"2021"							  BIGINT,
	"2022"							  BIGINT,
	"2023"							  BIGINT
);


/*===============================================================================
	TABLE: bronze.olympics_results
	Description: Raw athlete competition results & medal outcomes.
===============================================================================*/

DROP TABLE IF EXISTS bronze.olympics_results;

CREATE TABLE bronze.olympics_results (
	games							  VARCHAR(30),
	sport_event					VARCHAR(160),
	team							  VARCHAR(60),
	pos								  INT,
	medal							  VARCHAR(10),
	as_name							VARCHAR(50),
	athlete_id					NT,
	noc								  VARCHAR(5),
	discipline					VARCHAR(35),
	nationality					VARCHAR(15),
	unnamed_7						VARCHAR(10)
);
