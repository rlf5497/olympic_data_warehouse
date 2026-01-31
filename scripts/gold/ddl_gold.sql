/* =====================================================================
	File: ddl_gold.sql
	Layer: Gold (Business-Ready / Analytics Layer)
	
	Purpose:
		- Define dimensional and fact tables for analytics consumption
		- Implements a START SCHEMA optimized for BI and reporting workloads
		- Data is sourced, cleansed, and conformed from Silver layer

	Conventions:
		- Surrogate keys *_key naming convention
		- Primary keys are GENERATED ALWAYS AS IDENTITY
		- Foreign keys reference Gold-layer dimensions
		- Tables are DROP / CREATE for idempotent deployments

	Notes:
		- The fact table (gold.fact_olympic_results) represents Olympic participation
		- Some dimension entities (e.g., athletes) may exist without corresponding
		  fact records if no Olympic participation occured
===================================================================== */



/* =====================================================================
	Dimension: gold.dim_athletes
	Grain:
		- One row per athlete

	Source:
		- silver.olympics_bios
		- silver.olympics_bios_locs

	Notes:
		- Contains all athletes present in the source biographical data
		- Not all athletes are guaranteed to have participation records
		  in the Olympic results fact table
		- Fact-based analytics will therefore include only athletes
		  associated with at least one participation event
===================================================================== */

DROP TABLE IF EXISTS gold.dim_athletes;

CREATE TABLE IF NOT EXISTS gold.dim_athletes (
	athlete_key					INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, 	-- Surrogate key for athlete dimension
	athlete_id					INT,											-- Source-system athlete identifier
	name						VARCHAR(55),									-- Athlete full name
	gender						VARCHAR(10),									-- Athlete gender
	birth_date					DATE,											-- Date of birth
	height_cm					NUMERIC,										-- Height in centimeters
	weight_kg					NUMERIC,										-- Weight in kilograms
	born_city					VARCHAR(55),									-- City of birth
	born_region					VARCHAR(40),									-- Region/state of birth
	born_country_code			VARCHAR(10),									-- Country code of birth
	latitude					DOUBLE PRECISION,								-- Birth location latitude
	longitude					DOUBLE PRECISION,								-- Birth location longitude
	noc							VARCHAR(80),									-- National Olympic Committee
	died_date					DATE,											-- Date of death (if applicable)
	died_city					VARCHAR(55),									-- City of death
	died_region					VARCHAR(40),									-- Region/state of death
	died_country_code			VARCHAR(10)										-- Country code of death
);



/* =====================================================================
	Dimension: gold.dim_nocs
	Grain:
		- One row per National Olympic Committee (NOC)

	Source:
		- silver.olympics_noc_regions
		- silver.olympics_results
===================================================================== */

DROP TABLE IF EXISTS gold.dim_nocs;

CREATE TABLE IF NOT EXISTS gold.dim_nocs (
	noc_key						INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,	-- Surrogate key for NOC dimension
	noc							VARCHAR(5),										-- NOC code (e.g., USA, JPN)
	region						VARCHAR(35),									-- Country or region name
	notes						VARCHAR(30)										-- Additional notes or remarks
);



/* =====================================================================
	Dimension: gold.dim_games
	Grain:
		- One row per Olympic Games (year + season)

	Source:
		- silver.olympics_results
===================================================================== */

DROP TABLE IF EXISTS gold.dim_games;

CREATE TABLE IF NOT EXISTS gold.dim_games (
	game_key					INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,	-- Surrogate key for games dimension
	olympic_year				INT,											-- Olympic year
	season						VARCHAR(15)										-- Summer/Winter
);



/* =====================================================================
	Dimension: gold.dim_sport_events
	Grain:
		- One row per sport event

	Source:
		- silver.olympics_results
===================================================================== */

DROP TABLE IF EXISTS gold.dim_sport_events;

CREATE TABLE IF NOT EXISTS gold.dim_sport_events (
	sport_event_key				INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,	-- Surrogate key for sport event dimension
	discipline					VARCHAR(35),									-- Sport discipline (e.g., Athletics)
	sport_event					VARCHAR(160),									-- Event name (e.g., 100m Men)
	team						VARCHAR(60)										-- Team or individual indicator
);



/* =====================================================================
	Dimension: gold.fact_olympic_results
	Grain:
		- One row per athlete per Olympic Games per participation record

	Source:
		- silver.olympics_results

	Notes:
		- Defines the analytical scope for Olympic Participation
		- Only dimension records associated with at least one participation
		  event will appear in fact-drive results.
===================================================================== */

DROP TABLE IF EXISTS gold.fact_olympic_results;

CREATE TABLE IF NOT EXISTS gold.fact_olympic_results (
	athlete_key					INT NOT NULL,									-- FK to dim_athletes
	game_key					INT NOT NULL,									-- FK to dim_games
	sport_event_key				INT	NULL,										-- FK to dim_sport_events
	noc_key						INT NULL,										-- FK to dim_nocs
	place						INT,											-- Final placement/rank
	medal						VARCHAR(10),									-- Gold / Silver / Bronze
	is_tied						BOOLEAN,										-- Indicates tied result

	CONSTRAINT		fk_fact_athlete
	FOREIGN KEY		(athlete_key)
	REFERENCES		gold.dim_athletes (athlete_key),

	CONSTRAINT		fk_fact_game
	FOREIGN KEY		(game_key)
	REFERENCES		gold.dim_games (game_key),

	CONSTRAINT		fk_fact_sport_event
	FOREIGN KEY		(sport_event_key)
	REFERENCES		gold.dim_sport_events (sport_event_key),

	CONSTRAINT		fk_fact_noc
	FOREIGN KEY		(noc_key)
	REFERENCES		gold.dim_nocs (noc_key)
);
