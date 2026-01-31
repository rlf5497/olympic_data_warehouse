/* =============================================================================
	Gold Layer - Fact Table Indexing
	Layer:
		- Gold
		
	Table:
		- gold.fact_olympic_results

	Purpose:
		- Create indexes on all FOREIGN KEY columns in the fact table
		- Optimize STAR SCHEMA joins between fact and dimension tables
		- Improve aggregation and analytical query performance

	STAR SCHEMA CONTEXT:
		- Fact table is large and frequently scanned
		- Dimension tables are smaller and joined repeatedly
		- Indexing fact foreign keys is a best practice for OLAP workloads

	Notes:
		- Dimension primary keys are indexed implicitly
		- Composite indexes are intentionally avoided to maintain flexibility
		- Index creation assumes bulk loads are completed beforehand
		- IF NOT EXISTS ensures idempotent execution

	Execution Layer:
		- Gold Layer (PostgreSQL)
============================================================================= */



-- ============================================================================
-- FACT TABLE FOREIGN KEY INDEXES
-- ============================================================================

-------------------------------------
-- Index: athlete_key
--
-- Supports:
--   - Joins to gold.dim_athletes
--   - Athlete participation and medal analysis
-------------------------------------
CREATE INDEX IF NOT EXISTS		idx_fact_olympic_results_athlete_key
						ON		gold.fact_olympic_results(athlete_key);



-------------------------------------
-- Index: game_key
--
-- Supports:
--   - Joins to gold.dim_games
--   - Time-series analysis by Olympic year and season
-------------------------------------
CREATE INDEX IF NOT EXISTS		idx_fact_olympic_results_game_key
						ON		gold.fact_olympic_results(game_key);



-------------------------------------
-- Index: sport_event_key
--
-- Supports:
--   - Joins to gold.dim_sport_events
--   - Analysis by sport, discipline, and team
--
-- Notes:
--	- Column is nullable by design
-------------------------------------
CREATE INDEX IF NOT EXISTS		idx_fact_olympic_results_sport_event_key
						ON		gold.fact_olympic_results(sport_event_key);



-------------------------------------
-- Index: noc_key
--
-- Supports:
--   - Joins to gold.dim_nocs
--   - Country and region-level aggregations
--
-- Notes:
--	- Column is nullable by design
-------------------------------------
CREATE INDEX IF NOT EXISTS		idx_fact_olympic_results_noc_key
						ON		gold.fact_olympic_results(noc_key);





