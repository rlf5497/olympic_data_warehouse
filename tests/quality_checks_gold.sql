/* =============================================================================
	Gold Layer Data Quality & Star Schema Validation

	Purpose:
		The script validates the integrity, correctness, and analytical readiness
		of the Gold Layer Star Schema for the Olympic Data Warehouse.

		The Gold Layer follows a Star Schema design:
			- Fact Table: gold.fact_olympic_results
			- Dimensions:
				* gold.dim_athletes
				* gold.dim_games
				* gold.dim_sport_events
				* gold.dim_nocs

	Grain Definition:
		Fact Table grain is defined as:
			"One row per athlete per Olympic Games per participation record"

	This means:
		- An athlete may appear multiple times per Games (multiple events)
		- sport_event_key and noc_key are OPTIONAL (nullable)
		- athlete_key and game_key are MANDATORY

	Testing Strategy:
		The scripts validates:
			1. Surrogate key uniqueness
			2. Natural key completeness
			3. Referential integrity between fact and dimensions
			4. Row count reconciliation (Silver -> Gold)
			5. Domain validity for analytical fields (medals, placements)

	Expected Results:
		- Tests marked "EXPECT ZERO ROWS" must return 0
		- Tests marked "EXPECT NULLS" confirm nullable relationship by design
============================================================================= */



-- ============================================================================
-- DIM_ATHLETES VALIDATION
-- ============================================================================

-- 1. Check for duplicate athlete records
--	  Expectation:
--		- athlete_key is unique
--		- athlete_id (natural key) must not be NULL
-- 	  Expected Result: ZERO ROWS
SELECT
	athlete_key,
	athlete_id,
	COUNT(*) AS duplicate_count
FROM gold.dim_athletes
GROUP BY
	athlete_key,
	athlete_id
HAVING
	COUNT(*) > 1
OR	athlete_id IS NULL;


-- 2. Referential integrity check: FACT -> DIM_ATHLETES
--	  Expect fact row MUST have a valid athlete_key
--	  Expected Result: 0
SELECT 
	COUNT(*) AS orphan_athlete_keys
FROM gold.fact_olympic_results AS fr
LEFT JOIN gold.dim_athletes AS da
	   ON fr.athlete_key = da.athlete_key
WHERE
	da.athlete_key IS NULL;


-- 3. Sample data inspection (sanity check)
SELECT *
FROM gold.dim_athletes
LIMIT 20;



-- ============================================================================
-- DIM_GAMES VALIDATION
-- ============================================================================

-- 4. Check for duplicate or NULL game keys
--	  Expected Result: ZERO ROWS
SELECT
	game_key,
	COUNT(*) AS duplicate_count
FROM gold.dim_games
GROUP BY
	game_key
HAVING
	COUNT(*) > 1
OR	game_key IS NULL;



-- 5. Referential integrity check: FACT -> DIM_GAMES
--	  game_key is mandatory in the fact table
--	  Expected Results: 0
--
--	  Note:
--		- Natural keys are enforced during load.
--		- This check validates surrogate key integrity only
SELECT 
	COUNT(*) AS orphan_game_keys
FROM gold.fact_olympic_results AS r
LEFT JOIN gold.dim_games AS dg
	   ON r.game_key = dg.game_key
WHERE
	dg.game_key IS NULL;



-- ============================================================================
-- DIM_SPORT_EVENTS VALIDATION
-- ============================================================================

-- 6. Check for duplicate sport_event_keys
-- 	  sport_event_key must be unique and NOT NULL in the dimension
--	  Optional relationship applies ONLY in the fact table
-- 	  Expected result: ZERO ROWS
SELECT
	sport_event_key,
	COUNT(*) AS duplicate_count
FROM gold.dim_sport_events
GROUP BY
	sport_event_key
HAVING
	COUNT(*) > 1
OR	sport_event_key IS NULL;


-- 7. Referential integrity check: FACT -> DIM_SPORT_EVENTS
--	  NULL sport_event_key values are VALID (by design)
--	  This confirms optional relationship behavior
-- 	  Expected Results: NON-ZERO COUNT (NULLS EXPECTED)
--
--	  Note:
--		- Natural keys are enforced during load.
--		- This check validates surrogate key integrity only
SELECT 
	COUNT(*) AS null_or_unmatched_sport_event_keys
FROM gold.fact_olympic_results AS r
LEFT JOIN gold.dim_sport_events AS dse
	   ON r.sport_event_key = dse.sport_event_key
WHERE
	dse.sport_event_key IS NULL;



-- ============================================================================
-- DIM_NOCS VALIDATION
-- ============================================================================

-- 8. Validate nullable NOC relationship
--	  Some participation records do not have NOC information
--	  Expected Result: NULLS PRESENT
SELECT *
FROM gold.fact_olympic_results
WHERE
	noc_key IS NULL;


-- 9. Referential integrity check: FACT -> DIM_NOCS
--	  NULL noc_key is VALID by design
--	  Expected Result: NON-ZERO COUNT
--
--	  Note:
--		- Natural keys are enforced during load.
--		- This check validates surrogate key integrity only
SELECT
	COUNT(*) AS missing_noc_key
FROM gold.fact_olympic_results AS r
LEFT JOIN gold.dim_nocs AS dn
	   ON r.noc_key = dn.noc_key
WHERE
	dn.noc_key IS NULL;



-- ============================================================================
-- FACT TABLE VALIDATION
-- ============================================================================

-- 10. Row count reconciliation: Silver -> Gold
--	   Confirms no unintended row loss during transformation
SELECT 	'silver_row_counts' 		AS table_name, COUNT(*) AS record_count FROM silver.olympics_results
UNION ALL
SELECT 	'gold_row_counts',			COUNT(*) FROM gold.fact_olympic_results;


-- 11. Medal domain validation
--	   Expected values:
--			- Gold
--			- Silver
--			- Bronze
--			- NULL (no medal)
SELECT DISTINCT
	medal
FROM gold.fact_olympic_results;


-- 12. Placement Validation
--	   Placement values must be positive integers
-- 	   Expected Result: ZERO ROWS
--
--	   Note:
--		- NULL placements are allowed for non-ranked participation
SELECT *
FROM gold.fact_olympic_results
WHERE
	place <= 0;



-- ============================================================================
-- FINAL ROW COUNT SNAPSHOT (SCHEMA HEALTH CHECK)
-- ============================================================================
SELECT	'dim_athletes' 				AS table_name, COUNT(*) AS record_count FROM gold.dim_athletes
UNION ALL
SELECT 	'dim_games',				COUNT(*) FROM gold.dim_games
UNION ALL 
SELECT 'dim_sport_events', 			COUNT(*) FROM gold.dim_sport_events
UNION ALL
SELECT 'dim_nocs', 					COUNT(*) FROM gold.dim_nocs
UNION ALL
SELECT 'fact_olympic_results',		COUNT(*) FROM gold.fact_olympic_results;




