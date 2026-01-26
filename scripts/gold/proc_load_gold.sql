/* =====================================================================
	Procedure: gold.proc_load_gold
	Layer: Gold
	Purpose:
		- Load business-ready dimension and fact tables
		- Applies Star Schema loading order (Dimensions -> Facts)
		- Transform and conform data sourced from the Silver Layer

	Sources:
		- silver.olympics_bios
		- silver.olympics_bios_locs
		- silver.olympics_noc_regions
		- silver.olympics_results

	Usage:
		CALL gold.load_gold();

	Notes:
		- Surrogate keys are regenerated on each run
		- Fact table defines the analytical scope (Olympic participation)
		- Dimensions may contain records that do not appear in the fact table
===================================================================== */


CALL gold.load_gold();


CREATE OR REPLACE PROCEDURE gold.load_gold()
LANGUAGE plpgsql
AS $$
DECLARE
	-- Start/end timestamps for each table load
	start_time			TIMESTAMP;
	end_time			TIMESTAMP;

	-- Number of rows loaded  per table
	loaded_count		INTEGER;

	-- Tracks total execution duration
	batch_start_time	TIMESTAMP := clock_timestamp();

BEGIN
	RAISE NOTICE '====================================================';
	RAISE NOTICE 'Loading gold.dim_athletes';
	RAISE NOTICE '====================================================';


	-------------------------------------
	-- DIMENSION: gold.dim_athletes
	--
	-- Grain:
	--   One row per athlete
	--
	-- Coverage Note:
	--   This dimension includes all athletes present in the bios source,
	--   regardless of Olympic participation.
	-------------------------------------
	RAISE NOTICE 'Loading dimension: gold.dim_athletes';

	start_time := clock_timestamp();
	TRUNCATE TABLE gold.dim_athletes CASCADE;

	INSERT INTO gold.dim_athletes (
		athlete_id,
		name,
		gender,
		birth_date,
		height_cm,
		weight_kg,
		born_city,
		born_region,
		born_country_code,
		latitude,
		longitude,
		noc,
		died_date,
		died_city,
		died_region,
		died_country_code	
	)
	
	SELECT
		ob.athlete_id				AS athlete_id,
		ob.used_name				AS name,
		ob.sex						AS gender,
		ob.born_date				AS birth_date,
		ob.height_cm				AS height_cm,
		ob.weight_kg				AS weight_kg,
		ob.born_city				AS born_city,
		ob.born_region				AS born_region,
		ob.born_country_code		AS born_country_code,
		obl.lat						AS latitude,
		obl.long					AS longitude,
		ob.noc						AS noc,
		ob.died_date				AS died_date,
		ob.died_city				AS died_city,
		ob.died_region				AS died_region,
		ob.died_country_code		AS died_country_code
	FROM silver.olympics_bios AS ob
	LEFT JOIN silver.olympics_bios_locs AS obl
		   ON ob.athlete_id = obl.athlete_id;

	SELECT COUNT(*) INTO loaded_count FROM gold.dim_athletes;
	end_time := clock_timestamp();

	RAISE NOTICE ' -> Loaded % rows in % seconds',
		loaded_count, EXTRACT(epoch FROM end_time - start_time);



	-------------------------------------
	-- DIMENSION: gold.dim_nocs
	--
	-- Integration Logic:
	--   - Combines NOCs from reference data and results
	--   - Ensures coverage for all NOCs appearing in facts
	-------------------------------------
	RAISE NOTICE 'Loading dimension: gold.dim_nocs';

	start_time := clock_timestamp();
	TRUNCATE TABLE gold.dim_nocs CASCADE;

	INSERT INTO gold.dim_nocs (
		noc,
		region,
		notes
	)
	WITH integrated_nocs AS (
		SELECT DISTINCT
			noc
		FROM silver.olympics_noc_regions
	
		UNION
		
		SELECT DISTINCT
			noc
		FROM silver.olympics_results
	)
	SELECT
		inoc.noc,
		onr.region,
		onr.notes
	FROM integrated_nocs AS inoc
	LEFT JOIN silver.olympics_noc_regions AS onr
		   ON inoc.noc = onr.noc;

	SELECT COUNT(*) INTO loaded_count FROM gold.dim_nocs;
	end_time := clock_timestamp();

	RAISE NOTICE ' -> Loaded % rows in % seconds',
		loaded_count, EXTRACT(epoch FROM end_time - start_time);

		

	-------------------------------------
	-- DIMENSION: gold.dim_games
	--
	-- Grain:
	--   One row per Olympic Games (year + season)
	-------------------------------------
	RAISE NOTICE 'Loading dimension: gold.dim_games';

	start_time := clock_timestamp();
	TRUNCATE TABLE gold.dim_games CASCADE;
	
	INSERT INTO gold.dim_games (
		olympic_year,
		season
	)
	SELECT DISTINCT
		olympic_year,
		game_type
	FROM silver.olympics_results
	ORDER BY
		olympic_year ASC;

	SELECT COUNT(*) INTO loaded_count FROM gold.dim_games;
	end_time := clock_timestamp();

	RAISE NOTICE ' -> Loaded % rows in % seconds',
		loaded_count, EXTRACT(epoch FROM end_time - start_time);

		

	-------------------------------------
	-- DIMENSION: gold.dim_sport_events
	--
	-- Grain:
	--   One row per unique sport event
	-------------------------------------	
	RAISE NOTICE 'Loading dimension: gold.dim_sport_events';

	start_time := clock_timestamp();
	TRUNCATE TABLE gold.dim_sport_events CASCADE;
	
	INSERT INTO gold.dim_sport_events (
		discipline,
		sport_event,
		team
	)
	SELECT DISTINCT
		discipline,
		sport_event,
		team
	FROM silver.olympics_results
	ORDER BY
		discipline ASC,
		sport_event ASC,
		team ASC;

	SELECT COUNT(*) INTO loaded_count FROM gold.dim_sport_events;
	end_time := clock_timestamp();

	RAISE NOTICE ' -> Loaded % rows in % seconds',
		loaded_count, EXTRACT(epoch FROM end_time - start_time);

		
	
	-------------------------------------
	-- FACT: gold.fact_olympic_results
	--
	-- Grain:
	--   One row per athlete per Olympic Games per participation record
	--
	-- Notes:
	--   - LEFT JOINs are used to prevent loss of participation records
	--   - Fact rows may reference NULL dimension keys if lookup data is missing
	--   - Analytics based on this table reflect participation-driven scope
	-------------------------------------
	RAISE NOTICE 'Loading fact table: gold.fact_olympic_results';

	start_time := clock_timestamp();

	INSERT INTO gold.fact_olympic_results (
		athlete_key,
		game_key,
		sport_event_key,
		noc_key,
		place,
		medal,
		is_tied	
	)
	SELECT
		da.athlete_key,
		dg.game_key,
		dse.sport_event_key,
		dn.noc_key,
	
		-- Fact attributes
		sor.pos,
		sor.medal,
		sor.is_tied
	FROM silver.olympics_results AS sor
	LEFT JOIN gold.dim_athletes AS da
		   ON sor.athlete_id = da.athlete_id
	LEFT JOIN gold.dim_games AS dg
		   ON sor.olympic_year = dg.olympic_year
		  AND sor.game_type = dg.season
	LEFT JOIN gold.dim_sport_events AS dse
		   ON sor.discipline = dse.discipline
		  AND sor.sport_event = dse.sport_event
		  AND sor.team = dse.team
	LEFT JOIN gold.dim_nocs AS dn
		   ON sor.noc = dn.noc;

	SELECT COUNT(*) INTO loaded_count FROM gold.fact_olympic_results;
	end_time := clock_timestamp();

	RAISE NOTICE ' -> Loaded % rows in % seconds',
		loaded_count, EXTRACT(epoch FROM end_time - start_time);

		

    -------------------------------------
    -- Summary
    -------------------------------------
    RAISE NOTICE '================================================================================';
	RAISE NOTICE 'Gold layer load completed successfully';
	RAISE NOTICE 'Total Duration: % seconds',
		EXTRACT(epoch FROM clock_timestamp() - batch_start_time);
    RAISE NOTICE '================================================================================';


EXCEPTION
	WHEN OTHERS THEN
		RAISE NOTICE '========================================';
		RAISE NOTICE 'ERROR OCCURED: %', SQLERRM;
		RAISE;
END;
$$;
