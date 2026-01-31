/* =============================================================================
	File:		export_gold_bi_views.sql
	Layer:		Gold (BI / Semantic Layer)
	Schema:		gold_bi
	Object:		Procedure gold_bi.export_gold_bi_views

	Purpose:
		Exports business-facing BI views from the Gold BI layer into CSV files
		for downstream visualization and analytics tools (e.g., Tableau).

	Design Notes:
		- One CSV file per BI view
		- Read-only export (no transformation applied)
		- CSV files include headers for direct Tableau ingestion
		- Procedure is idempotent and safe to re-run

	Expected Usage:
		CALL gold_bi.export_gold_bi_views('C:\sql\olympic-data-warehouse\exports')

	Output Structure:
		<base_path>\
			- vw_olympics_analysis_base.csv
			- vw_athletes_by_noc.csv
			- vw_athlete_trend_over_time.csv
			- vw_kpi_overview.csv
			- vw_medal_efficiency_by_noc.csv
			- vw_medals_by_games.csv
			- vw_medals_by_noc.csv
			- vw_most_decorated_athletes.csv
			- vw_sport_participation.csv
			- vw_top_athletes_by_games.csv
============================================================================= */


/* ---------------------------------------------------------------------
   Example Invocation
--------------------------------------------------------------------- */
CALL gold_bi.export_gold_bi_views('C:\sql\olympic-data-warehouse\exports'); 



/* =====================================================================
   Procedure: export_gold_bi_views
   Purpose:
       Export curated Gold BI semantic views into CSV files for
       dashboarding and ad-hoc analysis.

   Notes:
       - Uses PostgreSQL COPY for efficient data extraction
       - File paths are constructed dynamically
===================================================================== */
CREATE OR REPLACE PROCEDURE gold_bi.export_gold_bi_views (
	base_path	TEXT -- Base directory where CSV files will be written
)
LANGUAGE plpgsql
AS $$
DECLARE
	batch_start_time	TIMESTAMP := clock_timestamp();
	view_name			TEXT;
	file_path			TEXT;
BEGIN
    RAISE NOTICE '=============================================================================';
	RAISE NOTICE 'Starting Gold BI export';
	RAISE NOTICE 'Target directory: %', base_path; 
    RAISE NOTICE '=============================================================================';



    /* =====================================================================
       OLYMPICS ANALYSIS BASE
       Grain:
           Detail-level (no aggregation)
    ===================================================================== */
	view_name := 'vw_olympics_analysis_base';
	file_path := base_path || '\vw_olympics_analysis_base.csv';

	EXECUTE format(
		'COPY (SELECT * FROM gold_bi.%I)
		 TO %L
		 WITH (FORMAT CSV, HEADER)', view_name, file_path
	);


	
    /* =====================================================================
       ATHLETE PARTICIPATION TREND OVER TIME
       Grain:
           One row per Olympic year and season
    ===================================================================== */
	view_name := 'vw_athlete_trend_over_time';
	file_path := base_path || '\vw_athlete_trend_over_time.csv';

	EXECUTE format(
		'COPY (SELECT * FROM gold_bi.%I)
		 TO %L
		 WITH (FORMAT CSV, HEADER)', view_name, file_path
	);



    /* =====================================================================
       ATHLETES BY NOC
       Grain:
           One row per NOC
    ===================================================================== */
	view_name := 'vw_athletes_by_noc';
	file_path := base_path || '\vw_athletes_by_noc.csv';

	EXECUTE format(
		'COPY (SELECT * FROM gold_bi.%I)
		 TO %L
		 WITH (FORMAT CSV, HEADER)', view_name, file_path
	);


	
    /* =====================================================================
       KPI OVERVIEW
       Grain:
           Single row (global KPIs)
    ===================================================================== */
	view_name := 'vw_kpi_overview';
	file_path := base_path || '\vw_kpi_overview.csv';

	EXECUTE format(
		'COPY (SELECT * FROM gold_bi.%I)
		 TO %L
		 WITH (FORMAT CSV, HEADER)', view_name, file_path
	);



    /* =====================================================================
       MEDAL EFFICIENCY BY NOC
       Grain:
           One row per NOC (filtered for statistical significance)
    ===================================================================== */
	view_name := 'vw_medal_efficiency_by_noc';
	file_path := base_path || '\vw_medal_efficiency_by_noc.csv';

	EXECUTE format(
		'COPY (SELECT * FROM gold_bi.%I)
		 TO %L
		 WITH (FORMAT CSV, HEADER)', view_name, file_path
	);



    /* =====================================================================
       MEDALS BY OLYMPIC GAMES
       Grain:
           One row per Olympic year and season
    ===================================================================== */
	view_name := 'vw_medals_by_games';
	file_path := base_path || '\vw_medals_by_games.csv';

	EXECUTE format(
		'COPY (SELECT * FROM gold_bi.%I)
		 TO %L
		 WITH (FORMAT CSV, HEADER)', view_name, file_path
	);



    /* =====================================================================
       MEDAL DISTRIBUTION BY NOC
       Grain:
           One row per NOC
    ===================================================================== */
	view_name := 'vw_medals_by_noc';
	file_path := base_path || '\vw_medals_by_noc.csv';

	EXECUTE format(
		'COPY (SELECT * FROM gold_bi.%I)
		 TO %L
		 WITH (FORMAT CSV, HEADER)', view_name, file_path
	);



    /* =====================================================================
       MOST DECORATED ATHLETES
       Grain:
           One row per athlete
    ===================================================================== */
	view_name := 'vw_most_decorated_athletes';
	file_path := base_path || '\vw_most_decorated_athletes.csv';

	EXECUTE format(
		'COPY (SELECT * FROM gold_bi.%I)
		 TO %L
		 WITH (FORMAT CSV, HEADER)', view_name, file_path
	);



    /* =====================================================================
       SPORTS WITH HIGHEST ATHLETE PARTICIPATION
       Grain:
           One row per sport discipline
    ===================================================================== */
	view_name := 'vw_sport_participation';
	file_path := base_path || '\vw_sport_participation.csv';

	EXECUTE format(
		'COPY (SELECT * FROM gold_bi.%I)
		 TO %L
		 WITH (FORMAT CSV, HEADER)', view_name, file_path
	);



    /* =====================================================================
       TOP ATHLETES BY OLYMPIC GAMES PARTICIPATED
       Grain:
           One row per athlete
    ===================================================================== */
	view_name := 'vw_top_athletes_by_games';
	file_path := base_path || '\vw_top_athletes_by_games.csv';

	EXECUTE format(
		'COPY (SELECT * FROM gold_bi.%I)
		 TO %L
		 WITH (FORMAT CSV, HEADER)', view_name, file_path
	);


    RAISE NOTICE '=============================================================================';
	RAISE NOTICE 'Gold BI export completed successfully';
	RAISE NOTICE 'Total duration: % seconds',
		EXTRACT(epoch FROM clock_timestamp() - batch_start_time);
	RAISE NOTICE '=============================================================================';


EXCEPTION
	WHEN OTHERS THEN
		RAISE NOTICE '========================================';
		RAISE NOTICE 'ERROR OCCURED: %', SQLERRM;
		RAISE;
END;
$$;
