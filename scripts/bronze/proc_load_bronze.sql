/*
====================================================================================================
Procedure: bronze.load_bronze
Layer: Bronze

Purpose:
	Load raw CSV files into the Bronze layer of the Olympic Data Warehouse.
	This procedure truncates and reloads all Bronze tables to ensure a clean,
	consistent raw ingestion layer.

Execution Guarantees:
	- All tables are fully replaced on each run
	- No data transformation or validation is applied
	- Failures abort execution to prevent partial ingestion

Usage Example:
	CALL bronze.load_bronze('C:\sql\olympic-data-warehouse\datasets\source_olympics');

Notes:
	- Bronze layer contains RAW, unmodified source data
	- Uses PostgreSQL COPY for high-performance ingestion
	- CSV structure must match table definitions exactly

Parameters:
	base_path (TEXT) - Absolute folder path containing source CSV files:
		- bios.csv
		- bios_locs.csv
		- noc_regions.csv
		- populations.csv
		- results.csv
====================================================================================================
*/


CREATE OR REPLACE PROCEDURE bronze.load_bronze(base_path TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
	-- Start/end timestamps for each ingestion
	start_time			  	TIMESTAMP;
	end_time			  	TIMESTAMP;

	-- Number of rows loaded per table
	loaded_count		  	INTEGER;

	-- Tracks total execution duration
	batch_start_time		TIMESTAMP := clock_timestamp();

	-- File Paths
	bios				    TEXT := base_path || '\bios.csv';
	bios_locs			    TEXT := base_path || '\bios_locs.csv';
	noc_regions			  	TEXT := base_path || '\noc_regions.csv';
	populations			  	TEXT := base_path || '\populations.csv';
	results				    TEXT := base_path || '\results.csv';

BEGIN
  	RAISE NOTICE '================================================================================';
	RAISE NOTICE 'Loading Bronze Layer';
	RAISE NOTICE 'Base Path: %', base_path;
  	RAISE NOTICE '================================================================================';

-------------------------------------
-- Load olympics_bios
-------------------------------------
  	RAISE NOTICE 'Loading bronze.olympics_bios...';

	start_time := clock_timestamp();
	TRUNCATE TABLE bronze.olympics_bios;
	
	EXECUTE format ($f$
		COPY bronze.olympics_bios
		FROM %L
		WITH (FORMAT CSV, HEADER, DELIMITER ',');
	$f$, bios);
	
	SELECT COUNT(*) INTO loaded_count FROM bronze.olympics_bios;
	end_time := clock_timestamp();
	
	RAISE NOTICE ' -> Loaded % rows in % seconds',
		loaded_count, EXTRACT (epoch FROM end_time - start_time);


-------------------------------------
-- Load olympics_bios_locs
-------------------------------------
  	RAISE NOTICE 'Loading bronze.olympics_bios_locs...';

	start_time := clock_timestamp();
	TRUNCATE TABLE bronze.olympics_bios_locs;
	
	EXECUTE format ($f$
		COPY bronze.olympics_bios_locs
		FROM %L
		WITH (FORMAT CSV, HEADER, DELIMITER ',');
	$f$, bios_locs);
	
	SELECT COUNT(*) INTO loaded_count FROM bronze.olympics_bios_locs;
	end_time := clock_timestamp();
	
	RAISE NOTICE ' -> Loaded % rows in % seconds',
		loaded_count, EXTRACT (epoch FROM end_time - start_time);

	
-------------------------------------
-- Load olympics_noc_regions
-------------------------------------
  	RAISE NOTICE 'Loading bronze.olympics_noc_regions...';
	
	start_time := clock_timestamp();
	TRUNCATE TABLE bronze.olympics_noc_regions;
	
	EXECUTE format ($f$
		COPY bronze.olympics_noc_regions
		FROM %L
		WITH (FORMAT CSV, HEADER, DELIMITER ',');
	$f$, noc_regions);
	
	SELECT COUNT(*) INTO loaded_count FROM bronze.olympics_noc_regions;
	end_time := clock_timestamp();
	
	RAISE NOTICE ' -> Loaded % rows in % seconds',
		loaded_count, EXTRACT (epoch FROM end_time - start_time);


-------------------------------------
-- Load olympics_populations
-------------------------------------
  	RAISE NOTICE 'Loading bronze.olympics_populations...';

	start_time := clock_timestamp();
	TRUNCATE TABLE bronze.olympics_populations;
	
	EXECUTE format ($f$
		COPY bronze.olympics_populations
		FROM %L
		WITH (FORMAT CSV, HEADER, DELIMITER ',');
	$f$, populations);
	
	SELECT COUNT(*) INTO loaded_count FROM bronze.olympics_populations;
	end_time := clock_timestamp();
	
	RAISE NOTICE ' -> Loaded % rows in % seconds',
		loaded_count, EXTRACT (epoch FROM end_time - start_time);

	
-------------------------------------
-- Load olympics_results
------------------------------------
  	RAISE NOTICE 'Loading bronze.olympics_results...';

	start_time := clock_timestamp();
	TRUNCATE TABLE bronze.olympics_results;
	
	EXECUTE format ($f$
		COPY bronze.olympics_results
		FROM %L
		WITH (FORMAT CSV, HEADER, DELIMITER ',');
	$f$, results);
	
	SELECT COUNT(*) INTO loaded_count FROM bronze.olympics_results;
	end_time := clock_timestamp();
	
	RAISE NOTICE ' -> Loaded % rows in % seconds',
		loaded_count, EXTRACT (epoch FROM end_time - start_time);


-------------------------------------
-- Summary
-------------------------------------

	RAISE NOTICE '================================================================================';
	RAISE NOTICE ' Bronze Layer Load Completed';
	RAISE NOTICE ' Total Duration: % seconds',
		EXTRACT (epoch FROM clock_timestamp() - batch_start_time);
	RAISE NOTICE '================================================================================';


EXCEPTION
	WHEN OTHERS THEN
    RAISE NOTICE '========================================';
	RAISE NOTICE 'ERROR OCCURED: %', SQLERRM;
	RAISE;
END;
$$
	


	
