/*
==========================================================================================
Script: proc_load_silver.sql
Layer: Silver (Curated & Standardized Layer)
==========================================================================================
Purpose:
	Loads cleaned, standardized, and enriched data into the Silver Layer of the
	Olympics Data Warehouse. This procedure truncates and reloads all Silver tables
	using transformed data from the Bronze layer.
	
	This procedure applies:
		- Data cleansing (trimming, null handling)
		- Data standardization (case normalization, type casting)
		- Data enrichment (parsed dates, locations, derived attributes)
		- Structural transformations (wide-to-long reshaping)

Usage:
	CALL silver.load_silver();

Notes:
	- Silver layer applies data quality rules and standardization.
	- Invalid or non-conforming values are converted to NULL.
	- No aggregations are performed in Silver; these are handled in the Gold layer.
	- Custom parsing functions used:
		* silver.parse_date(text)
		* silver.parse_location(text)
==========================================================================================
*/


CALL silver.load_silver();


CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
	-- Start/end timestamps for each table load
	start_time          TIMESTAMP;
	end_time            TIMESTAMP;

	-- Number or rows loaded per table
	loaded_count        INTEGER;

	-- Tracks total execution duration
	batch_start_time    TIMESTAMP := clock_timestamp();
	
BEGIN
  	RAISE NOTICE '====================================================';
	RAISE NOTICE 'Loading Silver Layer';
	RAISE NOTICE '====================================================';
	


    -------------------------------------
    -- Load silver.olympics_bios
    -------------------------------------
	RAISE NOTICE 'Loading silver.olympics_bios...';

	start_time := clock_timestamp();
	TRUNCATE TABLE silver.olympics_bios;

	INSERT INTO silver.olympics_bios (
		sex,
		used_name,
		born_date,
		born_city,
		born_region,
		born_country_code,
		died_date,
		died_city,
		died_region,
		died_country_code,
		noc,
		athlete_id,
		height_cm,
		weight_kg
	)
	
	SELECT
		sex,
		INITCAP(TRIM(REPLACE(used_name, '•', ' ')))							AS used_name,
		silver.parse_date(born)												AS born_date,
		(silver.parse_location(born)).city									AS born_city,
		(silver.parse_location(born)).region								AS born_region,
		(silver.parse_location(born)).country_code							AS born_country_code,
		silver.parse_date(died)												AS died_date,
		(silver.parse_location(died)).city									AS died_city,
		(silver.parse_location(died)).region								AS died_region,
		(silver.parse_location(died)).country_code							AS died_country_code,
		INITCAP(noc)														AS noc,
		athlete_id,
		
		-- Extract height in centimeters by capturing the numeric value directly followed by 'cm'
		-- (ensures the correct value is selected when multiple numbers are present)
		CASE
			WHEN	measurements~*		'(\d{2,3})\s*cm'
			THEN	SUBSTRING			(measurements FROM '(\d{2,3})\s*cm')::NUMERIC
			ELSE	NULL
		END																	AS height_cm,

		-- Extract weight in kilograms by capturing the numeric value directly followed by 'kg'
		-- (handles multiple values by selecting the one explicitly labeled with 'kg')
		CASE
			WHEN	measurements~*		'(\d{2,3})\s*kg'
			THEN	SUBSTRING			(measurements FROM '(\d{2,3})\s*kg')::NUMERIC
			ELSE	NULL
		END																	AS weight_kg
	FROM bronze.olympics_bios;

	SELECT COUNT(*) INTO loaded_count FROM silver.olympics_bios;
	end_time := clock_timestamp();

	RAISE NOTICE ' -> Loaded % rows in % seconds',
		loaded_count, EXTRACT(epoch FROM end_time - start_time);



    -------------------------------------
    -- Load silver.olympics_bios_locs
    -------------------------------------
	RAISE NOTICE 'Loading silver.olympics_bios_locs...';

	start_time := clock_timestamp();
	TRUNCATE TABLE silver.olympics_bios_locs;

	INSERT INTO silver.olympics_bios_locs (
		athlete_id,
		name,
		born_date,
		born_city,
		born_region,
		noc,
		height_cm,
		weight_kg,
		died_date,
		lat,
		long
	)

	SELECT
		athlete_id,
		TRIM(name) 														AS name,
		born_date::DATE 												AS born_date,
		TRIM(born_city) 												AS born_city,
		born_region,
		noc,
		height_cm,
		weight_kg,
		died_date::DATE 												AS died_date,
		lat,
		long
	FROM bronze.olympics_bios_locs;

	SELECT COUNT(*) INTO loaded_count FROM silver.olympics_bios_locs;
	end_time := clock_timestamp();

	RAISE NOTICE ' -> Loaded % rows in % seconds',
		loaded_count, EXTRACT(epoch FROM end_time - start_time);



    -------------------------------------
    -- Load silver.olympics_noc_regions
    -------------------------------------
	RAISE NOTICE 'Loading silver.olympics_noc_regions...';

	start_time := clock_timestamp();
	TRUNCATE TABLE silver.olympics_noc_regions;

	INSERT INTO silver.olympics_noc_regions (
		noc,
		region,
		notes
	)
	
	SELECT
		noc,
		region,
		notes
	FROM bronze.olympics_noc_regions;

	SELECT COUNT(*) INTO loaded_count FROM silver.olympics_noc_regions;
	end_time := clock_timestamp();

	RAISE NOTICE ' -> Loaded % rows in % seconds',
		loaded_count, EXTRACT(epoch FROM end_time - start_time);



    -------------------------------------
    -- Load silver.olympics_populations
    -------------------------------------
	RAISE NOTICE 'Loading silver.olympics_populations...';

	start_time := clock_timestamp();
	TRUNCATE TABLE silver.olympics_populations;

	INSERT INTO silver.olympics_populations (
		country_name,
		country_code,
		year,
		population
	)

	SELECT
		op.country_name,
		op.country_code,
		t.year_col::INT 												AS year,
		t.population	
	FROM bronze.olympics_populations 									AS op
	CROSS JOIN LATERAL (
		VALUES
	        ('1960', "1960"), ('1961', "1961"), ('1962', "1962"),
	        ('1963', "1963"), ('1964', "1964"), ('1965', "1965"),
	        ('1966', "1966"), ('1967', "1967"), ('1968', "1968"),
	        ('1969', "1969"), ('1970', "1970"), ('1971', "1971"),
	        ('1972', "1972"), ('1973', "1973"), ('1974', "1974"),
	        ('1975', "1975"), ('1976', "1976"), ('1977', "1977"),
	        ('1978', "1978"), ('1979', "1979"), ('1980', "1980"),
	        ('1981', "1981"), ('1982', "1982"), ('1983', "1983"),
	        ('1984', "1984"), ('1985', "1985"), ('1986', "1986"),
	        ('1987', "1987"), ('1988', "1988"), ('1989', "1989"),
	        ('1990', "1990"), ('1991', "1991"), ('1992', "1992"),
	        ('1993', "1993"), ('1994', "1994"), ('1995', "1995"),
	        ('1996', "1996"), ('1997', "1997"), ('1998', "1998"),
	        ('1999', "1999"), ('2000', "2000"), ('2001', "2001"),
	        ('2002', "2002"), ('2003', "2003"), ('2004', "2004"),
	        ('2005', "2005"), ('2006', "2006"), ('2007', "2007"),
	        ('2008', "2008"), ('2009', "2009"), ('2010', "2010"),
	        ('2011', "2011"), ('2012', "2012"), ('2013', "2013"),
	        ('2014', "2014"), ('2015', "2015"), ('2016', "2016"),
	        ('2017', "2017"), ('2018', "2018"), ('2019', "2019"),
	        ('2020', "2020"), ('2021', "2021"), ('2022', "2022"),
	        ('2023', "2023")
	) AS t(year_col, population)
	WHERE population IS NOT NULL;

	SELECT COUNT(*) INTO loaded_count FROM silver.olympics_populations;
	end_time := clock_timestamp();

	RAISE NOTICE ' -> Loaded % rows in % seconds',
		loaded_count, EXTRACT(epoch FROM end_time - start_time);



    -------------------------------------
    -- Load silver.olympics_results
    -------------------------------------
	RAISE NOTICE 'Loading silver.olympics_results...';

	start_time := clock_timestamp();
	TRUNCATE TABLE silver.olympics_results;

	INSERT INTO silver.olympics_results (
		olympic_year,
		game_type,
		sport_event,
		team,
		pos,
		is_tied,
		medal,
		as_name,
		athlete_id,
		noc,
		discipline
	)

	SELECT 
		CASE
			WHEN	games~*		'^\d{4}'
			THEN	SUBSTRING(games FROM '^\d{4}')::INT
			ELSE	NULL
		END 															AS olympic_year,
	
		CASE
			WHEN	games~*		'^\d{4}(?:-\d{2})?\s+(.*?)\s+(Olympic Games|Olympics|Olympic|Games)$'
			THEN	SUBSTRING(games FROM '^\d{4}(?:-\d{2})?\s+(.*?)\s+(Olympic Games|Olympics|Olympic|Games)$')
			ELSE	NULL
		END 															AS game_type,
		sport_event,
		team,
		SUBSTRING(pos FROM '^\s*=?(\d+)(?:\.0)?\s*$')::INT				AS pos,
		CASE
			WHEN pos ~ '^\s*=\d+(\.0)?\s*$' THEN TRUE
			WHEN pos ~ '^\s*\d+(\.0)?\s*$'  THEN FALSE
			ELSE NULL
		END 															AS is_tied,
		medal,
		as_name,
		athlete_id,
		UPPER(noc) 														AS noc,
		discipline
	FROM bronze.olympics_results;

	SELECT COUNT(*) INTO loaded_count FROM silver.olympics_results;
	end_time := clock_timestamp();

	RAISE NOTICE ' -> Loaded % rows in % seconds',
		loaded_count, EXTRACT(epoch FROM end_time - start_time);



    -------------------------------------
    -- Summary
    -------------------------------------
    RAISE NOTICE '================================================================================';
	RAISE NOTICE 'Silver Layer Load Completed';
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

