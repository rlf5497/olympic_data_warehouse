/* =============================================================================
	Layer: Gold (BI / Semantic Layer)

	Purpose:
		Business-facing semantic views designed for BI tools (e.g., Tableau).
		These views answer predefined analytical questions using curated
		Gold-layer fact and dimension tables.

	Design Principles:
		- No row-level transformations (aggregation only)
		- Grain is explicitly defined per view
		- Views are reusable, stable, and BI-friendly
		- Business questions are documented to avoid ambiguity.

	Source Tables:
		- gold.fact_olympic_results
		- gold.dim_athletes
		- gold.dim_games
		- gold.dim_sport_events
		- gold.dim_nocs
============================================================================= */



-- =====================================================================
-- ATHLETE PARTICIPATION TREND OVER TIME
-- Business Question:
-- "How has athlete participation changed across Olympic Games?"
-- Grain:
--	One row per Olympic year + season
-- =====================================================================
CREATE OR REPLACE VIEW gold_bi.vw_athlete_trend_over_time AS
SELECT
	dg.olympic_year,
	dg.season,
	COUNT(DISTINCT f.athlete_key)			AS athlete_count
FROM gold.fact_olympic_results AS f
INNER JOIN gold.dim_games AS dg
		ON f.game_key = dg.game_key
GROUP BY
	dg.olympic_year,
	dg.season
ORDER BY
	dg.olympic_year ASC,
	dg.season ASC;



-- =====================================================================
-- ATHLETE COUNT BY NOC
-- Business Question:
-- "How many unique athletes represented each NOC?"
-- Grain:
--	One row per NOC
-- =====================================================================
CREATE OR REPLACE VIEW gold_bi.vw_athletes_by_noc AS
SELECT
	dn.noc,
	dn.region,
	COUNT(DISTINCT f.athlete_key) AS total_athletes
FROM gold.fact_olympic_results AS f
INNER JOIN gold.dim_nocs AS dn
	    ON f.noc_key = dn.noc_key
GROUP BY
	dn.noc,
	dn.region
ORDER BY
	total_athletes DESC;



-- =====================================================================
-- KPI OVERVIEW
-- Business Question:
-- "What is the overall scale of the Olympic dataset?"
-- Grain:
-- 	Single row (global KPIs)
-- =====================================================================
CREATE OR REPLACE VIEW gold_bi.vw_kpi_overview AS
SELECT
	(SELECT COUNT(*) FROM gold.dim_athletes)					AS total_athletes,
	(SELECT COUNT(*) FROM gold.dim_games)						AS total_olympic_games,

	-- Medal counts derived from fact table
	COUNT(*) FILTER (WHERE medal IS NOT NULL)					AS total_medals,
	COUNT(*) FILTER (WHERE medal = 'Gold')						AS gold_medals,
	COUNT(*) FILTER (WHERE medal = 'Silver')					AS silver_medals,
	COUNT(*) FILTER (WHERE medal = 'Bronze')					AS bronze_medals
FROM gold.fact_olympic_results;



-- =====================================================================
-- MEDAL EFFICIENCY BY NOC
-- Business Question:
-- "Which countries are most efficient at converting athletes into medals?"
-- Notes:
--	- Filters out small samples to avoid statistical distortion
-- Grain:
--	One row per NOC
-- =====================================================================
CREATE OR REPLACE VIEW gold_bi.vw_medal_efficiency_by_noc AS 
WITH totals AS (
	SELECT
		dn.noc,
		dn.region,
		COUNT(*) FILTER (WHERE f.medal IS NOT NULL)					AS total_medals,
		COUNT(DISTINCT f.athlete_key)								AS total_athletes
	FROM gold.fact_olympic_results AS f
	INNER JOIN gold.dim_nocs AS dn
			ON f.noc_key = dn.noc_key
	GROUP BY
		dn.noc,
		dn.region	
)

SELECT
	noc,
	region,
	total_medals,
	total_athletes,
	-- Defensive division to prevent divide-by-zero
	ROUND(total_medals::NUMERIC / NULLIF(total_athletes, 0), 4)		AS medals_per_athlete 
FROM totals
WHERE
	total_athletes >= 50 -- avoid small-sample bias
ORDER BY
	medals_per_athlete DESC;



-- =====================================================================
-- MEDALS BY OLYMPIC GAMES
-- Business Question:
-- "How many medals were awarded per Olympic Games?"
-- Grain:
--	One row per Olympic year + season
-- =====================================================================
CREATE OR REPLACE VIEW gold_bi.vw_medals_by_games AS
SELECT
	dg.olympic_year,
	dg.season,
	COUNT(*) FILTER (WHERE f.medal IS NOT NULL)					AS total_medals,
	COUNT(*) FILTER (WHERE f.medal = 'Gold')					AS gold_medals,
	COUNT(*) FILTER	(WHERE f.medal = 'Silver')					AS silver_medals,
	COUNT(*) FILTER (WHERE f.medal = 'Bronze')					AS bronze_medals
FROM gold.fact_olympic_results AS f
INNER JOIN gold.dim_games AS dg
		ON f.game_key = dg.game_key
GROUP BY
	dg.olympic_year,
	dg.season
ORDER BY
	dg.olympic_year ASC;



-- =====================================================================
-- MEDAL DISTRIBUTION BY NOC
-- Business Question:
-- "How are medals distributed across NOC?"
-- Grain:
--	One row per NOC
-- =====================================================================
CREATE OR REPLACE VIEW gold_bi.vw_medals_by_noc AS 
SELECT
	dn.noc,
	dn.region,
	COUNT(*) FILTER (WHERE f.medal IS NOT NULL)					AS total_medals,
	COUNT(*) FILTER (WHERE f.medal = 'Gold')					AS gold_medals,
	COUNT(*) FILTER (WHERE f.medal = 'Silver')					AS silver_medals,
	COUNT(*) FILTER (WHERE f.medal = 'Bronze')					AS bronze_medals
FROM gold.fact_olympic_results AS f
INNER JOIN gold.dim_nocs AS dn
		ON f.noc_key = dn.noc_key
GROUP BY
	dn.noc,
	dn.region
ORDER BY
	total_medals DESC;



-- =====================================================================
-- MOST DECORATED ATHLETES
-- Business Question:
-- "Who are the most decorated Olympic athletes of all time?"
-- Note:
-- 	Includes all medal types.
-- 	Does NOT count participation without medals
-- Grain:
--	One row per athlete
-- =====================================================================
CREATE OR REPLACE VIEW gold_bi.vw_most_decorated_athletes AS
SELECT
	da.name,
	COUNT(*) FILTER (WHERE f.medal IS NOT NULL)				AS total_medals,
	COUNT(*) FILTER (WHERE f.medal = 'Gold')				AS gold_medals,
	COUNT(*) FILTER (WHERE f.medal = 'Silver')				AS silver_medals,
	COUNT(*) FILTER (WHERE f.medal = 'Bronze')				AS bronze_medals
FROM gold.fact_olympic_results AS f
INNER JOIN gold.dim_athletes AS da
		ON f.athlete_key = da.athlete_key
GROUP BY
	f.athlete_key,
	da.name
ORDER BY
	total_medals DESC;



-- =====================================================================
-- SPORTS WITH HIGHEST ATHLETE PARTICIPATION
-- Business Question:
-- "Which sports attracted the most unique athletes?"
-- Grain:
--	One row per sport discipline
-- =====================================================================
CREATE OR REPLACE VIEW gold_bi.vw_sport_participation AS
SELECT
	dse.discipline,
	COUNT(DISTINCT f.athlete_key) AS athlete_count
FROM gold.fact_olympic_results AS f
INNER JOIN gold.dim_sport_events AS dse
	    ON f.sport_event_key = dse.sport_event_key
GROUP BY
	dse.discipline
ORDER BY
	athlete_count DESC;



-- =====================================================================
-- TOP ATHLETES BY OLYMPIC GAMES PARTICIPATED
-- Business Question:
-- "Which athletes participated in the most Olympic Games?"
-- Grain:
-- 	One row per athlete
-- =====================================================================
CREATE OR REPLACE VIEW gold_bi.vw_top_athletes_by_games AS
SELECT
	da.name,
	COUNT(DISTINCT dg.game_key) AS olympic_games_participated
FROM gold.fact_olympic_results AS f
INNER JOIN gold.dim_athletes AS da
	    ON f.athlete_key = da.athlete_key
INNER JOIN gold.dim_games AS dg
	    ON f.game_key = dg.game_key
GROUP BY
	f.athlete_key,
	da.name
ORDER BY
	olympic_games_participated DESC;
