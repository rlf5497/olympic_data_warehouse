/*============================================================================
Function	: silver.parse_location
Layer		: Silver (Curated & Standardized Layer)
Purpose		: Parse and normalize location components from raw text fields.

Description	:
	Extracts structured geographic attributes (city, region, country_code)
	from semi-structured location text originating from the Bronze layer.

Supported Raw Patterns:
	- "in City, Region (NOC)"
	- "in ?, Region (NOC)"
	- "in City, Region (NOC) (circa YYYY)"
	- "(circa YYYY)"		-> returns all NULLs

Data Quality Rules:
	- Placeholder value such as '?' are normalized to NULL
	- Leading/trailing whitespace is removed
	- If no valid location pattern exists, all attributes return NULL

Notes:
	- This function performs parsing and normalization only
	- Date parsing (including circa handling) is handled separately
	- Designed for deterministic use in Silver-layer transformations

Returns	:
	city			TEXT
	region			TEXT
	country_code 	TEXT
============================================================================*/

CREATE OR REPLACE FUNCTION silver.parse_location(text)
RETURNS TABLE (
	city			TEXT,
	region			TEXT,
	country_code	TEXT
)
LANGUAGE sql
IMMUTABLE
AS $$
	SELECT
		/*------------------------------------------------------------
			City:
				Extracts value following 'in' and preceding the first comma.
				Normalizes '?' placeholder values to NULL.
		------------------------------------------------------------*/
		NULLIF(
			TRIM(SUBSTRING($1 FROM 'in\s+([^,]+),')),
			'?'
		) AS city,

		/*------------------------------------------------------------
			Region:
				Extracts value between comma and the country code.
				Normalizes '?' placeholder values to NULL.
		------------------------------------------------------------*/
		NULLIF(
			TRIM(SUBSTRING($1 FROM ',\s+([^,]+)\s+\(\w{3}\)')),
			'?'
		) AS region,

		/*------------------------------------------------------------
			Country Code:
				Extracts 3-letter code enclosed in parentheses.
				Returns NULL if no valid pattern is present.
		------------------------------------------------------------*/
		SUBSTRING($1 FROM '\((\w{3})\)') AS country_code;
$$;

ALTER FUNCTION silver.parse_location(text)
OWNER TO postgres;





/* ============================================================================
   Function : silver.parse_date
   Layer    : Silver (Curated & Standardized Layer)
   Purpose  : Extract and normalize partial and semi-structured date strings
              into a PostgreSQL DATE value.

   Supported inputs:
     - "1 April 1871"                          → 1871-04-01
     - "April 1871"                            → 1871-04-01
     - "1871"                                  → 1871-01-01
     - "(circa 1923)", "(c. 1915)"             → 1923-01-01
     - "circa 1923", "c. 1923"                 → 1923-01-01
     - "in Tokyo (JPN) circa 1914"             → 1914-01-01

   Data Quality Rules:
     - Ambiguous year ranges (e.g. "1926 or 1927") → NULL
     - Missing month and/or day values are imputed as January 1
     - Invalid or non-date strings return NULL

   Notes:
     - Date text is extracted FIRST, then converted
     - Regex evaluation order is critical to avoid invalid TO_DATE calls
	 - Designed for deterministic use in Silver-layer transformations
   ============================================================================ */

CREATE OR REPLACE FUNCTION silver.parse_date(text)
RETURNS DATE
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT
        CASE
            /* ------------------------------------------------------------
               Reject ambiguous year ranges
               Example: "(1926 or 1927)"
            ------------------------------------------------------------ */
            WHEN	$1~* 	'\d{4}\s+or\s+\d{4}' 
			THEN	NULL

            /* ------------------------------------------------------------
               Full date: DD Month YYYY
               Example: "1 April 1871"
            ------------------------------------------------------------ */
            WHEN	$1~* 	'^\d{1,2}\s+[A-Za-z]+\s+\d{4}' 
			THEN	TO_DATE(
                    SUBSTRING($1 FROM '^\d{1,2}\s+[A-Za-z]+\s+\d{4}'),
                   	'DD FMMonth YYYY'
                	)

            /* ------------------------------------------------------------
               Month + Year
               Example: "April 1871"
            ------------------------------------------------------------ */
            WHEN 	$1~* 	'^[A-Za-z]+\s+\d{4}' 
			THEN	TO_DATE(
                    SUBSTRING($1 FROM '^[A-Za-z]+\s+\d{4}'),
                    'FMMonth YYYY'
                	)

            /* ------------------------------------------------------------
               Circa year (parenthesized)
               Example: "(circa 1923)", "(c. 1915)"
            ------------------------------------------------------------ */
            WHEN 	$1~* 	'\((?:circa|c\.)\s+\d{4}\)' 
			THEN	TO_DATE(
                    SUBSTRING($1 FROM '\((?:circa|c\.)\s+(\d{4})\)'),
                    'YYYY'
                	)

            /* ------------------------------------------------------------
               Circa year (non-parenthesized, defensive)
               Example: "circa 1923", "born c. 1915"
            ------------------------------------------------------------ */
            WHEN 	$1~* 	'(?:^|\s)(?:circa|c\.)\s+\d{4}' 
			THEN	TO_DATE(
                    SUBSTRING($1 FROM '(?:^|\s)(?:circa|c\.)\s+(\d{4})'),
                    'YYYY'
               		)

            /* ------------------------------------------------------------
               Year only
               Example: "1871"
            ------------------------------------------------------------ */
            WHEN	$1~* 	'^\d{4}' 
			THEN	TO_DATE(
                    SUBSTRING($1 FROM '^\d{4}'),
                    'YYYY'
                	)

            /* ------------------------------------------------------------
               No valid date detected
            ------------------------------------------------------------ */
            ELSE NULL
        END;
$$;

ALTER FUNCTION silver.parse_date(text)
OWNER TO postgres;

