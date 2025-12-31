/*============================================================================
Function	: silver.parse_location
Layer		: Silver
Puporse		: Parse and normalize location components from raw text fields.

Description	:
	This function extracts structured location attributes (city, region, 
	country_code) from a semi-structured text  field originating from the
	Bronze Layer.

Supported raw patterns	:
	- "in City, Region (NOC)"
	- "in ?, Region (NOC)"
	- "in City, Region (NOC) (circa YYYY)"
	- "(circa YYYY)"		-> returns all NULLs

Data Quality Rules	:
	- Placeholder value such as '?' are normalized to NULL
	- Leading/trailing whitespace is removed
	- If no valid location pattern exists, NULLs are returned

Notes	:
	- This function performs parsing + normalization only
	- Date parsing (including circa handling) is handled separately
	- Designed for deterministic use in Silver transformations

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
				Extracts value after 'in' and before first comma.
				Converts '?' placeholder to NULL.
		------------------------------------------------------------*/
		NULLIF(
			TRIM(SUBSTRING($1 FROM 'in\s+([^,]+),')),
			'?'
		) AS city,

		/*------------------------------------------------------------
			Region:
				Extacts value between comma & country code.
				Converts '?' placeholder to NULL.
		------------------------------------------------------------*/
		NULLIF(
			TRIM(SUBSTRING($1 FROM ',\s+([^,]+)\s+\(\w{3}\)')),
			'?'
		) AS region,

		/*------------------------------------------------------------
			Country Code:
				Extracts 3-letter code inside parentheses.
				Returns NULL if pattern does not exist.
		------------------------------------------------------------*/
		SUBSTRING($1 FROM '\((\w{3})\)') AS country_code;
$$;

ALTER FUNCTION silver.parse_location(text)
OWNER TO postgres;





/*============================================================================
   Function : silver.parse_date
   Layer    : Silver
   Purpose  : Extract and normalize partial and semi-structured date strings
              into a PostgreSQL DATE.

   Supported inputs:
     - "1 April 1871"                          → 1871-04-01
     - "April 1871"                            → 1871-04-01
     - "1871"                                  → 1871-01-01
     - "(circa 1923)", "(c. 1915)"             → 1923-01-01
     - "circa 1923", "c. 1923"                 → 1923-01-01
     - "in Tokyo (JPN) circa 1914"             → 1914-01-01

   Data Quality Rules:
     - Ambiguous ranges (e.g. "1926 or 1927") → NULL
     - Missing month/day → imputed as January 1
     - Invalid / non-date strings → NULL

   Notes:
     - Date text is extracted FIRST, then converted
     - Regex order is critical to prevent invalid TO_DATE calls
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

