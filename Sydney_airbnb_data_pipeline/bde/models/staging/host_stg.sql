WITH mode_values AS (
  SELECT
    MODE() WITHIN GROUP (ORDER BY HOST_NAME) AS mode_host_name,
    MODE() WITHIN GROUP (ORDER BY HOST_SINCE) AS mode_host_since,
    MODE() WITHIN GROUP (ORDER BY HOST_IS_SUPERHOST) AS mode_host_is_superhost,
    MODE() WITHIN GROUP (ORDER BY HOST_NEIGHBOURHOOD) AS mode_host_neighbourhood
  FROM {{ ref('airbnb_listing_all_view') }}
)

SELECT
    al.HOST_ID,
    COALESCE(LOWER(al.HOST_NAME), LOWER(mv.mode_host_name)) AS HOST_NAME,
    COALESCE(al.HOST_SINCE, mv.mode_host_since) AS HOST_SINCE,
    COALESCE(al.HOST_IS_SUPERHOST, mv.mode_host_is_superhost) AS HOST_IS_SUPERHOST,
    COALESCE(LOWER(al.HOST_NEIGHBOURHOOD), LOWER(mv.mode_host_neighbourhood)) AS HOST_NEIGHBOURHOOD_SUBURB,
    al.listing_id,
    al.SCRAPED_DATE
    -- Include all other columns from airbnb_listing_all_view here, if needed
FROM {{ ref('airbnb_listing_all_view') }} al
CROSS JOIN mode_values mv
