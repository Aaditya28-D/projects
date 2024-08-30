SELECT
    DISTINCT
    HOST_ID,
    HOST_NAME,
    HOST_SINCE,
    HOST_IS_SUPERHOST,
    HOST_NEIGHBOURHOOD,
    SCRAPED_DATE  -- added this column
FROM {{ ref('airbnb_listing_all_view') }}