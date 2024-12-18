SELECT 
    LISTING_ID,
    SCRAPE_ID,
    SCRAPED_DATE,
    LISTING_NEIGHBOURHOOD,
    PROPERTY_TYPE,
    ROOM_TYPE,
    ACCOMMODATES,
    PRICE,
    HAS_AVAILABILITY,
    AVAILABILITY_30,
    HOST_ID
FROM {{ ref('airbnb_listing_all_view') }}
