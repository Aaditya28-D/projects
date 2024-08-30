SELECT 
    listing_id,
    scrape_id,
    scraped_date,
    LOWER(LISTING_NEIGHBOURHOOD) AS listing_neighbourhood_lga,
    LOWER(PROPERTY_TYPE) AS property_type,
    LOWER(ROOM_TYPE) AS room_type,
    accommodates,
    price,
    has_availability,
    availability_30,
    CASE 
        WHEN has_availability = 't' THEN (30 - availability_30) * price
        ELSE 0
    END AS estimated_revenue,
    host_id
FROM 
    raw_schema.airbnb_listing_all_view

