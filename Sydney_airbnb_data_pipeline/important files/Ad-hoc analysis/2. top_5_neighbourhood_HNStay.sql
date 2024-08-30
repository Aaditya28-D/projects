WITH Revenue_Per_Neighbourhood AS (
    SELECT 
        listing_neighbourhood_lga,
        SUM(estimated_revenue) AS Total_Revenue,
        COUNT(listing_id) AS Active_Listings
    FROM 
        dim_property
    WHERE 
        has_availability = 't'
    GROUP BY 
        listing_neighbourhood_lga
),
Average_Revenue AS (
    SELECT 
        listing_neighbourhood_lga,
        Total_Revenue / NULLIF(Active_Listings, 0) AS Avg_Revenue
    FROM 
        Revenue_Per_Neighbourhood
),
Top_Neighbourhoods AS (
    SELECT 
        listing_neighbourhood_lga
    FROM 
        Average_Revenue
    ORDER BY 
        Avg_Revenue DESC
    LIMIT 5
),
Listing_Analysis AS (
    SELECT 
        p.property_type,
        p.room_type,
        p.accommodates,
        (30 - p.availability_30) AS Number_of_Stays
    FROM 
        dim_property p
    JOIN 
        Top_Neighbourhoods t ON p.listing_neighbourhood_lga = t.listing_neighbourhood_lga
    WHERE 
        p.has_availability = 't'
)
SELECT 
    property_type,
    room_type,
    accommodates,
    AVG(Number_of_Stays) AS Avg_Stays
FROM 
    Listing_Analysis
GROUP BY 
    property_type, room_type, accommodates
ORDER BY 
    Avg_Stays DESC;
