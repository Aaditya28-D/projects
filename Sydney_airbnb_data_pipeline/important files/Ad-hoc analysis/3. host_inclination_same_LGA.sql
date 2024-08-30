WITH Host_Residential_LGA AS (
    SELECT
        h.host_id,
        g.lga_name AS Host_LGA
    FROM
        dim_host h
    JOIN
        dim_geographic g ON h.host_neighbourhood_suburb = g.suburb_name
),
Multiple_Listings_Hosts AS (
    SELECT 
        host_id,
        COUNT(listing_id) AS Listing_Count
    FROM 
        dim_property
    GROUP BY 
        host_id
    HAVING 
        COUNT(listing_id) > 1
),
Host_Listing_LGA_Comparison AS (
    SELECT 
        hl.host_id,
        hl.Host_LGA,
        p.listing_neighbourhood_lga AS Listing_LGA,
        (CASE WHEN hl.Host_LGA = p.listing_neighbourhood_lga THEN 1 ELSE 0 END) AS Is_Same_LGA
    FROM 
        Host_Residential_LGA hl
    JOIN 
        Multiple_Listings_Hosts mlh ON hl.host_id = mlh.host_id
    JOIN 
        dim_property p ON hl.host_id = p.host_id
),
LGA_Match_Count AS (
    SELECT 
        host_id,
        SUM(Is_Same_LGA) AS Same_LGA_Count,
        COUNT(*) AS Total_Listings
    FROM 
        Host_Listing_LGA_Comparison
    GROUP BY 
        host_id
)
SELECT 
    host_id,
    Same_LGA_Count,
    Total_Listings,
    (Same_LGA_Count * 100.0 / Total_Listings) AS Same_LGA_Percentage
FROM 
    LGA_Match_Count;
