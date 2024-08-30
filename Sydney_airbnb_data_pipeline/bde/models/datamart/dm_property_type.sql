
{{ config(materialized='view') }}

WITH property_data AS (
    SELECT
        listing_id,
        host_id,
        scraped_date,
        listing_neighbourhood_lga,
        has_availability,
        price,
        accommodates,
        availability_30,
        estimated_revenue
    FROM {{ ref('dim_property') }}
),

host_data AS (
    SELECT
        listing_id,
        host_id,
        scraped_date,
        host_is_superhost
    FROM {{ ref('dim_host') }}
),

listing_data AS (
    SELECT
        listing_id,
        host_id,
        scraped_date,
        review_scores_rating
    FROM {{ ref('fact_listings') }}
)

SELECT
    pd.listing_neighbourhood_lga AS listing_neighbourhood,
    EXTRACT(YEAR FROM pd.scraped_date) AS year,
    EXTRACT(MONTH FROM pd.scraped_date) AS month,
    -- Active Listings Rate
    AVG(CASE WHEN pd.has_availability = 't' THEN 1 ELSE 0 END) * 100 AS active_listing_rate,
    -- Price Statistics for Active Listings
    MIN(pd.price) FILTER (WHERE pd.has_availability = 't') AS min_price,
    MAX(pd.price) FILTER (WHERE pd.has_availability = 't') AS max_price,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pd.price) FILTER (WHERE pd.has_availability = 't') AS median_price,
    AVG(pd.price) FILTER (WHERE pd.has_availability = 't') AS avg_price,
    -- Number of Distinct Hosts
    COUNT(DISTINCT pd.host_id) AS distinct_hosts,
    -- Superhost Rate
    AVG(CASE WHEN hd.host_is_superhost = 't' THEN 1 ELSE 0 END) * 100 AS superhost_rate,
    -- Average Review Scores Rating for Active Listings
    AVG(ld.review_scores_rating) FILTER (WHERE pd.has_availability = 't') AS avg_review_score,
    -- Total Number of Stays for Active Listings
    SUM(30 - pd.availability_30) FILTER (WHERE pd.has_availability = 't') AS total_number_of_stays,
    -- Average Estimated Revenue for Active Listings
    AVG(pd.estimated_revenue) FILTER (WHERE pd.has_availability = 't') AS avg_estimated_revenue
FROM
    property_data pd
LEFT JOIN
    host_data hd ON pd.listing_id = hd.listing_id AND pd.host_id = hd.host_id AND pd.scraped_date = hd.scraped_date
LEFT JOIN
    listing_data ld ON pd.listing_id = ld.listing_id AND pd.host_id = ld.host_id AND pd.scraped_date = ld.scraped_date
GROUP BY
    pd.listing_neighbourhood_lga, year, month
ORDER BY
    pd.listing_neighbourhood_lga, year, month
