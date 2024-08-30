{{ config(materialized='view') }}

WITH host_revenue AS (
    SELECT
        h.host_id,
        h.host_neighbourhood_suburb AS host_neighbourhood,
        EXTRACT(YEAR FROM p.scraped_date) AS year,
        EXTRACT(MONTH FROM p.scraped_date) AS month,
        SUM(p.estimated_revenue) AS total_estimated_revenue
    FROM {{ ref('dim_host') }} h
    JOIN {{ ref('dim_property') }} p ON h.host_id = p.host_id
                                    AND h.listing_id = p.listing_id
                                    AND h.scraped_date = p.scraped_date
    GROUP BY h.host_id, h.host_neighbourhood_suburb, year, month
)

SELECT
    hr.host_neighbourhood,
    hr.year,
    hr.month,
    COUNT(hr.host_id) AS number_of_distinct_hosts,
    SUM(hr.total_estimated_revenue) AS total_estimated_revenue,
    AVG(hr.total_estimated_revenue) AS avg_estimated_revenue_per_host
FROM host_revenue hr
GROUP BY hr.host_neighbourhood, hr.year, hr.month
ORDER BY hr.host_neighbourhood, hr.year, hr.month
