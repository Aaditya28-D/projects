{{ config(materialized='table') }}

WITH lga_code AS (
    SELECT lga_code, lga_name
    FROM {{ ref('nsw_lga_code_stg') }}
),
lga_suburb AS (
    SELECT lga_name, suburb_name
    FROM {{ ref('nsw_lga_suburb_stg') }}
)

SELECT
    l.lga_code,
    l.lga_name,
    s.suburb_name
FROM
    lga_code l
JOIN
    lga_suburb s ON l.lga_name = s.lga_name
