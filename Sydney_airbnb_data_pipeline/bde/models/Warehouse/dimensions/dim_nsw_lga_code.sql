{{ config(materialized='table') }}

SELECT
    *
FROM
    {{ ref('nsw_lga_code_stg') }} 
