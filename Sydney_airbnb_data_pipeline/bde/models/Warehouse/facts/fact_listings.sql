{{ config(materialized='table') }}

SELECT
   *
FROM
 {{ ref('property_review_stg') }}
    