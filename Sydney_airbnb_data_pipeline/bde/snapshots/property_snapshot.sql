{% snapshot property_snapshot %}

{{ config(
    target_schema='raw_schema', 
    unique_key='listing_id', 
    strategy='timestamp', 
    updated_at='scraped_date'
) }}

SELECT * FROM {{ ref('property') }}

{% endsnapshot %}
