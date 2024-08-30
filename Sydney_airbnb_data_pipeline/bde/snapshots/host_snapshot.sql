{% snapshot host_snapshot %}

{{ config(
    target_schema='raw_schema', 
    unique_key='host_id', 
    strategy='timestamp', 
    updated_at='scraped_date'
) }}

SELECT * FROM {{ ref('host') }}

{% endsnapshot %}
