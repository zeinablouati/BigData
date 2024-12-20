with 

source as (

    select * from {{ source('test', 'leads_closed') }}

),

renamed as (

    select
        CAST(_line AS INT64) as _line,
        CAST(_fivetran_synced AS TIMESTAMP) as _fivetran_synced,
        CAST(mql_id AS STRING) as mql_id,
        CAST(seller_id AS STRING) as seller_id,
        CAST(sdr_id AS STRING) as sdr_id,
        CAST(sr_id AS STRING) as sr_id,
        CAST(won_date AS DATETIME) as won_date,
        CAST(business_segment AS STRING) as business_segment,
        CAST(lead_type AS STRING) as lead_type,
        CAST(lead_behaviour_profile AS STRING) as lead_behaviour_profile,
        CAST(has_company AS INT64) as has_company,
        CAST(has_gtin AS INT64) as has_gtin,
        CAST(average_stock AS STRING) as average_stock,
        CAST(business_type AS STRING) as business_type,
        CAST(declared_product_catalog_size AS FLOAT64) as declared_product_catalog_size,
        CAST(declared_monthly_revenue AS FLOAT64) as declared_monthly_revenue

    from source

)

select * from renamed