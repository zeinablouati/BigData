with 

source as (

    select * from {{ source('test', 'leads_qualified') }}

),

renamed as (

    select
        CAST(_line AS INT64) as _line,
        CAST(_fivetran_synced AS TIMESTAMP) as _fivetran_synced,
        CAST(mql_id AS STRING) as mql_id,
        CAST(first_contact_date AS DATE) as first_contact_date,
        CAST(landing_page_id AS STRING) as landing_page_id,
        CAST(origin AS STRING) as origin
        
    from source

)

select * from renamed
