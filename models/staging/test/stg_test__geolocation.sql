with 

source as (

    select * from {{ source('test', 'geolocation') }}

),

renamed as (

    select
        CAST(_line AS INT64) as _line,
        CAST(_fivetran_synced AS TIMESTAMP) as _fivetran_synced,
        CAST(geolocation_zip_code_prefix AS INT64) as geolocation_zip_code_prefix,
        CAST(geolocation_lat AS FLOAT64) as geolocation_lat,
        CAST(geolocation_lng AS FLOAT64) as geolocation_lng,
        CAST(geolocation_city AS STRING) as geolocation_city,
        CAST(geolocation_state AS STRING) as geolocation_state

    from source

)

select * from renamed