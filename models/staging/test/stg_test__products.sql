with 

source as (

    select * from {{ source('test', 'products') }}

),

renamed as (
    SELECT
        CAST(_line AS INT64) AS line_number,
        CAST(_fivetran_synced AS TIMESTAMP) AS sync_timestamp,
        CAST(product_id AS STRING) AS product_id,
        CAST(product_category_name AS STRING) AS product_category_name,
        CAST(product_name_lenght AS FLOAT64) AS product_name_length,
        CAST(product_description_lenght AS FLOAT64) AS product_description_length,
        CAST(product_photos_qty AS INT64) AS product_photos_qty,
        CAST(product_weight_g AS FLOAT64) AS product_weight_g,
        CAST(product_length_cm AS FLOAT64) AS product_length_cm,
        CAST(product_height_cm AS FLOAT64) AS product_height_cm,
        CAST(product_width_cm AS FLOAT64) AS product_width_cm
    FROM {{ source('test', 'products') }}
)

)

select * from renamed
