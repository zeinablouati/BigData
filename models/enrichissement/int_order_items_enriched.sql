WITH 
stg_products AS (
    select * from {{ source('test', 'products') }}
),

categories AS (
    SELECT 
        product_category_name,
        product_category_name_english
    FROM {{ source('test', 'product_category_name_translation') }}
)

SELECT
    p.line_number,
    p.sync_timestamp,
    p.product_id,
    COALESCE(c.product_category_name_english, p.product_category_name) AS product_category_name,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
FROM stg_products p
LEFT JOIN categories c
ON p.product_category_name = c.product_category_name;
