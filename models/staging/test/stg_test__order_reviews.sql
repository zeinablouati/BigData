with 

source as (

    select * from {{ source('test', 'order_reviews') }}

),

renamed as (

    select
        CAST(_line AS INT64) as _line,
        CAST(_fivetran_synced AS TIMESTAMP) as _fivetran_synced,
        CAST(review_id AS STRING) as review_id,
        CAST(order_id AS STRING) as order_id,
        CAST(review_score AS INT64) as review_score,
        CAST(review_comment_title AS STRING) as review_comment_title,
        CAST(review_comment_message AS STRING) as review_comment_message,
        CAST(review_creation_date AS TIMESTAMP) as review_creation_date,
        CAST(review_answer_timestamp AS TIMESTAMP) as review_answer_timestamp

    from source

)

select * from renamed