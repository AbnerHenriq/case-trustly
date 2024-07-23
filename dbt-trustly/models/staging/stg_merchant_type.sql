SELECT
    merchant_type_id,
    merchant_type_name,
    merchant_type_description,
    created_at,
    updated_at,
    load_created_at
FROM {{ ref('merchant_type') }}