SELECT
    merchant_id,
    merchant_name,
    merchant_type_id,
    merchant_country,
    created_at AS merchant_created_at,
    updated_at AS merchant_updated_at,
    load_created_at AS merchant_load_created_at
FROM {{ ref('merchants') }}