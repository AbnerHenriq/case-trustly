SELECT 
    merchant_id,merchant_name,merchant_type_id,merchant_country,created_at,updated_at,load_created_at
FROM {{ ref('merchants') }}
