SELECT 
    trans_type_id,
    trans_type_name,
    trans_type_description,
    created_at,
    updated_at,
    load_created_at
FROM {{ ref('transaction_type') }}
