SELECT 
    trans_status_id,trans_status_name,trans_status_description,created_at,updated_at,load_created_at
FROM {{ ref('transaction_status') }}
