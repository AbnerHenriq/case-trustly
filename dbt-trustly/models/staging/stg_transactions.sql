SELECT
    trans_id,
    trans_type_id,
    trans_status_id,
    trans_amount,
    merchant_id,
    user_id,
    session_id,
    account_number,
    created_at AS trans_created_at,
    updated_at AS trans_updated_at,
    load_created_at AS trans_load_created_at
FROM {{ ref('transactions') }}


