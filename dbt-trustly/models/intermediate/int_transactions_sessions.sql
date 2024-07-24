SELECT
    -- IDs
    CONCAT(transactions.trans_id, sessions.session_id) AS trans_sessions_id,
    transactions.trans_id,
    sessions.session_id,
    transactions.trans_type_id,
    transactions.trans_status_id,
    transactions.merchant_id,
    sessions.bank_name,
    
    -- dates
    sessions.first_session_created_at AS session_created_at,
    sessions.last_session_updated_at AS session_updated_at,
    transactions.trans_load_created_at AS session_load_created_at,
    
    -- session steps
    sessions.login_attempt_created_datetime,
    sessions.authorization_created_datetime,
    sessions.select_bank_created_datetime,
    sessions.initiated_lightbox_created_datetime,
    
    -- Campos calculados
    sessions.total_steps AS total_steps_per_session,
    EXTRACT(EPOCH FROM (sessions.last_session_updated_at - sessions.first_session_created_at)) AS session_duration_seconds,
    sessions.has_authorization_step AS is_session_complete

FROM {{ ref('stg_transactions') }} as transactions
LEFT JOIN {{ ref('int_sessions') }} AS sessions
    ON transactions.session_id = sessions.session_id 
LEFT JOIN {{ ref('stg_transactions_status') }} AS transactions_status
    ON transactions.trans_status_id = transactions_status.trans_status_id 
LEFT JOIN {{ ref('stg_transactions_type') }} AS transactions_type
    ON transactions_type.trans_type_id = transactions.trans_type_id 
LEFT JOIN {{ ref('int_merchants') }} AS merchants
    ON transactions.merchant_id = merchants.merchant_id 
WHERE merchants.merchant_type_id <> 3 -- remove merchant_type = 3 (tests)