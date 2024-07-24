SELECT 
	  transactions.trans_id 
	, transactions.merchant_id 
	, transactions.user_id 
	, transactions.session_id 
	
	-- value
	, transactions.trans_amount 
	
	-- general 
	, transactions_type.trans_type_name
	, transactions_status.trans_status_name 
    , transactions.account_number 
	, transactions.trans_created_at 
	, transactions.trans_updated_at 
	, transactions.trans_load_created_at 
	
	-- sessions
	, int_sessions.is_session_complete
	, int_sessions.last_session_updated_at
	, int_sessions.first_session_created_at
	, int_sessions.bank_name
	, EXTRACT(EPOCH FROM (int_sessions.last_session_updated_at - int_sessions.first_session_created_at)) AS session_duration_seconds

	-- session steps
	, int_sessions.login_attempt_created_datetime
    , int_sessions.authorization_created_datetime
    , int_sessions.select_bank_created_datetime
    , int_sessions.initiated_lightbox_created_datetime
	, int_sessions.total_steps

FROM {{ ref('stg_transactions') }} as transactions
LEFT JOIN {{ ref('stg_transactions_status') }} AS transactions_status
	ON transactions.trans_status_id = transactions_status.trans_status_id 
LEFT JOIN {{ ref('stg_transactions_type') }} AS transactions_type
	ON transactions_type.trans_type_id = transactions.trans_type_id 
LEFT JOIN {{ ref('int_merchants') }} AS merchants
	ON transactions.merchant_id = merchants.merchant_id 
LEFT JOIN {{ ref('int_sessions') }} AS int_sessions
	ON transactions.session_id = int_sessions.session_id
WHERE merchants.merchant_type_id <> 3 -- remove merchant_type = 3 (tests)
