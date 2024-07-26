SELECT 
    -- ids
      trans_id 
	, merchant_id 
	, user_id 
	, session_id 
	
	-- value
	, trans_amount 
	
	-- general transactions
	, trans_type_name
	, trans_status_name 
    , account_number 
	, trans_created_at 
	, trans_updated_at 
	, trans_load_created_at 
	
	-- sessions
	, is_session_complete
	, last_session_updated_at
	, first_session_created_at
	, bank_name
	, session_duration_seconds

	-- session steps
	, login_attempt_created_datetime
    , authorization_created_datetime
    , select_bank_created_datetime
    , initiated_lightbox_created_datetime
	, total_steps

FROM {{ ref('int_transactions') }}