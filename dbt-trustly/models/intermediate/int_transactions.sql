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
	, transactions.created_at -- TODO: rename colum
	, transactions.updated_at -- TODO: rename colum
	, transactions.load_created_at 	
	
FROM {{ ref('stg_transactions') }} as transactions
LEFT JOIN {{ ref('stg_transactions_status') }} AS transactions_status
	ON transactions.trans_status_id = transactions_status.trans_status_id 
LEFT JOIN {{ ref('stg_transactions_type') }} AS transactions_type
	ON transactions_type.trans_type_id = transactions.trans_type_id 
-- TODO: remove merchant_type = 3 (tests)
