SELECT 
	  merchants.merchant_id 
	 
	-- dates
    , merchants.created_at
	, merchants.updated_at
	, merchants.load_created_at 

    -- general
    , merchants.merchant_name
    , merchant_type.merchant_type_name 

FROM {{ ref('stg_merchants') }} AS merchants
LEFT JOIN {{ ref('stg_merchant_type') }} AS merchant_type
	ON merchants.merchant_type_id = merchant_type.merchant_type_id 
-- TODO: remove merchant_type = 3 (tests)