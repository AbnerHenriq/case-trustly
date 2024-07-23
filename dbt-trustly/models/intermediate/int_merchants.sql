SELECT 
	  merchants.merchant_id 
    , merchant_type.merchant_type_id
	 
	-- dates
    , merchants.merchant_created_at 
	, merchants.merchant_updated_at 
	, merchants.merchant_load_created_at 

    -- general
    , merchants.merchant_name
    , merchant_type.merchant_type_name 

FROM {{ ref('stg_merchants') }} AS merchants
LEFT JOIN {{ ref('stg_merchant_type') }} AS merchant_type
	ON merchants.merchant_type_id = merchant_type.merchant_type_id 
WHERE merchant_type.merchant_type_id <> 3 -- remove merchant_type = 3 (tests)