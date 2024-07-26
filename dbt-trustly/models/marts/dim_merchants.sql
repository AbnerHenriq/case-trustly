SELECT 
    -- ids
      merchant_id 
    , merchant_type_id
	 
	-- dates
    , merchant_created_at 
	, merchant_updated_at 
	, merchant_load_created_at 

    -- general
    , merchant_name
    , merchant_type_name 

FROM {{ ref('int_merchants') }}