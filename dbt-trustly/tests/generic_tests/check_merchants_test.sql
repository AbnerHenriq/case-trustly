SELECT 
	COUNT(merchant_id) AS count_merchant_tests
FROM public_marts.dim_merchants dm
WHERE merchant_name ILIKE '%Test%'
HAVING COUNT(merchant_id) >= 1