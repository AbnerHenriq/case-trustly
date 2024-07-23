-- QUESTION 1
select 
	dm.merchant_name
	, COUNT(distinct trans_id) as total_transactions
	, SUM(trans_amount) as total_transactions_value
from trustly.public_marts.fct_transactions ft
left join trustly.public_marts.dim_merchants dm 
	on ft.merchant_id = dm.merchant_id 
group by 1;



-- QUESTION 2
WITH transaction_status_count AS (
    SELECT
        m.merchant_id,
        m.merchant_name,
        t.trans_status_name,
        COUNT(t.trans_id) AS transaction_count
    FROM trustly.public_marts.dim_merchants m
    LEFT JOIN trustly.public_marts.fct_transactions t ON m.merchant_id = t.merchant_id
    GROUP BY m.merchant_id, m.merchant_name, t.trans_status_name
)
SELECT
    merchant_id,
    merchant_name,
    SUM(CASE WHEN trans_status_name = 'Completed' THEN transaction_count ELSE 0 END) AS completed_transactions,
    SUM(CASE WHEN trans_status_name = 'Pending' THEN transaction_count ELSE 0 END) AS failed_transactions,
    SUM(CASE WHEN trans_status_name = 'New' THEN transaction_count ELSE 0 END) AS new_transactions
FROM transaction_status_count
GROUP BY merchant_id, merchant_name
ORDER BY merchant_id;