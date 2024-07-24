
-- QUESTION 1) What is the total transaction value and total number of transactions for each merchant?
select 
	dm.merchant_name
	, COUNT(distinct trans_id) as total_transactions
	, SUM(trans_amount) as total_transactions_value
from trustly.public_marts.fct_transactions ft
left join trustly.public_marts.dim_merchants dm 
	on ft.merchant_id = dm.merchant_id 
group by 1;

-- QUESTION 2) What is the number of completed, failed, and new transactions for each merchant?
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
    SUM(CASE WHEN trans_status_name = 'Failed' THEN transaction_count ELSE 0 END) AS failed_transactions,
    SUM(CASE WHEN trans_status_name = 'Pending' THEN transaction_count ELSE 0 END) AS pending_transactions,
    SUM(CASE WHEN trans_status_name = 'New' THEN transaction_count ELSE 0 END) AS new_transactions
FROM transaction_status_count
GROUP BY merchant_id, merchant_name
ORDER BY merchant_id;

-- QUESTION 3) What is the average transaction value for each transaction type (PayIn and Payout)?
SELECT
    trans_type_name,
    AVG(trans_amount) AS avg_transaction_value
FROM trustly.public_marts.fct_transactions
GROUP BY trans_type_name;

-- QUESTION 4) What is the percentage of failed transactions for each merchant?
WITH transaction_counts AS (
    SELECT
        m.merchant_id,
        m.merchant_name,
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN t.trans_status_name = 'Failed' THEN 1 ELSE 0 END) AS failed_transactions
    FROM trustly.public_marts.dim_merchants m
    LEFT JOIN trustly.public_marts.fct_transactions t ON m.merchant_id = t.merchant_id
    GROUP BY m.merchant_id, m.merchant_name
)
SELECT
    merchant_id,
    merchant_name,
    ROUND(CAST((failed_transactions::FLOAT / NULLIF(total_transactions, 0)) * 100 AS NUMERIC), 2) AS failed_percentage
FROM transaction_counts
ORDER BY failed_percentage desc

-- QUESTION 5) What is the total transaction value for each user?
SELECT 
    user_id,
    SUM(trans_amount) AS total_transaction_value
FROM public_marts.fct_transactions ft 
GROUP BY user_id
ORDER BY total_transaction_value DESC

-- QUESTION 6) How many transactions were initiated and how many were completed?
SELECT 
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN trans_status_name = 'Completed' THEN 1 ELSE 0 END) AS completed_transactions,
    SUM(CASE WHEN trans_status_name = 'New' THEN 1 ELSE 0 END) AS initiated_transactions
FROM public_marts.fct_transactions ft;

-- QUESTION 7) What is the average time between the creation and authorization request of transactions?
WITH transaction_times AS (
    SELECT
        trans_created_at,
        authorization_created_datetime,
        EXTRACT(EPOCH FROM (authorization_created_datetime - trans_created_at)) AS time_to_authorization_seconds
    FROM public_marts.fct_transactions ft
    WHERE authorization_created_datetime IS NOT NULL
)
SELECT
    AVG(time_to_authorization_seconds) AS avg_time_to_authorization_seconds,
    AVG(time_to_authorization_seconds) / 60 AS avg_time_to_authorization_minutes
FROM transaction_times;

-- QUESTION 8) How many transactions did each user perform by transaction type?
SELECT 
    user_id,
    trans_type_name,
    COUNT(*) AS transaction_count
FROM public_marts.fct_transactions ft 
GROUP BY user_id, trans_type_name
ORDER BY user_id, transaction_count DESC

-- QUESTION 9) How many sessions were initiated per merchant and what is the average number of steps per session?
WITH session_steps AS (
    SELECT
        session_id,
        merchant_name,
        COUNT(DISTINCT step_id) AS num_steps
    FROM public_marts.fct_transactions_sessions trans_sessions
    left join public_marts.dim_merchants dm
    	on dm.merchant_id = trans_sessions.merchant_id
    GROUP BY session_id, merchant_name
)
SELECT
    merchant_name,
    COUNT(DISTINCT session_id) AS total_sessions,
    AVG(num_steps) AS avg_steps_per_session
FROM session_steps
GROUP BY merchant_name;

-- QUESTION 10) What is the most used bank in transactions?
SELECT 
	  bank_name 
	, COUNT(DISTINCT trans_id) AS total_transactions
FROM public_marts.fct_transactions_sessions fts
GROUP BY bank_name
ORDER BY 2 DESC;

-- QUESTION 11) What is the average time between each step in a session?
SELECT AVG(avg_step_duration_seconds) AS average_time_between_steps
FROM public_marts.fct_transactions_sessions

-- QUESTION 12) How many sessions were completed (containing the AUTHORIZATION step)?
SELECT COUNT(DISTINCT session_id) AS completed_sessions
FROM public_marts.fct_transactions_sessions
WHERE is_session_complete = 1

-- QUESTION 13) What is the most used bank in the LOGIN_ATTEMPT and AUTHORIZATION steps?
SELECT 
    bank_name,
    step_name,
	COUNT(bank_name) AS usage_bank_count
FROM public_marts.fct_transactions_sessions
WHERE step_name IN ('LOGIN_ATTEMPT', 'AUTHORIZATION')
GROUP BY bank_name, step_name
ORDER BY 3 DESC

-- QUESTION 14) What is the failure rate by bank? Failure are the transactions not completes.
WITH transaction_status AS (
    SELECT 
        ft.trans_id,
        ft.trans_status_name,
        fts.bank_name
    FROM public_marts.fct_transactions ft
    JOIN public_marts.fct_transactions_sessions fts ON ft.trans_id = fts.trans_id
)
, bank_transactions AS (
    SELECT 
        bank_name,
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN trans_status_name != 'Completed' THEN 1 ELSE 0 END) AS failed_transactions
    FROM transaction_status
    WHERE bank_name IS NOT NULL
    GROUP BY bank_name
)
SELECT 
    bank_name,
    total_transactions,
    failed_transactions,
     CAST((failed_transactions::FLOAT / total_transactions) * 100 AS DECIMAL(5,2)) AS failure_rate_percentage
FROM bank_transactions
ORDER BY failure_rate_percentage DESC