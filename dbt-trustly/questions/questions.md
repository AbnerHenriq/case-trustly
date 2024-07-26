
##  Question 1) What is the total transaction value and total number of transactions for each merchant?
```sql
SELECT 
	  merchants.merchant_name
	, COUNT(DISTINCT transactions.trans_id) AS total_transactions
	, SUM(transactions.trans_amount) AS total_transactions_value
FROM public_marts.fct_transactions AS transactions
LEFT JOIN public_marts.dim_merchants AS merchants
	ON transactions.merchant_id = merchants.merchant_id 
GROUP BY 1;
```

## Question 2) What is the number of completed, failed, and new transactions for each merchant?
```sql
WITH transaction_status_count AS (
    SELECT
        merchant.merchant_id,
        merchant.merchant_name,
        transactions.trans_status_name,
        COUNT(transactions.trans_id) AS transaction_count
    FROM public_marts.dim_merchants AS merchant  
    LEFT JOIN public_marts.fct_transactions AS transactions ON merchant.merchant_id = transactions.merchant_id
    GROUP BY merchant.merchant_id, merchant.merchant_name, transactions.trans_status_name
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
```

##  Question 3) What is the average transaction value for each transaction type (PayIn and Payout)?
```sql
SELECT
    trans_type_name,
    AVG(trans_amount) AS avg_transaction_value
FROM trustly.public_marts.fct_transactions
GROUP BY trans_type_name;
```

## Question 4) What is the percentage of failed transactions for each merchant?
```sql
WITH transaction_counts AS (
    SELECT
        merchant.merchant_id,
        merchant.merchant_name,
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN transactions.trans_status_name = 'Failed' THEN 1 ELSE 0 END) AS failed_transactions
    FROM public_marts.dim_merchants AS merchant
    LEFT JOIN public_marts.fct_transactions AS transactions ON merchant.merchant_id = transactions.merchant_id
    GROUP BY merchant.merchant_id, merchant.merchant_name
)
SELECT
    merchant_id,
    merchant_name,
    ROUND(CAST((failed_transactions::FLOAT / NULLIF(total_transactions, 0)) * 100 AS NUMERIC), 2) AS failed_percentage
FROM transaction_counts
ORDER BY failed_percentage desc
```

## Question 5) What is the total transaction value for each user?
```sql
SELECT 
    user_id,
    SUM(trans_amount) AS total_transaction_value
FROM public_marts.fct_transactions AS transactions
GROUP BY user_id
ORDER BY total_transaction_value DESC
```

## Question 6) How many transactions were initiated and how many were completed?

- Considered initiated transactions as transactions with status 'New', 'Pending', or 'Failed'
```sql
SELECT 
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN trans_status_name = 'Completed' THEN 1 ELSE 0 END) AS completed_transactions,
    SUM(CASE WHEN trans_status_name IN ('New', 'Pending', 'Failed') THEN 1 ELSE 0 END) AS initiated_transactions
FROM public_marts.fct_transactions AS transactions;
```

## Question 7) What is the average time between the creation and authorization request of transactions?
```sql
WITH transaction_times AS (
    SELECT
        trans_created_at,
        authorization_created_datetime,
        EXTRACT(EPOCH FROM (authorization_created_datetime - trans_created_at)) AS time_to_authorization_seconds
    FROM public_marts.fct_transactions AS transactions
    WHERE authorization_created_datetime IS NOT NULL
)
SELECT
    AVG(time_to_authorization_seconds) AS avg_time_to_authorization_seconds,
    AVG(time_to_authorization_seconds) / 60 AS avg_time_to_authorization_minutes
FROM transaction_times;
```

## Question 8) How many transactions did each user perform by transaction type?
```sql
SELECT 
    user_id,
    trans_type_name,
    COUNT(*) AS transaction_count
FROM public_marts.fct_transactions AS transactions
GROUP BY user_id, trans_type_name
ORDER BY user_id, transaction_count DESC
```

## Question 9) How many sessions were initiated per merchant and what is the average number of steps per session?

-- Removed sessions_id = 0, not being considered as a iniciated session.
-- To the second part, it was considered average number of steps, per sessions, per merchant.
```sql
SELECT 
	  merchant_id
	, COUNT(DISTINCT session_id) AS tt_sessions
	, AVG(total_steps) AS avg_steps_per_session
FROM public_marts.fct_transactions ft
WHERE session_id <> 0
GROUP BY 1;
```
 

## Question 10) What is the most used bank in transactions?
```sql
SELECT 
    bank_name,
    COUNT(*) as transaction_count
FROM public_marts.fct_transactions AS transactions
WHERE bank_name IS NOT NULL
GROUP BY bank_name
ORDER BY transaction_count DESC
LIMIT 1;
```

## Question 11) What is the average time between each step in a session?
```sql
WITH step_times AS (
  SELECT
    session_id,
    initiated_lightbox_created_datetime,
    select_bank_created_datetime,
    login_attempt_created_datetime,
    authorization_created_datetime
  FROM public_marts.fct_transactions AS transactions
  WHERE session_id IS NOT NULL
        AND session_id <> 0
),
time_differences AS (
  SELECT
    session_id,
    EXTRACT(EPOCH FROM (select_bank_created_datetime - initiated_lightbox_created_datetime)) / 60 AS lightbox_to_select_bank,
    EXTRACT(EPOCH FROM (login_attempt_created_datetime - select_bank_created_datetime)) / 60 AS select_bank_to_login,
    EXTRACT(EPOCH FROM (authorization_created_datetime - login_attempt_created_datetime)) / 60 AS login_to_auth
  FROM step_times
  WHERE 
    initiated_lightbox_created_datetime IS NOT NULL
    AND select_bank_created_datetime IS NOT NULL
    AND login_attempt_created_datetime IS NOT NULL
    AND authorization_created_datetime IS NOT NULL
)
SELECT
  AVG(lightbox_to_select_bank) AS avg_lightbox_to_select_bank_minutes,
  AVG(select_bank_to_login) AS avg_select_bank_to_login_minutes,
  AVG(login_to_auth) AS avg_login_to_auth_minutes
FROM time_differences;
```

## Question 12) How many sessions were completed (containing the AUTHORIZATION step)?
```sql
SELECT 
    COUNT(DISTINCT session_id) AS completed_sessions
FROM public_marts.fct_transactions AS transactions
WHERE is_session_complete = 1;
```

## Question 13) What is the most used bank in the LOGIN_ATTEMPT and AUTHORIZATION steps?
```sql
WITH login_attempts AS (
    SELECT bank_name, COUNT(*) AS login_count
    FROM public_marts.fct_transactions
    WHERE login_attempt_created_datetime IS NOT NULL
    GROUP BY bank_name
),
authorizations AS (
    SELECT bank_name, COUNT(*) AS auth_count
    FROM public_marts.fct_transactions
    WHERE authorization_created_datetime IS NOT NULL
    GROUP BY bank_name
)
SELECT 
    COALESCE(login_attempts.bank_name, authorizations.bank_name) AS bank_name,
    COALESCE(login_attempts.login_count, 0) AS login_attempts,
    COALESCE(authorizations.auth_count, 0) AS authorizations,
    COALESCE(login_attempts.login_count, 0) + COALESCE(authorizations.auth_count, 0) AS total_count
FROM login_attempts
FULL OUTER JOIN authorizations ON login_attempts.bank_name = authorizations.bank_name
ORDER BY total_count DESC
LIMIT 10
```

## Question 14) What is the failure rate by bank? Failure are the transactions not completes.
```sql
WITH bank_transactions AS (
    SELECT 
        bank_name,
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN trans_status_name != 'Completed' THEN 1 ELSE 0 END) AS failed_transactions
    FROM public_marts.fct_transactions
    WHERE bank_name IS NOT NULL
    GROUP BY bank_name
)
SELECT 
    bank_name,
    total_transactions,
    failed_transactions,
    CAST(ROUND(CAST(failed_transactions AS NUMERIC) / CAST(total_transactions AS NUMERIC) * 100, 2) AS NUMERIC(5,2)) AS failure_rate_percentage
FROM bank_transactions
ORDER BY failure_rate_percentage DESC;
```
