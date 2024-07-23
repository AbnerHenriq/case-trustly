SELECT 
    *
FROM {{ ref('int_transactions_sessions') }} AS transactions_sessions