SELECT trans_id, COUNT(DISTINCT session_id) AS qtd_sessions_by_transactions 
FROM public_staging.stg_transactions
GROUP BY 1 
HAVING COUNT(DISTINCT session_id) > 1