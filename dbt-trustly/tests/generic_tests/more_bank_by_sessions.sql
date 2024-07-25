SELECT session_id, COUNT(DISTINCT bank_name) AS tt_bank_by_session FROM public_staging.stg_sessions ss 
WHERE bank_name IS NOT NULL 
GROUP BY 1
HAVING COUNT(DISTINCT bank_name) > 1