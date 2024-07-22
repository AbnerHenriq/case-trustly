SELECT 
    session_id,step_id,step_name,bank_name,created_at,updated_at,load_created_at
FROM {{ ref('sessions') }}
