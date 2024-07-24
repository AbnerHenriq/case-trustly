SELECT
    session_id,
    MAX(CASE WHEN step_name = 'LOGIN_ATTEMPT' THEN session_created_at END) AS login_attempt_created_datetime,
    MAX(CASE WHEN step_name = 'AUTHORIZATION' THEN session_created_at END) AS authorization_created_datetime,
    MAX(CASE WHEN step_name = 'SELECT_BANK' THEN session_created_at END) AS select_bank_created_datetime,
    MAX(CASE WHEN step_name = 'INITIATED_LIGHTBOX' THEN session_created_at END) AS initiated_lightbox_created_datetime,
    COUNT(DISTINCT step_id) AS total_steps,
    MAX(CASE WHEN step_name = 'AUTHORIZATION' THEN 1 ELSE 0 END) AS is_session_complete,
    MAX(session_updated_at) AS last_session_updated_at,
    MIN(session_created_at) AS first_session_created_at,
    MAX(bank_name) AS bank_name
FROM {{ ref('stg_sessions') }}
GROUP BY session_id