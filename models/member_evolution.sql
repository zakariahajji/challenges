{{
  config(
    materialized = 'table',
    partition_by = {
      "field": "created_at",
      "data_type": "date",
    }
  )
}}

WITH user_activity AS ( {{ user_activity() }} )

SELECT user_activity.* , company.created_at FROM user_activity AS user_activity LEFT JOIN {{ ref('company') }} AS company
 ON user_activity.company_id = company.company_id   ORDER BY company.created_at