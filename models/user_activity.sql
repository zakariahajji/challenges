{{
  config(
    materialized = 'table',
    partition_by = {
      "field": "event_datetime",
      "data_type": "date",
    }
  )
}}

{% set event_names = ["space_create", "company_create", "edit_page_create","collection_create","space_install_gitsync"] %}

SELECT
    user_id,
    event_datetime,

    {% for event_name in event_names %}
    sum(CASE WHEN event_name = 'event_name' THEN 1 END else 0) AS {{event_name}},
    max(
         CASE WHEN event_name = 'event_name' THEN event_datetime END
     ) AS {{event_name}}_last_date
    {% endfor %}

FROM {{ ref('user_event') }}
WHERE event_datetime >= Date_add(Current_date(), INTERVAL -30 day)
GROUP BY user_id,
         event_datetime



