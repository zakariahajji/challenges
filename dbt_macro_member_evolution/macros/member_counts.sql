{%
    macro member_counter()
%}

SELECT
  user_event.user_id,
  user_event.event_datetime,
  users.company_id,
  COUNT(DISTINCT user_event.user_id) AS members_count
FROM
  {{ ref('user_event') }} AS user_event
LEFT JOIN
  {{ ref('users') }} AS users
ON
  user_event.user_id = users.user_id GROUP BY  user_event.user_id, user_event.event_name, user_event.event_datetime, user_event._extracted_at, users.company_id

    )

{% endmacro %}



{%
macro user_activity()
%}

SELECT
event_datetime AS activity_month,
company_id,
CASE
  WHEN members_count < 10 THEN "C"
  WHEN members_count < 50 THEN "B"
ELSE
"A"
END
AS company_tier,
 members_count,

FROM (
{{ member_counter() }}
 )
GROUP BY
event_datetime,
company_id,
members_count
ORDER BY event_datetime , company_id

{% endmacro %}