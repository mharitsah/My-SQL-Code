-- cara ambil jumlah total user beserta path dan tanggal
SELECT
    TIMESTAMP_MICROS(CAST(event_timestamp AS INT64)) AS timestamp_date,
    device.web_info.hostname,
    param.value.string_value AS page_location,
    COUNT(DISTINCT user_pseudo_id) AS total_users
FROM
    `mitutoyo-production.analytics_282716579.events_intraday_2023*`,
    UNNEST(event_params) AS param
WHERE
    (SELECT
        COUNT(*)
    FROM
        UNNEST(event_params) AS event_param
    WHERE
        param.key = 'page_location') > 0
GROUP BY
    timestamp_date, user_pseudo_id, device.web_info.hostname, param.value.string_value;




SELECT
    FORMAT_DATE('%d-%m-%Y', PARSE_DATE('%Y%m%d', event_date)) AS timestamp_date,
    device.web_info.hostname,
    param.value.string_value AS page_location,
    COUNT(DISTINCT user_pseudo_id) AS total_users
FROM
    `mitutoyo-production.analytics_282716579.events_intraday_2023*`,
    UNNEST(event_params) AS param
WHERE
    (SELECT
        COUNT(*)
    FROM
        UNNEST(event_params) AS event_param
    WHERE
        param.key = 'page_location') > 0
GROUP BY
    timestamp_date, device.web_info.hostname, param.value.string_value;





--parsing tanggal dari timestamp ke Year Month
SELECT 
  FORMAT_DATE('%Y-%m', DATETIME(TIMESTAMP_MICROS(event_timestamp), "Asia/Jakarta")) AS month_year, 
  COUNT(DISTINCT user_pseudo_id) AS total_users FROM `mitutoyo-production.analytics_282716579.events_intraday_2023*`
GROUP BY 1
ORDER by month_year desc





--ambil data channel grouping dari intraday BQ
WITH Intradata AS (
  SELECT
    event_date,
    event_name,
    traffic_source.medium as medium,
    COUNT(DISTINCT user_pseudo_id) AS total_users
  FROM
    `mitutoyo-production.analytics_282716579.events_intraday_2023*`
  GROUP BY 1, 2, 3
), final_Intradata AS (
  SELECT
    event_date,
    event_name,
    CASE
      WHEN REGEXP_CONTAINS(medium, r'(none)') THEN 'Direct'
      WHEN REGEXP_CONTAINS(medium, r'(organic)') THEN 'Organic'
      WHEN REGEXP_CONTAINS(medium, r'(referral)') THEN 'Referral'
      WHEN REGEXP_CONTAINS(medium, r'(cpc|ppc|cpm)') THEN 'Paid Search/Display'
      WHEN REGEXP_CONTAINS(medium, r'(email)') THEN 'Email'
      WHEN REGEXP_CONTAINS(medium, r'(social)') THEN 'Organic Social'
      WHEN REGEXP_CONTAINS(medium, r'(video)') THEN 'Organic Video'
      WHEN medium IS NULL THEN 'Null Medium'
      ELSE 'Unassigned'
    END AS default_channel_grouping,
    total_users
  FROM
    Intradata
)
SELECT
  event_date,
  event_name,
  default_channel_grouping,
  SUM(total_users) AS total_users,
  SUM(CASE WHEN default_channel_grouping LIKE '%Paid%' THEN total_users ELSE 0 END) AS paid_users,
  SUM(CASE WHEN default_channel_grouping NOT LIKE '%Paid Search/Display%' THEN total_users ELSE 0 END) AS non_paid_users,
  SUM(CASE WHEN default_channel_grouping LIKE '%Paid%' AND event_name LIKE '%Registration Success%' THEN total_users ELSE 0 END) AS membership_paid_users,
  SUM(CASE WHEN default_channel_grouping NOT LIKE '%Paid Search/Display%' AND event_name LIKE '%Registration Success%' THEN total_users ELSE 0 END) AS membership_non_paid_users
FROM
  final_Intradata
GROUP BY
  event_date, event_name, default_channel_grouping;



--ambil intraday cmn 2 tahun (pake Union)
SELECT FORMAT_DATE('%d-%m-%Y', PARSE_DATE('%Y%m%d', event_date)) as event_date, user_pseudo_id, event_name
FROM `prod-nissan-indonesia.analytics_262674952.events_intraday_2024*`
WHERE event_date >= '20240101'

UNION ALL

SELECT FORMAT_DATE('%d-%m-%Y', PARSE_DATE('%Y%m%d', event_date))as event_date, user_pseudo_id, event_name
FROM `prod-nissan-indonesia.analytics_262674952.events_intraday_2023*`
WHERE event_date >= '20230301'
ORDER BY event_date ASC
