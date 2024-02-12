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
  --then total_users berarti mengembalikan jumlah user yang berasal dari channel paid, jika then 1 maka belum tentu benar karena jika angka paid 5 orang dan malah dihitung then 1 maka 4 orang user akan hilang.
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

--Select event_params in BQ
SELECT
  DISTINCT
  traffic_source.medium as traffic_source_medium,
  (select value.string_value from unnest(event_params) where key = 'medium') as medium_param,
  collected_traffic_source.manual_medium as manual_medium,
  collected_traffic_source.gclid as source_gclid, 
  collected_traffic_source.dclid as source_dclid, 
  collected_traffic_source.srsltid as source_srsltid
  FROM
  `mitutoyo-production.analytics_282716579.events_intraday_2023*`


--fungsi extrack tanggal
SELECT c.date, 
min(c.date),
extract(YEAR FROM c.date) as YEAR,
extract(MONTH FROM c.date) as month,
extract(DAY FROM c.date) as day,
c.campaign_name, 
n.frequency
FROM (SELECT date, campaign_name, impressions from dax-dmp.citroen_supermetrics.fb) as c
INNER JOIN (select date, frequency from dax-dmp.nissan_fb_ads.fb) as n USING(date)
where c.impressions >= 20
group by 1,3,4,5,6,7


--soal Shopee SQL
--SOAL1
SELECT CITY, SUM(ORDER) AS TOTAL ORDER, MIN(ORDER_ID) as first_order --cukup gunakan min() untuk mengambil first order)
FROM CUSTOMER AS C
JOIN ORDERS USING(customer_id)
GROUP BY 1

--SOAL TEST DI BQ
SELECT 
  FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d',event_date)) as event_date,
  event_name,
  (select value.string_value from unnest(event_params) where key = 'page_location') as page_location,
  min(user_pseudo_id) as user_id,
FROM `prod-nissan-indonesia.analytics_262674952.events_intraday_202401*`
group by 1,2,3

UNION ALL

SELECT 
  FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d',event_date)) as event_date,
  event_name,
  (select value.string_value from unnest(event_params) where key = 'page_location') as page_location,
  min(user_pseudo_id) as user_id,
FROM `prod-nissan-indonesia.analytics_262674952.events_intraday_202312*`
group by 1,2,3
order by user_id asc

--Test BQ 2
with intradata as 
(SELECT 
  FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d',event_date)) as event_date,
  event_name,
  (select value.string_value from unnest(event_params) where key = 'page_location') as page_location,
  min(user_pseudo_id) as user_id,
FROM `prod-nissan-indonesia.analytics_262674952.events_intraday_202401*`
group by 1,2,3)

select 
  event_date,
  max(
  case
    when regexp_contains(page_location, r'(serena|terra)') THEN 'Type 1'
    when regexp_contains(page_location, r'(magnite|kicks)') THEN 'Type 2'
  ELSE 'Type 3'
  END) as type_of_car
    FROM intradata
  group by 1
  ORDER BY event_date