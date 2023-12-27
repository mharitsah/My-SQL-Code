WITH sign_in as (SELECT DISTINCT website.membership_sign_in.customer_id FROM website.membership_sign_in 
WHERE website.membership_sign_in.tracked_time >= timestamp '2023-06-13 00:00:00' 
AND website.membership_sign_in.tracked_time <= timestamp '2023-07-06 00:00:00'), 
product_view as (SELECT DISTINCT website.view_product.customer_id FROM website.view_product
WHERE website.view_product.tracked_time >= timestamp '2023-06-13 00:00:00' 
AND website.view_product.tracked_time <= timestamp '2023-07-06 00:00:00')
select DISTINCT sign_in.customer_id, c.email FROM sign_in
LEFT JOIN bos.customers as c ON pv.customer_id = c.customer_id
LEFT JOIN product_view as pv ON pv.customer_id = sign_in.customer_id

SELECT DISTINCT c.email
FROM bos.customers AS c
JOIN website.membership_sign_in AS msi ON c.customer_id = msi.customer_id
WHERE msi.tracked_time >= timestamp '2023-06-13 00:00:00' and msi.tracked_time <= timestamp '2023-07-06 00:00:00'


WITH sign_in as (SELECT DISTINCT website.membership_sign_in.customer_id FROM website.membership_sign_in 
WHERE website.membership_sign_in.tracked_time > timestamp '2023-06-30 00:00:00' 
AND website.membership_sign_in.tracked_time < timestamp '2023-08-01 00:00:00')
, 
product_view as (SELECT DISTINCT website.view_product.customer_id FROM website.view_product
WHERE website.view_product.tracked_time > timestamp '2023-06-30 00:00:00' 
AND website.view_product.tracked_time < timestamp '2023-08-01 00:00:00')
,
clicks_engagements as (SELECT DISTINCT website.clicks_and_engagements.customer_id FROM website.clicks_and_engagements
WHERE website.clicks_and_engagements.tracked_time > timestamp '2023-06-30 00:00:00'
AND website.clicks_and_engagements.tracked_time < timestamp '2023-08-01 00:00:00')
,
join_three as (select DISTINCT pv.customer_id FROM product_view as pv
FULL OUTER JOIN sign_in ON pv.customer_id = sign_in.customer_id
FULL OUTER JOIN clicks_engagements ON pv.customer_id = clicks_engagements.customer_id)
SELECT DISTINCT c.email, c.customer_id from bos.customers as c
INNER JOIN join_three as jt ON c.customer_id = jt.customer_id

WHERE DATE(cr.tracked_time) = DATE '2023-08-22';







WITH sign_in as (SELECT DISTINCT website.membership_sign_in.customer_id FROM website.membership_sign_in 
WHERE website.membership_sign_in.tracked_time > timestamp '2023-06-30 00:00:00' 
AND website.membership_sign_in.tracked_time < timestamp '2023-08-01 00:00:00')
, 
product_view as (SELECT DISTINCT website.view_product.customer_id FROM website.view_product
WHERE website.view_product.tracked_time > timestamp '2023-06-30 00:00:00' 
AND website.view_product.tracked_time < timestamp '2023-08-01 00:00:00')
,
clicks_engagements as (SELECT DISTINCT website.clicks_and_engagements.customer_id FROM website.clicks_and_engagements
WHERE website.clicks_and_engagements.tracked_time > timestamp '2023-06-30 00:00:00'
AND website.clicks_and_engagements.tracked_time < timestamp '2023-08-01 00:00:00')
,
join_three AS (
  SELECT DISTINCT COALESCE(msi.customer_id, pv.customer_id, ce.customer_id) AS customer_id
  FROM sign_in AS msi
  FULL OUTER JOIN product_view AS pv ON msi.customer_id = pv.customer_id
  FULL OUTER JOIN clicks_engagements AS ce ON msi.customer_id = ce.customer_id
)
SELECT DISTINCT COALESCE(c.customer_id, jt.customer_id) AS customer_id
FROM bos.customers AS c
INNER JOIN join_three AS jt ON c.customer_id = jt.customer_id;






WITH sign_in as (SELECT DISTINCT website.membership_sign_in.customer_id FROM website.membership_sign_in 
WHERE website.membership_sign_in.tracked_time BETWEEN timestamp '2023-07-08 00:00:00'
AND timestamp '2023-08-07 00:00:00')
, 
product_view as (SELECT DISTINCT website.view_product.customer_id FROM website.view_product
WHERE website.view_product.tracked_time BETWEEN timestamp '2023-07-08 00:00:00'
AND timestamp '2023-08-07 00:00:00')
,
clicks_engagements as (SELECT DISTINCT website.clicks_and_engagements.customer_id FROM website.clicks_and_engagements
WHERE website.clicks_and_engagements.tracked_time BETWEEN timestamp '2023-07-08 00:00:00'
AND timestamp '2023-08-07 00:00:00')
,
join_three AS (
  SELECT DISTINCT COALESCE(msi.customer_id, pv.customer_id, ce.customer_id) AS customer_id
  FROM sign_in AS msi
  FULL OUTER JOIN product_view AS pv ON msi.customer_id = pv.customer_id
  FULL OUTER JOIN clicks_engagements AS ce ON msi.customer_id = ce.customer_id
)
SELECT DISTINCT c.email FROM bos.customers AS c
LEFT JOIN join_three AS jt ON c.customer_id = jt.customer_id
WHERE jt.customer_id IS NULL
