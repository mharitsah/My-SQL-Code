SELECT main.date, main.campaign_name, main.impressions, main.clicks_all, subquery.sum_impressions, subquery.count_impressions
FROM `dax-dmp.citroen_supermetrics.fb` AS main
JOIN (
    SELECT campaign_name, SUM(impressions) as sum_impressions, COUNT(impressions) as count_impressions
    FROM `dax-dmp.citroen_supermetrics.fb`
    WHERE date BETWEEN '2023-08-01' AND '2023-08-07'
    GROUP BY campaign_name
) AS subquery
ON main.campaign_name = subquery.campaign_name
WHERE main.date BETWEEN '2023-08-01' AND '2023-08-07'
ORDER BY main.date asc, main.campaign_name asc;



-- query untuk menampilkan data conversion di BQ berdasarkan utm campaign_name
WITH test_drive AS
(
  SELECT *
  FROM `prod-nissan-indonesia.leads_utm_v2.test_drive_utm_matched_v2`
),
download_brochure AS
(
  SELECT *
  FROM `prod-nissan-indonesia.leads_utm_v2.download_brochure_utm_matched_v2`
  SUBSTR()
),
offer AS
(
  SELECT *
  FROM `prod-nissan-indonesia.leads_utm_v2.offer_utm_matched_v2`
),
quote AS
(
  SELECT *
  FROM `prod-nissan-indonesia.leads_utm_v2.quote_utm_matched_v2`
),
count_leads AS (
SELECT 'test drive' as lead_type, COUNT(*) as lead_count
FROM test_drive
WHERE utm_campaign = 'Terra_Aug_Sep_2023_FBIG_Conversion_Leads_Form' AND created_at BETWEEN '2023-08-01' AND '2023-08-31'
UNION ALL
SELECT 'download brochure' as lead_type, COUNT(*) as lead_count
FROM download_brochure
WHERE utm_campaign = 'Terra_Aug_Sep_2023_FBIG_Conversion_Leads_Form' AND created_at BETWEEN '2023-08-01' AND '2023-08-31'
UNION ALL
SELECT 'offer' as lead_type, COUNT(*) as lead_count
FROM offer
WHERE utm_campaign = 'Terra_Aug_Sep_2023_FBIG_Conversion_Leads_Form' AND created_at BETWEEN '2023-08-01' AND '2023-08-31'
UNION ALL
SELECT 'quote' as lead_type, COUNT(*) as lead_count
FROM quote
WHERE utm_campaign = 'Terra_Aug_Sep_2023_FBIG_Conversion_Leads_Form' AND created_at BETWEEN '2023-08-01' AND '2023-08-31')
SELECT *
FROM count_leads