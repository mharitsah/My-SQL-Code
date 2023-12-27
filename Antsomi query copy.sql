WITH customer_user_signin AS #This temp-table is used for enriching data user_id linked to customer_id
(
SELECT  website.sign_in_user.user_id,
        website.sign_in_user.customer_id
    FROM website.sign_in_user
GROUP BY website.sign_in_user.user_id,
        website.sign_in_user.customer_id
)
, customer_user_ AS
(
SELECT  COALESCE(bos.customer_users.user_id, customer_user_signin.user_id) AS user_id,
        COALESCE(bos.customer_users.customer_id, customer_user_signin.customer_id) AS customer_id
    FROM bos.customer_users #the table bos.customer_users is the default table used as the bridge-table that links customer_id with user_id (of website activities)
    FULL OUTER JOIN customer_user_signin ON bos.customer_users.user_id = customer_user_signin.user_id
)
, journey_a1_click_link AS
(
SELECT  ats.journey_alwayson_reactivation_30_nov_2023.customer_id,
        ats.journey_alwayson_reactivation_30_nov_2023.event_id,
        ats.journey_alwayson_reactivation_30_nov_2023.story_id,
        ats.journey_alwayson_reactivation_30_nov_2023.tracking_type,
        ats.journey_alwayson_reactivation_30_nov_2023.location_url,
        ats.journey_alwayson_reactivation_30_nov_2023.tracked_date
    FROM ats.journey_alwayson_reactivation_30_nov_2023
--JOIN customer_user_ ON journey_a1_click_link.user_id = customer_user_.user_id
--WHERE journey_a1_click_link_raw.tracking_type IS NOT NULL
)
, customer_active AS
(
SELECT  ats.customer_active_29_nov_2023.customer_id,
        ats.customer_active_29_nov_2023.story_id
    FROM ats.customer_active_29_nov_2023
GROUP BY ats.customer_active_29_nov_2023.customer_id,
        ats.customer_active_29_nov_2023.story_id
)
, only_forget_user AS
(
SELECT journey_a1_click_link.customer_id, journey_a1_click_link.tracking_type
FROM journey_a1_click_link
INNER JOIN customer_active ON journey_a1_click_link.customer_id = customer_active.customer_id
WHERE journey_a1_click_link.tracking_type = 'Forget password' AND 
    journey_a1_click_link.customer_id NOT IN (
        SELECT DISTINCT journey_a1_click_link.customer_id
        FROM journey_a1_click_link
        WHERE journey_a1_click_link.tracking_type <> 'Forget password')
)
, only_homepage_user AS
(
SELECT journey_a1_click_link.customer_id, journey_a1_click_link.tracking_type
FROM journey_a1_click_link
INNER JOIN customer_active ON journey_a1_click_link.customer_id = customer_active.customer_id
WHERE journey_a1_click_link.tracking_type = 'Homepage' AND 
    journey_a1_click_link.customer_id NOT IN (
        SELECT DISTINCT journey_a1_click_link.customer_id
        FROM journey_a1_click_link
        WHERE journey_a1_click_link.tracking_type <> 'Homepage')
)
, only_download_user AS
(
SELECT journey_a1_click_link.customer_id, journey_a1_click_link.tracking_type
FROM journey_a1_click_link
INNER JOIN customer_active ON journey_a1_click_link.customer_id = customer_active.customer_id
WHERE journey_a1_click_link.tracking_type = 'Download Catalogue' AND 
    journey_a1_click_link.customer_id NOT IN (
        SELECT DISTINCT journey_a1_click_link.customer_id
        FROM journey_a1_click_link
        WHERE journey_a1_click_link.tracking_type <> 'Download Catalogue')
)
, only_login_user AS
(
SELECT journey_a1_click_link.customer_id, journey_a1_click_link.tracking_type
FROM journey_a1_click_link
INNER JOIN customer_active ON journey_a1_click_link.customer_id = customer_active.customer_id
WHERE journey_a1_click_link.tracking_type = 'Log-in page' AND 
    journey_a1_click_link.customer_id NOT IN (
        SELECT DISTINCT journey_a1_click_link.customer_id
        FROM journey_a1_click_link
        WHERE journey_a1_click_link.tracking_type <> 'Log-in page')
)
, only_sosmed_user AS
(
SELECT journey_a1_click_link.customer_id, journey_a1_click_link.tracking_type
FROM journey_a1_click_link
INNER JOIN customer_active ON journey_a1_click_link.customer_id = customer_active.customer_id
WHERE journey_a1_click_link.tracking_type = 'Social Media' AND 
    journey_a1_click_link.customer_id NOT IN (
        SELECT DISTINCT journey_a1_click_link.customer_id
        FROM journey_a1_click_link
        WHERE journey_a1_click_link.tracking_type <> 'Social Media')
)
, distinct_each_category AS
(
SELECT DISTINCT journey_a1_click_link.customer_id, journey_a1_click_link.tracking_type
FROM journey_a1_click_link
INNER JOIN customer_active ON journey_a1_click_link.customer_id = customer_active.customer_id
WHERE journey_a1_click_link.tracking_type = 'Social Media'
UNION ALL
SELECT DISTINCT journey_a1_click_link.customer_id, journey_a1_click_link.tracking_type
FROM journey_a1_click_link
INNER JOIN customer_active ON journey_a1_click_link.customer_id = customer_active.customer_id
WHERE journey_a1_click_link.tracking_type = 'Log-in page'
UNION ALL
SELECT DISTINCT journey_a1_click_link.customer_id, journey_a1_click_link.tracking_type
FROM journey_a1_click_link
INNER JOIN customer_active ON journey_a1_click_link.customer_id = customer_active.customer_id
WHERE journey_a1_click_link.tracking_type = 'Download Catalogue'
UNION ALL
SELECT DISTINCT journey_a1_click_link.customer_id, journey_a1_click_link.tracking_type
FROM journey_a1_click_link
INNER JOIN customer_active ON journey_a1_click_link.customer_id = customer_active.customer_id
WHERE journey_a1_click_link.tracking_type = 'Forget password'
UNION ALL
SELECT DISTINCT journey_a1_click_link.customer_id, journey_a1_click_link.tracking_type
FROM journey_a1_click_link
INNER JOIN customer_active ON journey_a1_click_link.customer_id = customer_active.customer_id
WHERE journey_a1_click_link.tracking_type = 'Homepage'
)
, check_multiple AS
(
SELECT distinct_each_category.customer_id, COUNT(distinct_each_category.customer_id) AS count_user
FROM distinct_each_category
GROUP BY 1
)
, multiple_visit_user AS
(
SELECT check_multiple.customer_id, 'Multiple Visit' AS tracking_type
FROM check_multiple
WHERE check_multiple.count_user > 1
)
, all_active_customer_id AS
(
SELECT only_forget_user.customer_id, only_forget_user.tracking_type
FROM only_forget_user
UNION ALL
SELECT only_homepage_user.customer_id, only_homepage_user.tracking_type
FROM only_homepage_user
UNION ALL
SELECT only_download_user.customer_id, only_download_user.tracking_type
FROM only_download_user
UNION ALL
SELECT only_login_user.customer_id, only_login_user.tracking_type
FROM only_login_user
UNION ALL
SELECT only_sosmed_user.customer_id, only_sosmed_user.tracking_type
FROM only_sosmed_user
UNION ALL
SELECT multiple_visit_user.customer_id, multiple_visit_user.tracking_type
FROM multiple_visit_user
)
SELECT all_active_customer_id.tracking_type, COUNT(DISTINCT all_active_customer_id.customer_id) AS total_active_user
FROM all_active_customer_id
INNER JOIN customer_active ON customer_active.customer_id = all_active_customer_id.customer_id
GROUP BY 1
