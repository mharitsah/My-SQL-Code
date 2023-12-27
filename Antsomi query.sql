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
, journey_a1_click_link_raw AS
(
SELECT  delivery.click_advertising.user_id,
        delivery.click_advertising.event_id,
        delivery.click_advertising.story_id,
        CASE
            WHEN delivery.click_advertising.click_url LIKE '%account/forget-password%' THEN 'Forget password'
            WHEN delivery.click_advertising.click_url LIKE '%/sg-en?%' OR delivery.click_advertising.click_url LIKE '%/sg-en' THEN 'Homepage'
            WHEN delivery.click_advertising.click_url LIKE '%/sg-en/contactus/contact-us%' THEN 'Download Catalogue'
            WHEN delivery.click_advertising.click_url LIKE '%/sg-en/account/sign-in%' THEN 'Log-in page'
            ELSE 'Social Media'
        END AS tracking_type,
        delivery.click_advertising.click_url AS location_url,
        DATE_TRUNC('Day', delivery.click_advertising.tracked_time) AS tracked_date
    FROM delivery.click_advertising
WHERE delivery.click_advertising.tracked_time BETWEEN timestamp '2023-11-20 00:00:00' AND DATE_TRUNC('Day', NOW())
AND delivery.click_advertising.story_id = '8172654'
)
SELECT  customer_user_.customer_id,
        journey_a1_click_link_raw.event_id,
        journey_a1_click_link_raw.story_id,
        journey_a1_click_link_raw.tracking_type,
        journey_a1_click_link_raw.location_url,
        journey_a1_click_link_raw.tracked_date
    FROM journey_a1_click_link_raw
    JOIN customer_user_ ON journey_a1_click_link_raw.user_id = customer_user_.user_id
--WHERE journey_a1_click_link_raw.tracking_type IS NOT NULL
