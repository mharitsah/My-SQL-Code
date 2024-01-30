-- INSERT INTO `prod-nissan-indonesia.leads_utm_v2.brochure_download_utm_matched_v2`
WITH max_time AS (
  SELECT DATE_SUB(current_date(), INTERVAL 1000 DAY) AS max_time_
),
filt_time AS(
SELECT 
    max_time_,
    TIMESTAMP(max_time_, 'Asia/Jakarta'),
    DATETIME(TIMESTAMP_ADD(TIMESTAMP(max_time_), INTERVAL 1 MICROSECOND)) AS filt_time_
FROM 
    max_time),
brochure_download_1 AS(
-- Acquire brochure_download data from the mongo db connection: nissan_leads_data.brochure_download
SELECT DISTINCT
    customer.fullname AS fullname,
    customer.city AS city,
    customer.phone AS phone,
    -- clean phone number
    REPLACE(REPLACE(REPLACE(TRIM(
    CASE 
        WHEN REGEXP_CONTAINS(customer.phone, ' ') THEN customer.phone
        WHEN SUBSTR(customer.phone,1,1)='0' THEN CONCAT('62',SUBSTR(customer.phone,2))
        WHEN SUBSTR(customer.phone,1,2)='62' THEN customer.phone
        WHEN SUBSTR(customer.phone,1,1)='+' THEN SUBSTR(customer.phone,2)
        WHEN SUBSTR(customer.phone,1,2)<>'62' THEN CONCAT('62',customer.phone)
    END),' ',''),'O',''),'-','') AS phone_2,
    customer.email,
    customer.estimated_buying_time,
    DATETIME(TIMESTAMP(created_at), 'Asia/Jakarta') AS created_at,
    car_name,
    -- deduplicate data
    ROW_NUMBER() OVER(
        PARTITION BY 
        car_name,
        customer.fullname,
        customer.phone,
        customer.email,
        customer.city,
        customer.estimated_buying_time,
        created_at
        ORDER BY 
        created_at ASC
    ) AS row_num
FROM `prod-nissan-indonesia.nissan_leads_data.brochure_download`
-- filter only take registration data at one day before run date
WHERE 
    DATETIME(TIMESTAMP(created_at), 'Asia/Jakarta') >= (SELECT filt_time_ FROM filt_time)
    AND 
    DATETIME(TIMESTAMP(created_at), 'Asia/Jakarta') < CURRENT_DATE()
ORDER BY created_at ASC),
brochure_download_2 AS (
    SELECT 
        fullname,
        city,
        -- further clean the phone number
        IF(SUBSTR(IF(SUBSTR(IF(SUBSTR(phone_2,1,3)='620','62',phone_2),1,2)<>'62',
        CONCAT('62',IF(SUBSTR(phone_2,1,3)='620','62',phone_2)),
        IF(SUBSTR(phone_2,1,3)='620','62',phone_2)),1,3)='620',
        CONCAT('62',SUBSTR(IF(SUBSTR(IF(SUBSTR(phone_2,1,3)='620','62',phone_2),1,2)<>'62',
        CONCAT('62',IF(SUBSTR(phone_2,1,3)='620','62',phone_2)),IF(SUBSTR(phone_2,1,3)='620','62',phone_2)),3)),
        IF(SUBSTR(IF(SUBSTR(phone_2,1,3)='620','62',phone_2),1,2)<>'62',CONCAT('62',IF(SUBSTR(phone_2,1,3)='620','62',phone_2)),
        IF(SUBSTR(phone_2,1,3)='620','62',phone_2))) AS phone,
        email,
        estimated_buying_time,
        created_at,
        car_name,
    FROM brochure_download_1
    -- part of deduplication process
    WHERE row_num = 1
),
ga_1 AS(
    -- take data from GA, to get the page, for acquiring utm information
    SELECT 
    GA.user_pseudo_id as user_pseudo_id,
    -- clean phone number from phone number mapping
    IF(SUBSTR(PH.phone_no,1,4)='6262',CONCAT('62',SUBSTR(PH.phone_no,5)),PH.phone_no) AS phone_no,
    DATETIME(TIMESTAMP_MICROS(event_timestamp), "Asia/Jakarta") AS log_time,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'page_location') AS page_location,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'ga_session_id') AS ga_session_id,
    (CASE WHEN event_name = 'dax_downloadbrochure_conversion' THEN 'conv' ELSE 'null' END) as conv_flag,
FROM 
    `prod-nissan-indonesia.analytics_262674952.events_intraday_*`AS GA
-- right join on phone number, to only get GA data of registered users
RIGHT JOIN
`prod-nissan-indonesia.mapping_phone.mapping_phoneno_tbl` AS PH
ON PH.user_pseudo_id = GA.user_pseudo_id
WHERE 
-- take only the data 1 day before run date
    -- PARSE_DATE('%Y%m%d', _table_suffix)= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    _table_suffix 
    BETWEEN 
        REGEXP_REPLACE(FORMAT_DATE("%F", (SELECT filt_time_ FROM filt_time)), "-","") 
        AND
        REGEXP_REPLACE(FORMAT_DATE("%F", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)), "-","")
ORDER BY 
    event_timestamp ASC
),
ga_utm AS (
    SELECT 
        user_pseudo_id,
        phone_no,
        log_time,
        ga_session_id,
        (CASE WHEN ga_session_id IN (SELECT ga_session_id FROM ga_1 WHERE conv_flag = 'conv') THEN 'conv' ELSE 'null' END) as conv_flag,
        -- acquire utm information from page_location
        CASE 
            WHEN REGEXP_CONTAINS(page_location, 'utm_source') 
                THEN SPLIT(SPLIT(page_location,'\?utm_source=')[SAFE_OFFSET(1)],'&utm_medium')[OFFSET(0)] 
            ELSE 
                '!Non-Paid'
        END AS utm_source,
    CASE 
            WHEN REGEXP_CONTAINS(page_location, 'utm_medium') 
                THEN SPLIT(SPLIT(page_location,'&utm_medium=')[SAFE_OFFSET(1)],'&utm_campaign')[OFFSET(0)]
        END AS utm_medium,
    CASE 
            WHEN REGEXP_CONTAINS(page_location, 'utm_campaign') 
                THEN SPLIT(SPLIT(page_location,'&utm_campaign=')[SAFE_OFFSET(1)], '&utm_content')[OFFSET(0)]
        END AS utm_campaign,
    CASE 
            WHEN REGEXP_CONTAINS(page_location, 'utm_content') 
                THEN SPLIT(SPLIT(page_location,'&utm_content=')[SAFE_OFFSET(1)], '&')[OFFSET(0)]
        END AS utm_content,
    FROM 
        ga_1
),
first_click_contribution AS (
  SELECT
    ga_session_id,
    phone_no,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    log_time,
    conv_flag,
    ROW_NUMBER() OVER (PARTITION BY ga_session_id, phone_no ORDER BY CASE WHEN utm_source = '!Non-Paid' THEN 1 ELSE 0 END, log_time ASC) AS RowNum
  FROM ga_utm
  WHERE conv_flag = 'conv'
),
ranked_utm AS (
  SELECT 
    ga_session_id, 
    phone_no, 
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    log_time,
    conv_flag
  FROM first_click_contribution
  WHERE RowNum = (SELECT MIN(RowNum) from first_click_contribution)
),
phone_utm AS (
    -- distinct set of phone number - utm information
    SELECT 
        ga_utm_grouped.phone_no,
        ga_utm_grouped.utm_source,
        -- ga_utm_grouped.ga_session_id,
        -- ga_utm_unique.conv_flag,
        ga_utm_unique.utm_medium,
        ga_utm_unique.utm_campaign,
        ga_utm_unique.utm_content,
        ga_utm_grouped.log_time
    FROM 
        (-- get maximum utm source, for case where Non-Paid and Ads is available, get maximum to take the Ads since the 
        -- Non Paid is written to be !Non-Paid
            SELECT 
            ga_session_id,
            phone_no,
            utm_source,
            utm_medium,
            utm_campaign,
            utm_content,
            log_time,
            FROM 
            ranked_utm) AS ga_utm_grouped
    LEFT JOIN 
        (SELECT DISTINCT phone_no, utm_source, utm_medium, utm_campaign, utm_content, ga_session_id, log_time FROM ranked_utm) AS ga_utm_unique 
    ON ga_utm_grouped.ga_session_id = ga_utm_unique.ga_session_id AND ga_utm_grouped.utm_source = ga_utm_unique.utm_source AND ga_utm_grouped.phone_no = ga_utm_unique.phone_no AND ga_utm_grouped.utm_content = ga_utm_unique.utm_content AND ga_utm_grouped.utm_medium = ga_utm_unique.utm_medium AND ga_utm_grouped.utm_campaign = ga_utm_unique.utm_campaign
AND ga_utm_grouped.log_time = ga_utm_unique.log_time    
),
mapping_result AS (
    SELECT DISTINCT
    fullname,
    city,
    phone,
    email,
    estimated_buying_time,
    created_at,
    car_name,
    CASE 
        -- clean !Non-Paid into Non-Paid
        WHEN phone_utm.utm_source = '!Non-Paid' THEN 'Non-Paid'
        ELSE phone_utm.utm_source
    END AS utm_source,
    phone_utm.utm_medium,
    phone_utm.utm_campaign,
    phone_utm.utm_content,
    log_time
FROM brochure_download_2
LEFT JOIN
phone_utm
ON brochure_download_2.phone = phone_utm.phone_no
ORDER BY 
    created_at ASC
),
from_utm_table AS (
    SELECT DISTINCT
    fullname,
    city,
    phone,
    email,
    estimated_buying_time,
    created_at,
    car_name,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    FROM 
    `prod-nissan-indonesia.leads_utm_test.download_brochure_utm_matched`
    WHERE
    DATETIME(TIMESTAMP(created_at), 'Asia/Jakarta') >= (SELECT filt_time_ FROM filt_time)
    AND 
    DATETIME(TIMESTAMP(created_at), 'Asia/Jakarta') < CURRENT_DATE()
),
final_group_result AS (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY 
    fullname,
    city,
    phone,
    email,
    estimated_buying_time,
    created_at,
    car_name
    ORDER BY log_time DESC) AS RowNum
FROM mapping_result)
SELECT 
    fullname,
    city,
    phone,
    email,
    estimated_buying_time,
    created_at,
    car_name,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
 from final_group_result as m
WHERE
RowNum = 1
AND
NOT EXISTS (
    SELECT 1
    FROM from_utm_table AS u
    WHERE 
        m.fullname = u.fullname
        AND m.city = u.city
        AND m.phone = u.phone
        AND m.email = u.email
        AND m.created_at = u.created_at
        AND m.car_name = u.car_name
)
ORDER BY created_at ASC;