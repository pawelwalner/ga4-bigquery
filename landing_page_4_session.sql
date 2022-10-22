WITH _source as (
SELECT
  PARSE_DATE("%Y%m%d", event_date) as event_date,
  TIMESTAMP_MICROS(event_timestamp) as event_ts,

  MAX(CASE WHEN params.key = "ga_session_id" THEN params.value.int_value ELSE NULL END) OVER (PARTITION BY event_timestamp, user_pseudo_id) as ga_session_id,
  user_id,
  user_pseudo_id,
  TIMESTAMP_MICROS(user_first_touch_timestamp) as user_first_touch_ts,

  event_name,

  params.key as param_key,
  params.value.string_value as params_string_value,
  params.value.int_value as params_int_value,
  params.value.float_value as params_float_value,
  params.value.double_value as params_double_value,

  traffic_source.name as channel,
  traffic_source.medium as utm_medium,
  traffic_source.source as utm_source,

  geo.continent as continent,
  geo.country as country,
  geo.region as region,
  geo.city as city,

  device.category as device_category,
  device.mobile_brand_name as device_brand,
  device.mobile_model_name as device_model,
  device.operating_system as device_os,
  device.operating_system_version as device_os_version,

  device.language as device_language,
  device.is_limited_ad_tracking as device_is_limited_ad_tracking,

  device.web_info.browser as browser,
  device.web_info.browser_version as browser_version,

FROM `MY_SOURCE_DIRECT_DATE` as events
LEFT JOIN UNNEST(event_params) as params
--WHERE 
--   PARSE_DATE("%Y%m%d", _TABLE_SUFFIX) >= "2022-10-16"

), event_aggregated as (
SELECT 
  event_date,
  event_ts,
  ga_session_id,
  user_pseudo_id,
  user_first_touch_ts,
  event_name,

  MAX(utm_medium) as utm_medium,
  MAX(utm_source) as utm_source,

  MAX(CASE WHEN param_key = "page_title" THEN params_string_value ELSE NULL END) as page_title,
  MAX(CASE WHEN param_key = "page_location" THEN params_string_value ELSE NULL END) as page_location,
  MAX(CASE WHEN param_key = "page_referrer" THEN params_string_value ELSE NULL END) as page_referrer,
  MAX(CASE WHEN (param_key = "page_location" and event_name = "session_start") THEN params_string_value ELSE NULL END) as landing_page,


  MAX(continent) as continent,
  MAX(country) as country,
  MAX(region) as region,
  MAX(city) as city,

  MAX(device_category) as device_category,
  MAX(device_brand) as device_brand,
  MAX(device_model) as device_model,
  MAX(device_os_version) as device_os_version,
  MAX(device_language) as device_language,
  MAX(device_is_limited_ad_tracking) as device_is_limited_ad_tracking,

  MAX(browser) as browser,
  MAX(browser_version) as browser_version

FROM _source
GROUP BY 1,2,3,4,5,6
),
sessions as (
  SELECT
    ga_session_id,
    user_pseudo_id,
    MIN(event_date) as date,
    MAX(landing_page) as landing_page,
    
    MIN(user_first_touch_ts) as user_first_touch_ts,
    MAX(utm_medium) as utm_medium,
    MAX(utm_source) as utm_source,

    MAX(continent) as continent,
    MAX(country) as country,
    MAX(region) as region,
    MAX(city) as city,

    MAX(device_category) as device_category,
    MAX(device_brand) as device_brand,
    MAX(device_model) as device_model,
    MAX(device_os_version) as device_os_version,
    MAX(device_language) as device_language,
    MAX(device_is_limited_ad_tracking) as device_is_limited_ad_tracking,

    MAX(browser) as browser,
    MAX(browser_version) as browser_version
  FROM event_aggregated
  GROUP BY 1,2
  ORDER BY date DESC
)

SELECT * FROM sessions
