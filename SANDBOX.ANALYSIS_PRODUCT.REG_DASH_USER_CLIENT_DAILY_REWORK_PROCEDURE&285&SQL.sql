CREATE PROCEDURE SANDBOX.ANALYSIS_PRODUCT.REG_DASH_USER_CLIENT_DAILY_REWORK_PROCEDURE()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
AS
$$


 begin

 DELETE FROM SANDBOX.ANALYSIS_PRODUCT.REGISTRATION_DASHBOARD_USER_CLIENT_DAILY_REWORK WHERE date >= (current_date() - 8);

-- --------------------------------------------------------------------------------------------------------------------------------------------------
INSERT INTO REGISTRATION_DASHBOARD_USER_CLIENT_DAILY_REWORK

WITH dates as (
    SELECT (current_date() - 8) as start_dt,
        (current_date() - 1) as end_dt
),

reg_user_clients as (
select
    a.user_key,
    a.client_key,
    a.client_id,
    a.first_time_user_client_pair_utc,
    a.registration_client_ind,
    b.entitlement_name,
    b.entitlement_start_timestamp,
    case when a.client_key = b.client_key then 1 else 0 end as entitlement_registrant,
    min(a.first_time_user_client_pair_utc) OVER (PARTITION BY a.user_key) as signup_date, -- for each user_key, gather the first time it was seen e.g. signup date
    min(a.first_time_user_client_pair_utc) OVER (PARTITION BY a.client_key) as first_reg_date,
    row_number() over (partition by a.user_key order by a.first_time_user_client_pair_utc asc) as client_order,-- for each client_key, gather the first time it was seen with any user_key e.g. first_ever_reg_date
    case when a.registration_client_ind = 1 then 1 when client_order = 1 then 1 else 0 end as adjusted_registration_client_ind --- use to classify each client + user id combination as signup device or not
from
    "BI"."PLUTO_DW"."CLIENT_USER_MAPPING_VW" a
left join
    "BI"."PLUTO_DW"."USER_ENTITLEMENTS_VW" b
on
    a.user_key = b.user_key

group by 1,2,3,4,5,6,7,8
)

--- session level data per client. needed to determine if a client was an active user on a given day, session length >=15 seconds
---sessions as (
select
    date_trunc('day',a.video_segment_begin_utc) as date,
    a.user_key,
    a.client_key,
    a.odin_app_name,
    a.app_platform,
    a.session_id,
    d.client_first_seen_utc,
    case when a.international_ind = 'FALSE' then 'DOMESTIC'
        else 'INTERNATIONAL' end as region_type,
    case when rc.country = 'BR' then rc.country 
        else rc.region end as region,
    rc.country,
    case when rc.country = 'US' then 'DOMESTIC'
        else 'INTERNATIONAL' end as region_type_alt,
    case when date = date_trunc('day',d.client_first_seen_utc) then 1 else 0 end as new_user_flag, -- if client first seen = current day, user is considered new for the day
    
    case when b.client_key is not null then b.first_reg_date end as reg_date, -- grabs the minimum existing timestamp the given client_id was first associated with a registered account as long as it was on or before the given day
    case when reg_date is not null then 1 else 0 end as reg_flag, --- if there is a reg timestamp from the calc above, the user is considered registered for the day
    case when date = date_trunc('day',reg_date) then 1 else 0 end as new_reg_client_flag, -- if the reg timestamp from the calc above = current day, then user is considered new reg for the day
    ---case when date = date_trunc('day',reg_date) and b.adjusted_registration_client_ind = 1 then 1 else 0 end as new_reg_signup_client_flag,
     case when date = date_trunc('day',reg_date) and (b.registration_client_ind = 1 or client_order = 1) then 1 else 0 end as new_reg_signup_client_flag_adjusted,
     
     case when date = date_Trunc('day',b.signup_date) then 1 else 0 end as net_new_device_flag,
     case when date = date_Trunc('day',b.signup_date) and b.adjusted_registration_client_ind = 1 then 1 else 0 end as net_new_device_signup_flag,
     case when date = date_Trunc('day',b.signup_date) and b.adjusted_registration_client_ind = 0 then 1 else 0 end as net_new_device_signin_flag,
     
    case when a.user_key = b.user_key then b.user_key end as user_key_match,
    max_by(b.user_key,b.first_time_user_client_pair_utc) as last_paired_user_key,
    case when user_key_match is not null then user_key_match
         when user_key_match is null then last_paired_user_key end as adjusted_user_key,
         
    case when b.entitlement_name = 'walmart' and date >= '2023-04-27' and date_trunc('day',b.entitlement_start_timestamp) <= date and b.entitlement_registrant = 1 then 1 else 0 end as wm_entitled_registrant_flag,
    case when b.entitlement_name = 'walmart' and date >= '2023-04-27' and date_trunc('day',b.entitlement_start_timestamp) = date and b.entitlement_registrant = 1 then 1 else 0 end as wm_newly_entitled_registrant_flag,
    case when  b.entitlement_name = 'tmobile' and date_trunc('day',b.entitlement_start_timestamp) <= date and b.entitlement_registrant = 1 then 1 else 0 end as tmo_entitled_registrant_flag,
    case when  b.entitlement_name = 'tmobile' and date_trunc('day',b.entitlement_start_timestamp) = date and b.entitlement_registrant = 1 then 1 else 0 end as tmo_newly_entitled_registrant_flag,
    case when b.entitlement_name = 'walmart' and date >= '2023-04-27' and date_trunc('day',b.entitlement_start_timestamp) <= date then 1 else 0 end as wm_entitled_client_flag,
    case when b.entitlement_name = 'walmart' and date >= '2023-04-27' and date_trunc('day',b.entitlement_start_timestamp) = date  then 1 else 0 end as wm_newly_entitled_client_flag,
    case when  b.entitlement_name = 'tmobile' and date_trunc('day',b.entitlement_start_timestamp) <= date then 1 else 0 end as tmo_entitled_client_flag,
    case when  b.entitlement_name = 'tmobile' and date_trunc('day',b.entitlement_start_timestamp) = date then 1 else 0 end as tmo_newly_entitled_client_flag,
    
    sum(sum(a.total_viewing_seconds)) over (partition by a.session_id) as session_tvs,
    sum(a.total_viewing_seconds)/60 as tvms,
    sum(case when reg_date is not null then a.total_viewing_seconds end)/60 as reg_tvms,
    sum(case when a.loggedin_status_flag = 1 
        and a.fixed_app_version_feature_type = 1 
        then a.total_viewing_seconds end)/60 as loggedin_tvms,
    sum(case when a.channel_id = 'vod' and date >= '2023-04-27' and v.vod_category_id = '62869f9451e959000777d2e7' then a.total_viewing_seconds end)/60 as wm_exclusive_tvms,
    sum(case when a.channel_id = 'vod' and v.vod_category_id = '61a56207b7672a0007b5315f' then a.total_viewing_seconds end)/60 as tmo_exclusive_tvms
--- need to add sum for entitled content tvms here with content ids
from
    "BI"."PLUTO_DW"."USER_VIDEO_SEGMENT_FACT_VW" a
join
    "BI"."PLUTO_DW"."CLIENT_DIM_VW" c
on
    a.client_key = c.client_key
join
    "BI"."PLUTO_DW"."ALL_CLIENT_FIRST_SEEN_VW" d
on
    c.client_id = d.client_id
left join
    reg_user_clients b
on
    a.client_key = b.client_key
and
    date_trunc('day',b.first_time_user_client_pair_utc)<= date_trunc('day',a.video_segment_begin_utc) 
join
    "BI_UAT"."PLUTO_DW"."GEO_DIM_VW" g
on
    a.geo_key = g.geo_key
join
    (select country,region from "BI"."REFERENCE"."DEVICE_COUNTRY_MAPPING_VW" group by all) rc
on
    g.country_code = rc.country
left join (select
              ep.episode_key,
              ep.episode_id,
              ep.series_id,
              v.vod_category_id
           from
               "BI"."PLUTO_DW"."EPISODE_DIM_VW" ep 
           join "ODIN_PRD"."DW_ODIN"."CMS_VODCATEGORYENTRIES_DIM" v
               on ep.episode_id = v.episode_id or ep.series_id = v.series_id
           where
               v.vod_category_id in ('62869f9451e959000777d2e7','61a56207b7672a0007b5315f')
           group by all
           ) v on a.episode_key = v.episode_key

where
    date_trunc('day', a.VIDEO_SEGMENT_BEGIN_UTC) between (select start_dt from dates) and (select end_dt from dates)
    and a.GEO_ALIGNED_FLAG = True
	And a.FIXED_APP_VERSION = 1
    and a.live_flag = 'FALSE' --- filters out live_flag sessions/devices
    and a.odin_app_name not in ('marriott','androidmobilehuawei','airplay','vidaahisense','androidmobiletelecomar','stbverizon','viziowatchfree','lifefitness', --- filters out additional built-in devices as well as well as some o&o devices with insignificant share of reg devices 
                               'na','virginmedia','androidtvdeutschetelekom','facebook','nowtv','chromecast','rokuuk','androidtvdirectv','catalyst','androidtvhilton','watchfreeplus','androidtvlive',
                               'androidtvlivetivo','androidtvliveverizon','firetvlive','firetvliveverizon','googletvlive','lgchannels','viziowatchfree','samsungtvplus','rokuchannel'
                                )
group by ALL
qualify session_tvs >= 15
and session_tvs <=60000000;

return 'REGISTRATION_DASHBOARD_USER_CLIENT_DAILY_REWORK';
end;

$$