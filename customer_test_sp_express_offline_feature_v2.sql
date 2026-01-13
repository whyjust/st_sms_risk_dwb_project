
set tez.am.resource.memory.mb=8192;
set tez.am.resource.cpu.vcores=4;
set hive.tez.container.size=16384;
set tez.container.max.java.heap.size=12288;
-- CREATE TABLE sms_bd_data.customer_test_offline_express_feature_2026010901 AS
WITH cus_sample AS (
    SELECT 
        phone as mobile_md5,
        the_date as back_date
    FROM sms_bd_data.customer_test_sample_id
    group by phone, the_date
),customer_dwb_express AS (
    select
        mobile_md5, 
        back_date, 
        sign, 
        event_type,
        event_time
    from(
        select
            cus_smp.mobile_md5,
            cus_smp.back_date,
            dwb_express.sign,
            dwb_express.event_type,
            dwb_express.event_time
        from cus_sample cus_smp
        left join(
            select
                phone,
                sign,
                event_type,
                from_unixtime(unix_timestamp(the_date, 'yyyyMMdd'), 'yyyy-MM-dd') as event_time
            from sms_bd_data.sms_dwb_express_event_fdt
        )dwb_express
        on cus_smp.mobile_md5=dwb_express.phone 
        where cus_smp.back_date > dwb_express.event_time and dwb_express.event_time >= date_sub(cus_smp.back_date, 360)
    )cus_dwb
    group by mobile_md5, back_date, sign, event_type, event_time
), base_express_layer AS (
    SELECT 
        mobile_md5,
        back_date,
        sign,
        event_type,
        dt_tag,
        count(mobile_md5) as times,
        concat_ws(',',collect_set(sign)) as sign_concat,
        min(event_time) as min_event_time,
        max(event_time) as max_event_time
    FROM(
        SELECT
            mobile_md5,
            back_date,
            sign,
            event_type,
            event_time,
            -- 严格保留15天一组的时间分桶逻辑，不做任何改动
            CASE WHEN DATEDIFF(back_date, event_time) > 0 AND DATEDIFF(back_date, event_time) <= 15 THEN 1
                WHEN DATEDIFF(back_date, event_time) > 15 AND DATEDIFF(back_date, event_time) <= 30 THEN 2
                WHEN DATEDIFF(back_date, event_time) > 30 AND DATEDIFF(back_date, event_time) <= 45 THEN 3
                WHEN DATEDIFF(back_date, event_time) > 45 AND DATEDIFF(back_date, event_time) <= 60 THEN 4
                WHEN DATEDIFF(back_date, event_time) > 60 AND DATEDIFF(back_date, event_time) <= 75 THEN 5
                WHEN DATEDIFF(back_date, event_time) > 75 AND DATEDIFF(back_date, event_time) <= 90 THEN 6
                WHEN DATEDIFF(back_date, event_time) > 90 AND DATEDIFF(back_date, event_time) <= 105 THEN 7
                WHEN DATEDIFF(back_date, event_time) > 105 AND DATEDIFF(back_date, event_time) <= 120 THEN 8
                WHEN DATEDIFF(back_date, event_time) > 120 AND DATEDIFF(back_date, event_time) <= 135 THEN 9
                WHEN DATEDIFF(back_date, event_time) > 135 AND DATEDIFF(back_date, event_time) <= 150 THEN 10
                WHEN DATEDIFF(back_date, event_time) > 150 AND DATEDIFF(back_date, event_time) <= 165 THEN 11
                WHEN DATEDIFF(back_date, event_time) > 165 AND DATEDIFF(back_date, event_time) <= 180 THEN 12
                WHEN DATEDIFF(back_date, event_time) > 180 AND DATEDIFF(back_date, event_time) <= 195 THEN 13
                WHEN DATEDIFF(back_date, event_time) > 195 AND DATEDIFF(back_date, event_time) <= 210 THEN 14
                WHEN DATEDIFF(back_date, event_time) > 210 AND DATEDIFF(back_date, event_time) <= 225 THEN 15
                WHEN DATEDIFF(back_date, event_time) > 225 AND DATEDIFF(back_date, event_time) <= 240 THEN 16
                WHEN DATEDIFF(back_date, event_time) > 240 AND DATEDIFF(back_date, event_time) <= 255 THEN 17
                WHEN DATEDIFF(back_date, event_time) > 255 AND DATEDIFF(back_date, event_time) <= 270 THEN 18
                WHEN DATEDIFF(back_date, event_time) > 270 AND DATEDIFF(back_date, event_time) <= 285 THEN 19
                WHEN DATEDIFF(back_date, event_time) > 285 AND DATEDIFF(back_date, event_time) <= 300 THEN 20
                WHEN DATEDIFF(back_date, event_time) > 300 AND DATEDIFF(back_date, event_time) <= 315 THEN 21
                WHEN DATEDIFF(back_date, event_time) > 315 AND DATEDIFF(back_date, event_time) <= 330 THEN 22
                WHEN DATEDIFF(back_date, event_time) > 330 AND DATEDIFF(back_date, event_time) <= 345 THEN 23
                ELSE 24 END AS dt_tag
        FROM customer_dwb_express
        WHERE sign IS NOT NULL
    )customer_dwb_express 
    GROUP BY mobile_md5, back_date, sign, event_type, dt_tag
),express_times_stats AS (
    SELECT
        mobile_md5,
        back_date,
        SUM(CASE WHEN dt_tag <= 1 THEN times ELSE 0 END) AS sp_express_total_times_15d,
        SUM(CASE WHEN dt_tag <= 2 THEN times ELSE 0 END) AS sp_express_total_times_30d,
        SUM(CASE WHEN dt_tag <= 6 THEN times ELSE 0 END) AS sp_express_total_times_90d,
        SUM(CASE WHEN dt_tag <= 12 THEN times ELSE 0 END) AS sp_express_total_times_180d,
        SUM(CASE WHEN dt_tag <= 24 THEN times ELSE 0 END) AS sp_express_total_times_360d,
        SUM(CASE WHEN dt_tag <= 1 AND event_type='A01' THEN times ELSE 0 END) AS sp_express_event_a01_times_15d,
        SUM(CASE WHEN dt_tag <= 1 AND event_type='A02' THEN times ELSE 0 END) AS sp_express_event_a02_times_15d,
        SUM(CASE WHEN dt_tag <= 1 AND event_type='A03' THEN times ELSE 0 END) AS sp_express_event_a03_times_15d,
        SUM(CASE WHEN dt_tag <= 1 AND event_type='A04' THEN times ELSE 0 END) AS sp_express_event_a04_times_15d,
        SUM(CASE WHEN dt_tag <= 2 AND event_type='A01' THEN times ELSE 0 END) AS sp_express_event_a01_times_30d,
        SUM(CASE WHEN dt_tag <= 2 AND event_type='A02' THEN times ELSE 0 END) AS sp_express_event_a02_times_30d,
        SUM(CASE WHEN dt_tag <= 2 AND event_type='A03' THEN times ELSE 0 END) AS sp_express_event_a03_times_30d,
        SUM(CASE WHEN dt_tag <= 2 AND event_type='A04' THEN times ELSE 0 END) AS sp_express_event_a04_times_30d,
        SUM(CASE WHEN dt_tag <= 6 AND event_type='A01' THEN times ELSE 0 END) AS sp_express_event_a01_times_90d,
        SUM(CASE WHEN dt_tag <= 6 AND event_type='A02' THEN times ELSE 0 END) AS sp_express_event_a02_times_90d,
        SUM(CASE WHEN dt_tag <= 6 AND event_type='A03' THEN times ELSE 0 END) AS sp_express_event_a03_times_90d,
        SUM(CASE WHEN dt_tag <= 6 AND event_type='A04' THEN times ELSE 0 END) AS sp_express_event_a04_times_90d,
        SUM(CASE WHEN dt_tag <= 12 AND event_type='A01' THEN times ELSE 0 END) AS sp_express_event_a01_times_180d,
        SUM(CASE WHEN dt_tag <= 12 AND event_type='A02' THEN times ELSE 0 END) AS sp_express_event_a02_times_180d,
        SUM(CASE WHEN dt_tag <= 12 AND event_type='A03' THEN times ELSE 0 END) AS sp_express_event_a03_times_180d,
        SUM(CASE WHEN dt_tag <= 12 AND event_type='A04' THEN times ELSE 0 END) AS sp_express_event_a04_times_180d,
        SUM(CASE WHEN dt_tag <= 24 AND event_type='A01' THEN times ELSE 0 END) AS sp_express_event_a01_times_360d,
        SUM(CASE WHEN dt_tag <= 24 AND event_type='A02' THEN times ELSE 0 END) AS sp_express_event_a02_times_360d,
        SUM(CASE WHEN dt_tag <= 24 AND event_type='A03' THEN times ELSE 0 END) AS sp_express_event_a03_times_360d,
        SUM(CASE WHEN dt_tag <= 24 AND event_type='A04' THEN times ELSE 0 END) AS sp_express_event_a04_times_360d,
        datediff(back_date, MAX(CASE WHEN dt_tag <= 24 AND event_type='A01' THEN max_event_time ELSE NULL END)) AS sp_express_event_a01_recent_dura_d_beforep_360d,
        datediff(back_date, MIN(CASE WHEN dt_tag <= 24 AND event_type='A01' THEN min_event_time ELSE NULL END)) AS sp_express_event_a01_remote_dura_d_beforep_360d,
        datediff(back_date, MAX(CASE WHEN dt_tag <= 24 AND event_type='A02' THEN max_event_time ELSE NULL END)) AS sp_express_event_a02_recent_dura_d_beforep_360d,
        datediff(back_date, MIN(CASE WHEN dt_tag <= 24 AND event_type='A02' THEN min_event_time ELSE NULL END)) AS sp_express_event_a02_remote_dura_d_beforep_360d,
        datediff(back_date, MAX(CASE WHEN dt_tag <= 24 AND event_type='A03' THEN max_event_time ELSE NULL END)) AS sp_express_event_a03_recent_dura_d_beforep_360d,
        datediff(back_date, MIN(CASE WHEN dt_tag <= 24 AND event_type='A03' THEN min_event_time ELSE NULL END)) AS sp_express_event_a03_remote_dura_d_beforep_360d,
        datediff(back_date, MAX(CASE WHEN dt_tag <= 24 AND event_type='A04' THEN max_event_time ELSE NULL END)) AS sp_express_event_a04_recent_dura_d_beforep_360d,
        datediff(back_date, MIN(CASE WHEN dt_tag <= 24 AND event_type='A04' THEN min_event_time ELSE NULL END)) AS sp_express_event_a04_remote_dura_d_beforep_360d
    FROM base_express_layer
    GROUP BY mobile_md5, back_date
),express_plat_cnt_stats AS (
        SELECT
            mobile_md5,
            back_date,
            size(collect_set(CASE WHEN dt_tag <= 1 THEN sign_element ELSE NULL END)) AS sp_express_plat_cnt_15d,
            size(collect_set(CASE WHEN dt_tag <= 2 THEN sign_element ELSE NULL END)) AS sp_express_plat_cnt_30d,
            size(collect_set(CASE WHEN dt_tag <= 6 THEN sign_element ELSE NULL END)) AS sp_express_plat_cnt_90d,
            size(collect_set(CASE WHEN dt_tag <= 12 THEN sign_element ELSE NULL END)) AS sp_express_plat_cnt_180d,
            size(collect_set(CASE WHEN dt_tag <= 24 THEN sign_element ELSE NULL END)) AS sp_express_plat_cnt_360d,
            size(collect_set(CASE WHEN dt_tag <= 1 AND event_type='A01' THEN sign_element ELSE NULL END)) AS sp_express_event_a01_plat_cnt_15d,
            size(collect_set(CASE WHEN dt_tag <= 1 AND event_type='A02' THEN sign_element ELSE NULL END)) AS sp_express_event_a02_plat_cnt_15d,
            size(collect_set(CASE WHEN dt_tag <= 1 AND event_type='A03' THEN sign_element ELSE NULL END)) AS sp_express_event_a03_plat_cnt_15d,
            size(collect_set(CASE WHEN dt_tag <= 1 AND event_type='A04' THEN sign_element ELSE NULL END)) AS sp_express_event_a04_plat_cnt_15d,
            size(collect_set(CASE WHEN dt_tag <= 2 AND event_type='A01' THEN sign_element ELSE NULL END)) AS sp_express_event_a01_plat_cnt_30d,
            size(collect_set(CASE WHEN dt_tag <= 2 AND event_type='A02' THEN sign_element ELSE NULL END)) AS sp_express_event_a02_plat_cnt_30d,
            size(collect_set(CASE WHEN dt_tag <= 2 AND event_type='A03' THEN sign_element ELSE NULL END)) AS sp_express_event_a03_plat_cnt_30d,
            size(collect_set(CASE WHEN dt_tag <= 2 AND event_type='A04' THEN sign_element ELSE NULL END)) AS sp_express_event_a04_plat_cnt_30d,
            size(collect_set(CASE WHEN dt_tag <= 6 AND event_type='A01' THEN sign_element ELSE NULL END)) AS sp_express_event_a01_plat_cnt_90d,
            size(collect_set(CASE WHEN dt_tag <= 6 AND event_type='A02' THEN sign_element ELSE NULL END)) AS sp_express_event_a02_plat_cnt_90d,
            size(collect_set(CASE WHEN dt_tag <= 6 AND event_type='A03' THEN sign_element ELSE NULL END)) AS sp_express_event_a03_plat_cnt_90d,
            size(collect_set(CASE WHEN dt_tag <= 6 AND event_type='A04' THEN sign_element ELSE NULL END)) AS sp_express_event_a04_plat_cnt_90d,
            size(collect_set(CASE WHEN dt_tag <= 12 AND event_type='A01' THEN sign_element ELSE NULL END)) AS sp_express_event_a01_plat_cnt_180d,
            size(collect_set(CASE WHEN dt_tag <= 12 AND event_type='A02' THEN sign_element ELSE NULL END)) AS sp_express_event_a02_plat_cnt_180d,
            size(collect_set(CASE WHEN dt_tag <= 12 AND event_type='A03' THEN sign_element ELSE NULL END)) AS sp_express_event_a03_plat_cnt_180d,
            size(collect_set(CASE WHEN dt_tag <= 12 AND event_type='A04' THEN sign_element ELSE NULL END)) AS sp_express_event_a04_plat_cnt_180d,
            size(collect_set(CASE WHEN dt_tag <= 24 AND event_type='A01' THEN sign_element ELSE NULL END)) AS sp_express_event_a01_plat_cnt_360d,
            size(collect_set(CASE WHEN dt_tag <= 24 AND event_type='A02' THEN sign_element ELSE NULL END)) AS sp_express_event_a02_plat_cnt_360d,
            size(collect_set(CASE WHEN dt_tag <= 24 AND event_type='A03' THEN sign_element ELSE NULL END)) AS sp_express_event_a03_plat_cnt_360d,
            size(collect_set(CASE WHEN dt_tag <= 24 AND event_type='A04' THEN sign_element ELSE NULL END)) AS sp_express_event_a04_plat_cnt_360d
        FROM base_express_layer
        -- 严格按要求展开sign_concat后统计平台数
        LATERAL VIEW explode(split(sign_concat, ',')) t AS sign_element
        GROUP BY mobile_md5, back_date
    )
,aggregated_data AS (
    SELECT
        ts.*,
        pcs.sp_express_plat_cnt_15d,
        pcs.sp_express_plat_cnt_30d,
        pcs.sp_express_plat_cnt_90d,
        pcs.sp_express_plat_cnt_180d,
        pcs.sp_express_plat_cnt_360d,
        pcs.sp_express_event_a01_plat_cnt_15d,
        pcs.sp_express_event_a02_plat_cnt_15d,
        pcs.sp_express_event_a03_plat_cnt_15d,
        pcs.sp_express_event_a04_plat_cnt_15d,
        pcs.sp_express_event_a01_plat_cnt_30d,
        pcs.sp_express_event_a02_plat_cnt_30d,
        pcs.sp_express_event_a03_plat_cnt_30d,
        pcs.sp_express_event_a04_plat_cnt_30d,
        pcs.sp_express_event_a01_plat_cnt_90d,
        pcs.sp_express_event_a02_plat_cnt_90d,
        pcs.sp_express_event_a03_plat_cnt_90d,
        pcs.sp_express_event_a04_plat_cnt_90d,
        pcs.sp_express_event_a01_plat_cnt_180d,
        pcs.sp_express_event_a02_plat_cnt_180d,
        pcs.sp_express_event_a03_plat_cnt_180d,
        pcs.sp_express_event_a04_plat_cnt_180d,
        pcs.sp_express_event_a01_plat_cnt_360d,
        pcs.sp_express_event_a02_plat_cnt_360d,
        pcs.sp_express_event_a03_plat_cnt_360d,
        pcs.sp_express_event_a04_plat_cnt_360d
    FROM express_times_stats ts
    LEFT JOIN express_plat_cnt_stats pcs
    ON ts.mobile_md5 = pcs.mobile_md5 AND ts.back_date = pcs.back_date
)
insert overwrite table sms_bd_data.customer_test_express_feature_2026011301
SELECT
   *,
    ROUND(CASE WHEN sp_express_total_times_15d = 0 THEN 0 ELSE sp_express_event_a01_times_15d / sp_express_total_times_15d END, 6) AS sp_express_event_a01_ratio_total_15d,
    ROUND(CASE WHEN sp_express_total_times_15d = 0 THEN 0 ELSE sp_express_event_a02_times_15d / sp_express_total_times_15d END, 6) AS sp_express_event_a02_ratio_total_15d,
    ROUND(CASE WHEN sp_express_total_times_15d = 0 THEN 0 ELSE sp_express_event_a03_times_15d / sp_express_total_times_15d END, 6) AS sp_express_event_a03_ratio_total_15d,
    ROUND(CASE WHEN sp_express_total_times_15d = 0 THEN 0 ELSE sp_express_event_a04_times_15d / sp_express_total_times_15d END, 6) AS sp_express_event_a04_ratio_total_15d,
    ROUND(CASE WHEN sp_express_total_times_30d = 0 THEN 0 ELSE sp_express_event_a01_times_30d / sp_express_total_times_30d END, 6) AS sp_express_event_a01_ratio_total_30d,
    ROUND(CASE WHEN sp_express_total_times_30d = 0 THEN 0 ELSE sp_express_event_a02_times_30d / sp_express_total_times_30d END, 6) AS sp_express_event_a02_ratio_total_30d,
    ROUND(CASE WHEN sp_express_total_times_30d = 0 THEN 0 ELSE sp_express_event_a03_times_30d / sp_express_total_times_30d END, 6) AS sp_express_event_a03_ratio_total_30d,
    ROUND(CASE WHEN sp_express_total_times_30d = 0 THEN 0 ELSE sp_express_event_a04_times_30d / sp_express_total_times_30d END, 6) AS sp_express_event_a04_ratio_total_30d,
    ROUND(CASE WHEN sp_express_total_times_90d = 0 THEN 0 ELSE sp_express_event_a01_times_90d / sp_express_total_times_90d END, 6) AS sp_express_event_a01_ratio_total_90d,
    ROUND(CASE WHEN sp_express_total_times_90d = 0 THEN 0 ELSE sp_express_event_a02_times_90d / sp_express_total_times_90d END, 6) AS sp_express_event_a02_ratio_total_90d,
    ROUND(CASE WHEN sp_express_total_times_90d = 0 THEN 0 ELSE sp_express_event_a03_times_90d / sp_express_total_times_90d END, 6) AS sp_express_event_a03_ratio_total_90d,
    ROUND(CASE WHEN sp_express_total_times_90d = 0 THEN 0 ELSE sp_express_event_a04_times_90d / sp_express_total_times_90d END, 6) AS sp_express_event_a04_ratio_total_90d,
    ROUND(CASE WHEN sp_express_total_times_180d = 0 THEN 0 ELSE sp_express_event_a01_times_180d / sp_express_total_times_180d END, 6) AS sp_express_event_a01_ratio_total_180d,
    ROUND(CASE WHEN sp_express_total_times_180d = 0 THEN 0 ELSE sp_express_event_a02_times_180d / sp_express_total_times_180d END, 6) AS sp_express_event_a02_ratio_total_180d,
    ROUND(CASE WHEN sp_express_total_times_180d = 0 THEN 0 ELSE sp_express_event_a03_times_180d / sp_express_total_times_180d END, 6) AS sp_express_event_a03_ratio_total_180d,
    ROUND(CASE WHEN sp_express_total_times_180d = 0 THEN 0 ELSE sp_express_event_a04_times_180d / sp_express_total_times_180d END, 6) AS sp_express_event_a04_ratio_total_180d,
    ROUND(CASE WHEN sp_express_total_times_360d = 0 THEN 0 ELSE sp_express_event_a01_times_360d / sp_express_total_times_360d END, 6) AS sp_express_event_a01_ratio_total_360d,
    ROUND(CASE WHEN sp_express_total_times_360d = 0 THEN 0 ELSE sp_express_event_a02_times_360d / sp_express_total_times_360d END, 6) AS sp_express_event_a02_ratio_total_360d,
    ROUND(CASE WHEN sp_express_total_times_360d = 0 THEN 0 ELSE sp_express_event_a03_times_360d / sp_express_total_times_360d END, 6) AS sp_express_event_a03_ratio_total_360d,
    ROUND(CASE WHEN sp_express_total_times_360d = 0 THEN 0 ELSE sp_express_event_a04_times_360d / sp_express_total_times_360d END, 6) AS sp_express_event_a04_ratio_total_360d,
    ROUND(CASE WHEN sp_express_event_a01_times_30d = 0 THEN 0 ELSE sp_express_event_a01_times_15d / sp_express_event_a01_times_30d END, 6) AS sp_express_event_a01_ratio_15to30d,
    ROUND(CASE WHEN sp_express_event_a02_times_30d = 0 THEN 0 ELSE sp_express_event_a02_times_15d / sp_express_event_a02_times_30d END, 6) AS sp_express_event_a02_ratio_15to30d,
    ROUND(CASE WHEN sp_express_event_a03_times_30d = 0 THEN 0 ELSE sp_express_event_a03_times_15d / sp_express_event_a03_times_30d END, 6) AS sp_express_event_a03_ratio_15to30d,
    ROUND(CASE WHEN sp_express_event_a04_times_30d = 0 THEN 0 ELSE sp_express_event_a04_times_15d / sp_express_event_a04_times_30d END, 6) AS sp_express_event_a04_ratio_15to30d,
    ROUND(CASE WHEN sp_express_event_a01_times_90d = 0 THEN 0 ELSE sp_express_event_a01_times_30d / sp_express_event_a01_times_90d END, 6) AS sp_express_event_a01_ratio_30to90d,
    ROUND(CASE WHEN sp_express_event_a02_times_90d = 0 THEN 0 ELSE sp_express_event_a02_times_30d / sp_express_event_a02_times_90d END, 6) AS sp_express_event_a02_ratio_30to90d,
    ROUND(CASE WHEN sp_express_event_a03_times_90d = 0 THEN 0 ELSE sp_express_event_a03_times_30d / sp_express_event_a03_times_90d END, 6) AS sp_express_event_a03_ratio_30to90d,
    ROUND(CASE WHEN sp_express_event_a04_times_90d = 0 THEN 0 ELSE sp_express_event_a04_times_30d / sp_express_event_a04_times_90d END, 6) AS sp_express_event_a04_ratio_30to90d
FROM aggregated_data

