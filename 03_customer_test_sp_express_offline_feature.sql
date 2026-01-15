CREATE TABLE sms_bd_data.customer_test_offline_express_feature_2026010902
WITH cus_sample AS (
    SELECT 
        mobile_md5,
        back_date
    FROM(
        -- 快递业务样本表（示例结构）
        select 
            mobile_md5,
            substr(back_date, 0, 10) as back_date
        from risk.express_intent_samples_20241023
    )smp
    group by mobile_md5, back_date
),
customer_dwb_express AS (
    select
        mobile_md5, 
        back_date, 
        sign, 
        event_type,
        event_time  -- 统一时间字段命名
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
)
,aggregated_data AS (
    SELECT
        mobile_md5,
        back_date,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) THEN 1 ELSE 0 END) AS sp_express_total_times_15d,
COUNT(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) THEN sign ELSE NULL END) AS sp_express_plat_cnt_15d,
SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) THEN 1 ELSE 0 END) AS sp_express_total_times_30d,
COUNT(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) THEN sign ELSE NULL END) AS sp_express_plat_cnt_30d,
SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) THEN 1 ELSE 0 END) AS sp_express_total_times_90d,
COUNT(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) THEN sign ELSE NULL END) AS sp_express_plat_cnt_90d,
SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) THEN 1 ELSE 0 END) AS sp_express_total_times_180d,
COUNT(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) THEN sign ELSE NULL END) AS sp_express_plat_cnt_180d,
SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) THEN 1 ELSE 0 END) AS sp_express_total_times_360d,
COUNT(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) THEN sign ELSE NULL END) AS sp_express_plat_cnt_360d,
SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='A01' THEN 1 ELSE 0 END) AS sp_express_event_a01_times_15d,
COUNT(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='A01' THEN sign ELSE NULL END) AS sp_express_event_a01_plat_cnt_15d,
SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='A02' THEN 1 ELSE 0 END) AS sp_express_event_a02_times_15d,
COUNT(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='A02' THEN sign ELSE NULL END) AS sp_express_event_a02_plat_cnt_15d,
SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='A03' THEN 1 ELSE 0 END) AS sp_express_event_a03_times_15d,
COUNT(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='A03' THEN sign ELSE NULL END) AS sp_express_event_a03_plat_cnt_15d,
SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='A04' THEN 1 ELSE 0 END) AS sp_express_event_a04_times_15d,
COUNT(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='A04' THEN sign ELSE NULL END) AS sp_express_event_a04_plat_cnt_15d,
SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='A01' THEN 1 ELSE 0 END) AS sp_express_event_a01_times_30d,
COUNT(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='A01' THEN sign ELSE NULL END) AS sp_express_event_a01_plat_cnt_30d,
SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='A02' THEN 1 ELSE 0 END) AS sp_express_event_a02_times_30d,
COUNT(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='A02' THEN sign ELSE NULL END) AS sp_express_event_a02_plat_cnt_30d,
SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='A03' THEN 1 ELSE 0 END) AS sp_express_event_a03_times_30d,
COUNT(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='A03' THEN sign ELSE NULL END) AS sp_express_event_a03_plat_cnt_30d,
SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='A04' THEN 1 ELSE 0 END) AS sp_express_event_a04_times_30d,
COUNT(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='A04' THEN sign ELSE NULL END) AS sp_express_event_a04_plat_cnt_30d,
SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='A01' THEN 1 ELSE 0 END) AS sp_express_event_a01_times_90d,
COUNT(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='A01' THEN sign ELSE NULL END) AS sp_express_event_a01_plat_cnt_90d,
SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='A02' THEN 1 ELSE 0 END) AS sp_express_event_a02_times_90d,
COUNT(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='A02' THEN sign ELSE NULL END) AS sp_express_event_a02_plat_cnt_90d,
SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='A03' THEN 1 ELSE 0 END) AS sp_express_event_a03_times_90d,
COUNT(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='A03' THEN sign ELSE NULL END) AS sp_express_event_a03_plat_cnt_90d,
SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='A04' THEN 1 ELSE 0 END) AS sp_express_event_a04_times_90d,
COUNT(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='A04' THEN sign ELSE NULL END) AS sp_express_event_a04_plat_cnt_90d,
SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='A01' THEN 1 ELSE 0 END) AS sp_express_event_a01_times_180d,
COUNT(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='A01' THEN sign ELSE NULL END) AS sp_express_event_a01_plat_cnt_180d,
SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='A02' THEN 1 ELSE 0 END) AS sp_express_event_a02_times_180d,
COUNT(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='A02' THEN sign ELSE NULL END) AS sp_express_event_a02_plat_cnt_180d,
SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='A03' THEN 1 ELSE 0 END) AS sp_express_event_a03_times_180d,
COUNT(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='A03' THEN sign ELSE NULL END) AS sp_express_event_a03_plat_cnt_180d,
SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='A04' THEN 1 ELSE 0 END) AS sp_express_event_a04_times_180d,
COUNT(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='A04' THEN sign ELSE NULL END) AS sp_express_event_a04_plat_cnt_180d,
SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='A01' THEN 1 ELSE 0 END) AS sp_express_event_a01_times_360d,
COUNT(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='A01' THEN sign ELSE NULL END) AS sp_express_event_a01_plat_cnt_360d,
SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='A02' THEN 1 ELSE 0 END) AS sp_express_event_a02_times_360d,
COUNT(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='A02' THEN sign ELSE NULL END) AS sp_express_event_a02_plat_cnt_360d,
SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='A03' THEN 1 ELSE 0 END) AS sp_express_event_a03_times_360d,
COUNT(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='A03' THEN sign ELSE NULL END) AS sp_express_event_a03_plat_cnt_360d,
SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='A04' THEN 1 ELSE 0 END) AS sp_express_event_a04_times_360d,
COUNT(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='A04' THEN sign ELSE NULL END) AS sp_express_event_a04_plat_cnt_360d,
datediff(back_date, MAX(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='A01' THEN event_time ELSE NULL END)) AS sp_express_event_a01_recent_dura_d_beforep_15d,
datediff(back_date, MIN(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='A01' THEN event_time ELSE NULL END)) AS sp_express_event_a01_remote_dura_d_beforep_15d,
datediff(back_date, MAX(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='A02' THEN event_time ELSE NULL END)) AS sp_express_event_a02_recent_dura_d_beforep_15d,
datediff(back_date, MIN(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='A02' THEN event_time ELSE NULL END)) AS sp_express_event_a02_remote_dura_d_beforep_15d,
datediff(back_date, MAX(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='A03' THEN event_time ELSE NULL END)) AS sp_express_event_a03_recent_dura_d_beforep_15d,
datediff(back_date, MIN(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='A03' THEN event_time ELSE NULL END)) AS sp_express_event_a03_remote_dura_d_beforep_15d,
datediff(back_date, MAX(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='A04' THEN event_time ELSE NULL END)) AS sp_express_event_a04_recent_dura_d_beforep_15d,
datediff(back_date, MIN(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='A04' THEN event_time ELSE NULL END)) AS sp_express_event_a04_remote_dura_d_beforep_15d,
datediff(back_date, MAX(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='A01' THEN event_time ELSE NULL END)) AS sp_express_event_a01_recent_dura_d_beforep_30d,
datediff(back_date, MIN(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='A01' THEN event_time ELSE NULL END)) AS sp_express_event_a01_remote_dura_d_beforep_30d,
datediff(back_date, MAX(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='A02' THEN event_time ELSE NULL END)) AS sp_express_event_a02_recent_dura_d_beforep_30d,
datediff(back_date, MIN(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='A02' THEN event_time ELSE NULL END)) AS sp_express_event_a02_remote_dura_d_beforep_30d,
datediff(back_date, MAX(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='A03' THEN event_time ELSE NULL END)) AS sp_express_event_a03_recent_dura_d_beforep_30d,
datediff(back_date, MIN(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='A03' THEN event_time ELSE NULL END)) AS sp_express_event_a03_remote_dura_d_beforep_30d,
datediff(back_date, MAX(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='A04' THEN event_time ELSE NULL END)) AS sp_express_event_a04_recent_dura_d_beforep_30d,
datediff(back_date, MIN(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='A04' THEN event_time ELSE NULL END)) AS sp_express_event_a04_remote_dura_d_beforep_30d,
datediff(back_date, MAX(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='A01' THEN event_time ELSE NULL END)) AS sp_express_event_a01_recent_dura_d_beforep_90d,
datediff(back_date, MIN(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='A01' THEN event_time ELSE NULL END)) AS sp_express_event_a01_remote_dura_d_beforep_90d,
datediff(back_date, MAX(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='A02' THEN event_time ELSE NULL END)) AS sp_express_event_a02_recent_dura_d_beforep_90d,
datediff(back_date, MIN(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='A02' THEN event_time ELSE NULL END)) AS sp_express_event_a02_remote_dura_d_beforep_90d,
datediff(back_date, MAX(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='A03' THEN event_time ELSE NULL END)) AS sp_express_event_a03_recent_dura_d_beforep_90d,
datediff(back_date, MIN(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='A03' THEN event_time ELSE NULL END)) AS sp_express_event_a03_remote_dura_d_beforep_90d,
datediff(back_date, MAX(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='A04' THEN event_time ELSE NULL END)) AS sp_express_event_a04_recent_dura_d_beforep_90d,
datediff(back_date, MIN(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='A04' THEN event_time ELSE NULL END)) AS sp_express_event_a04_remote_dura_d_beforep_90d,
datediff(back_date, MAX(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='A01' THEN event_time ELSE NULL END)) AS sp_express_event_a01_recent_dura_d_beforep_180d,
datediff(back_date, MIN(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='A01' THEN event_time ELSE NULL END)) AS sp_express_event_a01_remote_dura_d_beforep_180d,
datediff(back_date, MAX(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='A02' THEN event_time ELSE NULL END)) AS sp_express_event_a02_recent_dura_d_beforep_180d,
datediff(back_date, MIN(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='A02' THEN event_time ELSE NULL END)) AS sp_express_event_a02_remote_dura_d_beforep_180d,
datediff(back_date, MAX(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='A03' THEN event_time ELSE NULL END)) AS sp_express_event_a03_recent_dura_d_beforep_180d,
datediff(back_date, MIN(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='A03' THEN event_time ELSE NULL END)) AS sp_express_event_a03_remote_dura_d_beforep_180d,
datediff(back_date, MAX(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='A04' THEN event_time ELSE NULL END)) AS sp_express_event_a04_recent_dura_d_beforep_180d,
datediff(back_date, MIN(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='A04' THEN event_time ELSE NULL END)) AS sp_express_event_a04_remote_dura_d_beforep_180d,
datediff(back_date, MAX(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='A01' THEN event_time ELSE NULL END)) AS sp_express_event_a01_recent_dura_d_beforep_360d,
datediff(back_date, MIN(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='A01' THEN event_time ELSE NULL END)) AS sp_express_event_a01_remote_dura_d_beforep_360d,
datediff(back_date, MAX(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='A02' THEN event_time ELSE NULL END)) AS sp_express_event_a02_recent_dura_d_beforep_360d,
datediff(back_date, MIN(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='A02' THEN event_time ELSE NULL END)) AS sp_express_event_a02_remote_dura_d_beforep_360d,
datediff(back_date, MAX(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='A03' THEN event_time ELSE NULL END)) AS sp_express_event_a03_recent_dura_d_beforep_360d,
datediff(back_date, MIN(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='A03' THEN event_time ELSE NULL END)) AS sp_express_event_a03_remote_dura_d_beforep_360d,
datediff(back_date, MAX(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='A04' THEN event_time ELSE NULL END)) AS sp_express_event_a04_recent_dura_d_beforep_360d,
datediff(back_date, MIN(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='A04' THEN event_time ELSE NULL END)) AS sp_express_event_a04_remote_dura_d_beforep_360d
    FROM customer_dwb_express
    GROUP BY mobile_md5, back_date
)
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
ROUND(CASE WHEN sp_express_event_a04_times_90d = 0 THEN 0 ELSE sp_express_event_a04_times_30d / sp_express_event_a04_times_90d END, 6) AS sp_express_event_a04_ratio_30to90d,
ROUND(CASE WHEN sp_express_event_a01_times_180d = 0 THEN 0 ELSE sp_express_event_a01_times_90d / sp_express_event_a01_times_180d END, 6) AS sp_express_event_a01_ratio_90to180d,
ROUND(CASE WHEN sp_express_event_a02_times_180d = 0 THEN 0 ELSE sp_express_event_a02_times_90d / sp_express_event_a02_times_180d END, 6) AS sp_express_event_a02_ratio_90to180d,
ROUND(CASE WHEN sp_express_event_a03_times_180d = 0 THEN 0 ELSE sp_express_event_a03_times_90d / sp_express_event_a03_times_180d END, 6) AS sp_express_event_a03_ratio_90to180d,
ROUND(CASE WHEN sp_express_event_a04_times_180d = 0 THEN 0 ELSE sp_express_event_a04_times_90d / sp_express_event_a04_times_180d END, 6) AS sp_express_event_a04_ratio_90to180d,
ROUND(CASE WHEN sp_express_event_a01_times_360d = 0 THEN 0 ELSE sp_express_event_a01_times_180d / sp_express_event_a01_times_360d END, 6) AS sp_express_event_a01_ratio_180to360d,
ROUND(CASE WHEN sp_express_event_a02_times_360d = 0 THEN 0 ELSE sp_express_event_a02_times_180d / sp_express_event_a02_times_360d END, 6) AS sp_express_event_a02_ratio_180to360d,
ROUND(CASE WHEN sp_express_event_a03_times_360d = 0 THEN 0 ELSE sp_express_event_a03_times_180d / sp_express_event_a03_times_360d END, 6) AS sp_express_event_a03_ratio_180to360d,
ROUND(CASE WHEN sp_express_event_a04_times_360d = 0 THEN 0 ELSE sp_express_event_a04_times_180d / sp_express_event_a04_times_360d END, 6) AS sp_express_event_a04_ratio_180to360d,
ROUND(CASE WHEN sp_express_event_a01_times_360d = 0 THEN 0 ELSE sp_express_event_a01_times_360d / sp_express_event_a01_times_360d END, 6) AS sp_express_event_a01_ratio_360to360d,
ROUND(CASE WHEN sp_express_event_a02_times_360d = 0 THEN 0 ELSE sp_express_event_a02_times_360d / sp_express_event_a02_times_360d END, 6) AS sp_express_event_a02_ratio_360to360d,
ROUND(CASE WHEN sp_express_event_a03_times_360d = 0 THEN 0 ELSE sp_express_event_a03_times_360d / sp_express_event_a03_times_360d END, 6) AS sp_express_event_a03_ratio_360to360d,
ROUND(CASE WHEN sp_express_event_a04_times_360d = 0 THEN 0 ELSE sp_express_event_a04_times_360d / sp_express_event_a04_times_360d END, 6) AS sp_express_event_a04_ratio_360to360d
FROM aggregated_data

