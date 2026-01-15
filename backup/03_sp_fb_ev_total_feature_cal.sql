
WITH base_cus_sample as(
    select
        base_cus.mobile_md5,
        base_cus.back_date
    from(
        SELECT 
            phone as mobile_md5,
            the_date as back_date
        FROM sms_bd_data.customer_test_sample_id
    )cus_sample
    inner join sms_bd_data.sp_sms_total_user_id base_cus
    on cus_sample.mobile_md5 = base_cus.phone
),cus_sample AS (
    SELECT 
        bcs.mobile_md5,
        bcs.back_date
    FROM base_cus_sample bcs
    group by bcs.mobile_md5, bcs.back_date
),customer_dwb_sms AS (
    select
        mobile_md5, 
        back_date, 
        sign, 
        event_time,
        event_type, 
        ind_tag
    from(
        select
            cus_smp.mobile_md5,
            cus_smp.back_date,
            dwb_loan.sign,
            dwb_loan.event_type,
            dwb_loan.event_time,
            ind_tag
        from cus_sample cus_smp
        inner join(
            select
                phone,
                sign,
                event_type,
                ind_tag,
                from_unixtime(unix_timestamp(the_date, 'yyyyMMdd'), 'yyyy-MM-dd') as event_time
            from sms_bd_data.sms_dwb_fin_loan_ind_event_fdt
        )dwb_loan
        on cus_smp.mobile_md5=dwb_loan.phone 
        where cus_smp.back_date > dwb_loan.event_time and dwb_loan.event_time >= date_sub(cus_smp.back_date, 360)
    )cus_dwb
    group by mobile_md5, back_date, sign, event_type, event_time, ind_tag
), base_tag_layer AS (
    SELECT 
        mobile_md5,
        back_date,
        sign,
        event_type,
        ind_tag,
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
            ind_tag,
            event_time,
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
        FROM customer_dwb_sms
        WHERE sign IS NOT NULL
    )customer_dwb_sms 
    GROUP BY mobile_md5, back_date, sign, event_type, ind_tag, dt_tag
),event_stats AS (
    SELECT
        mobile_md5,
        back_date,
        SUM(CASE WHEN dt_tag <= 1 AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_event_b01_times_15d,
        SUM(CASE WHEN dt_tag <= 1 AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_event_b02_times_15d,
        SUM(CASE WHEN dt_tag <= 1 AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_event_b03_times_15d,
        SUM(CASE WHEN dt_tag <= 2 AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_event_b01_times_30d,
        SUM(CASE WHEN dt_tag <= 2 AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_event_b02_times_30d,
        SUM(CASE WHEN dt_tag <= 2 AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_event_b03_times_30d,
        SUM(CASE WHEN dt_tag <= 6 AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_event_b01_times_90d,
        SUM(CASE WHEN dt_tag <= 6 AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_event_b02_times_90d,
        SUM(CASE WHEN dt_tag <= 6 AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_event_b03_times_90d,
        SUM(CASE WHEN dt_tag <= 12 AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_event_b01_times_180d,
        SUM(CASE WHEN dt_tag <= 12 AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_event_b02_times_180d,
        SUM(CASE WHEN dt_tag <= 12 AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_event_b03_times_180d,
        SUM(CASE WHEN dt_tag <= 24 AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_event_b01_times_360d,
        SUM(CASE WHEN dt_tag <= 24 AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_event_b02_times_360d,
        SUM(CASE WHEN dt_tag <= 24 AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_event_b03_times_360d,
        SUM(CASE WHEN dt_tag <= 1 AND ind_tag='indA' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_inda_event_b01_times_15d,
        SUM(CASE WHEN dt_tag <= 1 AND ind_tag='indA' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_inda_event_b02_times_15d,
        SUM(CASE WHEN dt_tag <= 1 AND ind_tag='indA' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_inda_event_b03_times_15d,
        SUM(CASE WHEN dt_tag <= 1 AND ind_tag='indB' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_indb_event_b01_times_15d,
        SUM(CASE WHEN dt_tag <= 1 AND ind_tag='indB' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_indb_event_b02_times_15d,
        SUM(CASE WHEN dt_tag <= 1 AND ind_tag='indB' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_indb_event_b03_times_15d,
        SUM(CASE WHEN dt_tag <= 1 AND ind_tag='indC' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_indc_event_b01_times_15d,
        SUM(CASE WHEN dt_tag <= 1 AND ind_tag='indC' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_indc_event_b02_times_15d,
        SUM(CASE WHEN dt_tag <= 1 AND ind_tag='indC' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_indc_event_b03_times_15d,
        SUM(CASE WHEN dt_tag <= 1 AND ind_tag='indE' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_inde_event_b01_times_15d,
        SUM(CASE WHEN dt_tag <= 1 AND ind_tag='indE' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_inde_event_b02_times_15d,
        SUM(CASE WHEN dt_tag <= 1 AND ind_tag='indE' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_inde_event_b03_times_15d,
        SUM(CASE WHEN dt_tag <= 1 AND ind_tag='indF' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_indf_event_b01_times_15d,
        SUM(CASE WHEN dt_tag <= 1 AND ind_tag='indF' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_indf_event_b02_times_15d,
        SUM(CASE WHEN dt_tag <= 1 AND ind_tag='indF' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_indf_event_b03_times_15d,
        SUM(CASE WHEN dt_tag <= 1 AND ind_tag='indG' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_indg_event_b01_times_15d,
        SUM(CASE WHEN dt_tag <= 1 AND ind_tag='indG' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_indg_event_b02_times_15d,
        SUM(CASE WHEN dt_tag <= 1 AND ind_tag='indG' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_indg_event_b03_times_15d,
        SUM(CASE WHEN dt_tag <= 2 AND ind_tag='indA' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_inda_event_b01_times_30d,
        SUM(CASE WHEN dt_tag <= 2 AND ind_tag='indA' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_inda_event_b02_times_30d,
        SUM(CASE WHEN dt_tag <= 2 AND ind_tag='indA' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_inda_event_b03_times_30d,
        SUM(CASE WHEN dt_tag <= 2 AND ind_tag='indB' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_indb_event_b01_times_30d,
        SUM(CASE WHEN dt_tag <= 2 AND ind_tag='indB' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_indb_event_b02_times_30d,
        SUM(CASE WHEN dt_tag <= 2 AND ind_tag='indB' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_indb_event_b03_times_30d,
        SUM(CASE WHEN dt_tag <= 2 AND ind_tag='indC' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_indc_event_b01_times_30d,
        SUM(CASE WHEN dt_tag <= 2 AND ind_tag='indC' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_indc_event_b02_times_30d,
        SUM(CASE WHEN dt_tag <= 2 AND ind_tag='indC' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_indc_event_b03_times_30d,
        SUM(CASE WHEN dt_tag <= 2 AND ind_tag='indE' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_inde_event_b01_times_30d,
        SUM(CASE WHEN dt_tag <= 2 AND ind_tag='indE' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_inde_event_b02_times_30d,
        SUM(CASE WHEN dt_tag <= 2 AND ind_tag='indE' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_inde_event_b03_times_30d,
        SUM(CASE WHEN dt_tag <= 2 AND ind_tag='indF' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_indf_event_b01_times_30d,
        SUM(CASE WHEN dt_tag <= 2 AND ind_tag='indF' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_indf_event_b02_times_30d,
        SUM(CASE WHEN dt_tag <= 2 AND ind_tag='indF' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_indf_event_b03_times_30d,
        SUM(CASE WHEN dt_tag <= 2 AND ind_tag='indG' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_indg_event_b01_times_30d,
        SUM(CASE WHEN dt_tag <= 2 AND ind_tag='indG' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_indg_event_b02_times_30d,
        SUM(CASE WHEN dt_tag <= 2 AND ind_tag='indG' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_indg_event_b03_times_30d,
        SUM(CASE WHEN dt_tag <= 6 AND ind_tag='indA' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_inda_event_b01_times_90d,
        SUM(CASE WHEN dt_tag <= 6 AND ind_tag='indA' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_inda_event_b02_times_90d,
        SUM(CASE WHEN dt_tag <= 6 AND ind_tag='indA' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_inda_event_b03_times_90d,
        SUM(CASE WHEN dt_tag <= 6 AND ind_tag='indB' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_indb_event_b01_times_90d,
        SUM(CASE WHEN dt_tag <= 6 AND ind_tag='indB' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_indb_event_b02_times_90d,
        SUM(CASE WHEN dt_tag <= 6 AND ind_tag='indB' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_indb_event_b03_times_90d,
        SUM(CASE WHEN dt_tag <= 6 AND ind_tag='indC' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_indc_event_b01_times_90d,
        SUM(CASE WHEN dt_tag <= 6 AND ind_tag='indC' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_indc_event_b02_times_90d,
        SUM(CASE WHEN dt_tag <= 6 AND ind_tag='indC' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_indc_event_b03_times_90d,
        SUM(CASE WHEN dt_tag <= 6 AND ind_tag='indE' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_inde_event_b01_times_90d,
        SUM(CASE WHEN dt_tag <= 6 AND ind_tag='indE' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_inde_event_b02_times_90d,
        SUM(CASE WHEN dt_tag <= 6 AND ind_tag='indE' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_inde_event_b03_times_90d,
        SUM(CASE WHEN dt_tag <= 6 AND ind_tag='indF' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_indf_event_b01_times_90d,
        SUM(CASE WHEN dt_tag <= 6 AND ind_tag='indF' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_indf_event_b02_times_90d,
        SUM(CASE WHEN dt_tag <= 6 AND ind_tag='indF' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_indf_event_b03_times_90d,
        SUM(CASE WHEN dt_tag <= 6 AND ind_tag='indG' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_indg_event_b01_times_90d,
        SUM(CASE WHEN dt_tag <= 6 AND ind_tag='indG' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_indg_event_b02_times_90d,
        SUM(CASE WHEN dt_tag <= 6 AND ind_tag='indG' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_indg_event_b03_times_90d,
        SUM(CASE WHEN dt_tag <= 12 AND ind_tag='indA' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_inda_event_b01_times_180d,
        SUM(CASE WHEN dt_tag <= 12 AND ind_tag='indA' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_inda_event_b02_times_180d,
        SUM(CASE WHEN dt_tag <= 12 AND ind_tag='indA' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_inda_event_b03_times_180d,
        SUM(CASE WHEN dt_tag <= 12 AND ind_tag='indB' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_indb_event_b01_times_180d,
        SUM(CASE WHEN dt_tag <= 12 AND ind_tag='indB' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_indb_event_b02_times_180d,
        SUM(CASE WHEN dt_tag <= 12 AND ind_tag='indB' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_indb_event_b03_times_180d,
        SUM(CASE WHEN dt_tag <= 12 AND ind_tag='indC' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_indc_event_b01_times_180d,
        SUM(CASE WHEN dt_tag <= 12 AND ind_tag='indC' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_indc_event_b02_times_180d,
        SUM(CASE WHEN dt_tag <= 12 AND ind_tag='indC' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_indc_event_b03_times_180d,
        SUM(CASE WHEN dt_tag <= 12 AND ind_tag='indE' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_inde_event_b01_times_180d,
        SUM(CASE WHEN dt_tag <= 12 AND ind_tag='indE' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_inde_event_b02_times_180d,
        SUM(CASE WHEN dt_tag <= 12 AND ind_tag='indE' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_inde_event_b03_times_180d,
        SUM(CASE WHEN dt_tag <= 12 AND ind_tag='indF' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_indf_event_b01_times_180d,
        SUM(CASE WHEN dt_tag <= 12 AND ind_tag='indF' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_indf_event_b02_times_180d,
        SUM(CASE WHEN dt_tag <= 12 AND ind_tag='indF' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_indf_event_b03_times_180d,
        SUM(CASE WHEN dt_tag <= 12 AND ind_tag='indG' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_indg_event_b01_times_180d,
        SUM(CASE WHEN dt_tag <= 12 AND ind_tag='indG' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_indg_event_b02_times_180d,
        SUM(CASE WHEN dt_tag <= 12 AND ind_tag='indG' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_indg_event_b03_times_180d,
        SUM(CASE WHEN dt_tag <= 24 AND ind_tag='indA' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_inda_event_b01_times_360d,
        SUM(CASE WHEN dt_tag <= 24 AND ind_tag='indA' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_inda_event_b02_times_360d,
        SUM(CASE WHEN dt_tag <= 24 AND ind_tag='indA' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_inda_event_b03_times_360d,
        SUM(CASE WHEN dt_tag <= 24 AND ind_tag='indB' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_indb_event_b01_times_360d,
        SUM(CASE WHEN dt_tag <= 24 AND ind_tag='indB' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_indb_event_b02_times_360d,
        SUM(CASE WHEN dt_tag <= 24 AND ind_tag='indB' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_indb_event_b03_times_360d,
        SUM(CASE WHEN dt_tag <= 24 AND ind_tag='indC' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_indc_event_b01_times_360d,
        SUM(CASE WHEN dt_tag <= 24 AND ind_tag='indC' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_indc_event_b02_times_360d,
        SUM(CASE WHEN dt_tag <= 24 AND ind_tag='indC' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_indc_event_b03_times_360d,
        SUM(CASE WHEN dt_tag <= 24 AND ind_tag='indE' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_inde_event_b01_times_360d,
        SUM(CASE WHEN dt_tag <= 24 AND ind_tag='indE' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_inde_event_b02_times_360d,
        SUM(CASE WHEN dt_tag <= 24 AND ind_tag='indE' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_inde_event_b03_times_360d,
        SUM(CASE WHEN dt_tag <= 24 AND ind_tag='indF' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_indf_event_b01_times_360d,
        SUM(CASE WHEN dt_tag <= 24 AND ind_tag='indF' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_indf_event_b02_times_360d,
        SUM(CASE WHEN dt_tag <= 24 AND ind_tag='indF' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_indf_event_b03_times_360d,
        SUM(CASE WHEN dt_tag <= 24 AND ind_tag='indG' AND event_type='B01' THEN times ELSE 0 END) AS sp_fin_loan_indg_event_b01_times_360d,
        SUM(CASE WHEN dt_tag <= 24 AND ind_tag='indG' AND event_type='B02' THEN times ELSE 0 END) AS sp_fin_loan_indg_event_b02_times_360d,
        SUM(CASE WHEN dt_tag <= 24 AND ind_tag='indG' AND event_type='B03' THEN times ELSE 0 END) AS sp_fin_loan_indg_event_b03_times_360d
    FROM base_tag_layer
    GROUP BY mobile_md5, back_date
)
insert overwrite table sms_bd_data.customer_test_fin_loan_feature_2026011302
SELECT
    bcs.mobile_md5,
    bcs.back_date,
    NVL(es.sp_fin_loan_event_b01_times_15d, 0) AS fb_loan_ev1,
    NVL(es.sp_fin_loan_event_b02_times_15d, 0) AS fb_loan_ev2,
    NVL(es.sp_fin_loan_event_b03_times_15d, 0) AS fb_loan_ev3,
    NVL(es.sp_fin_loan_event_b01_times_30d, 0) AS fb_loan_ev4,
    NVL(es.sp_fin_loan_event_b02_times_30d, 0) AS fb_loan_ev5,
    NVL(es.sp_fin_loan_event_b03_times_30d, 0) AS fb_loan_ev6,
    NVL(es.sp_fin_loan_event_b01_times_90d, 0) AS fb_loan_ev7,
    NVL(es.sp_fin_loan_event_b02_times_90d, 0) AS fb_loan_ev8,
    NVL(es.sp_fin_loan_event_b03_times_90d, 0) AS fb_loan_ev9,
    NVL(es.sp_fin_loan_event_b01_times_180d, 0) AS fb_loan_ev10,
    NVL(es.sp_fin_loan_event_b02_times_180d, 0) AS fb_loan_ev11,
    NVL(es.sp_fin_loan_event_b03_times_180d, 0) AS fb_loan_ev12,
    NVL(es.sp_fin_loan_event_b01_times_360d, 0) AS fb_loan_ev13,
    NVL(es.sp_fin_loan_event_b02_times_360d, 0) AS fb_loan_ev14,
    NVL(es.sp_fin_loan_event_b03_times_360d, 0) AS fb_loan_ev15,
    NVL(es.sp_fin_loan_inda_event_b01_times_15d, 0) AS fb_loan_ev16,
    NVL(es.sp_fin_loan_inda_event_b02_times_15d, 0) AS fb_loan_ev17,
    NVL(es.sp_fin_loan_inda_event_b03_times_15d, 0) AS fb_loan_ev18,
    NVL(es.sp_fin_loan_indb_event_b01_times_15d, 0) AS fb_loan_ev19,
    NVL(es.sp_fin_loan_indb_event_b02_times_15d, 0) AS fb_loan_ev20,
    NVL(es.sp_fin_loan_indb_event_b03_times_15d, 0) AS fb_loan_ev21,
    NVL(es.sp_fin_loan_indc_event_b01_times_15d, 0) AS fb_loan_ev22,
    NVL(es.sp_fin_loan_indc_event_b02_times_15d, 0) AS fb_loan_ev23,
    NVL(es.sp_fin_loan_indc_event_b03_times_15d, 0) AS fb_loan_ev24,
    NVL(es.sp_fin_loan_inde_event_b01_times_15d, 0) AS fb_loan_ev25,
    NVL(es.sp_fin_loan_inde_event_b02_times_15d, 0) AS fb_loan_ev26,
    NVL(es.sp_fin_loan_inde_event_b03_times_15d, 0) AS fb_loan_ev27,
    NVL(es.sp_fin_loan_indf_event_b01_times_15d, 0) AS fb_loan_ev28,
    NVL(es.sp_fin_loan_indf_event_b02_times_15d, 0) AS fb_loan_ev29,
    NVL(es.sp_fin_loan_indf_event_b03_times_15d, 0) AS fb_loan_ev30,
    NVL(es.sp_fin_loan_indg_event_b01_times_15d, 0) AS fb_loan_ev31,
    NVL(es.sp_fin_loan_indg_event_b02_times_15d, 0) AS fb_loan_ev32,
    NVL(es.sp_fin_loan_indg_event_b03_times_15d, 0) AS fb_loan_ev33,
    NVL(es.sp_fin_loan_inda_event_b01_times_30d, 0) AS fb_loan_ev34,
    NVL(es.sp_fin_loan_inda_event_b02_times_30d, 0) AS fb_loan_ev35,
    NVL(es.sp_fin_loan_inda_event_b03_times_30d, 0) AS fb_loan_ev36,
    NVL(es.sp_fin_loan_indb_event_b01_times_30d, 0) AS fb_loan_ev37,
    NVL(es.sp_fin_loan_indb_event_b02_times_30d, 0) AS fb_loan_ev38,
    NVL(es.sp_fin_loan_indb_event_b03_times_30d, 0) AS fb_loan_ev39,
    NVL(es.sp_fin_loan_indc_event_b01_times_30d, 0) AS fb_loan_ev40,
    NVL(es.sp_fin_loan_indc_event_b02_times_30d, 0) AS fb_loan_ev41,
    NVL(es.sp_fin_loan_indc_event_b03_times_30d, 0) AS fb_loan_ev42,
    NVL(es.sp_fin_loan_inde_event_b01_times_30d, 0) AS fb_loan_ev43,
    NVL(es.sp_fin_loan_inde_event_b02_times_30d, 0) AS fb_loan_ev44,
    NVL(es.sp_fin_loan_inde_event_b03_times_30d, 0) AS fb_loan_ev45,
    NVL(es.sp_fin_loan_indf_event_b01_times_30d, 0) AS fb_loan_ev46,
    NVL(es.sp_fin_loan_indf_event_b02_times_30d, 0) AS fb_loan_ev47,
    NVL(es.sp_fin_loan_indf_event_b03_times_30d, 0) AS fb_loan_ev48,
    NVL(es.sp_fin_loan_indg_event_b01_times_30d, 0) AS fb_loan_ev49,
    NVL(es.sp_fin_loan_indg_event_b02_times_30d, 0) AS fb_loan_ev50,
    NVL(es.sp_fin_loan_indg_event_b03_times_30d, 0) AS fb_loan_ev51,
    NVL(es.sp_fin_loan_inda_event_b01_times_90d, 0) AS fb_loan_ev52,
    NVL(es.sp_fin_loan_inda_event_b02_times_90d, 0) AS fb_loan_ev53,
    NVL(es.sp_fin_loan_inda_event_b03_times_90d, 0) AS fb_loan_ev54,
    NVL(es.sp_fin_loan_indb_event_b01_times_90d, 0) AS fb_loan_ev55,
    NVL(es.sp_fin_loan_indb_event_b02_times_90d, 0) AS fb_loan_ev56,
    NVL(es.sp_fin_loan_indb_event_b03_times_90d, 0) AS fb_loan_ev57,
    NVL(es.sp_fin_loan_indc_event_b01_times_90d, 0) AS fb_loan_ev58,
    NVL(es.sp_fin_loan_indc_event_b02_times_90d, 0) AS fb_loan_ev59,
    NVL(es.sp_fin_loan_indc_event_b03_times_90d, 0) AS fb_loan_ev60,
    NVL(es.sp_fin_loan_inde_event_b01_times_90d, 0) AS fb_loan_ev61,
    NVL(es.sp_fin_loan_inde_event_b02_times_90d, 0) AS fb_loan_ev62,
    NVL(es.sp_fin_loan_inde_event_b03_times_90d, 0) AS fb_loan_ev63,
    NVL(es.sp_fin_loan_indf_event_b01_times_90d, 0) AS fb_loan_ev64,
    NVL(es.sp_fin_loan_indf_event_b02_times_90d, 0) AS fb_loan_ev65,
    NVL(es.sp_fin_loan_indf_event_b03_times_90d, 0) AS fb_loan_ev66,
    NVL(es.sp_fin_loan_indg_event_b01_times_90d, 0) AS fb_loan_ev67,
    NVL(es.sp_fin_loan_indg_event_b02_times_90d, 0) AS fb_loan_ev68,
    NVL(es.sp_fin_loan_indg_event_b03_times_90d, 0) AS fb_loan_ev69,
    NVL(es.sp_fin_loan_inda_event_b01_times_180d, 0) AS fb_loan_ev70,
    NVL(es.sp_fin_loan_inda_event_b02_times_180d, 0) AS fb_loan_ev71,
    NVL(es.sp_fin_loan_inda_event_b03_times_180d, 0) AS fb_loan_ev72,
    NVL(es.sp_fin_loan_indb_event_b01_times_180d, 0) AS fb_loan_ev73,
    NVL(es.sp_fin_loan_indb_event_b02_times_180d, 0) AS fb_loan_ev74,
    NVL(es.sp_fin_loan_indb_event_b03_times_180d, 0) AS fb_loan_ev75,
    NVL(es.sp_fin_loan_indc_event_b01_times_180d, 0) AS fb_loan_ev76,
    NVL(es.sp_fin_loan_indc_event_b02_times_180d, 0) AS fb_loan_ev77,
    NVL(es.sp_fin_loan_indc_event_b03_times_180d, 0) AS fb_loan_ev78,
    NVL(es.sp_fin_loan_inde_event_b01_times_180d, 0) AS fb_loan_ev79,
    NVL(es.sp_fin_loan_inde_event_b02_times_180d, 0) AS fb_loan_ev80,
    NVL(es.sp_fin_loan_inde_event_b03_times_180d, 0) AS fb_loan_ev81,
    NVL(es.sp_fin_loan_indf_event_b01_times_180d, 0) AS fb_loan_ev82,
    NVL(es.sp_fin_loan_indf_event_b02_times_180d, 0) AS fb_loan_ev83,
    NVL(es.sp_fin_loan_indf_event_b03_times_180d, 0) AS fb_loan_ev84,
    NVL(es.sp_fin_loan_indg_event_b01_times_180d, 0) AS fb_loan_ev85,
    NVL(es.sp_fin_loan_indg_event_b02_times_180d, 0) AS fb_loan_ev86,
    NVL(es.sp_fin_loan_indg_event_b03_times_180d, 0) AS fb_loan_ev87,
    NVL(es.sp_fin_loan_inda_event_b01_times_360d, 0) AS fb_loan_ev88,
    NVL(es.sp_fin_loan_inda_event_b02_times_360d, 0) AS fb_loan_ev89,
    NVL(es.sp_fin_loan_inda_event_b03_times_360d, 0) AS fb_loan_ev90,
    NVL(es.sp_fin_loan_indb_event_b01_times_360d, 0) AS fb_loan_ev91,
    NVL(es.sp_fin_loan_indb_event_b02_times_360d, 0) AS fb_loan_ev92,
    NVL(es.sp_fin_loan_indb_event_b03_times_360d, 0) AS fb_loan_ev93,
    NVL(es.sp_fin_loan_indc_event_b01_times_360d, 0) AS fb_loan_ev94,
    NVL(es.sp_fin_loan_indc_event_b02_times_360d, 0) AS fb_loan_ev95,
    NVL(es.sp_fin_loan_indc_event_b03_times_360d, 0) AS fb_loan_ev96,
    NVL(es.sp_fin_loan_inde_event_b01_times_360d, 0) AS fb_loan_ev97,
    NVL(es.sp_fin_loan_inde_event_b02_times_360d, 0) AS fb_loan_ev98,
    NVL(es.sp_fin_loan_inde_event_b03_times_360d, 0) AS fb_loan_ev99,
    NVL(es.sp_fin_loan_indf_event_b01_times_360d, 0) AS fb_loan_ev100,
    NVL(es.sp_fin_loan_indf_event_b02_times_360d, 0) AS fb_loan_ev101,
    NVL(es.sp_fin_loan_indf_event_b03_times_360d, 0) AS fb_loan_ev102,
    NVL(es.sp_fin_loan_indg_event_b01_times_360d, 0) AS fb_loan_ev103,
    NVL(es.sp_fin_loan_indg_event_b02_times_360d, 0) AS fb_loan_ev104,
    NVL(es.sp_fin_loan_indg_event_b03_times_360d, 0) AS fb_loan_ev105
FROM base_cus_sample bcs
LEFT JOIN event_stats es 
ON bcs.mobile_md5 = es.mobile_md5 AND bcs.back_date = es.back_date;

