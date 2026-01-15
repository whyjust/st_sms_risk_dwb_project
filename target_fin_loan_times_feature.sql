
WITH cus_sample AS (
    SELECT 
        phone as mobile_md5,
        the_date as back_date
    FROM sms_bd_data.customer_test_sample_id
    group by phone, the_date
),
customer_dwb_sms AS (
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
)

, base_tag_layer AS (
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
    )
,event_stats AS (
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
    mobile_md5,
    back_date,
    sp_fin_loan_event_b01_times_15d AS fb_loan_ev1,
    sp_fin_loan_event_b02_times_15d AS fb_loan_ev2,
    sp_fin_loan_event_b03_times_15d AS fb_loan_ev3,
    sp_fin_loan_event_b01_times_30d AS fb_loan_ev4,
    sp_fin_loan_event_b02_times_30d AS fb_loan_ev5,
    sp_fin_loan_event_b03_times_30d AS fb_loan_ev6,
    sp_fin_loan_event_b01_times_90d AS fb_loan_ev7,
    sp_fin_loan_event_b02_times_90d AS fb_loan_ev8,
    sp_fin_loan_event_b03_times_90d AS fb_loan_ev9,
    sp_fin_loan_event_b01_times_180d AS fb_loan_ev10,
    sp_fin_loan_event_b02_times_180d AS fb_loan_ev11,
    sp_fin_loan_event_b03_times_180d AS fb_loan_ev12,
    sp_fin_loan_event_b01_times_360d AS fb_loan_ev13,
    sp_fin_loan_event_b02_times_360d AS fb_loan_ev14,
    sp_fin_loan_event_b03_times_360d AS fb_loan_ev15,
    sp_fin_loan_inda_event_b01_times_15d AS fb_loan_ev16,
    sp_fin_loan_inda_event_b02_times_15d AS fb_loan_ev17,
    sp_fin_loan_inda_event_b03_times_15d AS fb_loan_ev18,
    sp_fin_loan_indb_event_b01_times_15d AS fb_loan_ev19,
    sp_fin_loan_indb_event_b02_times_15d AS fb_loan_ev20,
    sp_fin_loan_indb_event_b03_times_15d AS fb_loan_ev21,
    sp_fin_loan_indc_event_b01_times_15d AS fb_loan_ev22,
    sp_fin_loan_indc_event_b02_times_15d AS fb_loan_ev23,
    sp_fin_loan_indc_event_b03_times_15d AS fb_loan_ev24,
    sp_fin_loan_inde_event_b01_times_15d AS fb_loan_ev25,
    sp_fin_loan_inde_event_b02_times_15d AS fb_loan_ev26,
    sp_fin_loan_inde_event_b03_times_15d AS fb_loan_ev27,
    sp_fin_loan_indf_event_b01_times_15d AS fb_loan_ev28,
    sp_fin_loan_indf_event_b02_times_15d AS fb_loan_ev29,
    sp_fin_loan_indf_event_b03_times_15d AS fb_loan_ev30,
    sp_fin_loan_indg_event_b01_times_15d AS fb_loan_ev31,
    sp_fin_loan_indg_event_b02_times_15d AS fb_loan_ev32,
    sp_fin_loan_indg_event_b03_times_15d AS fb_loan_ev33,
    sp_fin_loan_inda_event_b01_times_30d AS fb_loan_ev34,
    sp_fin_loan_inda_event_b02_times_30d AS fb_loan_ev35,
    sp_fin_loan_inda_event_b03_times_30d AS fb_loan_ev36,
    sp_fin_loan_indb_event_b01_times_30d AS fb_loan_ev37,
    sp_fin_loan_indb_event_b02_times_30d AS fb_loan_ev38,
    sp_fin_loan_indb_event_b03_times_30d AS fb_loan_ev39,
    sp_fin_loan_indc_event_b01_times_30d AS fb_loan_ev40,
    sp_fin_loan_indc_event_b02_times_30d AS fb_loan_ev41,
    sp_fin_loan_indc_event_b03_times_30d AS fb_loan_ev42,
    sp_fin_loan_inde_event_b01_times_30d AS fb_loan_ev43,
    sp_fin_loan_inde_event_b02_times_30d AS fb_loan_ev44,
    sp_fin_loan_inde_event_b03_times_30d AS fb_loan_ev45,
    sp_fin_loan_indf_event_b01_times_30d AS fb_loan_ev46,
    sp_fin_loan_indf_event_b02_times_30d AS fb_loan_ev47,
    sp_fin_loan_indf_event_b03_times_30d AS fb_loan_ev48,
    sp_fin_loan_indg_event_b01_times_30d AS fb_loan_ev49,
    sp_fin_loan_indg_event_b02_times_30d AS fb_loan_ev50,
    sp_fin_loan_indg_event_b03_times_30d AS fb_loan_ev51,
    sp_fin_loan_inda_event_b01_times_90d AS fb_loan_ev52,
    sp_fin_loan_inda_event_b02_times_90d AS fb_loan_ev53,
    sp_fin_loan_inda_event_b03_times_90d AS fb_loan_ev54,
    sp_fin_loan_indb_event_b01_times_90d AS fb_loan_ev55,
    sp_fin_loan_indb_event_b02_times_90d AS fb_loan_ev56,
    sp_fin_loan_indb_event_b03_times_90d AS fb_loan_ev57,
    sp_fin_loan_indc_event_b01_times_90d AS fb_loan_ev58,
    sp_fin_loan_indc_event_b02_times_90d AS fb_loan_ev59,
    sp_fin_loan_indc_event_b03_times_90d AS fb_loan_ev60,
    sp_fin_loan_inde_event_b01_times_90d AS fb_loan_ev61,
    sp_fin_loan_inde_event_b02_times_90d AS fb_loan_ev62,
    sp_fin_loan_inde_event_b03_times_90d AS fb_loan_ev63,
    sp_fin_loan_indf_event_b01_times_90d AS fb_loan_ev64,
    sp_fin_loan_indf_event_b02_times_90d AS fb_loan_ev65,
    sp_fin_loan_indf_event_b03_times_90d AS fb_loan_ev66,
    sp_fin_loan_indg_event_b01_times_90d AS fb_loan_ev67,
    sp_fin_loan_indg_event_b02_times_90d AS fb_loan_ev68,
    sp_fin_loan_indg_event_b03_times_90d AS fb_loan_ev69,
    sp_fin_loan_inda_event_b01_times_180d AS fb_loan_ev70,
    sp_fin_loan_inda_event_b02_times_180d AS fb_loan_ev71,
    sp_fin_loan_inda_event_b03_times_180d AS fb_loan_ev72,
    sp_fin_loan_indb_event_b01_times_180d AS fb_loan_ev73,
    sp_fin_loan_indb_event_b02_times_180d AS fb_loan_ev74,
    sp_fin_loan_indb_event_b03_times_180d AS fb_loan_ev75,
    sp_fin_loan_indc_event_b01_times_180d AS fb_loan_ev76,
    sp_fin_loan_indc_event_b02_times_180d AS fb_loan_ev77,
    sp_fin_loan_indc_event_b03_times_180d AS fb_loan_ev78,
    sp_fin_loan_inde_event_b01_times_180d AS fb_loan_ev79,
    sp_fin_loan_inde_event_b02_times_180d AS fb_loan_ev80,
    sp_fin_loan_inde_event_b03_times_180d AS fb_loan_ev81,
    sp_fin_loan_indf_event_b01_times_180d AS fb_loan_ev82,
    sp_fin_loan_indf_event_b02_times_180d AS fb_loan_ev83,
    sp_fin_loan_indf_event_b03_times_180d AS fb_loan_ev84,
    sp_fin_loan_indg_event_b01_times_180d AS fb_loan_ev85,
    sp_fin_loan_indg_event_b02_times_180d AS fb_loan_ev86,
    sp_fin_loan_indg_event_b03_times_180d AS fb_loan_ev87,
    sp_fin_loan_inda_event_b01_times_360d AS fb_loan_ev88,
    sp_fin_loan_inda_event_b02_times_360d AS fb_loan_ev89,
    sp_fin_loan_inda_event_b03_times_360d AS fb_loan_ev90,
    sp_fin_loan_indb_event_b01_times_360d AS fb_loan_ev91,
    sp_fin_loan_indb_event_b02_times_360d AS fb_loan_ev92,
    sp_fin_loan_indb_event_b03_times_360d AS fb_loan_ev93,
    sp_fin_loan_indc_event_b01_times_360d AS fb_loan_ev94,
    sp_fin_loan_indc_event_b02_times_360d AS fb_loan_ev95,
    sp_fin_loan_indc_event_b03_times_360d AS fb_loan_ev96,
    sp_fin_loan_inde_event_b01_times_360d AS fb_loan_ev97,
    sp_fin_loan_inde_event_b02_times_360d AS fb_loan_ev98,
    sp_fin_loan_inde_event_b03_times_360d AS fb_loan_ev99,
    sp_fin_loan_indf_event_b01_times_360d AS fb_loan_ev100,
    sp_fin_loan_indf_event_b02_times_360d AS fb_loan_ev101,
    sp_fin_loan_indf_event_b03_times_360d AS fb_loan_ev102,
    sp_fin_loan_indg_event_b01_times_360d AS fb_loan_ev103,
    sp_fin_loan_indg_event_b02_times_360d AS fb_loan_ev104,
    sp_fin_loan_indg_event_b03_times_360d AS fb_loan_ev105
FROM event_stats;
