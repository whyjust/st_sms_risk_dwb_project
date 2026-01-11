WITH cus_sample AS (
    SELECT 
        mobile_md5,
        back_date
    FROM(
        -- 需要满足回溯日期
        -- back_date要求YYYYMMDD格式
        select 
            mobile_md5,
            substr(back_date, 0, 10) as back_date
        from risk.loan_intent_samples_20241023
    )smp
    group by mobile_md5, back_date
),
customer_dws_logs AS (
    select
        mobile_md5, 
        back_date, 
        sign, 
        event_type, 
        ind_tag
    from(
        select
            cus_smp.mobile_md5,
            cus_smp.back_date,
            dwb_loan.sign,
            dwb_loan.event_type,
            case when dwb_loan.sign regexp "抖音月付|放心借|微众银行|花呗" then 'indA'
                 when dwb_loan.sign regexp "美团月付|携程金融|360借|拍拍贷|分期乐|拍拍金融|360贷款|上海拍拍贷|美团金融服务" then 'indB'
                 when dwb_loan.sign regexp "还呗|信用飞钱包|省呗|乐享借|融360|洋钱罐借款|众安金融|分期金融|信用飞|乐逸花|极融借款|好分期|信用卡贷|金瀛分期|小赢卡贷|宜享花|众安贷借钱|融逸花|卡贷金融|吉用花|时光分期|小花钱包|借钱呗|金豆花|信用飞金融|来分期|小花借款|好会借|乐贷分期|及贷|榕树贷款|融易分期|借小花|薇钱包|你我贷|玖富借条" then 'indC'
                 when dwb_loan.sign regexp "金融纠纷调解|数科金融|满松科技|利信普惠|普信金融|卡卡金融|和信普惠|和信金融|利信金融|数信普惠|普惠快信|普惠信息|首山金融|普惠金融|数信普惠|玖富万卡|钱站|鹰潭市金融纠纷调解中心|国美易卡|普惠分期|上海金融|数科纠纷调解中心" or sign in ('玖富') then 'indD'
                 when dwb_loan.sign regexp "消费分期|消费金融|招联金融|移动白条|捷信金融|马上金融|中原消费金融|马上消费|分期消费" then 'indE'
                 when dwb_loan.sign regexp "银行|农信|农商" then 'indF'
            else 'indG' end as ind_tag
        from cus_sample cus_smp
        left join(
            select
                phone,
                sign,
                event_type,
                from_unixtime(unix_timestamp(the_date, 'yyyyMMdd'), 'yyyy-MM-dd') as the_date
            from sms_bd_data.sms_dwb_fin_loan_event_fdt
        )dwb_loan
        on cus_smp.mobile_md5=dwb_loan.phone 
        where cus_smp.back_date > dwb_loan.the_date and dwb_loan.the_date >= date_sub(cus_smp.back_date, 360)
    )cus_dwb
    group by mobile_md5, back_date, sign, event_type, ind_tag
),aggregated_data AS (
    SELECT
        mobile_md5,
        back_date,
        -- 所有金融事件活跃记录数与平台数
        SUM(CASE WHEN the_date < back_date AND event_time >= date_sub(back_date, 15) THEN 1 ELSE 0 END) AS sp_fin_loan_times_15d,
        SUM(CASE WHEN the_date < back_date AND event_time >= date_sub(back_date, 30) THEN 1 ELSE 0 END) AS sp_fin_loan_times_30d,
        SUM(CASE WHEN the_date < back_date AND event_time >= date_sub(back_date, 90) THEN 1 ELSE 0 END) AS sp_fin_loan_times_90d,
        SUM(CASE WHEN the_date < back_date AND event_time >= date_sub(back_date, 180) THEN 1 ELSE 0 END) AS sp_fin_loan_times_180d,
        SUM(CASE WHEN the_date < back_date AND event_time >= date_sub(back_date, 360) THEN 1 ELSE 0 END) AS sp_fin_loan_times_360d,
        count(DISTINCT CASE WHEN the_date < back_date AND the_date >= date_sub(back_date, 15) then sign ELSE NULL END) AS sp_fin_loan_plat_cnt_15d,
        count(DISTINCT CASE WHEN the_date < back_date AND the_date >= date_sub(back_date, 30) then sign ELSE NULL END) AS sp_fin_loan_plat_cnt_30d,
        count(DISTINCT CASE WHEN the_date < back_date AND the_date >= date_sub(back_date, 90) then sign ELSE NULL END) AS sp_fin_loan_plat_cnt_90d,
        count(DISTINCT CASE WHEN the_date < back_date AND the_date >= date_sub(back_date, 180) then sign ELSE NULL END) AS sp_fin_loan_plat_cnt_180d,
        count(DISTINCT CASE WHEN the_date < back_date AND the_date >= date_sub(back_date, 360) then sign ELSE NULL END) AS sp_fin_loan_plat_cnt_360d,

        -- 不同细分行业活跃记录数
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND ind_tag='indA' THEN 1 ELSE 0 END) AS sp_fin_loan_inda_times_15d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND ind_tag='indA' THEN 1 ELSE 0 END) AS sp_fin_loan_inda_times_30d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND ind_tag='indA' THEN 1 ELSE 0 END) AS sp_fin_loan_inda_times_90d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND ind_tag='indA' THEN 1 ELSE 0 END) AS sp_fin_loan_inda_times_180d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND ind_tag='indA' THEN 1 ELSE 0 END) AS sp_fin_loan_inda_times_360d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND ind_tag='indB' THEN 1 ELSE 0 END) AS sp_fin_loan_indb_times_15d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND ind_tag='indB' THEN 1 ELSE 0 END) AS sp_fin_loan_indb_times_30d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND ind_tag='indB' THEN 1 ELSE 0 END) AS sp_fin_loan_indb_times_90d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND ind_tag='indB' THEN 1 ELSE 0 END) AS sp_fin_loan_indb_times_180d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND ind_tag='indB' THEN 1 ELSE 0 END) AS sp_fin_loan_indb_times_360d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND ind_tag='indC' THEN 1 ELSE 0 END) AS sp_fin_loan_indc_times_15d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND ind_tag='indC' THEN 1 ELSE 0 END) AS sp_fin_loan_indc_times_30d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND ind_tag='indC' THEN 1 ELSE 0 END) AS sp_fin_loan_indc_times_90d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND ind_tag='indC' THEN 1 ELSE 0 END) AS sp_fin_loan_indc_times_180d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND ind_tag='indC' THEN 1 ELSE 0 END) AS sp_fin_loan_indc_times_360d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND ind_tag='indD' THEN 1 ELSE 0 END) AS sp_fin_loan_indd_times_15d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND ind_tag='indD' THEN 1 ELSE 0 END) AS sp_fin_loan_indd_times_30d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND ind_tag='indD' THEN 1 ELSE 0 END) AS sp_fin_loan_indd_times_90d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND ind_tag='indD' THEN 1 ELSE 0 END) AS sp_fin_loan_indd_times_180d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND ind_tag='indD' THEN 1 ELSE 0 END) AS sp_fin_loan_indd_times_360d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND ind_tag='indE' THEN 1 ELSE 0 END) AS sp_fin_loan_inde_times_15d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND ind_tag='indE' THEN 1 ELSE 0 END) AS sp_fin_loan_inde_times_30d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND ind_tag='indE' THEN 1 ELSE 0 END) AS sp_fin_loan_inde_times_90d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND ind_tag='indE' THEN 1 ELSE 0 END) AS sp_fin_loan_inde_times_180d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND ind_tag='indE' THEN 1 ELSE 0 END) AS sp_fin_loan_inde_times_360d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND ind_tag='indF' THEN 1 ELSE 0 END) AS sp_fin_loan_indf_times_15d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND ind_tag='indF' THEN 1 ELSE 0 END) AS sp_fin_loan_indf_times_30d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND ind_tag='indF' THEN 1 ELSE 0 END) AS sp_fin_loan_indf_times_90d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND ind_tag='indF' THEN 1 ELSE 0 END) AS sp_fin_loan_indf_times_180d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND ind_tag='indF' THEN 1 ELSE 0 END) AS sp_fin_loan_indf_times_360d,

        -- 不同事件类型的次数
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='b01' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b01_times_15d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='b01' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b01_times_30d,  
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='b01' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b01_times_90d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='b01' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b01_times_180d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='b01' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b01_times_360d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='b02' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b02_times_15d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='b02' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b02_times_30d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='b02' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b02_times_90d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='b02' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b02_times_180d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='b02' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b02_times_360d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='b03' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b03_times_15d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='b03' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b03_times_30d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='b03' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b03_times_90d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='b03' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b03_times_180d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='b03' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b03_times_360d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='b04' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b04_times_15d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='b04' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b04_times_30d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='b04' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b04_times_90d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='b04' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b04_times_180d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='b04' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b04_times_360d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='b05' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b05_times_15d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='b05' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b05_times_30d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='b05' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b05_times_90d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='b05' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b05_times_180d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='b05' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b05_times_360d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='b06' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b06_times_15d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='b06' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b06_times_30d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='b06' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b06_times_90d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='b06' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b06_times_180d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='b06' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b06_times_360d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='b07' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b07_times_15d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='b07' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b07_times_30d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='b07' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b07_times_90d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='b07' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b07_times_180d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='b07' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b07_times_360d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='b08' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b08_times_15d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='b08' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b08_times_30d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='b08' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b08_times_90d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='b08' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b08_times_180d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='b08' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b08_times_360d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='b09' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b09_times_15d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='b09' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b09_times_30d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='b09' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b09_times_90d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='b09' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b09_times_180d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='b09' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b09_times_360d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='b10' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b10_times_15d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='b10' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b10_times_30d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='b10' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b10_times_90d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='b10' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b10_times_180d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='b10' THEN 1 ELSE 0 END) AS sp_fin_loan_event_b10_times_360d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='b01' THEN sign ELSE NULL END) AS sp_fin_loan_event_b01_plat_cnt_15d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='b01' THEN sign ELSE NULL END) AS sp_fin_loan_event_b01_plat_cnt_30d,  
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='b01' THEN sign ELSE NULL END) AS sp_fin_loan_event_b01_plat_cnt_90d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='b01' THEN sign ELSE NULL END) AS sp_fin_loan_event_b01_plat_cnt_180d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='b01' THEN sign ELSE NULL END) AS sp_fin_loan_event_b01_plat_cnt_360d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='b02' THEN sign ELSE NULL END) AS sp_fin_loan_event_b02_plat_cnt_15d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='b02' THEN sign ELSE NULL END) AS sp_fin_loan_event_b02_plat_cnt_30d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='b02' THEN sign ELSE NULL END) AS sp_fin_loan_event_b02_plat_cnt_90d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='b02' THEN sign ELSE NULL END) AS sp_fin_loan_event_b02_plat_cnt_180d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='b02' THEN sign ELSE NULL END) AS sp_fin_loan_event_b02_plat_cnt_360d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='b03' THEN sign ELSE NULL END) AS sp_fin_loan_event_b03_plat_cnt_15d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='b03' THEN sign ELSE NULL END) AS sp_fin_loan_event_b03_plat_cnt_30d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='b03' THEN sign ELSE NULL END) AS sp_fin_loan_event_b03_plat_cnt_90d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='b03' THEN sign ELSE NULL END) AS sp_fin_loan_event_b03_plat_cnt_180d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='b03' THEN sign ELSE NULL END) AS sp_fin_loan_event_b03_plat_cnt_360d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='b04' THEN sign ELSE NULL END) AS sp_fin_loan_event_b04_plat_cnt_15d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='b04' THEN sign ELSE NULL END) AS sp_fin_loan_event_b04_plat_cnt_30d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='b04' THEN sign ELSE NULL END) AS sp_fin_loan_event_b04_plat_cnt_90d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='b04' THEN sign ELSE NULL END) AS sp_fin_loan_event_b04_plat_cnt_180d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='b04' THEN sign ELSE NULL END) AS sp_fin_loan_event_b04_plat_cnt_360d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='b05' THEN sign ELSE NULL END) AS sp_fin_loan_event_b05_plat_cnt_15d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='b05' THEN sign ELSE NULL END) AS sp_fin_loan_event_b05_plat_cnt_30d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='b05' THEN sign ELSE NULL END) AS sp_fin_loan_event_b05_plat_cnt_90d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='b05' THEN sign ELSE NULL END) AS sp_fin_loan_event_b05_plat_cnt_180d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='b05' THEN sign ELSE NULL END) AS sp_fin_loan_event_b05_plat_cnt_360d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='b06' THEN sign ELSE NULL END) AS sp_fin_loan_event_b06_plat_cnt_15d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='b06' THEN sign ELSE NULL END) AS sp_fin_loan_event_b06_plat_cnt_30d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='b06' THEN sign ELSE NULL END) AS sp_fin_loan_event_b06_plat_cnt_90d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='b06' THEN sign ELSE NULL END) AS sp_fin_loan_event_b06_plat_cnt_180d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='b06' THEN sign ELSE NULL END) AS sp_fin_loan_event_b06_plat_cnt_360d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='b07' THEN sign ELSE NULL END) AS sp_fin_loan_event_b07_plat_cnt_15d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='b07' THEN sign ELSE NULL END) AS sp_fin_loan_event_b07_plat_cnt_30d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='b07' THEN sign ELSE NULL END) AS sp_fin_loan_event_b07_plat_cnt_90d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='b07' THEN sign ELSE NULL END) AS sp_fin_loan_event_b07_plat_cnt_180d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='b07' THEN sign ELSE NULL END) AS sp_fin_loan_event_b07_plat_cnt_360d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='b08' THEN sign ELSE NULL END) AS sp_fin_loan_event_b08_plat_cnt_15d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='b08' THEN sign ELSE NULL END) AS sp_fin_loan_event_b08_plat_cnt_30d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='b08' THEN sign ELSE NULL END) AS sp_fin_loan_event_b08_plat_cnt_90d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='b08' THEN sign ELSE NULL END) AS sp_fin_loan_event_b08_plat_cnt_180d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='b08' THEN sign ELSE NULL END) AS sp_fin_loan_event_b08_plat_cnt_360d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='b09' THEN sign ELSE NULL END) AS sp_fin_loan_event_b09_plat_cnt_15d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='b09' THEN sign ELSE NULL END) AS sp_fin_loan_event_b09_plat_cnt_30d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='b09' THEN sign ELSE NULL END) AS sp_fin_loan_event_b09_plat_cnt_90d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='b09' THEN sign ELSE NULL END) AS sp_fin_loan_event_b09_plat_cnt_180d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='b09' THEN sign ELSE NULL END) AS sp_fin_loan_event_b09_plat_cnt_360d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND event_type='b10' THEN sign ELSE NULL END) AS sp_fin_loan_event_b10_plat_cnt_15d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND event_type='b10' THEN sign ELSE NULL END) AS sp_fin_loan_event_b10_plat_cnt_30d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND event_type='b10' THEN sign ELSE NULL END) AS sp_fin_loan_event_b10_plat_cnt_90d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 180) AND event_type='b10' THEN sign ELSE NULL END) AS sp_fin_loan_event_b10_plat_cnt_180d,
        count(DISTINCT CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 360) AND event_type='b10' THEN sign ELSE NULL END) AS sp_fin_loan_event_b10_plat_cnt_360d,

        -- 不同平台与事件类型的组合次数
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 15) AND ind_tag='indA' AND event_type='b01' THEN 1 ELSE 0 END) AS sp_fin_loan_inda_event_b01_times_15d,
        SUM(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 30) AND ind_tag='indA' AND event_type='b01' THEN 1 ELSE 0 END) AS sp_fin_loan_inda_event_b01_times_30d,

        
        -- 所有事件工作日活跃记录数
        sum(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) AND weekday_tag < 6 then 1 ELSE NULL END) AS hl_fin_workday_times_90d_v2,
       
        -- 所有事件最近一次记录距今时长（单位天）
        datediff(back_date, max(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) then event_time ELSE NULL END)) AS hl_fin_active_recent_dura_d_beforep_90d_v2,
        
        -- 所有事件最远一次记录距今时长（单位天）
        datediff(back_date, min(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) then event_time ELSE NULL END)) AS hl_fin_active_remote_dura_d_beforep_90d_v2,
        
        -- 所有事件记录日期极差（单位天）
        datediff(max(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) then event_time ELSE NULL END), min(CASE WHEN event_time < back_date AND event_time >= date_sub(back_date, 90) then event_time ELSE NULL END)) AS hl_fin_active_date_range_90d_v2,
        
        -- 所有事件时间序列错位差值的平均值
        round(avg(CASE WHEN lead_event_time < back_date AND lead_event_time >= date_sub(back_date, 90) then datediff(event_time, lead_event_time) ELSE NULL END), 6) AS hl_fin_active_diffshift_date_avg_90d_v2,
        
        -- 所有事件时间序列错位差值的最大值
        max(CASE WHEN lead_event_time < back_date AND lead_event_time >= date_sub(back_date, 90) then datediff(event_time, lead_event_time) ELSE NULL END) AS hl_fin_active_diffshift_date_max_90d_v2,
        
        -- 所有事件时间序列错位差值的标准差
        round(STDDEV_POP(CASE WHEN lead_event_time < back_date AND lead_event_time >= date_sub(back_date, 90) then datediff(event_time, lead_event_time) ELSE NULL END), 6) AS hl_fin_active_diffshift_date_std_90d_v2,
           FROM customer_dws_logs
    GROUP BY mobile_id, back_date
)
INSERT OVERWRITE TABLE risk.dws_log_online_offline_customer_feature_1024 
SELECT
    *,
    round(hl_fin_workday_times_90d_v2 / hl_fin_times_90d_v2, 6) AS hl_fin_workday_active_ratio_90d_v2,
    round(hl_fin_workday_times_180d_v2 / hl_fin_times_180d_v2, 6) AS hl_fin_workday_active_ratio_180d_v2,
    round(hl_fin_workday_times_360d_v2 / hl_fin_times_360d_v2, 6) AS hl_fin_workday_active_ratio_360d_v2,
    round(hl_fin_workday_times_540d_v2 / hl_fin_times_540d_v2, 6) AS hl_fin_workday_active_ratio_540d_v2,
    round(hl_fin_workday_times_720d_v2 / hl_fin_times_720d_v2, 6) AS hl_fin_workday_active_ratio_720d_v2,
    round(hl_fin_workday_times_900d_v2 / hl_fin_times_900d_v2, 6) AS hl_fin_workday_active_ratio_900d_v2,
    round(hl_fin_workday_times_1080d_v2 / hl_fin_times_1080d_v2, 6) AS hl_fin_workday_active_ratio_1080d_v2,
    
    round(hl_fin_times_90d_v2 / hl_fin_times_180d_v2, 6) AS hl_fin_active_ratio_90to180d_v2,
    round(hl_fin_times_90d_v2 / hl_fin_times_360d_v2, 6) AS hl_fin_active_ratio_90to360d_v2,
    round(hl_fin_times_180d_v2 / hl_fin_times_360d_v2, 6) AS hl_fin_active_ratio_180to360d_v2,
    round(hl_fin_times_180d_v2 / hl_fin_times_720d_v2, 6) AS hl_fin_active_ratio_180to720d_v2,
    round(hl_fin_times_360d_v2 / hl_fin_times_720d_v2, 6) AS hl_fin_active_ratio_360to720d_v2,
    round(hl_fin_times_180d_v2 / hl_fin_times_1080d_v2, 6) AS hl_fin_active_ratio_180to1080d_v2,

    IF(hl_fin_active_diffshift_date_std_90d_v2=0, NULL, round(hl_fin_active_diffshift_date_avg_90d_v2 / hl_fin_active_diffshift_date_std_90d_v2, 6)) AS hl_fin_active_diffshift_date_cv_90d_v2,
    IF(hl_fin_active_diffshift_date_std_180d_v2=0, NULL, round(hl_fin_active_diffshift_date_avg_180d_v2 / hl_fin_active_diffshift_date_std_180d_v2, 6)) AS hl_fin_active_diffshift_date_cv_180d_v2,
    IF(hl_fin_active_diffshift_date_std_360d_v2=0, NULL, round(hl_fin_active_diffshift_date_avg_360d_v2 / hl_fin_active_diffshift_date_std_360d_v2, 6)) AS hl_fin_active_diffshift_date_cv_360d_v2,
    IF(hl_fin_active_diffshift_date_std_540d_v2=0, NULL, round(hl_fin_active_diffshift_date_avg_540d_v2 / hl_fin_active_diffshift_date_std_540d_v2, 6)) AS hl_fin_active_diffshift_date_cv_540d_v2,
    IF(hl_fin_active_diffshift_date_std_720d_v2=0, NULL, round(hl_fin_active_diffshift_date_avg_720d_v2 / hl_fin_active_diffshift_date_std_720d_v2, 6)) AS hl_fin_active_diffshift_date_cv_720d_v2,
    IF(hl_fin_active_diffshift_date_std_900d_v2=0, NULL, round(hl_fin_active_diffshift_date_avg_900d_v2 / hl_fin_active_diffshift_date_std_900d_v2, 6)) AS hl_fin_active_diffshift_date_cv_900d_v2,
    IF(hl_fin_active_diffshift_date_std_1080d_v2=0, NULL, round(hl_fin_active_diffshift_date_avg_1080d_v2 / hl_fin_active_diffshift_date_std_1080d_v2, 6)) AS hl_fin_active_diffshift_date_cv_1080d_v2
FROM aggregated_data;


