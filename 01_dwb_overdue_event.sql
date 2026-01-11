
-- 快递事件
select * from ods_sms_content_sample_5pct 
where sign in ('极兔速递','申通快递','中通快递','圆通快递','韵达快递','邮政快递包裹','多多代收点','妈妈驿站','兔喜生活','韵达超市',
'菜鸟','丰巢','菜鸟驿站','京东','京东配送','圆通速递','邮政电商标快','递管家','取件提醒','顺丰快递','邮政EMS','快宝驿站','取件通知',
'驿收发','德邦快递','菜鸟裹裹','邮政驿站','邻里驿站')

case when content regexp '(?:码为?|凭|号)[0-9A-Za-z-]+|已.*?签收|快递柜|自提柜|门口|送货上门|已放到您|[及时尽快]来?取|代签|即将发往|领取.*?包裹|暂存|包裹已送至|手机号或运单号取件|订单[0-9]+已完成|发货' then 'A01'
     when content regexp '投诉|咨询|已受理|失败' then 'A02'
     when content regexp '寄件|投递.*?包裹|' then 'A03'
else 'A04' end as event_type


-- 金融事件
select * from ods_sms_content_sample_5pct 
where sign in ('抖音月付','美团月付','放心借','美团支付','携程金融','还呗','信用飞钱包','省呗','360借条','翼支付','辽沈银行',
'拍拍贷','首山金融','普惠金融','乐享借','融360','洋钱罐借款','众安金融','分期金融','信用飞','乐逸花','极融借款','微众银行','建设银行','和信普惠')


select sign,count(1),max(submitdate),min(submitdate) from  ods_sms_content_sample_5pct
group by sign order by count(1) desc limit 600


--------------------------------
--  快递dwb中间层表
--------------------------------
drop table sms_bd_data.sms_dwb_express_event_fdt;
create table sms_bd_data.sms_dwb_express_event_fdt(
     phone string  comment '手机号',
     sign  string  comment '签名',
     event_type string comment '事件类型'
)comment '快递事件dwb中间层表'
partitioned by (
     the_date string comment 'YYYYMMDD 数据日期'
)
STORED AS ORC;

select *,substr(submitdate, 0, 10) as the_date from sms_bd_data.ods_yxx_spn_sms_detail_di limit 100

SET hive.exec.dynamic.partition = true
SET hive.exec.dynamic.partition.mode = nonstrict
insert overwrite table sms_bd_data.sms_dwb_express_event_fdt partition(the_date)
select
     phone,
     sign,
     case when content regexp '(?:码为?|凭|号)[0-9A-Za-z-]+|已.*?签收|快递柜|自提柜|门口|送货上门|已放到您|[及时尽快]来?取|代签|即将发往|领取.*?包裹|暂存|包裹已送至|手机号或运单号取件|订单[0-9]+已完成|发货' then 'A01'
          when content regexp '投诉|咨询|已受理|失败' then 'A02'
          when content regexp '寄件|投递.*?(包裹|快递|快件)|上门取件' then 'A03'
     else 'A04' end as event_type,
     the_date
from(
     select
          phone,
          sign,
          content,
          imp_date as the_date
     from sms_bd_data.ods_yxx_spn_sms_detail_di
     where sign in ("极兔速递", "申通快递", "中通快递", "圆通快递", "韵达快递", "邮政快递包裹", "多多代收点", "兔喜生活", "菜鸟", "妈妈驿站", "韵达超市", "丰巢", "菜鸟驿站", "京东配送", "圆通速递", "邮政电商标快", "取件提醒", 
     "美团跑腿", "顺丰快递", "递管家", "邮政EMS", "快宝驿站", "取件通知", "德邦快递", "驿收发", "邮政驿站", "邻里驿站", "美团配送", "袋鼠智柜", "顺丰速运", "中国邮政", "驿小哥", "兔喜快递柜", "蜜罐", "心甜智能柜", "邮政快递", "微快递", "菜鸟裹裹"
     )
)sms


--------------------------------
--  信贷类dwb中间层表
--------------------------------
drop table sms_bd_data.dwb_fin_loan_event_fdt;
create table sms_bd_data.sms_dwb_fin_loan_event_fdt(
     phone string  comment '手机号',
     sign  string  comment '签名',
     event_type string comment '事件类型'
)comment '金融事件dwb中间层表'
partitioned by (
     the_date string comment 'YYYYMMDD 数据日期'
)
STORED AS ORC;
-----------------------
-- B01: 严重逾期
-- B02：三方催收
-- B03：中度逾期
-- B04：还款失败
-- B05：申请失败
-- B06：还款成功
-- B07：放款成功
-- B08：还款提醒
-- B09：贷款营销
-- B10：金融其他
-----------------------
insert overwrite table sms_bd_data.sms_dwb_fin_loan_event_fdt partition(the_date)
select
     phone,
     sign,
     case when content regexp '严重逾期|[催清追]缴|拒绝|严重违约|恶意违约|债权方|逃废债|诉前调查组|逾期材料|强制冻结全款|限高令|欠款.*?处理|无法全额清偿|逾期已久|调解方案|欠款数次|严重失信|执行记录|法院诉讼|恢复征信|纳入失信黑名单|减免协商|法院|诉讼|借款逾期|多次提醒.*?欠款' then 'B01'
          when content regexp '已[逾超过]期|未按时还款|已过还款日|已.*?逾期' then 'B03'
          when content regexp '(?:还款|扣款|扣划)失败|扣款.*?失败' then 'B04'
          when content regexp '申请未通过|未获得借款|放款失败|未放款成功' then 'B05'
          when content regexp '已[结还]清|成功还款|还款成功|自动扣款' then 'B06'
          when content regexp '(?:消费|下单).*?元|申请办理贷款|开通成功|成功发放|已获得.*?额度|借款成功' then 'B07'
          when content regexp '本期应还|还款提醒|及时还款|还款卡余额充足|今日需还款|应还.*?元|即将到期|已出账' then 'B08'
          when content regexp '最高.*?额度|免费额度|距离提现|领取[您你]的[0-9]+元|确认提款|提额福利|优惠利率|立即提现|额度.*?失效|立即提额|获取最新额度|放款特权' then 'B09'
     else 'B10' end as event_type,
     the_date
from(
     select
          phone,
          sign,
          content,
          imp_date as the_date
     from sms_bd_data.ods_yxx_spn_sms_detail_di
     where sign in ("抖音月付", "美团月付", "放心借", "携程金融", "消费分期", "还呗", "信用飞钱包", "广西北部湾银行", "省呗", "360借", "广西农信", "拍拍贷", "乐享借", "融360", "洋钱罐借款", "众安金融", "分期金融", "信用飞", "乐逸花", "极融借款", "微众银行",
      "辽沈银行", "建设银行", "好分期", "信用卡贷", "农业银行", "工商银行", "金瀛分期", "小赢卡贷", "分期乐", "宜享花", "众安贷借钱", "上海农商银行", "消费金融", "拍拍金融", "融逸花", "农商银行", "卡贷金融", "吉用花", "时光分期", "小花钱包", "借钱呗", "金豆花", 
      "信用飞金融", "来分期", "兴业银行", "小花借款", "好会借", "乐贷分期", "及贷", "柳州银行", "招联金融", "移动白条", "榕树贷款", "360贷款", "上饶银行", "上海拍拍贷", "融易分期", "借小花", "花呗", "捷信金融", "马上金融", "薇钱包", "你我贷", "北京银行", 
      "华瑞银行", "中原消费金融", "玖富借条", "美团金融服务", "马上消费", "分期消费")
)fin1
union all
select
     phone,
     sign,
     'B02' as event_type,
     the_date
from(
     select
          phone,
          sign,
          content,
          imp_date as the_date
     from sms_bd_data.ods_yxx_spn_sms_detail_di
     where sign in ("首山金融", "普惠金融", "和信普惠", "卡卡金融", "数信普惠", "利信金融", "玖富万卡", "钱站", "鹰潭市金融纠纷调解中心", "国美易卡", "普惠分期", "普惠快信", "上海金融", "玖富", "数科纠纷调解中心")
     or sign regexp '金融调解|数科金融|满松科技|利信普惠|普信金融|卡卡金融|和信普惠|和信金融|普惠信息'
)fin2

select * from ods_sms_content_sample_5pct where submitdate>='2025-12-20' and submitdate<='2025-12-22' and sign in ('抖音月付','美团月付','放心借','美团支付','携程金融','还呗','信用飞钱包','省呗','360借条','翼支付','辽沈银行',
    '拍拍贷','普惠金融','乐享借','融360','洋钱罐借款','众安金融','分期金融','信用飞','乐逸花','极融借款','微众银行','建设银行')
and content not rlike '(消费|下单).*?元|申请办理贷款|开通成功|开通成功|成功发放|已获得.*?额度|借款成功|成功开通|申请办理贷款|已[结还]清|成功还款|还款成功|自动扣款|(还款|扣款|扣划)失败|已过还款日|已[逾超]期|已过还款日|已.*?逾期|未按时还款|严重逾期|催缴|追缴|拒绝|严重违约|恶意违约|债权方|逃废债|诉前调查组|逾期材料|冻结全款|未通过|未放款成功|放款失败|本期应还|还款提醒|及时还款|还款卡余额充足|今日需还款|最高.*?额度|免费额度|距离提现|领取[您你]的[0-9]+元|确认提款|提额福利|优惠利率|立即提现|额度.*?失效|立即提额|获取最新额度|限高令|欠款.*?处理|无法全额清偿|逾期已久|调解方案|欠款数次|严重失信|执行记录|法院诉讼|恢复征信|纳入失信黑名单|减免协商|法院|节约诉讼|申请未通过|未获得借款|放款失败|未放款成功'
limit 500


select
     phone,
     sign,
     case when content regexp '(?:消费|下单).*?元|申请办理贷款|开通成功|成功发放|已获得.*?额度|借款成功' then 'B01'
          when content regexp '申请未通过|未获得借款|放款失败|未放款成功' then 'B02'
          when content regexp '已[结还]清|成功还款|还款成功|自动扣款' then 'B03'
          when content regexp '本期应还|还款提醒|及时还款|还款卡余额充足|今日需还款|应还.*?元|即将到期|已出账' then 'B04'
          when content regexp '(?:还款|扣款|扣划)失败|扣款.*?失败' then 'B05'
          when content regexp '已[逾超过]期|未按时还款|已过还款日|已.*?逾期' then 'B06'
          when content regexp '严重逾期|[催清追]缴|拒绝|严重违约|恶意违约|债权方|逃废债|诉前调查组|逾期材料|强制冻结全款|限高令|欠款.*?处理|无法全额清偿|逾期已久|调解方案|欠款数次|严重失信|执行记录|法院诉讼|恢复征信|纳入失信黑名单|减免协商|法院|诉讼|借款逾期|多次提醒' then 'B07'
          when content regexp '最高.*?额度|免费额度|距离提现|领取[您你]的[0-9]+元|确认提款|提额福利|优惠利率|立即提现|额度.*?失效|立即提额|获取最新额度|放款特权' then 'B09'
     else 'B10' end as event_type,
     ingestion_time as the_date
from(
     select
          phone,
          sign,
          content,
          ingestion_time
     from sms_bd_data.ods_yxx_spn_sms_detail_di
     where submitdate>='2025-12-01' and submitdate<='2025-12-01' and sign in ('抖音月付','美团月付','放心借','美团支付','携程金融','还呗','信用飞钱包','省呗','360借条','翼支付','辽沈银行',
    '拍拍贷','普惠金融','乐享借','融360','洋钱罐借款','众安金融','分期金融','信用飞','乐逸花','极融借款','微众银行','建设银行')
)sms


