
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



--------------------------------
--  快递dwb中间层表
--------------------------------
create table sms_bd_data.dwb_express_event_fdt(
     phone string  comment '手机号',
     sign  string  comment '签名',
     event_type int comment '事件类型'
)comment '快递事件dwb中间层表'
partitioned by (
     the_date string comment 'YYYYMMDD 数据日期'
)
stored as orc;

insert overwrite table sms_bd_data.dwb_express_event_fdt partition(the_date)
select
     phone,
     sign,
     case when content regexp '(?:码为?|凭|号)[0-9A-Za-z-]+|已.*?签收|快递柜|自提柜|门口|送货上门|已放到您|[及时尽快]来?取|代签|即将发往|领取.*?包裹|暂存|包裹已送至|手机号或运单号取件|订单[0-9]+已完成|发货' then 'A01'
          when content regexp '投诉|咨询|已受理|失败' then 'A02'
          when content regexp '寄件|投递.*?包裹' then 'A03'
     else 'A04' end as event_type
     ingestion_time as the_date
from(
     select
          phone,
          sign,
          content,
          ingestion_time
     from sms_bd_data.ods_yxx_spn_sms_detail_di
     where sign in ('极兔速递','申通快递','中通快递','圆通快递','韵达快递','邮政快递包裹','多多代收点','妈妈驿站','兔喜生活','韵达超市',
          '菜鸟','丰巢','菜鸟驿站','京东','京东配送','圆通速递','邮政电商标快','递管家','取件提醒','顺丰快递','邮政EMS','快宝驿站','取件通知',
          '驿收发','德邦快递','菜鸟裹裹','邮政驿站','邻里驿站'
     )
)sms


--------------------------------
--  信贷类dwb中间层表
--------------------------------
create table sms_bd_data.dwb_fin_loan_event_fdt(
     phone string  comment '手机号',
     sign  string  comment '签名',
     event_type int comment '事件类型'
)comment '金融事件dwb中间层表'
partitioned by (
     the_date string comment 'YYYYMMDD 数据日期'
)
stored as orc;


-----------------------
-- B01: 还款提醒
-- B02：重度逾期
-- B03：轻度逾期
-- B04：贷款营销
-- B05：放款成功
-- B06：还款成功
-- B07：消费
-----------------------
insert overwrite table sms_bd_data.dwb_fin_loan_event_fdt partition(the_date)
select
     phone,
     sign,
     case when content regexp '(?:消费|下单).*?元' then 'B01'
          when content regexp '已[结还]清' then 'B02'
          when content regexp '(?:还款|扣款|扣划)失败|已过还款日' then 'B03'
          when content regexp '已[逾超]期|未按时还款' then 'B04'
          when content regexp '严重逾期|(上.?门)?催.?缴|追缴|拒绝|严重违约|债权方' then 'B05'
          when content regexp '营销' then 'B06'
     else 'B08' end as event_type,
     ingestion_time as the_date
from(
     select
          phone,
          sign,
          content,
          ingestion_time
     from sms_bd_data.ods_yxx_spn_sms_detail_di
     where sign in ('抖音月付','美团月付','放心借','美团支付','携程金融','还呗','信用飞钱包','省呗','360借条','翼支付','辽沈银行',
    '拍拍贷','普惠金融','乐享借','融360','洋钱罐借款','众安金融','分期金融','信用飞','乐逸花','极融借款','微众银行','建设银行')
)sms
union all
select
     phone,
     sign,
     'B07' as event_type,
     ingestion_time as the_date
from(
     select
          phone,
          sign,
          content,
          ingestion_time
     from sms_bd_data.ods_yxx_spn_sms_detail_di
     where sign regexp '委外|调解|仲裁委|委托|法律|数科金融|满松科技|利信普惠|和信普惠|普信金融|卡卡金融|和信普惠|和信金融|利信金融|数信普惠|普惠快信|普惠信息|首山金融'
)sms





