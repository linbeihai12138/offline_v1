set hive.exec.mode.local.auto=true;
use sx_003;


select
    count(distinct pl.user_id), -- 商品访客数
    count(pl.user_id),          -- 商品浏览量
    0,                          --有访问商品数
    avg(pl.during_time),        --商品平均停留时长
    0,                          --商品详情页跳出率
    count(distinct pl.user_id), --商品收藏人数
    sum(dci.sku_num),           --商品加购件数
    count(distinct dci.user_id),--商品加购人数
    count(distinct pl.user_id) / count(distinct pl.user_id),  --访问收藏转化率
    count(distinct dci.user_id) / count(distinct pl.user_id), --访问加购转化率
    count(distinct doi.user_id),--下单买家数
    count(doi.id),              --下单件数
    sum(doi.final_amount),      --下单金额
    count(distinct doi.user_id) / count(distinct pl.user_id), --下单转化率
    sum(if(doi.order_status=1,1,0)),                       --支付件数
    sum(if(doi.order_status=1,doi.final_amount,0)),        --支付金额
    count(if(doi.order_status=1,1,null)),                  --有支付商品数
    count(if(doi.order_status=1,1,null)) / count(doi.id),  --支付转化率
    sum(if(doi.order_status=1,doi.final_amount,0)) / count(if(doi.order_status=4877,1,null)),--客单价
    sum(if(doi.order_status!=1,doi.final_amount,0)),       --成功退款退货金额
    sum(if(doi.order_status=1,doi.final_amount,0)) / count(distinct pl.user_id),--访客平均价值
    0                           --商品微详情访客数
from sx_003.ods_sku_info osi
         left join sx_003.dwd_page_log pl
                   on osi.sku_name=pl.model and pl.dt= osi.dt
         left join sx_003.dwd_favor_info dfi
                   on osi.id=dfi.sku_id and dfi.dt=osi.dt
         left join sx_003.dwd_cart_info dci
                   on dci.sku_id=osi.id and dci.dt=osi.dt
         left join sx_003.dwd_order_detail dod
                   on dod.sku_id=osi.id and dod.dt=osi.dt
         left join sx_003.dwd_order_info doi
                   on doi.id=dod.order_id and doi.dt=dod.dt
         left join sx_003.ods_user_info oui
                   on doi.user_id=oui.id and doi.dt=oui.dt
where osi.dt ='2025-03-24' group by osi.sku_name;


drop table sx_004.ads_sku_gd1;
select * from sx_004.ads_sku_gd1;
CREATE EXTERNAL TABLE sx_004.ads_sku_gd1 (
    recent_days STRING COMMENT '日期',
    visit_uv STRING COMMENT '商品访客数',
    order_id STRING COMMENT '商品浏览量',
    sku_visit_pv STRING COMMENT '有访问商品数',
    avg_stay_time STRING COMMENT '商品平均停留时长',
    bounce_rate STRING COMMENT '商品详情页跳出率',
    favor_uv STRING COMMENT '商品收藏人数',
    cart_uv STRING COMMENT '商品加购件数',
    cart_num STRING COMMENT '商品加购人数',
    favor_visit_uv STRING COMMENT '访问收藏转化率',
    cart_visit_uv STRING COMMENT '访问加购转化率',
    user_id STRING COMMENT '下单买家数',
    order_num STRING COMMENT '下单件数',
    order_amount STRING COMMENT '下单金额',
    user_visit_id STRING COMMENT '下单转化率',
    order_status STRING COMMENT '支付买家数',
    order_status_num STRING COMMENT '支付件数',
    order_status_amount STRING COMMENT '支付金额',
    sku_status STRING COMMENT '有支付商品数',
    order_sku_id STRING COMMENT '支付转化率',
    order_sku_amount STRING COMMENT '客单价',
    refund_amount STRING COMMENT '成功退款退货金额',
    order_id_start STRING COMMENT '年累计支付金额',
    order_id_uv STRING COMMENT '访客平均价值',
    total_score STRING COMMENT '竞争力评分'

) COMMENT '商品订单表'
row format delimited fields terminated by '\t';
select * from sx_004.ads_sku_gd1;
insert into table sx_004.ads_sku_gd1
select
    dpo.recent_days,
    sum(dpvl.visit_uv),        --商品访客数
    sum(dpvl.visit_pv),        --商品浏览量
    count(if(dpvl.visit_uv!=0,1,null)),     --有访问商品数
    avg(dpvl.avg_stay_time),   --商品平均停留时长
    sum(dpvl.bounce_rate),     --商品详情页跳出率
    sum(dpvl.favor_uv),        --商品收藏人数
    sum(dpvl.cart_uv),         --商品加购件数
    sum(dpvl.cart_num),        --商品加购人数
    sum(dpvl.favor_uv) / sum(dpvl.visit_uv),--访问收藏转化率
    sum(dpvl.cart_uv) / sum(dpvl.visit_pv), --访问加购转化率
    count(distinct dpo.user_id),            --下单买家数
    sum(dpo.order_num),                     --下单件数
    sum(dpo.order_amount),                  --下单金额
    count(distinct dpo.user_id) / sum(dpvl.visit_uv),--下单转化率
    count(if(dpo.order_status='已支付',1,null)),      --支付买家数
    case when sum(if(dpo.order_status='已支付',order_num,0))<=50 then '0~50'
         when  sum(if(dpo.order_status='已支付',order_num,0))>50 and sum(if(dpo.order_status='已支付',order_num,0))<= 100 then '50~100'--支付件数
         when  sum(if(dpo.order_status='已支付',order_num,0))>100 and sum(if(dpo.order_status='已支付',order_num,0)) <=150 then '150~100'--支付件数
         else '150~300'
        end,
    sum(if(dpo.order_status='已支付',order_amount,0)),--支付金额
    count(if(dpo.order_status='已支付',1,null)),      --有支付商品数
    sum(if(dpo.order_status='已支付',1,0)) / count(dpo.sku_id),--支付转化率
    sum(dpo.order_amount) / count(dpo.sku_id),       --客单价
    sum(dpp.refund_amount),                          --成功退款退货金额
    0,                                               --年累计支付金额
    sum(if(dpo.order_status='已支付',order_amount,0)) / sum(dpvl.visit_uv),--访客平均价值
    sum(dpc.total_score)                             --竞争力评分
from (select * from sx_004.dwd_product_order lateral view explode(Array(1,7,30)) tmp as recent_days where from_unixtime(unix_timestamp(ds, 'yyyyMMdd'), 'yyyy-MM-dd') >= date_sub(from_unixtime(unix_timestamp('20250331', 'yyyyMMdd'), 'yyyy-MM-dd'), recent_days)) dpo
         left join sx_004.dwd_product_visit_log dpvl
                   on dpo.sku_id=dpvl.sku_id and dpo.ds=dpvl.ds
         left join sx_004.dwd_product_payment dpp
                   on dpp.sku_id=dpo.sku_id and dpp.ds=dpo.ds
         left join sx_004.dwd_product_competition dpc
                   on dpo.sku_id=dpc.sku_id and dpo.ds=dpc.ds
group by dpo.sku_id,dpo.recent_days
UNION ALL
select
    recent_days,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    '0',
    0,
    0,
    0,
    0,
    0,
    sum(if(dpo2.order_status='已支付',order_amount,0)),
    0,
    0
from (select * from sx_004.dwd_product_order lateral view explode(Array(1,7,30)) tmp as recent_days where ds <= '20250401'  and ds > '20250401') dpo2
         left join sx_004.dwd_product_payment dpp2
                   on dpp2.sku_id=dpo2.sku_id and dpp2.ds=dpo2.ds
group by dpo2.sku_id,recent_days;













select
    dpo.recent_days,
    sum(dpvl.visit_uv),        --商品访客数
    sum(dpvl.visit_pv),        --商品浏览量
    sum(dpvl.visit_pv),        --有访问商品数
    avg(dpvl.avg_stay_time),   --商品平均停留时长
    sum(dpvl.bounce_rate),     --商品详情页跳出率
    sum(dpvl.favor_uv),        --商品收藏人数
    sum(dpvl.cart_uv),         --商品加购件数
    sum(dpvl.cart_num),        --商品加购人数
    sum(dpvl.favor_uv) / sum(dpvl.visit_uv),--访问收藏转化率
    sum(dpvl.cart_uv) / sum(dpvl.visit_pv), --访问加购转化率
    count(distinct dpo.user_id),            --下单买家数
    sum(dpo.order_num),                     --下单件数
    sum(dpo.order_amount),                  --下单金额
    count(distinct dpo.user_id) / sum(dpvl.visit_uv),--下单转化率
    count(if(dpo.order_status='已支付',1,null)),      --支付买家数
    sum(if(dpo.order_status='已支付',order_num,0)),   --支付件数
    sum(if(dpo.order_status='已支付',order_amount,0)),--支付金额
    0,                                               --有支付商品数
    sum(if(dpo.order_status='已支付',1,0)) / count(dpo.sku_id),--支付转化率
    sum(dpo.order_amount) / count(dpo.sku_id),       --客单价
    sum(dpp.refund_amount),                          --成功退款退货金额
    0,                                               --年累计支付金额
    sum(if(dpo.order_status='已支付',order_amount,0)) / sum(dpvl.visit_uv),--访客平均价值
    sum(dpc.total_score)                             --竞争力评分
from (select * from sx_004.dwd_product_order lateral view explode(Array(1,7,30)) tmp as recent_days where ds >= '20250331' -recent_days) dpo
         left join sx_004.dwd_product_visit_log dpvl
                   on dpo.sku_id=dpvl.sku_id and dpo.ds=dpvl.ds
         left join sx_004.dwd_product_payment dpp
                   on dpp.sku_id=dpo.sku_id and dpp.ds=dpo.ds
         left join sx_004.dwd_product_competition dpc
                   on dpo.sku_id=dpc.sku_id and dpo.ds=dpc.ds
group by dpo.sku_id,dpo.recent_days;


set hive.exec.mode.local.auto=true;
create database if not exists sx_004;
use sx_004;
drop table dwd_product_visit_log;
CREATE EXTERNAL TABLE dwd_product_visit_log (
    sku_id STRING COMMENT '商品ID',
    visit_uv BIGINT COMMENT '访客数',
    visit_pv BIGINT COMMENT '浏览量',
    avg_stay_time BIGINT COMMENT '平均停留时长（秒）',
    bounce_rate DECIMAL(4,2) COMMENT '跳出率',
    favor_uv BIGINT COMMENT '收藏人数',
    cart_uv BIGINT COMMENT '加购人数',
    cart_num BIGINT COMMENT '加购件数'
) COMMENT '商品访问日志表'
PARTITIONED BY (ds STRING);
select * from dwd_product_visit_log;


CREATE EXTERNAL TABLE dwd_product_order (
    order_id STRING COMMENT '订单ID',
    sku_id STRING COMMENT '商品ID',
    user_id STRING COMMENT '用户ID',
    order_amount DECIMAL(16,2) COMMENT '订单金额',
    order_num BIGINT COMMENT '订单件数',
    order_status STRING COMMENT '订单状态'
) COMMENT '商品订单表'
PARTITIONED BY (ds STRING);
select * from dwd_product_order;

drop table dwd_product_payment;
CREATE EXTERNAL TABLE dwd_product_payment (
    payment_id STRING COMMENT '支付ID',
    order_id STRING COMMENT '订单ID',
    sku_id STRING COMMENT '商品ID',
    payment_amount DECIMAL(16,2) COMMENT '支付金额',
    payment_time STRING COMMENT '支付时间',
    refund_amount DECIMAL(16,2) COMMENT '退款金额'
) COMMENT '商品支付表'
PARTITIONED BY (ds STRING);

select *
from dwd_product_payment;

CREATE EXTERNAL TABLE dwd_product_competition (
    sku_id STRING COMMENT '商品ID',
    traffic_score DECIMAL(4,2) COMMENT '流量评分',
    conversion_score DECIMAL(4,2) COMMENT '转化评分',
    content_score DECIMAL(4,2) COMMENT '内容评分',
    service_score DECIMAL(4,2) COMMENT '服务评分',
    total_score DECIMAL(4,2) COMMENT '综合评分'
) COMMENT '商品竞争力评分表'
PARTITIONED BY (ds STRING);

select * from dwd_product_competition;



set hive.exec.mode.local.auto=true;
use sx_004;
CREATE EXTERNAL TABLE sx_004.ads_goods_gd2 (
    ds string ,
    source_type STRING COMMENT '流量来源类型',
    visitors STRING COMMENT '访客数',
    conversion_rate DECIMAL(5,2) COMMENT '转化率'
) COMMENT '商品订单表'
row format delimited fields terminated by '\t';
insert into table sx_004.ads_goods_gd2
select
    recent_days,
    source_type,
    sum(visitors) as visitore_sum,
    sum(conversion_rate)
from sx_004.dwd_traffic_source dts lateral view explode(Array(1,7,30)) tmp as recent_days where ds >= '20250331' -recent_days
group by dts.source_type,recent_days order by visitore_sum desc limit 10;




CREATE EXTERNAL TABLE sx_004.ads_goods_gd2_1 (
    ds string ,
    source_type STRING COMMENT 'sku_id',
    sales_qty STRING COMMENT '销售数量',
    stock STRING COMMENT '当前库存',
    stock_days DECIMAL(5,2) COMMENT '库存可售天数'
) COMMENT '商品订单表'
row format delimited fields terminated by '\t';
insert into table sx_004.ads_goods_gd2_1
select
    recent_days,
    sku_id,
    sum(sales_qty) as sales_qty_sum,
    sum(stock),
    sum(stock_days)
from sx_004.dwd_sku_sales dss lateral view explode(Array(1,7,30)) tmp as recent_days where ds >= '20250331' -recent_days
group by dss.sku_id,recent_days order by sales_qty_sum desc limit 5;



CREATE TABLE sx_004.ads_search_gd2_2 (
                                         search_term string COMMENT '搜索关键词',
                                         search_count string COMMENT '搜索次数'
) COMMENT '搜索词分析表';
insert into table sx_004.ads_search_gd2_2
select
    search_term,
    sum(search_count) as search_count_sum
from sx_004.dwd_search_terms dst where ds='20250331'
group by search_term order by search_count_sum desc limit 10;


CREATE EXTERNAL TABLE sx_004.ads_sku_gd2_3 (
    ds string ,
    sku_id STRING COMMENT '商品ID',
    visit_uv BIGINT COMMENT '访客数',
    visit_pv BIGINT COMMENT '浏览量',
    avg_stay_time BIGINT COMMENT '平均停留时长（秒）',
    favor_uv BIGINT COMMENT '收藏人数'
) COMMENT '商品访问日志表';
insert into table sx_004.ads_sku_gd2_3
select
    recent_days,
    dpv.sku_id,
    sum(visit_uv),
    sum(visit_pv),
    avg(avg_stay_time),
    sum(favor_uv)
from sx_004.dwd_product_visit_log dpv lateral view explode(Array(1,7,30)) tmp as recent_days where ds >= date_sub(from_unixtime(unix_timestamp(ds, 'yyyyMMdd'), 'yyyy-MM-dd'), recent_days) >= date_sub(from_unixtime(unix_timestamp('20250331', 'yyyyMMdd'), 'yyyy-MM-dd'), recent_days)
group by dpv.sku_id,recent_days;


CREATE TABLE sx_004.ads_price_analysis (
                                           ds string ,
                                           goods_id String COMMENT '商品ID',
                                           price_star String COMMENT '价格力星级(1-5)',
                                           price_warning String COMMENT '价格力预警',
                                           product_warning String COMMENT '商品力预警',
                                           market_avg_rate DECIMAL(5,2) COMMENT '市场平均转化率'
) COMMENT '价格力商品分析表';
insert into table sx_004.ads_price_analysis
select
    recent_days,
    goods_id,
    sum(price_star),
    sum(price_warning),
    sum(product_warning),
    sum(market_avg_rate)
from sx_004.dwd_price_analysis dpa lateral view explode(Array(1,7,30)) tmp as recent_days where date_sub(from_unixtime(unix_timestamp(ds, 'yyyyMMdd'), 'yyyy-MM-dd'), recent_days) >= date_sub(from_unixtime(unix_timestamp('20250331', 'yyyyMMdd'), 'yyyy-MM-dd'), recent_days)
group by dpa.goods_id,recent_daysset hive.exec.mode.local.auto=true;
use sx_004;

CREATE TABLE sx_004.dwd_price_analysis (
                                           goods_id String COMMENT '商品ID',
                                           price_star String COMMENT '价格力星级(1-5)',
                                           coupon_price DECIMAL(18,2) COMMENT '普惠券后价',
                                           price_warning String COMMENT '价格力预警',
                                           product_warning String COMMENT '商品力预警',
                                           market_avg_rate DECIMAL(5,2) COMMENT '市场平均转化率',
                                           stat_date String COMMENT '统计日期'
) COMMENT '价格力商品分析表'
partitioned by (ds string);

select * from sx_004.dwd_price_analysis;

CREATE TABLE sx_004.dwd_traffic_source (
                                           goods_id STRING COMMENT '商品ID',
                                           source_type STRING COMMENT '流量来源类型',
                                           visitors STRING COMMENT '访客数',
                                           conversion_rate DECIMAL(5,2) COMMENT '转化率',
                                           stat_date STRING COMMENT '统计日期'
) COMMENT '商品流量来源表'partitioned by (ds string);

select * from sx_004.dwd_traffic_source;

CREATE TABLE sx_004.dwd_sku_sales (
                                      sku_id string COMMENT 'SKU ID',
                                      goods_id string COMMENT '商品ID',
                                      sales_qty string COMMENT '销售数量',
                                      stock string COMMENT '当前库存',
                                      stock_days string COMMENT '库存可售天数',
                                      stat_date string COMMENT '统计日期'
) COMMENT 'SKU销售监控表'partitioned by (ds string);

select * from sx_004.dwd_sku_sales;

CREATE TABLE sx_004.dwd_search_terms (
                                         goods_id string COMMENT '商品ID',
                                         search_term string COMMENT '搜索关键词',
                                         search_count string COMMENT '搜索次数',
                                         stat_date string COMMENT '统计日期'
) COMMENT '搜索词分析表'partitioned by (ds string);


select * from sx_004.dwd_search_terms;


CREATE TABLE sx_004.fact_sales_detail (
                                          goods_id string COMMENT '商品ID',
                                          sale_date string COMMENT '销售日期',
                                          sale_qty string COMMENT '销售数量',
                                          sale_amount DECIMAL(18,2) COMMENT '销售金额',
                                          pay_buyers string COMMENT '支付买家数'
) COMMENT '商品销售明细表'partitioned by (ds string);

select * from sx_004.fact_sales_detail;