set hive.exec.mode.local.auto=true;
use dev_realtime_v1_tianxin_yue;
--用户信息表ods_user_info_inc、用户地址表ods_user_address_inc、
-- 字典表base_dic、小区表base_complex、地区表base_region_info、机构表base_organ
drop table if exists ods_order_info_tms01;
create table if not exists ods_order_info_tms01
(
    id bigint COMMENT 'id',
    order_no string COMMENT '订单号',
    status string COMMENT '订单状态',
    collect_type string COMMENT '取件类型，1为网点自寄，2为上门取件',
    user_id bigint COMMENT '客户id',
    reciver_complex_id bigint COMMENT '收件人小区id',
    receiver_province_id string COMMENT '收件人省份id',
    receiver_city_id string COMMENT '收件人城市id',
    receiver_district_id string COMMENT '收件人区县id',
    receiver_address string COMMENT '收件人详细地址',
    receiver_name string COMMENT '收件人姓名',
    receiver_phone string COMMENT '收件人电话',
    receive_location string COMMENT '起始点经纬度',
    sender_complex_id bigint COMMENT '发件人小区id',
    sender_province_id string COMMENT '发件人省份id',
    sender_city_id string COMMENT '发件人城市id',
    sender_district_id string COMMENT '发件人区县id',
    sender_address string COMMENT '发件人详细地址',
    sender_name string COMMENT '发件人姓名',
    sender_phone string COMMENT '发件人电话',
    send_location string COMMENT '发件人坐标',
    payment_type string COMMENT '支付方式',
    cargo_num int COMMENT '货物个数',
    amount decimal(32,2) COMMENT '金额',
    estimate_arrive_time string COMMENT '预计到达时间',
    distance decimal(10,2) COMMENT '距离，单位：公里',
    create_time string COMMENT '创建时间',
    update_time string COMMENT '更新时间',
    is_deleted string COMMENT '删除标记（0:不可用 1:可用）'
)partitioned by (dt string)
    row format delimited fields terminated by '\t';
load data inpath "/2207A/tianxin_yue/tms01/order_info/2025-03-25" into table ods_order_info_tms01 partition (dt='2025-03-25');

CREATE TABLE `ods_user_info_tms01` (
                             `id` bigint  COMMENT '编号',
                             `login_name` varchar(200)  COMMENT '用户名称',
                             `nick_name` varchar(200)  COMMENT '用户昵称',
                             `passwd` varchar(200)  COMMENT '用户密码',
                             `real_name` varchar(200)  COMMENT '用户姓名',
                             `phone_num` varchar(200)  COMMENT '手机号',
                             `email` varchar(200)  COMMENT '邮箱',
                             `head_img` varchar(200)  COMMENT '头像',
                             `user_level` varchar(200)  COMMENT '用户级别',
                             `birthday` string  COMMENT '用户生日',
                             `gender` varchar(1)  COMMENT '性别 M男,F女',
                             `create_time` string  COMMENT '创建时间',
                             `update_time` string  COMMENT '修改时间',
                             `is_deleted` varchar(200)  COMMENT '状态'

)partitioned by (dt string);
load data inpath "/2207A/tianxin_yue/tms01/user_info/2025-03-25" into table ods_user_info_tms01 partition (dt='2025-03-25');

CREATE TABLE `ods_user_address_tms01` (
                                `id` bigint  COMMENT 'id',
                                `user_id` bigint  COMMENT '用户id',
                                `phone` varchar(20)  COMMENT '电话号',
                                `province_id` bigint  COMMENT '所属省份id',
                                `city_id` bigint  COMMENT '所属城市id',
                                `district_id` bigint  COMMENT '所属区县id',
                                `complex_id` bigint  COMMENT '所属小区id',
                                `address` varchar(255)  COMMENT '详细地址',
                                `is_default` tinyint COMMENT '是否默认 0:否，1:是',
                                `create_time` string COMMENT '创建时间',
                                `update_time` string   COMMENT '更新时间',
                                `is_deleted` varchar(20) COMMENT '删除标记（0:不可用 1:可用）'

) partitioned by (dt string)
    row format delimited fields terminated by '\t';
load data inpath "/2207A/tianxin_yue/tms01/user_address/2025-03-25" into table ods_user_address_tms01 partition (dt='2025-03-25');

CREATE TABLE `ods_base_dic_tms01` (
                            `id` bigint  COMMENT 'id',
                            `parent_id` bigint   COMMENT '上级id',
                            `name` varchar(100)   COMMENT '名称',
                            `dict_code` varchar(20)  COMMENT '编码',
                            `create_time` string  COMMENT '创建时间',
                            `update_time` string COMMENT '更新时间',
                            `is_deleted` tinyint  COMMENT '删除标记（0:不可用 1:可用）'
)  partitioned by (dt string)
    row format delimited fields terminated by '\t';
load data inpath "/2207A/tianxin_yue/tms01/base_dic/2025-03-25" into table ods_base_dic_tms01 partition (dt='2025-03-25');


CREATE TABLE `ods_base_complex_tms01` (
                                `id` bigint,
                                `complex_name` varchar(200) ,
                                `province_id` bigint ,
                                `city_id` bigint ,
                                `district_id` bigint ,
                                `district_name` varchar(200) ,
                                `create_time` string ,
                                `update_time` string ,
                                `is_deleted` varchar(2)

) partitioned by (dt string)
    row format delimited fields terminated by '\t';
load data inpath "/2207A/tianxin_yue/tms01/base_complex/2025-03-25" into table ods_base_complex_tms01 partition (dt='2025-03-25');

CREATE TABLE `ods_base_region_info_tms01` (
                                    `id` bigint  COMMENT 'id',
                                    `parent_id` bigint  COMMENT '上级id',
                                    `name` varchar(100)  COMMENT '名称',
                                    `dict_code` varchar(20)  COMMENT '编码',
                                    `short_name` varchar(20)  COMMENT '简称',
                                    `create_time` string COMMENT '创建时间',
                                    `update_time` string COMMENT '更新时间',
                                    `is_deleted` tinyint  COMMENT '删除标记（0:不可用 1:可用）'

) partitioned by (dt string)
    row format delimited fields terminated by '\t';
load data inpath "/2207A/tianxin_yue/tms01/base_region_info/2025-03-25" into table ods_base_region_info_tms01 partition (dt='2025-03-25');


CREATE TABLE `ods_base_organ_tms01` (
                              `id` bigint ,
                              `org_name` varchar(200)  COMMENT '机构名称',
                              `org_level` bigint  COMMENT '行政级别',
                              `region_id` bigint  COMMENT '区域id，1级机构为city ,2级机构为district',
                              `org_parent_id` bigint  COMMENT '上级机构id',
                              `points` string COMMENT '多边形经纬度坐标集合',
                              `create_time` string COMMENT '创建时间',
                              `update_time` string COMMENT '更新时间',
                              `is_deleted` varchar(20)  COMMENT '删除标记（0:不可用 1:可用）'

) partitioned by (dt string)
    row format delimited fields terminated by '\t';
load data inpath "/2207A/tianxin_yue/tms01/base_organ/2025-03-25" into table ods_base_organ_tms01 partition (dt='2025-03-25');

CREATE TABLE `ods_order_cargo_tms01` (
                               `id` bigint ,
                               `order_id` varchar(20) COMMENT '订单id',
                               `cargo_type` varchar(20)  COMMENT '货物类型id',
                               `volume_length` int  COMMENT '长cm',
                               `volume_width` int  COMMENT '宽cm',
                               `volume_height` int  COMMENT '高cm',
                               `weight` decimal(16,2)  COMMENT '重量 kg',
                               `remark` string COMMENT '备注',
                               `create_time` string COMMENT '创建时间',
                               `update_time` string   COMMENT '更新时间',
                               `is_deleted` varchar(20)  COMMENT '删除标记（0:不可用 1:可用）'

) partitioned by (dt string)
    row format delimited fields terminated by '\t';
load data inpath "/2207A/tianxin_yue/tms01/order_cargo/2025-03-25" into table ods_order_cargo_tms01 partition (dt='2025-03-25');
--------------------dim----------------------
create table if not exists dim_base_complex_tms01
(
    id bigint,
    complex_name string,
    province_id bigint,
    city_id bigint,
    district_id bigint,
    district_name string,
    create_time string,
    update_time string,
    is_deleted string
)partitioned by (dt string)
    row format delimited fields terminated by '\t'

insert overwrite table dim_base_complex_tms01
select * from ods_base_complex_tms01;

-- 地区表base_region_info、
create table if not exists dim_base_region_info_tms01
(
    id bigint COMMENT 'id',
    parent_id bigint COMMENT '上级id',
    name string COMMENT '名称',
    dict_code string COMMENT '编码',
    short_name string COMMENT '简称',
    create_time string COMMENT '创建时间',
    update_time string COMMENT '更新时间',
    is_deleted int COMMENT '删除标记（0:不可用 1:可用）'
)partitioned by (dt string)
    row format delimited fields terminated by '\t';
insert overwrite table dim_base_region_info_tms01
select * from ods_base_region_info_tms01;

-- 机构表base_organ
create table if not exists dim_base_organ_tms01
(
    id bigint,
    org_name string COMMENT '机构名称',
    org_level bigint COMMENT '行政级别',
    region_id bigint COMMENT '区域id，1级机构为city ,2级机构为district',
    org_parent_id bigint COMMENT '上级机构id',
    points string COMMENT '多边形经纬度坐标集合',
    create_time string COMMENT '创建时间',
    update_time string COMMENT '更新时间',
    is_deleted string COMMENT '删除标记（0:不可用 1:可用）'
)partitioned by (dt string)
    row format delimited fields terminated by '\t';
insert overwrite table dim_base_organ_tms01
select * from ods_base_organ_tms01;



---------------dwd---------------------
-- 交易域-下单-事务事实表
drop table dwd_order_info_tms01;
create table if not exists dwd_order_info_tms01
(
    id bigint COMMENT 'id',
    order_no string COMMENT '订单号',
    status string COMMENT '订单状态',
    collect_type string COMMENT '取件类型，1为网点自寄，2为上门取件',
    user_id bigint COMMENT '客户id',
    reciver_complex_id bigint COMMENT '收件人小区id',
    receiver_province_id string COMMENT '收件人省份id',
    receiver_city_id string COMMENT '收件人城市id',
    receiver_district_id string COMMENT '收件人区县id',
    receiver_address string COMMENT '收件人详细地址',
    receiver_name string COMMENT '收件人姓名',
    receiver_phone string COMMENT '收件人电话',
    receive_location string COMMENT '起始点经纬度',
    sender_complex_id bigint COMMENT '发件人小区id',
    sender_province_id string COMMENT '发件人省份id',
    sender_city_id string COMMENT '发件人城市id',
    sender_district_id string COMMENT '发件人区县id',
    sender_address string COMMENT '发件人详细地址',
    sender_name string COMMENT '发件人姓名',
    sender_phone string COMMENT '发件人电话',
    send_location string COMMENT '发件人坐标',
    payment_type string COMMENT '支付方式',
    cargo_num int COMMENT '货物个数',
    amount decimal(32,2) COMMENT '金额',
    estimate_arrive_time string COMMENT '预计到达时间',
    distance decimal(10,2) COMMENT '距离，单位：公里',
    create_time string COMMENT '创建时间',
    update_time string COMMENT '更新时间',
    is_deleted string COMMENT '删除标记（0:不可用 1:可用）',

    re_city_name string comment '收件人超市',
    ca_type_id string comment '货物id',
    ca_type_name string comment '货物类型',
    co_type_name string comment '取件类型',
    staus_name string comment '订单状态'
)partitioned by (dt string)
    row format delimited fields terminated by '\t';
insert into dwd_order_info_tms01
select
oi.id,
oi.order_no,
oi.status,
oi.collect_type,
oi.user_id,
oi.reciver_complex_id,
oi.receiver_province_id,
oi.receiver_city_id,
oi.receiver_district_id,
oi.receiver_address,
oi.receiver_name,
oi.receiver_phone,
oi.receive_location,
oi.sender_complex_id,
oi.sender_province_id,
oi.sender_city_id,
oi.sender_district_id,
oi.sender_address,
oi.sender_name,
oi.sender_phone,
oi.send_location,
oi.payment_type,
oi.cargo_num,
oi.amount,
oi.estimate_arrive_time,
oi.distance,
oi.create_time,
oi.update_time,
oi.is_deleted,
br.name,
oc.cargo_type,
obd.name,
obdt01.name,
obdt012.name,
oi.dt
from ods_order_info_tms01 oi
left join ods_base_region_info_tms01 br on oi.receiver_city_id=br.id-- 地区表
left join ods_order_cargo_tms01 oc on oc.order_id=oi.id --运单明细
left join ods_base_dic_tms01 obd on oc.cargo_type=obd.id
left join  ods_base_dic_tms01 obdt01 on oi.collect_type=obdt01.id //字典表
left join ods_base_dic_tms01 obdt012 on oi.status=obdt012.id;


--数据倾斜
set hive.optimize.skewjoin=true;
set hive.skewjoin.key=100000;




