create table if not exists cust_mining2.c1_person_feature_train
(
customer_no  string comment '客户id',   
op_date string comment '当前日期',
customer_gender string comment '客户性别',
open_age double comment '开户年限',
degree_code  string comment '学历', 
profession_code string comment '职业',
age double comment '年龄',
income double comment '年收入',
industry_type string comment '行业类别',
aml_risk_level string comment '反洗钱风险等级',
corp_risk_level string comment '风险等级',
label string comment'样本标签 '
)
partitioned by (part_date varchar(8)) 
stored as parquet




create table if not exists cust_mining2.c1_person_feature_test
(
customer_no  string comment '客户id',   
op_date string comment '当前日期',
customer_gender string comment '客户性别',
open_age double comment '开户年限',
degree_code  string comment '学历', 
profession_code string comment '职业',
age double  comment '年龄',
income double comment '年收入',
industry_type string comment '行业类别',
aml_risk_level string comment '反洗钱风险等级',
corp_risk_level string comment '风险等级'
)
partitioned by (part_date varchar(8)) 
stored as parquet