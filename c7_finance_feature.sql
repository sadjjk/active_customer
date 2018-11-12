create table if not exists cust_mining2.c7_finance_feature_train 
(
customer_no  string comment '客户id',   
op_date string comment '当前日期',
top1_finance_prob_up  double ,
top1_finance_volatility  double , 
top1_finance_average_change  double , 
top1_finance_max_retreat  double ,
top1_finance_annual  double ,
top1_finance_beta  double ,
top1_finance_info  double ,
top1_finance_alpha  double ,
top2_finance_prob_up  double ,
top2_finance_volatility  double , 
top2_finance_average_change  double , 
top2_finance_max_retreat  double ,
top2_finance_annual  double ,
top2_finance_beta  double ,
top2_finance_info  double ,
top2_finance_alpha  double ,
label string comment'样本标签 '
)
partitioned by (part_date varchar(8)) 
stored as parquet



create table if not exists cust_mining2.c7_finance_feature_test
(
customer_no  string comment '客户id',   
op_date string comment '当前日期',
top1_finance_prob_up  double ,
top1_finance_volatility  double , 
top1_finance_average_change  double , 
top1_finance_max_retreat  double ,
top1_finance_annual  double ,
top1_finance_beta  double ,
top1_finance_info  double ,
top1_finance_alpha  double ,
top2_finance_prob_up  double ,
top2_finance_volatility  double , 
top2_finance_average_change  double , 
top2_finance_max_retreat  double ,
top2_finance_annual  double ,
top2_finance_beta  double ,
top2_finance_info  double ,
top2_finance_alpha  double ,

)
partitioned by (part_date varchar(8)) 
stored as parquet
