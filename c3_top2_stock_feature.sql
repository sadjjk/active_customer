create table if not exists cust_mining2.c3_top2_stock_feature_train
(
customer_no  string comment '客户id',   
op_date string comment '当前日期',
five_top2_stock_openprice double ,
four_top2_stock_openprice double ,
three_top2_stock_openprice double ,
two_top2_stock_openprice double ,
one_top2_stock_openprice double ,
null_top2_stock_openprice double ,
diff_four_top2_stock_openprice double ,
diff_three_top2_stock_openprice double ,
diff_two_top2_stock_openprice double ,
diff_one_top2_stock_openprice double ,
mean_top2_stock_openprice double ,
std_top2_stock_openprice double ,
more_avg_num_top2_stock_openprice double ,
diff_more_avg_num_top2_stock_openprice double ,
five_top2_stock_closeprice double ,
four_top2_stock_closeprice double ,
three_top2_stock_closeprice double ,
two_top2_stock_closeprice double ,
one_top2_stock_closeprice double ,
null_top2_stock_closeprice double ,
diff_four_top2_stock_closeprice double ,
diff_three_top2_stock_closeprice double ,
diff_two_top2_stock_closeprice double ,
diff_one_top2_stock_closeprice double ,
mean_top2_stock_closeprice double ,
std_top2_stock_closeprice double ,
more_avg_num_top2_stock_closeprice double ,
diff_more_avg_num_top2_stock_closeprice double ,
five_top2_stock_highprice double ,
four_top2_stock_highprice double ,
three_top2_stock_highprice double ,
two_top2_stock_highprice double ,
one_top2_stock_highprice double ,
null_top2_stock_highprice double ,
diff_four_top2_stock_highprice double ,
diff_three_top2_stock_highprice double ,
diff_two_top2_stock_highprice double ,
diff_one_top2_stock_highprice double ,
mean_top2_stock_highprice double ,
std_top2_stock_highprice double ,
more_avg_num_top2_stock_highprice double ,
diff_more_avg_num_top2_stock_highprice double ,
five_top2_stock_lowprice double ,
four_top2_stock_lowprice double ,
three_top2_stock_lowprice double ,
two_top2_stock_lowprice double ,
one_top2_stock_lowprice double ,
null_top2_stock_lowprice double ,
diff_four_top2_stock_lowprice double ,
diff_three_top2_stock_lowprice double ,
diff_two_top2_stock_lowprice double ,
diff_one_top2_stock_lowprice double ,
mean_top2_stock_lowprice double ,
std_top2_stock_lowprice double ,
more_avg_num_top2_stock_lowprice double ,
diff_more_avg_num_top2_stock_lowprice double ,
five_top2_stock_turnovervolume double ,
four_top2_stock_turnovervolume double ,
three_top2_stock_turnovervolume double ,
two_top2_stock_turnovervolume double ,
one_top2_stock_turnovervolume double ,
null_top2_stock_turnovervolume double ,
diff_four_top2_stock_turnovervolume double ,
diff_three_top2_stock_turnovervolume double ,
diff_two_top2_stock_turnovervolume double ,
diff_one_top2_stock_turnovervolume double ,
mean_top2_stock_turnovervolume double ,
std_top2_stock_turnovervolume double ,
more_avg_num_top2_stock_turnovervolume double ,
diff_more_avg_num_top2_stock_turnovervolume double ,
five_top2_stock_turnovervalue double ,
four_top2_stock_turnovervalue double ,
three_top2_stock_turnovervalue double ,
two_top2_stock_turnovervalue double ,
one_top2_stock_turnovervalue double ,
null_top2_stock_turnovervalue double ,
diff_four_top2_stock_turnovervalue double ,
diff_three_top2_stock_turnovervalue double ,
diff_two_top2_stock_turnovervalue double ,
diff_one_top2_stock_turnovervalue double ,
mean_top2_stock_turnovervalue double ,
std_top2_stock_turnovervalue double ,
more_avg_num_top2_stock_turnovervalue double ,
diff_more_avg_num_top2_stock_turnovervalue double ,
five_top2_stock_changepct double ,
four_top2_stock_changepct double ,
three_top2_stock_changepct double ,
two_top2_stock_changepct double ,
one_top2_stock_changepct double ,
null_top2_stock_changepct double ,
diff_four_top2_stock_changepct double ,
diff_three_top2_stock_changepct double ,
diff_two_top2_stock_changepct double ,
diff_one_top2_stock_changepct double ,
mean_top2_stock_changepct double ,
std_top2_stock_changepct double ,
more_avg_num_top2_stock_changepct double ,
diff_more_avg_num_top2_stock_changepct double ,
five_top2_stock_turnoverrate double ,
four_top2_stock_turnoverrate double ,
three_top2_stock_turnoverrate double ,
two_top2_stock_turnoverrate double ,
one_top2_stock_turnoverrate double ,
null_top2_stock_turnoverrate double ,
diff_four_top2_stock_turnoverrate double ,
diff_three_top2_stock_turnoverrate double ,
diff_two_top2_stock_turnoverrate double ,
diff_one_top2_stock_turnoverrate double ,
mean_top2_stock_turnoverrate double ,
std_top2_stock_turnoverrate double ,
more_avg_num_top2_stock_turnoverrate double ,
diff_more_avg_num_top2_stock_turnoverrate double ,
five_top2_stock_rzye double ,
four_top2_stock_rzye double ,
three_top2_stock_rzye double ,
two_top2_stock_rzye double ,
one_top2_stock_rzye double ,
null_top2_stock_rzye double ,
diff_four_top2_stock_rzye double ,
diff_three_top2_stock_rzye double ,
diff_two_top2_stock_rzye double ,
diff_one_top2_stock_rzye double ,
mean_top2_stock_rzye double ,
std_top2_stock_rzye double ,
more_avg_num_top2_stock_rzye double ,
diff_more_avg_num_top2_stock_rzye double ,
five_top2_stock_rzmre double ,
four_top2_stock_rzmre double ,
three_top2_stock_rzmre double ,
two_top2_stock_rzmre double ,
one_top2_stock_rzmre double ,
null_top2_stock_rzmre double ,
diff_four_top2_stock_rzmre double ,
diff_three_top2_stock_rzmre double ,
diff_two_top2_stock_rzmre double ,
diff_one_top2_stock_rzmre double ,
mean_top2_stock_rzmre double ,
std_top2_stock_rzmre double ,
more_avg_num_top2_stock_rzmre double ,
diff_more_avg_num_top2_stock_rzmre double ,
five_top2_stock_rzche double ,
four_top2_stock_rzche double ,
three_top2_stock_rzche double ,
two_top2_stock_rzche double ,
one_top2_stock_rzche double ,
null_top2_stock_rzche double ,
diff_four_top2_stock_rzche double ,
diff_three_top2_stock_rzche double ,
diff_two_top2_stock_rzche double ,
diff_one_top2_stock_rzche double ,
mean_top2_stock_rzche double ,
std_top2_stock_rzche double ,
more_avg_num_top2_stock_rzche double ,
diff_more_avg_num_top2_stock_rzche double ,
five_top2_stock_rqyl double ,
four_top2_stock_rqyl double ,
three_top2_stock_rqyl double ,
two_top2_stock_rqyl double ,
one_top2_stock_rqyl double ,
null_top2_stock_rqyl double ,
diff_four_top2_stock_rqyl double ,
diff_three_top2_stock_rqyl double ,
diff_two_top2_stock_rqyl double ,
diff_one_top2_stock_rqyl double ,
mean_top2_stock_rqyl double ,
std_top2_stock_rqyl double ,
more_avg_num_top2_stock_rqyl double ,
diff_more_avg_num_top2_stock_rqyl double ,
five_top2_stock_rqmcl double ,
four_top2_stock_rqmcl double ,
three_top2_stock_rqmcl double ,
two_top2_stock_rqmcl double ,
one_top2_stock_rqmcl double ,
null_top2_stock_rqmcl double ,
diff_four_top2_stock_rqmcl double ,
diff_three_top2_stock_rqmcl double ,
diff_two_top2_stock_rqmcl double ,
diff_one_top2_stock_rqmcl double ,
mean_top2_stock_rqmcl double ,
std_top2_stock_rqmcl double ,
more_avg_num_top2_stock_rqmcl double ,
diff_more_avg_num_top2_stock_rqmcl double ,
five_top2_stock_rqchl double ,
four_top2_stock_rqchl double ,
three_top2_stock_rqchl double ,
two_top2_stock_rqchl double ,
one_top2_stock_rqchl double ,
null_top2_stock_rqchl double ,
diff_four_top2_stock_rqchl double ,
diff_three_top2_stock_rqchl double ,
diff_two_top2_stock_rqchl double ,
diff_one_top2_stock_rqchl double ,
mean_top2_stock_rqchl double ,
std_top2_stock_rqchl double ,
more_avg_num_top2_stock_rqchl double ,
diff_more_avg_num_top2_stock_rqchl double ,
label string comment'样本标签 '
)
partitioned by (part_date varchar(8)) 
stored as parquet


create table if not exists cust_mining2.c3_top2_stock_feature_test
(
customer_no  string comment '客户id',   
op_date string comment '当前日期',
five_top2_stock_openprice double ,
four_top2_stock_openprice double ,
three_top2_stock_openprice double ,
two_top2_stock_openprice double ,
one_top2_stock_openprice double ,
null_top2_stock_openprice double ,
diff_four_top2_stock_openprice double ,
diff_three_top2_stock_openprice double ,
diff_two_top2_stock_openprice double ,
diff_one_top2_stock_openprice double ,
mean_top2_stock_openprice double ,
std_top2_stock_openprice double ,
more_avg_num_top2_stock_openprice double ,
diff_more_avg_num_top2_stock_openprice double ,
five_top2_stock_closeprice double ,
four_top2_stock_closeprice double ,
three_top2_stock_closeprice double ,
two_top2_stock_closeprice double ,
one_top2_stock_closeprice double ,
null_top2_stock_closeprice double ,
diff_four_top2_stock_closeprice double ,
diff_three_top2_stock_closeprice double ,
diff_two_top2_stock_closeprice double ,
diff_one_top2_stock_closeprice double ,
mean_top2_stock_closeprice double ,
std_top2_stock_closeprice double ,
more_avg_num_top2_stock_closeprice double ,
diff_more_avg_num_top2_stock_closeprice double ,
five_top2_stock_highprice double ,
four_top2_stock_highprice double ,
three_top2_stock_highprice double ,
two_top2_stock_highprice double ,
one_top2_stock_highprice double ,
null_top2_stock_highprice double ,
diff_four_top2_stock_highprice double ,
diff_three_top2_stock_highprice double ,
diff_two_top2_stock_highprice double ,
diff_one_top2_stock_highprice double ,
mean_top2_stock_highprice double ,
std_top2_stock_highprice double ,
more_avg_num_top2_stock_highprice double ,
diff_more_avg_num_top2_stock_highprice double ,
five_top2_stock_lowprice double ,
four_top2_stock_lowprice double ,
three_top2_stock_lowprice double ,
two_top2_stock_lowprice double ,
one_top2_stock_lowprice double ,
null_top2_stock_lowprice double ,
diff_four_top2_stock_lowprice double ,
diff_three_top2_stock_lowprice double ,
diff_two_top2_stock_lowprice double ,
diff_one_top2_stock_lowprice double ,
mean_top2_stock_lowprice double ,
std_top2_stock_lowprice double ,
more_avg_num_top2_stock_lowprice double ,
diff_more_avg_num_top2_stock_lowprice double ,
five_top2_stock_turnovervolume double ,
four_top2_stock_turnovervolume double ,
three_top2_stock_turnovervolume double ,
two_top2_stock_turnovervolume double ,
one_top2_stock_turnovervolume double ,
null_top2_stock_turnovervolume double ,
diff_four_top2_stock_turnovervolume double ,
diff_three_top2_stock_turnovervolume double ,
diff_two_top2_stock_turnovervolume double ,
diff_one_top2_stock_turnovervolume double ,
mean_top2_stock_turnovervolume double ,
std_top2_stock_turnovervolume double ,
more_avg_num_top2_stock_turnovervolume double ,
diff_more_avg_num_top2_stock_turnovervolume double ,
five_top2_stock_turnovervalue double ,
four_top2_stock_turnovervalue double ,
three_top2_stock_turnovervalue double ,
two_top2_stock_turnovervalue double ,
one_top2_stock_turnovervalue double ,
null_top2_stock_turnovervalue double ,
diff_four_top2_stock_turnovervalue double ,
diff_three_top2_stock_turnovervalue double ,
diff_two_top2_stock_turnovervalue double ,
diff_one_top2_stock_turnovervalue double ,
mean_top2_stock_turnovervalue double ,
std_top2_stock_turnovervalue double ,
more_avg_num_top2_stock_turnovervalue double ,
diff_more_avg_num_top2_stock_turnovervalue double ,
five_top2_stock_changepct double ,
four_top2_stock_changepct double ,
three_top2_stock_changepct double ,
two_top2_stock_changepct double ,
one_top2_stock_changepct double ,
null_top2_stock_changepct double ,
diff_four_top2_stock_changepct double ,
diff_three_top2_stock_changepct double ,
diff_two_top2_stock_changepct double ,
diff_one_top2_stock_changepct double ,
mean_top2_stock_changepct double ,
std_top2_stock_changepct double ,
more_avg_num_top2_stock_changepct double ,
diff_more_avg_num_top2_stock_changepct double ,
five_top2_stock_turnoverrate double ,
four_top2_stock_turnoverrate double ,
three_top2_stock_turnoverrate double ,
two_top2_stock_turnoverrate double ,
one_top2_stock_turnoverrate double ,
null_top2_stock_turnoverrate double ,
diff_four_top2_stock_turnoverrate double ,
diff_three_top2_stock_turnoverrate double ,
diff_two_top2_stock_turnoverrate double ,
diff_one_top2_stock_turnoverrate double ,
mean_top2_stock_turnoverrate double ,
std_top2_stock_turnoverrate double ,
more_avg_num_top2_stock_turnoverrate double ,
diff_more_avg_num_top2_stock_turnoverrate double ,
five_top2_stock_rzye double ,
four_top2_stock_rzye double ,
three_top2_stock_rzye double ,
two_top2_stock_rzye double ,
one_top2_stock_rzye double ,
null_top2_stock_rzye double ,
diff_four_top2_stock_rzye double ,
diff_three_top2_stock_rzye double ,
diff_two_top2_stock_rzye double ,
diff_one_top2_stock_rzye double ,
mean_top2_stock_rzye double ,
std_top2_stock_rzye double ,
more_avg_num_top2_stock_rzye double ,
diff_more_avg_num_top2_stock_rzye double ,
five_top2_stock_rzmre double ,
four_top2_stock_rzmre double ,
three_top2_stock_rzmre double ,
two_top2_stock_rzmre double ,
one_top2_stock_rzmre double ,
null_top2_stock_rzmre double ,
diff_four_top2_stock_rzmre double ,
diff_three_top2_stock_rzmre double ,
diff_two_top2_stock_rzmre double ,
diff_one_top2_stock_rzmre double ,
mean_top2_stock_rzmre double ,
std_top2_stock_rzmre double ,
more_avg_num_top2_stock_rzmre double ,
diff_more_avg_num_top2_stock_rzmre double ,
five_top2_stock_rzche double ,
four_top2_stock_rzche double ,
three_top2_stock_rzche double ,
two_top2_stock_rzche double ,
one_top2_stock_rzche double ,
null_top2_stock_rzche double ,
diff_four_top2_stock_rzche double ,
diff_three_top2_stock_rzche double ,
diff_two_top2_stock_rzche double ,
diff_one_top2_stock_rzche double ,
mean_top2_stock_rzche double ,
std_top2_stock_rzche double ,
more_avg_num_top2_stock_rzche double ,
diff_more_avg_num_top2_stock_rzche double ,
five_top2_stock_rqyl double ,
four_top2_stock_rqyl double ,
three_top2_stock_rqyl double ,
two_top2_stock_rqyl double ,
one_top2_stock_rqyl double ,
null_top2_stock_rqyl double ,
diff_four_top2_stock_rqyl double ,
diff_three_top2_stock_rqyl double ,
diff_two_top2_stock_rqyl double ,
diff_one_top2_stock_rqyl double ,
mean_top2_stock_rqyl double ,
std_top2_stock_rqyl double ,
more_avg_num_top2_stock_rqyl double ,
diff_more_avg_num_top2_stock_rqyl double ,
five_top2_stock_rqmcl double ,
four_top2_stock_rqmcl double ,
three_top2_stock_rqmcl double ,
two_top2_stock_rqmcl double ,
one_top2_stock_rqmcl double ,
null_top2_stock_rqmcl double ,
diff_four_top2_stock_rqmcl double ,
diff_three_top2_stock_rqmcl double ,
diff_two_top2_stock_rqmcl double ,
diff_one_top2_stock_rqmcl double ,
mean_top2_stock_rqmcl double ,
std_top2_stock_rqmcl double ,
more_avg_num_top2_stock_rqmcl double ,
diff_more_avg_num_top2_stock_rqmcl double ,
five_top2_stock_rqchl double ,
four_top2_stock_rqchl double ,
three_top2_stock_rqchl double ,
two_top2_stock_rqchl double ,
one_top2_stock_rqchl double ,
null_top2_stock_rqchl double ,
diff_four_top2_stock_rqchl double ,
diff_three_top2_stock_rqchl double ,
diff_two_top2_stock_rqchl double ,
diff_one_top2_stock_rqchl double ,
mean_top2_stock_rqchl double ,
std_top2_stock_rqchl double ,
more_avg_num_top2_stock_rqchl double ,
diff_more_avg_num_top2_stock_rqchl double 
)
partitioned by (part_date varchar(8)) 
stored as parquet