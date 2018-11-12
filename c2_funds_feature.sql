create table if not exists cust_mining2.c2_funds_feature_train
(
customer_no  string comment '客户id',   
op_date string comment '当前日期',
one_date_cash_in_num double comment'近一天资金转入次数',
one_date_cash_in_sum double comment'近一天资金转入总金额',
week_date_cash_in_num double comment'近一周资金转入次数',
week_date_cash_in_sum double comment'近一周资金转入总金额',
month_date_cash_in_num double comment'近一月资金转入次数',
month_date_cash_in_sum double comment'近一月资金转入总金额',
one_date_cash_out_num double comment'近一天资金转出次数',
one_date_cash_out_sum double comment'近一天资金转出总金额',
week_date_cash_out_num double comment'近一周资金转出次数',
week_date_cash_out_sum double comment'近一周资金转出总金额',
month_date_cash_out_num double comment'近一月资金转出次数',
month_date_cash_out_sum double comment'近一月资金转出总金额',
one_date_buy_stock_num double comment'近一天证券买入次数',
one_date_buy_stock_sum double comment'近一天证券买入总金额',
week_date_buy_stock_num double comment'近一周证券买入次数',
week_date_buy_stock_sum double comment'近一周证券买入总金额',
month_date_buy_stock_num double comment'近一月证券买入次数',
month_date_buy_stock_sum double comment'近一月证券买入总金额',
one_date_sell_stock_num double comment'近一天证券卖出次数',
one_date_sell_stock_sum double comment'近一天证券卖出总金额',
week_date_sell_stock_num double comment'近一周证券卖出次数',
week_date_sell_stock_sum double comment'近一周证券卖出总金额',
month_date_sell_stock_num double comment'近一月证券卖出次数',
month_date_sell_stock_sum double comment'近一月证券卖出总金额',
one_date_asset_amount double comment'近一天平均总资产',
week_date_asset_amount double comment'近一周平均总资产',
month_date_asset_amount double comment'近一月平均总资产',
five_stock_position double,
four_stock_position double,
three_stock_position double,
two_stock_position double,
one_stock_position double,
null_stock_position double,
diff_four_stock_position double,
diff_three_stock_position double,
diff_two_stock_position double,
diff_one_stock_position double,
mean_stock_position double,
std_stock_position double,
more_avg_num_stock_position double,
diff_more_avg_num_stock_position double,
five_deposit_rate double,
four_deposit_rate double,
three_deposit_rate double,
two_deposit_rate double,
one_deposit_rate double,
null_deposit_rate double,
diff_four_deposit_rate double,
diff_three_deposit_rate double,
diff_two_deposit_rate double,
diff_one_deposit_rate double,
mean_deposit_rate double,
std_deposit_rate double,
more_avg_num_deposit_rate double,
diff_more_avg_num_deposit_rate double,
label string comment'样本标签'
)
partitioned by (part_date varchar(8)) 
stored as parquet


create table if not exists cust_mining2.c2_funds_feature_test
(
customer_no  string comment '客户id',   
op_date string comment '当前日期',
one_date_cash_in_num double comment'近一天资金转入次数',
one_date_cash_in_sum double comment'近一天资金转入总金额',
week_date_cash_in_num double comment'近一周资金转入次数',
week_date_cash_in_sum double comment'近一周资金转入总金额',
month_date_cash_in_num double comment'近一月资金转入次数',
month_date_cash_in_sum double comment'近一月资金转入总金额',
one_date_cash_out_num double comment'近一天资金转出次数',
one_date_cash_out_sum double comment'近一天资金转出总金额',
week_date_cash_out_num double comment'近一周资金转出次数',
week_date_cash_out_sum double comment'近一周资金转出总金额',
month_date_cash_out_num double comment'近一月资金转出次数',
month_date_cash_out_sum double comment'近一月资金转出总金额',
one_date_buy_stock_num double comment'近一天证券买入次数',
one_date_buy_stock_sum double comment'近一天证券买入总金额',
week_date_buy_stock_num double comment'近一周证券买入次数',
week_date_buy_stock_sum double comment'近一周证券买入总金额',
month_date_buy_stock_num double comment'近一月证券买入次数',
month_date_buy_stock_sum double comment'近一月证券买入总金额',
one_date_sell_stock_num double comment'近一天证券卖出次数',
one_date_sell_stock_sum double comment'近一天证券卖出总金额',
week_date_sell_stock_num double comment'近一周证券卖出次数',
week_date_sell_stock_sum double comment'近一周证券卖出总金额',
month_date_sell_stock_num double comment'近一月证券卖出次数',
month_date_sell_stock_sum double comment'近一月证券卖出总金额',
one_date_asset_amount double comment'近一天平均总资产',
week_date_asset_amount double comment'近一周平均总资产',
month_date_asset_amount double comment'近一月平均总资产',
five_stock_position double,
four_stock_position double,
three_stock_position double,
two_stock_position double,
one_stock_position double,
null_stock_position double,
diff_four_stock_position double,
diff_three_stock_position double,
diff_two_stock_position double,
diff_one_stock_position double,
mean_stock_position double,
std_stock_position double,
more_avg_num_stock_position double,
diff_more_avg_num_stock_position double,
five_deposit_rate double,
four_deposit_rate double,
three_deposit_rate double,
two_deposit_rate double,
one_deposit_rate double,
null_deposit_rate double,
diff_four_deposit_rate double,
diff_three_deposit_rate double,
diff_two_deposit_rate double,
diff_one_deposit_rate double,
mean_deposit_rate double,
std_deposit_rate double,
more_avg_num_deposit_rate double,
diff_more_avg_num_deposit_rate double
)
partitioned by (part_date varchar(8)) 
stored as parquet