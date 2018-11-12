# coding:utf-8
from pyspark import SparkConf, SparkContext, HiveContext
import pandas as pd
import datetime
import sys 

conf = SparkConf()
sc = SparkContext()
hql = HiveContext(sc)


args = sys.argv[1:]
if len(args) == 0:    
    print '未传入参数一flag  默认使用flag=0 即训练集样本'
    flag = 0
    print '未传入参数二run_date，将使用当前日期！'
    run_date = datetime.datetime.now().strftime('%Y%m%d')
elif len(args) == 1:
    flag = args[0]
    print '未传入参数二run_date，将使用当前日期！'
    run_date = datetime.datetime.now().strftime('%Y%m%d')  
else:
    flag = args[0]
    run_date = args[1]

if int(flag) == 0:
    table1 = 'c1_person_feature_train'
    table2 = 'c2_funds_feature_train'
    table3_1 = 'c3_top1_stock_feature_train'
    table3_2 = 'c3_top2_stock_feature_train'
    table4 = 'c4_industry_feature_train'
    table5 = 'c5_market_feature_train'
    table6 = 'c6_company_feature_train'

    save_table = 'c99_raw_feature_train'
    tablename = 'stock_active_customer' 

    is_a_label = ',a.label'

else:
    table1 = 'c1_person_feature_test'
    table2 = 'c2_funds_feature_test'
    table3_1 = 'c3_top1_stock_feature_test'
    table3_2 = 'c3_top2_stock_feature_test'
    table4 = 'c4_industry_feature_test'
    table5 = 'c5_market_feature_test'
    table6 = 'c6_company_feature_test'

    save_table = 'c99_raw_feature_test'
    tablename = 'stock_active_customer_pred' 

    is_a_label = ''

try:
    hql.sql('''
        insert overwrite table cust_mining.'''+save_table+'''
        partition (part_date=''' + run_date + ''')
        select  a.customer_no,a.op_date,
      			customer_gender,open_age,degree_code,profession_code,age,income,industry_type,aml_risk_level,corp_risk_level,one_date_cash_in_num,one_date_cash_in_sum,week_date_cash_in_num,week_date_cash_in_sum,month_date_cash_in_num,month_date_cash_in_sum,one_date_cash_out_num,one_date_cash_out_sum,week_date_cash_out_num,week_date_cash_out_sum,month_date_cash_out_num,month_date_cash_out_sum,one_date_buy_stock_num,one_date_buy_stock_sum,week_date_buy_stock_num,week_date_buy_stock_sum,month_date_buy_stock_num,month_date_buy_stock_sum,one_date_sell_stock_num,one_date_sell_stock_sum,week_date_sell_stock_num,week_date_sell_stock_sum,month_date_sell_stock_num,month_date_sell_stock_sum,one_date_asset_amount,week_date_asset_amount,month_date_asset_amount,five_stock_position,four_stock_position,three_stock_position,two_stock_position,one_stock_position,five_deposit_rate,four_deposit_rate,three_deposit_rate,two_deposit_rate,one_deposit_rate,five_stock_num,four_stock_num,three_stock_num,two_stock_num,one_stock_num,five_stock_sum,four_stock_sum,three_stock_sum,two_stock_sum,one_stock_sum,five_top1_stock_openprice,four_top1_stock_openprice,three_top1_stock_openprice,two_top1_stock_openprice,one_top1_stock_openprice,five_top1_stock_closeprice,four_top1_stock_closeprice,three_top1_stock_closeprice,two_top1_stock_closeprice,one_top1_stock_closeprice,five_top1_stock_highprice,four_top1_stock_highprice,three_top1_stock_highprice,two_top1_stock_highprice,one_top1_stock_highprice,five_top1_stock_lowprice,four_top1_stock_lowprice,three_top1_stock_lowprice,two_top1_stock_lowprice,one_top1_stock_lowprice,five_top1_stock_turnovervolume,four_top1_stock_turnovervolume,three_top1_stock_turnovervolume,two_top1_stock_turnovervolume,one_top1_stock_turnovervolume,five_top1_stock_turnovervalue,four_top1_stock_turnovervalue,three_top1_stock_turnovervalue,two_top1_stock_turnovervalue,one_top1_stock_turnovervalue,five_top1_stock_changepct,four_top1_stock_changepct,three_top1_stock_changepct,two_top1_stock_changepct,one_top1_stock_changepct,five_top1_stock_turnoverrate,four_top1_stock_turnoverrate,three_top1_stock_turnoverrate,two_top1_stock_turnoverrate,one_top1_stock_turnoverrate,five_top1_stock_rzye,four_top1_stock_rzye,three_top1_stock_rzye,two_top1_stock_rzye,one_top1_stock_rzye,five_top1_stock_rzmre,four_top1_stock_rzmre,three_top1_stock_rzmre,two_top1_stock_rzmre,one_top1_stock_rzmre,five_top1_stock_rzche,four_top1_stock_rzche,three_top1_stock_rzche,two_top1_stock_rzche,one_top1_stock_rzche,five_top1_stock_rqyl,four_top1_stock_rqyl,three_top1_stock_rqyl,two_top1_stock_rqyl,one_top1_stock_rqyl,five_top1_stock_rqmcl,four_top1_stock_rqmcl,three_top1_stock_rqmcl,two_top1_stock_rqmcl,one_top1_stock_rqmcl,five_top1_stock_rqchl,four_top1_stock_rqchl,three_top1_stock_rqchl,two_top1_stock_rqchl,one_top1_stock_rqchl,five_top2_stock_openprice,four_top2_stock_openprice,three_top2_stock_openprice,two_top2_stock_openprice,one_top2_stock_openprice,five_top2_stock_closeprice,four_top2_stock_closeprice,three_top2_stock_closeprice,two_top2_stock_closeprice,one_top2_stock_closeprice,five_top2_stock_highprice,four_top2_stock_highprice,three_top2_stock_highprice,two_top2_stock_highprice,one_top2_stock_highprice,five_top2_stock_lowprice,four_top2_stock_lowprice,three_top2_stock_lowprice,two_top2_stock_lowprice,one_top2_stock_lowprice,five_top2_stock_turnovervolume,four_top2_stock_turnovervolume,three_top2_stock_turnovervolume,two_top2_stock_turnovervolume,one_top2_stock_turnovervolume,five_top2_stock_turnovervalue,four_top2_stock_turnovervalue,three_top2_stock_turnovervalue,two_top2_stock_turnovervalue,one_top2_stock_turnovervalue,five_top2_stock_changepct,four_top2_stock_changepct,three_top2_stock_changepct,two_top2_stock_changepct,one_top2_stock_changepct,five_top2_stock_turnoverrate,four_top2_stock_turnoverrate,three_top2_stock_turnoverrate,two_top2_stock_turnoverrate,one_top2_stock_turnoverrate,five_top2_stock_rzye,four_top2_stock_rzye,three_top2_stock_rzye,two_top2_stock_rzye,one_top2_stock_rzye,five_top2_stock_rzmre,four_top2_stock_rzmre,three_top2_stock_rzmre,two_top2_stock_rzmre,one_top2_stock_rzmre,five_top2_stock_rzche,four_top2_stock_rzche,three_top2_stock_rzche,two_top2_stock_rzche,one_top2_stock_rzche,five_top2_stock_rqyl,four_top2_stock_rqyl,three_top2_stock_rqyl,two_top2_stock_rqyl,one_top2_stock_rqyl,five_top2_stock_rqmcl,four_top2_stock_rqmcl,three_top2_stock_rqmcl,two_top2_stock_rqmcl,one_top2_stock_rqmcl,five_top2_stock_rqchl,four_top2_stock_rqchl,three_top2_stock_rqchl,two_top2_stock_rqchl,one_top2_stock_rqchl,five_top1_industry_open_price,four_top1_industry_open_price,three_top1_industry_open_price,two_top1_industry_open_price,one_top1_industry_open_price,five_top1_industry_close_price,four_top1_industry_close_price,three_top1_industry_close_price,two_top1_industry_close_price,one_top1_industry_close_price,five_top1_industry_high_price,four_top1_industry_high_price,three_top1_industry_high_price,two_top1_industry_high_price,one_top1_industry_high_price,five_top1_industry_low_price,four_top1_industry_low_price,three_top1_industry_low_price,two_top1_industry_low_price,one_top1_industry_low_price,five_top1_industry_amount,four_top1_industry_amount,three_top1_industry_amount,two_top1_industry_amount,one_top1_industry_amount,five_top1_industry_volume,four_top1_industry_volume,three_top1_industry_volume,two_top1_industry_volume,one_top1_industry_volume,five_top1_industry_change_rate,four_top1_industry_change_rate,three_top1_industry_change_rate,two_top1_industry_change_rate,one_top1_industry_change_rate,five_top2_industry_open_price,four_top2_industry_open_price,three_top2_industry_open_price,two_top2_industry_open_price,one_top2_industry_open_price,five_top2_industry_close_price,four_top2_industry_close_price,three_top2_industry_close_price,two_top2_industry_close_price,one_top2_industry_close_price,five_top2_industry_high_price,four_top2_industry_high_price,three_top2_industry_high_price,two_top2_industry_high_price,one_top2_industry_high_price,five_top2_industry_low_price,four_top2_industry_low_price,three_top2_industry_low_price,two_top2_industry_low_price,one_top2_industry_low_price,five_top2_industry_amount,four_top2_industry_amount,three_top2_industry_amount,two_top2_industry_amount,one_top2_industry_amount,five_top2_industry_volume,four_top2_industry_volume,three_top2_industry_volume,two_top2_industry_volume,one_top2_industry_volume,five_top2_industry_change_rate,four_top2_industry_change_rate,three_top2_industry_change_rate,two_top2_industry_change_rate,one_top2_industry_change_rate,five_sh_market_financevalue,four_sh_market_financevalue,three_sh_market_financevalue,two_sh_market_financevalue,one_sh_market_financevalue,five_sh_market_financebuyvalue,four_sh_market_financebuyvalue,three_sh_market_financebuyvalue,two_sh_market_financebuyvalue,one_sh_market_financebuyvalue,five_sh_market_securityvolume,four_sh_market_securityvolume,three_sh_market_securityvolume,two_sh_market_securityvolume,one_sh_market_securityvolume,five_sh_market_securityvalue,four_sh_market_securityvalue,three_sh_market_securityvalue,two_sh_market_securityvalue,one_sh_market_securityvalue,five_sh_market_securitysellvolume,four_sh_market_securitysellvolume,three_sh_market_securitysellvolume,two_sh_market_securitysellvolume,one_sh_market_securitysellvolume,five_sh_market_tradingvalue,four_sh_market_tradingvalue,three_sh_market_tradingvalue,two_sh_market_tradingvalue,one_sh_market_tradingvalue,five_sz_market_financevalue,four_sz_market_financevalue,three_sz_market_financevalue,two_sz_market_financevalue,one_sz_market_financevalue,five_sz_market_financebuyvalue,four_sz_market_financebuyvalue,three_sz_market_financebuyvalue,two_sz_market_financebuyvalue,one_sz_market_financebuyvalue,five_sz_market_securityvolume,four_sz_market_securityvolume,three_sz_market_securityvolume,two_sz_market_securityvolume,one_sz_market_securityvolume,five_sz_market_securityvalue,four_sz_market_securityvalue,three_sz_market_securityvalue,two_sz_market_securityvalue,one_sz_market_securityvalue,five_sz_market_securitysellvolume,four_sz_market_securitysellvolume,three_sz_market_securitysellvolume,two_sz_market_securitysellvolume,one_sz_market_securitysellvolume,five_sz_market_tradingvalue,four_sz_market_tradingvalue,three_sz_market_tradingvalue,two_sz_market_tradingvalue,one_sz_market_tradingvalue,top1_basiceps,top1_netassetps,top1_cashflowps,top1_netprofit,top1_pe,top1_pb,top1_roe,top1_roa,top1_grossincomeratio,top1_operatingrevenuegrowrate,top1_netprofitgrowrate,top1_netassetgrowrate,top1_basicepsyoy,top1_operatingnitotp,top1_opercashintoasset,top1_currentratio,top2_basiceps,top2_netassetps,top2_cashflowps,top2_netprofit,top2_pe,top2_pb,top2_roe,top2_roa,top2_grossincomeratio,top2_operatingrevenuegrowrate,top2_netprofitgrowrate,top2_netassetgrowrate,top2_basicepsyoy,top2_operatingnitotp,top2_opercashintoasset,top2_currentratio'''+is_a_label+'''

    from cust_mining.'''+tablename+''' a 
    left join
    (select * from cust_mining.'''+table1+''' 
    where part_date = '''+run_date +''')b 
    on a.customer_no = b.customer_no and a.op_date = b.op_date
    left join
    (select * from cust_mining.'''+table2+''' 
    where part_date = '''+run_date +''')c 
    on a.customer_no = c.customer_no and a.op_date = c.op_date
    left join
    (select * from cust_mining.'''+table3_1+''' 
    where part_date = '''+run_date +''')d1 
    on a.customer_no = d1.customer_no and a.op_date = d1.op_date
    left join
    (select * from cust_mining.'''+table3_2+''' 
    where part_date = '''+run_date +''')d2 
    on a.customer_no = d2.customer_no and a.op_date = d2.op_date
    left join
    (select * from cust_mining.'''+table4+''' 
    where part_date = '''+run_date +''')e 
    on a.customer_no = e.customer_no and a.op_date = e.op_date
    left join
    (select * from cust_mining.'''+table5+''' 
    where part_date = '''+run_date +''')f 
    on a.customer_no = f.customer_no and a.op_date = f.op_date
    left join
    (select * from cust_mining.'''+table6+''' 
    where part_date = '''+run_date +''')g 
    on a.customer_no = g.customer_no and a.op_date = g.op_date
    ''')
    print('write cust_mining.' +save_table + run_date + ' to hive Successfully')
except:
    print('write cust_mining.'+ save_table + run_date + ' to hive Failed !!!')


sc.stop()
