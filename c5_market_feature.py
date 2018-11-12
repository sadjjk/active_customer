# coding:utf-8
from pyspark import SparkConf, SparkContext, HiveContext
from a2_week_add_index import  *
import pandas as pd
import datetime
import sys 

conf = SparkConf()
sc = SparkContext()
hql = HiveContext(sc)


def get_save_table(flag):
    if int(flag) == 0:
        save_table = 'c5_market_feature_train'
    else:
        save_table = 'c5_market_feature_test'
    return save_table

def get_is_label(flag):
    if int(flag) == 0:
        is_label = ',label'
    else:
        is_label = ''
    return is_label


args = sys.argv[1:]
if len(args) == 0:    
    print '未传入参数一flag  默认使用flag=0 即训练集样本'
    flag = 0
    print '未传入参数二run_date，将使用当前日期！'
    run_date = datetime.datetime.now().strftime('%Y%m%d')
    print '未传入参数三table_name，将使用stock_active_customer表！'
    tablename = 'stock_active_customer' 
elif len(args) == 1:
    flag = args[0]
    print '未传入参数二run_date，将使用当前日期！'
    run_date = datetime.datetime.now().strftime('%Y%m%d')  
    print '未传入参数三table_name，将使用stock_active_customer表！'
    tablename = 'stock_active_customer'
elif len(args) == 2:
    flag = args[0]
    run_date = args[1]
    print '未传入参数三table_name，将使用stock_active_customer表！'
    tablename = 'stock_active_customer'
else:
    flag = args[0]
    run_date = args[1]
    tablename = args[2]

save_table = get_save_table(flag)
is_label = get_is_label(flag)


#上证融资融券

market_sh_finance_info = hql.sql('''
    select customer_no,op_date,tradingdate,
        financevalue,financebuyvalue,securityvolume,
        securityvalue,securitysellvolume,tradingvalue,
        rank()over(partition by customer_no,op_date order by tradingdate) as date_rank
from 
    (select *
    from ctprod.schema_hushen_fin_slo
    where secumarket = 83)a 
join
    (select customer_no,op_date,op_before_one_date,op_before_week_date
    from cust_mining.'''+ tablename +''')b 
on  a.tradingdate>= b.op_before_week_date
    and a.tradingdate <= b.op_before_one_date
    
''').toPandas()


workday_list = ['five_','four_','three_','two_','one_']

##融资余额
market_sh_finance_financevalue = pd.pivot_table(market_sh_finance_info,index=['customer_no','op_date'],columns=['date_rank'],values=['financevalue'])
market_sh_finance_financevalue.columns=[x + 'sh_market_financevalue' for x in workday_list ]
market_sh_finance_financevalue = market_sh_finance_financevalue.reset_index()
market_sh_finance_financevalue = add_statistics(market_sh_finance_financevalue,'sh_market_financevalue')
market_sh_finance_financevalue = hql.createDataFrame(market_sh_finance_financevalue)
market_sh_finance_financevalue.registerTempTable('market_sh_finance_financevalue')


##融资买入额
market_sh_finance_financebuyvalue = pd.pivot_table(market_sh_finance_info,index=['customer_no','op_date'],columns=['date_rank'],values=['financebuyvalue'])
market_sh_finance_financebuyvalue.columns=[x + 'sh_market_financebuyvalue' for x in workday_list ]
market_sh_finance_financebuyvalue = market_sh_finance_financebuyvalue.reset_index()
market_sh_finance_financebuyvalue = add_statistics(market_sh_finance_financebuyvalue,'sh_market_financebuyvalue')
market_sh_finance_financebuyvalue = hql.createDataFrame(market_sh_finance_financebuyvalue)
market_sh_finance_financebuyvalue.registerTempTable('market_sh_finance_financebuyvalue')

##融券余量
market_sh_finance_securityvolume = pd.pivot_table(market_sh_finance_info,index=['customer_no','op_date'],columns=['date_rank'],values=['securityvolume'])
market_sh_finance_securityvolume.columns=[x + 'sh_market_securityvolume' for x in workday_list ]
market_sh_finance_securityvolume = market_sh_finance_securityvolume.reset_index()
market_sh_finance_securityvolume = add_statistics(market_sh_finance_securityvolume,'sh_market_securityvolume')
market_sh_finance_securityvolume = hql.createDataFrame(market_sh_finance_securityvolume)
market_sh_finance_securityvolume.registerTempTable('market_sh_finance_securityvolume')

##融券余量金额(元)
market_sh_finance_securityvalue = pd.pivot_table(market_sh_finance_info,index=['customer_no','op_date'],columns=['date_rank'],values=['securityvalue'])
market_sh_finance_securityvalue.columns=[x + 'sh_market_securityvalue' for x in workday_list ]
market_sh_finance_securityvalue = market_sh_finance_securityvalue.reset_index()
market_sh_finance_securityvalue = add_statistics(market_sh_finance_securityvalue,'sh_market_securityvalue')
market_sh_finance_securityvalue = hql.createDataFrame(market_sh_finance_securityvalue)
market_sh_finance_securityvalue.registerTempTable('market_sh_finance_securityvalue')

##融券卖出量
market_sh_finance_securitysellvolume = pd.pivot_table(market_sh_finance_info,index=['customer_no','op_date'],columns=['date_rank'],values=['securitysellvolume'])
market_sh_finance_securitysellvolume.columns=[x + 'sh_market_securitysellvolume' for x in workday_list ]
market_sh_finance_securitysellvolume = market_sh_finance_securitysellvolume.reset_index()
market_sh_finance_securitysellvolume = add_statistics(market_sh_finance_securitysellvolume,'sh_market_securitysellvolume')
market_sh_finance_securitysellvolume = hql.createDataFrame(market_sh_finance_securitysellvolume)
market_sh_finance_securitysellvolume.registerTempTable('market_sh_finance_securitysellvolume')

##融资融券余额(元)
market_sh_finance_tradingvalue = pd.pivot_table(market_sh_finance_info,index=['customer_no','op_date'],columns=['date_rank'],values=['tradingvalue'])
market_sh_finance_tradingvalue.columns=[x + 'sh_market_tradingvalue' for x in workday_list ]
market_sh_finance_tradingvalue = market_sh_finance_tradingvalue.reset_index()
market_sh_finance_tradingvalue = add_statistics(market_sh_finance_tradingvalue,'sh_market_tradingvalue')
market_sh_finance_tradingvalue = hql.createDataFrame(market_sh_finance_tradingvalue)
market_sh_finance_tradingvalue.registerTempTable('market_sh_finance_tradingvalue')


# 深证融资融券
market_sz_finance_info = hql.sql('''
    select customer_no,op_date,tradingdate,
        financevalue,financebuyvalue,securityvolume,
        securityvalue,securitysellvolume,tradingvalue,
        rank()over(partition by customer_no,op_date order by tradingdate) as date_rank
from 
    (select *
    from ctprod.schema_hushen_fin_slo
    where secumarket = 90)a 
join
    (select customer_no,op_date,op_before_one_date,op_before_week_date
    from cust_mining.'''+ tablename +''')b 
on  a.tradingdate>= b.op_before_week_date
    and a.tradingdate <= b.op_before_one_date
    
''').toPandas()


##融资余额
market_sz_finance_financevalue = pd.pivot_table(market_sz_finance_info,index=['customer_no','op_date'],columns=['date_rank'],values=['financevalue'])
market_sz_finance_financevalue.columns=[x + 'sz_market_financevalue' for x in workday_list ]
market_sz_finance_financevalue = market_sz_finance_financevalue.reset_index()
market_sz_finance_financevalue = add_statistics(market_sz_finance_financevalue,'sz_market_financevalue')
market_sz_finance_financevalue = hql.createDataFrame(market_sz_finance_financevalue)
market_sz_finance_financevalue.registerTempTable('market_sz_finance_financevalue')


##融资买入额
market_sz_finance_financebuyvalue = pd.pivot_table(market_sz_finance_info,index=['customer_no','op_date'],columns=['date_rank'],values=['financebuyvalue'])
market_sz_finance_financebuyvalue.columns=[x + 'sz_market_financebuyvalue' for x in workday_list ]
market_sz_finance_financebuyvalue = market_sz_finance_financebuyvalue.reset_index()
market_sz_finance_financebuyvalue = add_statistics(market_sz_finance_financebuyvalue,'sz_market_financebuyvalue')
market_sz_finance_financebuyvalue = hql.createDataFrame(market_sz_finance_financebuyvalue)
market_sz_finance_financebuyvalue.registerTempTable('market_sz_finance_financebuyvalue')

##融券余量
market_sz_finance_securityvolume = pd.pivot_table(market_sz_finance_info,index=['customer_no','op_date'],columns=['date_rank'],values=['securityvolume'])
market_sz_finance_securityvolume.columns=[x + 'sz_market_securityvolume' for x in workday_list ]
market_sz_finance_securityvolume = market_sz_finance_securityvolume.reset_index()
market_sz_finance_securityvolume = add_statistics(market_sz_finance_securityvolume,'sz_market_securityvolume')
market_sz_finance_securityvolume = hql.createDataFrame(market_sz_finance_securityvolume)
market_sz_finance_securityvolume.registerTempTable('market_sz_finance_securityvolume')

##融券余量金额(元)
market_sz_finance_securityvalue = pd.pivot_table(market_sz_finance_info,index=['customer_no','op_date'],columns=['date_rank'],values=['securityvalue'])
market_sz_finance_securityvalue.columns=[x + 'sz_market_securityvalue' for x in workday_list ]
market_sz_finance_securityvalue = market_sz_finance_securityvalue.reset_index()
market_sz_finance_securityvalue = add_statistics(market_sz_finance_securityvalue,'sz_market_securityvalue')
market_sz_finance_securityvalue = hql.createDataFrame(market_sz_finance_securityvalue)
market_sz_finance_securityvalue.registerTempTable('market_sz_finance_securityvalue')

##融券卖出量
market_sz_finance_securitysellvolume = pd.pivot_table(market_sz_finance_info,index=['customer_no','op_date'],columns=['date_rank'],values=['securitysellvolume'])
market_sz_finance_securitysellvolume.columns=[x + 'sz_market_securitysellvolume' for x in workday_list ]
market_sz_finance_securitysellvolume = market_sz_finance_securitysellvolume.reset_index()
market_sz_finance_securitysellvolume = add_statistics(market_sz_finance_securitysellvolume,'sz_market_securitysellvolume')
market_sz_finance_securitysellvolume = hql.createDataFrame(market_sz_finance_securitysellvolume)
market_sz_finance_securitysellvolume.registerTempTable('market_sz_finance_securitysellvolume')

##融资融券余额(元)
market_sz_finance_tradingvalue = pd.pivot_table(market_sz_finance_info,index=['customer_no','op_date'],columns=['date_rank'],values=['tradingvalue'])
market_sz_finance_tradingvalue.columns=[x + 'sz_market_tradingvalue' for x in workday_list ]
market_sz_finance_tradingvalue = market_sz_finance_tradingvalue.reset_index()
market_sz_finance_tradingvalue = add_statistics(market_sz_finance_tradingvalue,'sz_market_tradingvalue')
market_sz_finance_tradingvalue = hql.createDataFrame(market_sz_finance_tradingvalue)
market_sz_finance_tradingvalue.registerTempTable('market_sz_finance_tradingvalue')


#主表
stock_active_customer_label = hql.sql('''
select customer_no,op_date'''+is_label+'''
from cust_mining.'''+tablename+'''    
''')
stock_active_customer_label.registerTempTable('stock_active_customer_label')



try: 
    hql.sql('''
        insert overwrite table cust_mining2.'''+save_table+'''
            partition (part_date=''' + run_date + ''')
            select  a.customer_no,
                    a.op_date,  
                    null_sh_market_financevalue,
                    diff_four_sh_market_financevalue,
                    diff_three_sh_market_financevalue,
                    diff_two_sh_market_financevalue,
                    diff_one_sh_market_financevalue,
                    mean_sh_market_financevalue,
                    std_sh_market_financevalue,
                    more_avg_num_sh_market_financevalue,
                    diff_more_avg_num_sh_market_financevalue,
                    null_sh_market_financebuyvalue,
                    diff_four_sh_market_financebuyvalue,
                    diff_three_sh_market_financebuyvalue,
                    diff_two_sh_market_financebuyvalue,
                    diff_one_sh_market_financebuyvalue,
                    mean_sh_market_financebuyvalue,
                    std_sh_market_financebuyvalue,
                    more_avg_num_sh_market_financebuyvalue,
                    diff_more_avg_num_sh_market_financebuyvalue,
                    null_sh_market_securityvolume,
                    diff_four_sh_market_securityvolume,
                    diff_three_sh_market_securityvolume,
                    diff_two_sh_market_securityvolume,
                    diff_one_sh_market_securityvolume,
                    mean_sh_market_securityvolume,
                    std_sh_market_securityvolume,
                    more_avg_num_sh_market_securityvolume,
                    diff_more_avg_num_sh_market_securityvolume,
                    null_sh_market_securityvalue,
                    diff_four_sh_market_securityvalue,
                    diff_three_sh_market_securityvalue,
                    diff_two_sh_market_securityvalue,
                    diff_one_sh_market_securityvalue,
                    mean_sh_market_securityvalue,
                    std_sh_market_securityvalue,
                    more_avg_num_sh_market_securityvalue,
                    diff_more_avg_num_sh_market_securityvalue,
                    null_sh_market_securitysellvolume,
                    diff_four_sh_market_securitysellvolume,
                    diff_three_sh_market_securitysellvolume,
                    diff_two_sh_market_securitysellvolume,
                    diff_one_sh_market_securitysellvolume,
                    mean_sh_market_securitysellvolume,
                    std_sh_market_securitysellvolume,
                    more_avg_num_sh_market_securitysellvolume,
                    diff_more_avg_num_sh_market_securitysellvolume,
                    null_sh_market_tradingvalue,
                    diff_four_sh_market_tradingvalue,
                    diff_three_sh_market_tradingvalue,
                    diff_two_sh_market_tradingvalue,
                    diff_one_sh_market_tradingvalue,
                    mean_sh_market_tradingvalue,
                    std_sh_market_tradingvalue,
                    more_avg_num_sh_market_tradingvalue,
                    diff_more_avg_num_sh_market_tradingvalue,
                    null_sz_market_financevalue,
                    diff_four_sz_market_financevalue,
                    diff_three_sz_market_financevalue,
                    diff_two_sz_market_financevalue,
                    diff_one_sz_market_financevalue,
                    mean_sz_market_financevalue,
                    std_sz_market_financevalue,
                    more_avg_num_sz_market_financevalue,
                    diff_more_avg_num_sz_market_financevalue,
                    null_sz_market_financebuyvalue,
                    diff_four_sz_market_financebuyvalue,
                    diff_three_sz_market_financebuyvalue,
                    diff_two_sz_market_financebuyvalue,
                    diff_one_sz_market_financebuyvalue,
                    mean_sz_market_financebuyvalue,
                    std_sz_market_financebuyvalue,
                    more_avg_num_sz_market_financebuyvalue,
                    diff_more_avg_num_sz_market_financebuyvalue,
                    null_sz_market_securityvolume,
                    diff_four_sz_market_securityvolume,
                    diff_three_sz_market_securityvolume,
                    diff_two_sz_market_securityvolume,
                    diff_one_sz_market_securityvolume,
                    mean_sz_market_securityvolume,
                    std_sz_market_securityvolume,
                    more_avg_num_sz_market_securityvolume,
                    diff_more_avg_num_sz_market_securityvolume,
                    null_sz_market_securityvalue,
                    diff_four_sz_market_securityvalue,
                    diff_three_sz_market_securityvalue,
                    diff_two_sz_market_securityvalue,
                    diff_one_sz_market_securityvalue,
                    mean_sz_market_securityvalue,
                    std_sz_market_securityvalue,
                    more_avg_num_sz_market_securityvalue,
                    diff_more_avg_num_sz_market_securityvalue,
                    null_sz_market_securitysellvolume,
                    diff_four_sz_market_securitysellvolume,
                    diff_three_sz_market_securitysellvolume,
                    diff_two_sz_market_securitysellvolume,
                    diff_one_sz_market_securitysellvolume,
                    mean_sz_market_securitysellvolume,
                    std_sz_market_securitysellvolume,
                    more_avg_num_sz_market_securitysellvolume,
                    diff_more_avg_num_sz_market_securitysellvolume,
                    null_sz_market_tradingvalue,
                    diff_four_sz_market_tradingvalue,
                    diff_three_sz_market_tradingvalue,
                    diff_two_sz_market_tradingvalue,
                    diff_one_sz_market_tradingvalue,
                    mean_sz_market_tradingvalue,
                    std_sz_market_tradingvalue,
                    more_avg_num_sz_market_tradingvalue,
                    diff_more_avg_num_sz_market_tradingvalue
                    '''+is_label+'''
            from stock_active_customer_label a 
            left join market_sh_finance_financevalue b  
            on a.customer_no = b.customer_no and a.op_date = b.op_date
            left join market_sh_finance_financebuyvalue c   
            on a.customer_no = c.customer_no and a.op_date = c.op_date
            left join market_sh_finance_securityvolume d    
            on a.customer_no = d.customer_no and a.op_date = d.op_date
            left join market_sh_finance_securityvalue e    
            on a.customer_no = e.customer_no and a.op_date = e.op_date
            left join market_sh_finance_securitysellvolume f   
            on a.customer_no = f.customer_no and a.op_date = f.op_date
            left join market_sh_finance_tradingvalue g  
            on a.customer_no = g.customer_no and a.op_date = g.op_date
            left join market_sz_finance_financevalue b1  
            on a.customer_no = b1.customer_no and a.op_date = b1.op_date
            left join market_sz_finance_financebuyvalue c1   
            on a.customer_no = c1.customer_no and a.op_date = c1.op_date
            left join market_sz_finance_securityvolume d1    
            on a.customer_no = d1.customer_no and a.op_date = d1.op_date
            left join market_sz_finance_securityvalue e1    
            on a.customer_no = e1.customer_no and a.op_date = e1.op_date
            left join market_sz_finance_securitysellvolume f1  
            on a.customer_no = f1.customer_no and a.op_date = f1.op_date
            left join market_sz_finance_tradingvalue g1  
            on a.customer_no = g1.customer_no and a.op_date = g1.op_date

        ''')

    print('write cust_mining2.' +save_table + run_date + ' to hive Successfully')
except:
    print('write cust_mining2.'+ save_table + run_date + ' to hive Failed !!!')

sc.stop()



