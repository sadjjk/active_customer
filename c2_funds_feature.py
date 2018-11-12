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
        save_table = 'c2_funds_feature_train'
    else:
        save_table = 'c2_funds_feature_test'

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

fact_cust_fund_detail = hql.sql('''
select customer_no,init_date,occur_balance,exchange_type,
concat(substr(occur_date, 0,4), '-', substr(occur_date, 5,2),'-', substr(occur_date, 7,2)) as occur_date
from ctprod.fact_cust_fund_detail
where init_date >= 20180101
    ''')
fact_cust_fund_detail.registerTempTable('fact_cust_fund_detail')




#主表
stock_active_customer_label = hql.sql('''
select customer_no,op_before_one_date,op_before_week_date,op_before_month_date,op_date'''+is_label+'''
from cust_mining.'''+tablename+'''    
''')
stock_active_customer_label.registerTempTable('stock_active_customer_label')


#1.资金仓位

#1.1 资金转入次数、转入总金额--（银行转存）

#1.1.1近一天资金转入次数、转入总金额--（银行转存）

one_date_cash_in = hql.sql('''
select  a.customer_no,op_date,count(*) one_date_cash_in_num,
        sum(occur_balance)  as one_date_cash_in_sum
from
    (select customer_no,init_date,occur_balance,
    concat(substr(occur_date, 0,4), '-', substr(occur_date, 5,2),'-', substr(occur_date, 7,2)) as occur_date
    from fact_cust_fund_detail
    where exchange_type = '银行转存')a
    join 
    (select customer_no,op_before_one_date,op_date
    from stock_active_customer_label)b 
    on a.customer_no = b.customer_no and a.occur_date = b.op_before_one_date
group by a.customer_no,op_date    
    
''')

one_date_cash_in.registerTempTable('one_date_cash_in')

#1.1.2近一周资金转入次数、转入总金额--（银行转存）

week_date_cash_in = hql.sql('''
select  a.customer_no,op_date,count(*) as week_date_cash_in_num,
        sum(occur_balance) as week_date_cash_in_sum
from 
    (select customer_no,init_date,occur_balance,
    concat(substr(occur_date, 0,4), '-', substr(occur_date, 5,2),'-', substr(occur_date, 7,2)) as occur_date
    from fact_cust_fund_detail
    where exchange_type = '银行转存')a
    join 
    (select customer_no,op_date,op_before_one_date,op_before_week_date
    from stock_active_customer_label)b 
    on a.customer_no = b.customer_no 
    and a.occur_date >=b.op_before_week_date
    and a.occur_date <=b.op_before_one_date 
group by a.customer_no,op_date   
    
''')

week_date_cash_in.registerTempTable('week_date_cash_in')

#1.1.3近一月资金转入次数、转入总金额--（银行转存）
month_date_cash_in = hql.sql('''
select  a.customer_no,op_date,count(*) as  month_date_cash_in_num,
        sum(occur_balance) as month_date_cash_in_sum
from 
    (select customer_no,init_date,occur_balance,
    concat(substr(occur_date, 0,4), '-', substr(occur_date, 5,2),'-', substr(occur_date, 7,2)) as occur_date
    from fact_cust_fund_detail
    where exchange_type = '银行转存')a
    join 
    (select customer_no,op_date,op_before_one_date,op_before_month_date
    from stock_active_customer_label)b 
    on a.customer_no = b.customer_no 
    and a.occur_date >=b.op_before_month_date
    and a.occur_date <=b.op_before_one_date 
group by a.customer_no,op_date       
''')

month_date_cash_in.registerTempTable('month_date_cash_in')


#1.2 资金转出次数、转出总金额--（银行转取）

#1.2.1近一天资金转出次数、转出总金额--（银行转取）
one_date_cash_out = hql.sql('''
select  a.customer_no,op_date,count(*) as one_date_cash_out_num,
        -sum(occur_balance) as one_date_cash_out_sum
from  
    (select customer_no,init_date,occur_balance,
    concat(substr(occur_date, 0,4), '-', substr(occur_date, 5,2),'-', substr(occur_date, 7,2)) as occur_date
    from fact_cust_fund_detail
    where exchange_type = '银行转取')a
    join 
    (select customer_no,op_before_one_date,op_date
    from stock_active_customer_label)b 
    on a.customer_no = b.customer_no and a.occur_date = b.op_before_one_date
group by a.customer_no,op_date       
''')
one_date_cash_out.registerTempTable('one_date_cash_out')

#1.2.2近一周资金转出次数、转出总金额--（银行转取）
week_date_cash_out = hql.sql('''
select  a.customer_no,op_date, count(*) as week_date_cash_out_num,
        -sum(occur_balance) as week_date_cash_out_sum
from
    (select customer_no,init_date,occur_balance,
    concat(substr(occur_date, 0,4), '-', substr(occur_date, 5,2),'-', substr(occur_date, 7,2)) as occur_date
    from fact_cust_fund_detail
    where exchange_type = '银行转取')a
    join 
    (select customer_no,op_date,op_before_one_date,op_before_week_date
    from stock_active_customer_label)b 
    on a.customer_no = b.customer_no 
    and a.occur_date >=b.op_before_week_date
    and a.occur_date <=b.op_before_one_date 
group by a.customer_no,op_date   
    
''')
week_date_cash_out.registerTempTable('week_date_cash_out')

#1.2.3近一月资金转出次数、转出总金额--（银行转取）
month_date_cash_out = hql.sql('''
select  a.customer_no,op_date,count(*) as month_date_cash_out_num,
        -sum(occur_balance) as month_date_cash_out_sum
from
    (select customer_no,init_date,occur_balance,
    concat(substr(occur_date, 0,4), '-', substr(occur_date, 5,2),'-', substr(occur_date, 7,2)) as occur_date
    from fact_cust_fund_detail
    where exchange_type = '银行转取')a
    join 
    (select customer_no,op_date,op_before_one_date,op_before_month_date
    from stock_active_customer_label)b 
    on a.customer_no = b.customer_no 
    and a.occur_date >=b.op_before_month_date
    and a.occur_date <=b.op_before_one_date 
group by a.customer_no,op_date       
''')

month_date_cash_out.registerTempTable('month_date_cash_out')

#1.3证券买入次数及买入总金额--（证券买入）

#1.3.1近一天证券买入次数及买入总金额--（证券买入）
one_date_buy_stock = hql.sql('''
select  a.customer_no,op_date,count(*) as one_date_buy_stock_num,
        -sum(occur_balance) as one_date_buy_stock_sum
from
    (select customer_no,init_date,occur_balance,
    concat(substr(occur_date, 0,4), '-', substr(occur_date, 5,2),'-', substr(occur_date, 7,2)) as occur_date
    from fact_cust_fund_detail
    where exchange_type = '证券买入')a
    join 
    (select customer_no,op_before_one_date,op_date
    from stock_active_customer_label)b 
    on a.customer_no = b.customer_no and a.occur_date = b.op_before_one_date
group by a.customer_no,op_date        
''')
one_date_buy_stock.registerTempTable('one_date_buy_stock')

#1.3.2近一周证券买入次数及买入总金额--（证券买入）
week_date_buy_stock = hql.sql('''
select  a.customer_no,op_date,count(*) as week_date_buy_stock_num,
        -sum(occur_balance)  as week_date_buy_stock_sum
from
    (select customer_no,init_date,occur_balance,
    concat(substr(occur_date, 0,4), '-', substr(occur_date, 5,2),'-', substr(occur_date, 7,2)) as occur_date
    from fact_cust_fund_detail
    where exchange_type = '证券买入')a
    join 
    (select customer_no,op_date,op_before_one_date,op_before_week_date
    from stock_active_customer_label)b 
    on a.customer_no = b.customer_no 
    and a.occur_date >=b.op_before_week_date
    and a.occur_date <=b.op_before_one_date 
group by a.customer_no,op_date   
    
''')
week_date_buy_stock.registerTempTable('week_date_buy_stock')

#1.3.3近一月证券买入次数及买入总金额--（证券买入）
month_date_buy_stock = hql.sql('''
select  a.customer_no,op_date,count(*) as month_date_buy_stock_num,
        -sum(occur_balance) as month_date_buy_stock_sum
from
    (select customer_no,init_date,occur_balance,
    concat(substr(occur_date, 0,4), '-', substr(occur_date, 5,2),'-', substr(occur_date, 7,2)) as occur_date
    from fact_cust_fund_detail
    where exchange_type = '证券买入')a
    join 
    (select customer_no,op_date,op_before_one_date,op_before_month_date
    from stock_active_customer_label)b 
    on a.customer_no = b.customer_no 
    and a.occur_date >=b.op_before_month_date
    and a.occur_date <=b.op_before_one_date 
group by a.customer_no,op_date   
    
''')
month_date_buy_stock.registerTempTable('month_date_buy_stock')

#1.4 证券卖出次数及卖出总金额--（证券卖出）

#1.4.1近一天证券卖出次数及卖出总金额--（证券卖出）
one_date_sell_stock = hql.sql('''
select  a.customer_no,op_date,count(*) as one_date_sell_stock_num,
        sum(occur_balance) as one_date_sell_stock_sum
from
    (select customer_no,init_date,occur_balance,
    concat(substr(occur_date, 0,4), '-', substr(occur_date, 5,2),'-', substr(occur_date, 7,2)) as occur_date
    from fact_cust_fund_detail
    where exchange_type = '证券卖出')a
    join 
    (select customer_no,op_before_one_date,op_date
    from stock_active_customer_label)b 
    on a.customer_no = b.customer_no and a.occur_date = b.op_before_one_date
group by a.customer_no,op_date        
''')

one_date_sell_stock.registerTempTable('one_date_sell_stock')

#1.4.2近一周证券卖出次数及卖出总金额--（证券卖出）
week_date_sell_stock = hql.sql('''
select  a.customer_no,op_date,count(*) as week_date_sell_stock_num,
        sum(occur_balance) as week_date_sell_stock_sum
from
    (select customer_no,init_date,occur_balance,
    concat(substr(occur_date, 0,4), '-', substr(occur_date, 5,2),'-', substr(occur_date, 7,2)) as occur_date
    from fact_cust_fund_detail
    where exchange_type = '证券卖出')a
    join 
    (select customer_no,op_date,op_before_one_date,op_before_week_date
    from stock_active_customer_label)b 
    on a.customer_no = b.customer_no 
    and a.occur_date >=b.op_before_week_date
    and a.occur_date <=b.op_before_one_date 
group by a.customer_no,op_date   
    
''')
week_date_sell_stock.registerTempTable('week_date_sell_stock')

#1.4.3 近一月证券卖出次数及卖出总金额--（证券卖出）
month_date_sell_stock = hql.sql('''
select  a.customer_no,op_date,count(*) as month_date_sell_stock_num,
        sum(occur_balance) as month_date_sell_stock_sum
from
    (select customer_no,init_date,occur_balance,
    concat(substr(occur_date, 0,4), '-', substr(occur_date, 5,2),'-', substr(occur_date, 7,2)) as occur_date
    from fact_cust_fund_detail
    where exchange_type = '证券卖出')a
    join 
    (select customer_no,op_date,op_before_one_date,op_before_month_date
    from stock_active_customer_label)b 
    on a.customer_no = b.customer_no 
    and a.occur_date >=b.op_before_month_date
    and a.occur_date <=b.op_before_one_date 
group by a.customer_no,op_date   
    
''')
month_date_sell_stock.registerTempTable('month_date_sell_stock')

#1.5.1 近一天平均总资产
one_date_asset = hql.sql('''
select  a.customer_no,op_date,avg(total_close_asset) as one_date_asset_amount
from
    (select customer_no,total_close_asset,
    concat(substr(init_date, 0,4), '-', substr(init_date, 5,2),'-', substr(init_date, 7,2)) as init_date
    from ctprod.agg_cust_balance where part_date >=20180101
        )a
    join 
    (select customer_no,op_date,op_before_one_date
    from stock_active_customer_label)b 
    on a.customer_no = b.customer_no 
    and a.init_date = b.op_before_one_date   
group by a.customer_no,op_date
''')
one_date_asset.registerTempTable('one_date_asset')



#获取工作日
agg_cust_balance_workday = hql.sql('''
    select *
    from 
    (select customer_no,total_close_asset,init_date,stock_close_value,
           total_close_asset,close_rate
    from ctprod.agg_cust_balance where part_date >= 20180101)a 
    join
    (select distinct jyr
    from ctprod.sys_calendar)b 
    on a.init_date = b.jyr 
    ''')
agg_cust_balance_workday.registerTempTable('agg_cust_balance_workday')


#1.5.2 近一周平均总资产 
week_date_asset = hql.sql('''
select  a.customer_no,op_date,avg(total_close_asset) as week_date_asset_amount
from 
    (select customer_no,total_close_asset,
    concat(substr(init_date, 0,4), '-', substr(init_date, 5,2),'-', substr(init_date, 7,2)) as init_date
    from agg_cust_balance_workday
        )a
    join 
    (select customer_no,op_date,op_before_week_date,op_before_one_date
    from stock_active_customer_label)b 
    on a.customer_no = b.customer_no 
    and a.init_date >= b.op_before_week_date 
    and a.init_date <= b.op_before_one_date
group by a.customer_no,op_date
''')
week_date_asset.registerTempTable('week_date_asset')


#1.5.3 近一月平均总资产
month_date_asset = hql.sql('''
select  a.customer_no,op_date,
avg(total_close_asset) as month_date_asset_amount
from 
    (select customer_no,total_close_asset,
    concat(substr(init_date, 0,4), '-', substr(init_date, 5,2),'-', substr(init_date, 7,2)) as init_date
    from agg_cust_balance_workday
        )a
    join 
    (select customer_no,op_date,op_before_month_date,op_before_one_date
    from stock_active_customer_label)b 
    on a.customer_no = b.customer_no 
    and a.init_date >= b.op_before_month_date 
    and a.init_date <= b.op_before_one_date
group by a.customer_no,op_date
''')
month_date_asset.registerTempTable('month_date_asset')


#1.6近一周各天仓位
week_every_date_stock_position = hql.sql('''
select customer_no,op_date,date_rank,position
from 
    (select a.customer_no,op_date,init_date,round(position,1) as position,
            rank()over(partition by a.customer_no,op_date order by init_date ) as date_rank
    from 
        (select customer_no,stock_close_value/total_close_asset as position,
        concat(substr(init_date, 0,4), '-', substr(init_date, 5,2),'-', substr(init_date, 7,2)) as init_date
        from agg_cust_balance_workday)a
        join 
        (select customer_no,op_date,op_before_one_date,op_before_week_date
        from stock_active_customer_label)b 
        on a.customer_no = b.customer_no
        where a.init_date >= b.op_before_week_date
        and a.init_date <= b.op_before_one_date
    )aa  
''').toPandas()

workday_list = ['five_','four_','three_','two_','one_']

#行列转换 透视表
week_every_date_stock_position =  pd.pivot_table(week_every_date_stock_position,index=['customer_no','op_date'],columns=['date_rank'],values=['position'])
week_every_date_stock_position.columns=[x + 'stock_position' for x in workday_list ]
week_every_date_stock_position = week_every_date_stock_position.reset_index()
week_every_date_stock_position = add_statistics(week_every_date_stock_position,'stock_position')

week_every_date_stock_position = hql.createDataFrame(week_every_date_stock_position)
week_every_date_stock_position.registerTempTable('week_every_date_stock_position')



    #1.7近一周各天维保比例
week_every_date_deposit_rate = hql.sql('''
select customer_no,op_date,date_rank,deposit_rate
from 
    (select a.customer_no,op_date,init_date,deposit_rate,
            rank()over(partition by a.customer_no,op_date order by init_date ) as date_rank
    from 
        (select customer_no,close_rate as deposit_rate,
        concat(substr(init_date, 0,4), '-', substr(init_date, 5,2),'-', substr(init_date, 7,2)) as init_date
        from agg_cust_balance_workday)a
        join 
        (select customer_no,op_date,op_before_one_date,op_before_week_date
        from stock_active_customer_label)b 
        on a.customer_no = b.customer_no
        and a.init_date >= b.op_before_week_date
        and a.init_date <= b.op_before_one_date
    )aa
''').toPandas()

week_every_date_deposit_rate  =  pd.pivot_table(week_every_date_deposit_rate,index=['customer_no','op_date'],columns=['date_rank'],values=['deposit_rate'])
week_every_date_deposit_rate.columns=[x + 'deposit_rate' for x in workday_list ]
week_every_date_deposit_rate  = week_every_date_deposit_rate.reset_index()
week_every_date_deposit_rate = add_statistics(week_every_date_deposit_rate,'deposit_rate')
week_every_date_deposit_rate= hql.createDataFrame(week_every_date_deposit_rate)
week_every_date_deposit_rate.registerTempTable('week_every_date_deposit_rate')



try:
    hql.sql('''
            insert overwrite table cust_mining2.'''+save_table+'''
            partition (part_date=''' + run_date + ''')
            select a.customer_no ,a.op_date ,
                nvl(one_date_cash_in_num ,0) as one_date_cash_in_num ,
                nvl(one_date_cash_in_sum ,0) as one_date_cash_in_sum ,
                nvl(week_date_cash_in_num,0) as week_date_cash_in_num,
                nvl(week_date_cash_in_sum,0) as week_date_cash_in_sum,
                nvl(month_date_cash_in_num,0) as month_date_cash_in_num,
                nvl(month_date_cash_in_sum,0) as month_date_cash_in_sum,
                nvl(one_date_cash_out_num,0) as one_date_cash_out_num,
                nvl(one_date_cash_out_sum,0) as one_date_cash_out_sum,
                nvl(week_date_cash_out_num,0) as week_date_cash_out_num,
                nvl(week_date_cash_out_sum,0) as week_date_cash_out_sum,
                nvl(month_date_cash_out_num,0) as month_date_cash_out_num,
                nvl(month_date_cash_out_sum,0) as month_date_cash_out_sum,
                nvl(one_date_buy_stock_num,0) as one_date_buy_stock_num,
                nvl(one_date_buy_stock_sum,0) as one_date_buy_stock_sum,
                nvl(week_date_buy_stock_num,0) as week_date_buy_stock_num,
                nvl(week_date_buy_stock_sum,0) as week_date_buy_stock_sum,
                nvl(month_date_buy_stock_num,0) as month_date_buy_stock_num,
                nvl(month_date_buy_stock_sum,0) as month_date_buy_stock_sum,
                nvl(one_date_sell_stock_num,0) as one_date_sell_stock_num,
                nvl(one_date_sell_stock_sum,0) as one_date_sell_stock_sum,
                nvl(week_date_sell_stock_num,0) as week_date_sell_stock_num,
                nvl(week_date_sell_stock_sum,0) as week_date_sell_stock_sum,
                nvl(month_date_sell_stock_num,0) as month_date_sell_stock_num,
                nvl(month_date_sell_stock_sum,0) as month_date_sell_stock_sum,
                nvl(one_date_asset_amount,0) as one_date_asset_amount,
                nvl(week_date_asset_amount,0) as week_date_asset_amount,
                nvl(month_date_asset_amount,0) as month_date_asset_amount,
                five_stock_position,
                four_stock_position,
                three_stock_position,
                two_stock_position,
                one_stock_position,
                null_stock_position,
                diff_four_stock_position,
                diff_three_stock_position,
                diff_two_stock_position,
                diff_one_stock_position,
                mean_stock_position,
                std_stock_position,
                more_avg_num_stock_position,
                diff_more_avg_num_stock_position,
                five_deposit_rate,
                four_deposit_rate,
                three_deposit_rate,
                two_deposit_rate,
                one_deposit_rate,
                null_deposit_rate,
                diff_four_deposit_rate,
                diff_three_deposit_rate,
                diff_two_deposit_rate,
                diff_one_deposit_rate,
                mean_deposit_rate,
                std_deposit_rate,
                more_avg_num_deposit_rate,
                diff_more_avg_num_deposit_rate
                '''+ is_label+'''
    		from stock_active_customer_label a
    		left join one_date_cash_in b 
    		on a.customer_no = b.customer_no and a.op_date = b.op_date
    		left join week_date_cash_in c 
    		on a.customer_no = c.customer_no  and a.op_date = c.op_date
    		left join month_date_cash_in d
    		on a.customer_no = d.customer_no  and a.op_date = d.op_date
    		left join one_date_cash_out e 
    		on a.customer_no = e.customer_no and a.op_date = e.op_date
    		left join week_date_cash_out f
    		on a.customer_no = f.customer_no and a.op_date = f.op_date
    		left join month_date_cash_out g
    		on a.customer_no = g.customer_no and a.op_date = g.op_date
    		left join one_date_buy_stock h 
    		on a.customer_no = h.customer_no and a.op_date = h.op_date
    		left join week_date_buy_stock i 
    		on a.customer_no = i.customer_no and a.op_date = i.op_date
    		left join month_date_buy_stock j
    		on a.customer_no = j.customer_no and a.op_date = j.op_date
    		left join one_date_sell_stock k
    		on a.customer_no = k.customer_no and a.op_date =k.op_date
    		left join week_date_sell_stock l
    		on a.customer_no = l.customer_no and a.op_date =l.op_date
    		left join month_date_sell_stock m
    		on a.customer_no = m.customer_no and a.op_date =m.op_date
    		left join one_date_asset n 
    		on a.customer_no = n.customer_no and a.op_date =n.op_date 
    		left join week_date_asset o 
    		on a.customer_no = o.customer_no and a.op_date =o.op_date
    		left join month_date_asset p 
    		on a.customer_no = p.customer_no and a.op_date =p.op_date 
    		left join week_every_date_stock_position q
    		on a.customer_no = q.customer_no and a.op_date = q.op_date  
            left join week_every_date_deposit_rate u
            on a.customer_no = u.customer_no and a.op_date = u.op_date
                ''')
    print('write cust_mining.'+ save_table + run_date + ' to hive Successfully')
except:
    print('write cust_mining.'+ save_table + run_date + ' to hive Failed !!!')


sc.stop()

