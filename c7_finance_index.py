# coding:utf-8
from pyspark import SparkConf, SparkContext, HiveContext
from a1_finance_engine_index import  *
import pandas as pd
import datetime
import sys 


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



def get_save_table(flag):
    if int(flag) == 0:
        save_table = 'c7_finance_index_train'
    else:
        save_table = 'c7_finance_index_test'
    return save_table

def get_is_label(flag):
    if int(flag) == 0:
        is_label = ',label'
    else:
        is_label = ''
    return is_label


save_table = get_save_table(flag)
is_label = get_is_label(flag)


conf = SparkConf()
sc = SparkContext()
hql = HiveContext(sc)

#主表
stock_active_customer_label = hql.sql('''
select customer_no,op_date'''+is_label+'''
from cust_mining.'''+tablename+'''    
''')
stock_active_customer_label.registerTempTable('stock_active_customer_label')


#计算持股集中度(单只股票金额/总市值)
stock_concentrate_table = hql.sql('''
    select a.customer_no,a.init_date,stock_no,stock_name,
    round(amount/total_amount,2) as stock_concentrate
    from 
    (select customer_no,init_date,
          sum(current_qty*cost_price) as total_amount
    from ctprod.agg_cust_stock
    where init_date >= '20170101'
    group by customer_no,init_date)a 
    join
    (select customer_no,init_date,
      stock_no,stock_name,current_qty*cost_price as amount  
      from ctprod.agg_cust_stock
      where init_date >= '20170101') b
    on a.customer_no = b.customer_no 
    and a.init_date = b.init_date
''')
stock_concentrate_table.registerTempTable('stock_concentrate_table')


#持股集中度top1的股票
stock_concentrate_top1 = hql.sql('''
    select a.customer_no,op_date,op_before_one_date,op_before_week_date,stock_no
    from 
   (select customer_no,init_date,stock_no
    from 
        (select customer_no,stock_no,stock_name,stock_concentrate,
        concat(substr(init_date, 0,4), '-', substr(init_date, 5,2),'-', substr(init_date, 7,2)) as init_date,
        row_number()over(partition by customer_no,init_date order by stock_concentrate desc) as rank
        from stock_concentrate_table
        )xxx
    where rank =1)a
    join 
    (select customer_no,op_date,op_before_one_date,op_before_week_date
    from cust_mining.'''+ tablename +''')b 
    on a.customer_no = b.customer_no
    and a.init_date = b.op_before_one_date
    ''')
stock_concentrate_top1.registerTempTable('stock_concentrate_top1')

#持股集中度top1的股票相关info
stock_concentrate_top1_stock_index = hql.sql('''

    select customer_no,op_date,a.stock_no,date,close_price,change,
    first_value(close_price) over(partition by customer_no,op_date order by date ) as first_price,
    first_value(close_price) over(partition by customer_no,op_date order by date desc ) as last_price,
    max(close_price) over(partition by customer_no,op_date order by date ) as max_curr_price,
    count(close_price)over(partition by customer_no,op_date  ) as shape
    from 
    (select customer_no,op_date,op_before_one_date,stock_no
    from stock_concentrate_top1)a 
    left join
    (select tradingdate as date,secucode as stock_no,changepct as change,closeprice as close_price
        from ctprod.schema_stock_market)b 
    on a.stock_no = b.stock_no 
    and date >=  date_sub(op_before_one_date,30) and 
    date <=  op_before_one_date
    where date is not null
    
    ''')
stock_concentrate_top1_stock_index.registerTempTable('stock_concentrate_top1_stock_index')

#指数基准线info 这里以上证指数为例
stock_concentrate_top1_market_index = hql.sql('''

    select customer_no,op_date,a.stock_no,date,close_price as market_close_price,change as market_change,
    first_value(close_price) over(partition by customer_no,op_date order by date ) as first_market_price,
    first_value(close_price) over(partition by customer_no,op_date order by date desc ) as last_market_price,
    count(close_price)over(partition by customer_no,op_date  ) as market_shape
    from 
    (select customer_no,op_date,op_before_one_date,stock_no
    from stock_concentrate_top1)a 
    left join
    (select tradingdate as date,secucode as market_no,changepct as change,closeprice as close_price
    from ctprod.schema_grail_market
    where secucode = '000001')b 
    on b.date >=  date_sub(op_before_one_date,30) and 
    b.date <=  op_before_one_date
    
    ''')
stock_concentrate_top1_market_index.registerTempTable('stock_concentrate_top1_market_index')


# 金工指标part1
# top1_finance_prob_up 上涨概率
# top1_finance_volatility  收益波动率
# top1_finance_average_change  平均涨幅
# top1_finance_max_retreat 最大回撤
# top1_finance_annual 年化收益率
stock_concentrate_top1_finance_index_1 = hql.sql('''
    select customer_no,op_date,stock_no,
    sum(if(change >0 ,1,0))  / count(*) as top1_finance_prob_up,
    stddev(change) * sqrt(250) as top1_finance_volatility,
    mean(change) as top1_finance_average_change,
    min(close_price /max_curr_price -1) as top1_finance_max_retreat,
    (pow(last_price/first_price,250/shape)-1) as top1_finance_annual
    from  stock_concentrate_top1_stock_index
    group by customer_no,op_date,stock_no
    ''')
stock_concentrate_top1_finance_index_1.registerTempTable('stock_concentrate_top1_finance_index_1')

# 金工指标part2
# top1_finance_beta 贝塔系数
# top1_finance_info 信息比率
stock_concentrate_top1_finance_index_2 = hql.sql('''
    select  a.customer_no,a.op_date,a.stock_no,
            covar_pop(change,market_change) / variance(market_change) as top1_finance_beta,
            (mean(change - market_change) * 250) / (stddev(change - market_change) * sqrt(250)) as top1_finance_info 
    from 
    (select *
    from stock_concentrate_top1_finance_index)a 
    join
    (select *
    from stock_concentrate_top1_market_index)b 
    on a.customer_no = b.customer_no
    and a.op_date = b.op_date
    and a.date = b.date 
    group by a.customer_no,a.op_date,a.stock_no

    ''')
stock_concentrate_top1_finance_index_2.registerTempTable('stock_concentrate_top1_finance_index_2')


# 金工指标part3
# top1_finance_alpha 阿尔法系数
stock_concentrate_top1_finance_index_3 = hql.sql('''
    select a.customer_no,a.op_date,a.stock_no,
            (top1_finance_annual - 0.0284 ) - top1_finance_beta *(top1_market_annual - 0.0284) as top1_finance_alpha
    from 
    stock_concentrate_top1_finance_index_1 a 
    join
    (select customer_no,op_date,stock_no,
    (pow(last_market_price/first_market_price,250/market_shape)-1) as top1_market_annual
    from stock_concentrate_top1_market_index
    group by customer_no,op_date,stock_no)b 
    on a.customer_no = b.customer_no
    and a.op_date = b.op_date
    join
    stock_concentrate_top1_finance_index_2  c 
    on a.customer_no = c.customer_no
    and a.op_date = c.op_date 
    group by a.customer_no,a.op_date,a.stock_no
''')
stock_concentrate_top1_finance_index_3.registerTempTable('stock_concentrate_top1_finance_index_3')


#持股集中度top2的股票
stock_concentrate_top2 = hql.sql('''
    select a.customer_no,op_date,op_before_one_date,op_before_week_date,stock_no
    from 
   (select customer_no,init_date,stock_no
    from 
        (select customer_no,stock_no,stock_name,stock_concentrate,
        concat(substr(init_date, 0,4), '-', substr(init_date, 5,2),'-', substr(init_date, 7,2)) as init_date,
        row_number()over(partition by customer_no,init_date order by stock_concentrate desc) as rank
        from stock_concentrate_table
        )xxx
    where rank = 2)a
    join 
    (select customer_no,op_date,op_before_one_date,op_before_week_date
    from cust_mining.'''+ tablename +''')b 
    on a.customer_no = b.customer_no
    and a.init_date = b.op_before_one_date
    ''')
stock_concentrate_top2.registerTempTable('stock_concentrate_top2')

#持股集中度top2的股票相关info
stock_concentrate_top2_stock_index = hql.sql('''

    select customer_no,op_date,a.stock_no,date,close_price,change,
    first_value(close_price) over(partition by customer_no,op_date order by date ) as first_price,
    first_value(close_price) over(partition by customer_no,op_date order by date desc ) as last_price,
    max(close_price) over(partition by customer_no,op_date order by date ) as max_curr_price,
    count(close_price)over(partition by customer_no,op_date  ) as shape
    from 
    (select customer_no,op_date,op_before_one_date,stock_no
    from stock_concentrate_top2)a 
    left join
    (select tradingdate as date,secucode as stock_no,changepct as change,closeprice as close_price
        from ctprod.schema_stock_market)b 
    on a.stock_no = b.stock_no 
    and date >=  date_sub(op_before_one_date,30) and 
    date <=  op_before_one_date
    where date is not null
    
    ''')
stock_concentrate_top2_stock_index.registerTempTable('stock_concentrate_top2_stock_index')

#指数基准线info 这里以上证指数为例
stock_concentrate_top2_market_index = hql.sql('''

    select customer_no,op_date,a.stock_no,date,close_price as market_close_price,change as market_change,
    first_value(close_price) over(partition by customer_no,op_date order by date ) as first_market_price,
    first_value(close_price) over(partition by customer_no,op_date order by date desc ) as last_market_price,
    count(close_price)over(partition by customer_no,op_date  ) as market_shape
    from 
    (select customer_no,op_date,op_before_one_date,stock_no
    from stock_concentrate_top2)a 
    left join
    (select tradingdate as date,secucode as market_no,changepct as change,closeprice as close_price
    from ctprod.schema_grail_market
    where secucode = '000001')b 
    on b.date >=  date_sub(op_before_one_date,30) and 
    b.date <=  op_before_one_date
    
    ''')
stock_concentrate_top2_market_index.registerTempTable('stock_concentrate_top2_market_index')


# 金工指标part1
# top2_finance_prob_up 上涨概率
# top2_finance_volatility  收益波动率
# top2_finance_average_change  平均涨幅
# top2_finance_max_retreat 最大回撤
# top2_finance_annual 年化收益率
stock_concentrate_top2_finance_index_1 = hql.sql('''
    select customer_no,op_date,stock_no,
    sum(if(change >0 ,1,0))  / count(*) as top2_finance_prob_up,
    stddev(change) * sqrt(250) as top2_finance_volatility,
    mean(change) as top2_finance_average_change,
    min(close_price /max_curr_price -1) as top2_finance_max_retreat,
    (pow(last_price/first_price,250/shape)-1) as top2_finance_annual,
    from  stock_concentrate_top2_finance_index
    group by customer_no,op_date,stock_no
    ''')
stock_concentrate_top2_finance_index_1.registerTempTable('stock_concentrate_top2_finance_index_1')

# 金工指标part2
# top2_finance_beta 贝塔系数
# top2_finance_info 信息比率
stock_concentrate_top2_finance_index_2 = hql.sql('''
    select  a.customer_no,a.op_date,a.stock_no,
            covar_pop(change,market_change) / variance(market_change) as top2_finance_beta,
            (mean(change - market_change) * 250) / (stddev(change - market_change) * sqrt(250)) as top2_finance_info 
    from 
    (select *
    from stock_concentrate_top2_finance_index)a 
    join
    (select *
    from stock_concentrate_top2_market_index)b 
    on a.customer_no = b.customer_no
    and a.op_date = b.op_date
    and a.date = b.date 
    group by a.customer_no,a.op_date,a.stock_no

    ''')
stock_concentrate_top2_finance_index_2.registerTempTable('stock_concentrate_top2_finance_index_2')


# 金工指标part3
# top2_finance_alpha 阿尔法系数
stock_concentrate_top2_finance_index_3 = hql.sql('''
    select a.customer_no,a.op_date,a.stock_no,
            (top2_finance_annual - 0.0284 ) - top2_finance_beta *(top2_market_annual - 0.0284) as top2_finance_alpha
    from 
    stock_concentrate_top2_finance_index_1 a 
    join
    (select customer_no,op_date,stock_no,
    (pow(last_market_price/first_market_price,250/market_shape)-1) as top2_market_annual
    from stock_concentrate_top2_market_index
    group by customer_no,op_date,stock_no)b 
    on a.customer_no = b.customer_no
    and a.op_date = b.op_date
    join
    stock_concentrate_top2_finance_index_2  c 
    on a.customer_no = c.customer_no
    and a.op_date = c.op_date 
    group by a.customer_no,a.op_date,a.stock_no
''')
stock_concentrate_top2_finance_index_3.registerTempTable('stock_concentrate_top2_finance_index_3')




try:
    hql.sql('''
            insert overwrite table cust_mining2.'''+save_table+'''
            partition (part_date=''' + run_date + ''')
            select a.customer_no ,a.op_date,
            top1_finance_prob_up ,
            top1_finance_volatility , 
            top1_finance_average_change , 
            top1_finance_max_retreat ,
            top1_finance_annual ,
            top1_finance_beta ,
            top1_finance_info ,
            top1_finance_alpha ,
            top2_finance_prob_up ,
            top2_finance_volatility , 
            top2_finance_average_change , 
            top2_finance_max_retreat ,
            top2_finance_annual ,
            top2_finance_beta ,
            top2_finance_info ,
            top2_finance_alpha 
            from stock_active_customer_label a 
            left join stock_concentrate_top1_finance_index_1 b 
            on a.customer_no = b.customer_no and a.op_date = b.op_date
            left join stock_concentrate_top1_finance_index_2 c 
            on a.customer_no = c.customer_no and a.op_date = c.op_date
            left join stock_concentrate_top1_finance_index_3 d 
            on a.dustomer_no = d.dustomer_no and a.op_date = d.op_date
            left join stock_concentrate_top2_finance_index_1 e  
            on a.customer_no = e.customer_no and a.op_date = e.op_date
            left join stock_concentrate_top2_finance_index_2 f 
            on a.customer_no = f.customer_no and a.op_date = f.op_date
            left join stock_concentrate_top2_finance_index_3 d 
            on a.dustomer_no = g.dustomer_no and a.op_date = g.op_date


                ''')
    print('write cust_mining.'+ save_table + run_date + ' to hive Successfully')
except:
    print('write cust_mining.'+ save_table + run_date + ' to hive Failed !!!')

sc.stop()
