# coding:utf-8
from pyspark import SparkConf, SparkContext, HiveContext
from a2_week_add_index import  *
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
        save_table = 'c4_industry_feature_train'
    else:
        save_table = 'c4_industry_feature_test'
    return save_table

def get_is_label(flag):
    if int(flag) == 0:
        is_label = ',label'
    else:
        is_label = ''
    return is_label



save_table = get_save_table(flag)
is_label = get_is_label(flag)


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

#行业 增加涨跌幅
industry_history_add = hql.sql('''
    select code,date,open_price,high_price,
        low_price,close_price,amount,volume,
        nvl((close_price-previous_close_price)/previous_close_price,0) as change_rate
    from 
    (   select code,date,open_price,high_price,
        low_price,close_price,amount,volume,
        lag(close_price,1)over(partition by code order by date) as previous_close_price
        from ths_data.industry_history)a 
    ''')
industry_history_add.registerTempTable('industry_history_add')


#股票-行业数据
industry_info = hql.sql('''
    select industry_code,stock_code,
    date,open_price,high_price,
    low_price,close_price,amount,volume,change_rate
    from 
    (select industry_code,stock_code
    from ths_data.industry_classify
    where part_date = 20180901)a 
    join 
    (select code,date,open_price,high_price,
    low_price,close_price,amount,volume,change_rate
    from industry_history_add)b 
    on a.industry_code =  b.code
    ''')
industry_info.registerTempTable('industry_info')



#持股集中度top1股票对应行业的信息
stock_concentrate_top1_industry_info = hql.sql('''
    select customer_no,op_date,stock_no,
        open_price,high_price,
    low_price,close_price,amount,volume,change_rate,
    row_number()over(partition by customer_no,op_date order by date   ) as date_rank 
    from 
    (select *
    from stock_concentrate_top1)a  
    join
    (select industry_code,stock_code,
    open_price,high_price,
    low_price,close_price,amount,volume,change_rate,
    concat(substr(date, 0,4), '-', substr(date, 5,2),'-', substr(date, 7,2)) as date
    from industry_info)b 
    on a.stock_no = b.stock_code 
    and b.date >= a.op_before_week_date
    and b.date <= a.op_before_one_date
    ''').toPandas()

stock_concentrate_top1_industry_info.open_price = stock_concentrate_top1_industry_info.open_price.astype('float')
stock_concentrate_top1_industry_info.close_price = stock_concentrate_top1_industry_info.close_price.astype('float')
stock_concentrate_top1_industry_info.high_price = stock_concentrate_top1_industry_info.high_price.astype('float')
stock_concentrate_top1_industry_info.low_price = stock_concentrate_top1_industry_info.low_price.astype('float')
stock_concentrate_top1_industry_info.amount = stock_concentrate_top1_industry_info.amount.astype('float')
stock_concentrate_top1_industry_info.volume = stock_concentrate_top1_industry_info.volume.astype('float')
stock_concentrate_top1_industry_info.change_rate = stock_concentrate_top1_industry_info.change_rate.astype('float')

workday_list = ['five_','four_','three_','two_','one_']

#开盘价
top1_open_price_info = pd.pivot_table(stock_concentrate_top1_industry_info,index=['customer_no','op_date'],columns=['date_rank'],values=['open_price'])
top1_open_price_info.columns=[x + 'top1_industry_open_price' for x in workday_list ]
top1_open_price_info = top1_open_price_info.reset_index()
top1_open_price_info = add_statistics(top1_open_price_info,'top1_industry_open_price')
top1_open_price_info = hql.createDataFrame(top1_open_price_info)
top1_open_price_info.registerTempTable('top1_open_price_info')


#收盘价
top1_close_price_info = pd.pivot_table(stock_concentrate_top1_industry_info,index=['customer_no','op_date'],columns=['date_rank'],values=['close_price'])
top1_close_price_info.columns=[x + 'top1_industry_close_price' for x in workday_list ]
top1_close_price_info = top1_close_price_info.reset_index()
top1_close_price_info = add_statistics(top1_close_price_info,'top1_industry_close_price')
top1_close_price_info = hql.createDataFrame(top1_close_price_info)
top1_close_price_info.registerTempTable('top1_close_price_info')


#最高价
top1_high_price_info = pd.pivot_table(stock_concentrate_top1_industry_info,index=['customer_no','op_date'],columns=['date_rank'],values=['high_price'])
top1_high_price_info.columns=[x + 'top1_industry_high_price' for x in workday_list ]
top1_high_price_info = top1_high_price_info.reset_index()
top1_high_price_info = add_statistics(top1_high_price_info,'top1_industry_high_price')
top1_high_price_info = hql.createDataFrame(top1_high_price_info)
top1_high_price_info.registerTempTable('top1_high_price_info')


#最低价
top1_low_price_info = pd.pivot_table(stock_concentrate_top1_industry_info,index=['customer_no','op_date'],columns=['date_rank'],values=['low_price'])
top1_low_price_info.columns=[x + 'top1_industry_low_price' for x in workday_list ]
top1_low_price_info = top1_low_price_info.reset_index()
top1_low_price_info = add_statistics(top1_low_price_info,'top1_industry_low_price')
top1_low_price_info = hql.createDataFrame(top1_low_price_info)
top1_low_price_info.registerTempTable('top1_low_price_info')


#成交金额
top1_amount_info = pd.pivot_table(stock_concentrate_top1_industry_info,index=['customer_no','op_date'],columns=['date_rank'],values=['amount'])
top1_amount_info.columns=[x + 'top1_industry_amount' for x in workday_list ]
top1_amount_info = top1_amount_info.reset_index()
top1_amount_info = add_statistics(top1_amount_info,'top1_industry_amount')
top1_amount_info = hql.createDataFrame(top1_amount_info)
top1_amount_info.registerTempTable('top1_amount_info')

#成交量
top1_volume_info = pd.pivot_table(stock_concentrate_top1_industry_info,index=['customer_no','op_date'],columns=['date_rank'],values=['volume'])
top1_volume_info.columns=[x + 'top1_industry_volume' for x in workday_list ]
top1_volume_info = top1_volume_info.reset_index()
top1_volume_info = add_statistics(top1_volume_info,'top1_industry_volume')
top1_volume_info = hql.createDataFrame(top1_volume_info)
top1_volume_info.registerTempTable('top1_volume_info')


#涨跌幅
top1_change_rate_info = pd.pivot_table(stock_concentrate_top1_industry_info,index=['customer_no','op_date'],columns=['date_rank'],values=['change_rate'])
top1_change_rate_info.columns=[x + 'top1_industry_change_rate' for x in workday_list ]
top1_change_rate_info = top1_change_rate_info.reset_index()
top1_change_rate_info = add_statistics(top1_change_rate_info ,'top1_industry_change_rate')
top1_change_rate_info = hql.createDataFrame(top1_change_rate_info)
top1_change_rate_info.registerTempTable('top1_change_rate_info')

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
    where rank =2)a
    join 
    (select customer_no,op_date,op_before_one_date,op_before_week_date
    from cust_mining.'''+ tablename +''')b 
    on a.customer_no = b.customer_no
    and a.init_date = b.op_before_one_date
    ''') 
stock_concentrate_top2.registerTempTable('stock_concentrate_top2')


#持股集中度top2股票对应行业的信息
stock_concentrate_top2_industry_info = hql.sql('''
    select customer_no,op_date,stock_no,
        open_price,high_price,
    low_price,close_price,amount,volume,change_rate,
    row_number()over(partition by customer_no,op_date order by date  ) as date_rank 
    from 
    (select *
    from stock_concentrate_top2)a  
    join
    (select industry_code,stock_code,
    open_price,high_price,
    low_price,close_price,amount,volume,change_rate,
    concat(substr(date, 0,4), '-', substr(date, 5,2),'-', substr(date, 7,2)) as date
    from industry_info)b 
    on a.stock_no = b.stock_code 
    and b.date >= a.op_before_week_date
    and b.date <= a.op_before_one_date
    ''').toPandas()

stock_concentrate_top2_industry_info.open_price = stock_concentrate_top2_industry_info.open_price.astype('float')
stock_concentrate_top2_industry_info.close_price = stock_concentrate_top2_industry_info.close_price.astype('float')
stock_concentrate_top2_industry_info.high_price = stock_concentrate_top2_industry_info.high_price.astype('float')
stock_concentrate_top2_industry_info.low_price = stock_concentrate_top2_industry_info.low_price.astype('float')
stock_concentrate_top2_industry_info.amount = stock_concentrate_top2_industry_info.amount.astype('float')
stock_concentrate_top2_industry_info.volume = stock_concentrate_top2_industry_info.volume.astype('float')
stock_concentrate_top2_industry_info.change_rate = stock_concentrate_top2_industry_info.change_rate.astype('float')

#开盘价
top2_open_price_info = pd.pivot_table(stock_concentrate_top2_industry_info,index=['customer_no','op_date'],columns=['date_rank'],values=['open_price'])
top2_open_price_info.columns=[x + 'top2_industry_open_price' for x in workday_list ]
top2_open_price_info = top2_open_price_info.reset_index()
top2_open_price_info = add_statistics(top2_open_price_info,'top2_industry_open_price')
top2_open_price_info = hql.createDataFrame(top2_open_price_info)
top2_open_price_info.registerTempTable('top2_open_price_info')


#收盘价
top2_close_price_info = pd.pivot_table(stock_concentrate_top2_industry_info,index=['customer_no','op_date'],columns=['date_rank'],values=['close_price'])
top2_close_price_info.columns=[x + 'top2_industry_close_price' for x in workday_list ]
top2_close_price_info = top2_close_price_info.reset_index()
top2_close_price_info = add_statistics(top2_close_price_info,'top2_industry_close_price')
top2_close_price_info = hql.createDataFrame(top2_close_price_info)
top2_close_price_info.registerTempTable('top2_close_price_info')


#最高价
top2_high_price_info = pd.pivot_table(stock_concentrate_top2_industry_info,index=['customer_no','op_date'],columns=['date_rank'],values=['high_price'])
top2_high_price_info.columns=[x + 'top2_industry_high_price' for x in workday_list ]
top2_high_price_info = top2_high_price_info.reset_index()
top2_high_price_info = add_statistics(top2_high_price_info,'top2_industry_high_price')
top2_high_price_info = hql.createDataFrame(top2_high_price_info)
top2_high_price_info.registerTempTable('top2_high_price_info')


#最低价
top2_low_price_info = pd.pivot_table(stock_concentrate_top2_industry_info,index=['customer_no','op_date'],columns=['date_rank'],values=['low_price'])
top2_low_price_info.columns=[x + 'top2_industry_low_price' for x in workday_list ]
top2_low_price_info = top2_low_price_info.reset_index()
top2_low_price_info = add_statistics(top2_low_price_info,'top2_industry_low_price')
top2_low_price_info = hql.createDataFrame(top2_low_price_info)
top2_low_price_info.registerTempTable('top2_low_price_info')


#成交金额
top2_amount_info = pd.pivot_table(stock_concentrate_top2_industry_info,index=['customer_no','op_date'],columns=['date_rank'],values=['amount'])
top2_amount_info.columns=[x + 'top2_industry_amount' for x in workday_list ]
top2_amount_info = top2_amount_info.reset_index()
top2_amount_info = add_statistics(top2_amount_info,'top2_industry_amount')
top2_amount_info = hql.createDataFrame(top2_amount_info)
top2_amount_info.registerTempTable('top2_amount_info')

#成交量
top2_volume_info = pd.pivot_table(stock_concentrate_top2_industry_info,index=['customer_no','op_date'],columns=['date_rank'],values=['volume'])
top2_volume_info.columns=[x + 'top2_industry_volume' for x in workday_list ]
top2_volume_info = top2_volume_info.reset_index()
top2_volume_info = add_statistics(top2_volume_info,'top2_industry_volume')
top2_volume_info = hql.createDataFrame(top2_volume_info)
top2_volume_info.registerTempTable('top2_volume_info')

#涨跌幅
top2_change_rate_info = pd.pivot_table(stock_concentrate_top2_industry_info,index=['customer_no','op_date'],columns=['date_rank'],values=['change_rate'])
top2_change_rate_info.columns=[x + 'top2_industry_change_rate' for x in workday_list ]
top2_change_rate_info = top2_change_rate_info.reset_index()
top2_change_rate_info = add_statistics(top2_change_rate_info ,'top2_industry_change_rate')
top2_change_rate_info = hql.createDataFrame(top2_change_rate_info)
top2_change_rate_info.registerTempTable('top2_change_rate_info')


try:
    hql.sql('''
        insert overwrite table cust_mining2.'''+save_table+'''
            partition (part_date=''' + run_date + ''')
            select  a.customer_no,
                    a.op_date,  
                    null_top1_industry_open_price,
                    diff_four_top1_industry_open_price,
                    diff_three_top1_industry_open_price,
                    diff_two_top1_industry_open_price,
                    diff_one_top1_industry_open_price,
                    mean_top1_industry_open_price,
                    std_top1_industry_open_price,
                    more_avg_num_top1_industry_open_price,
                    diff_more_avg_num_top1_industry_open_price,
                    null_top1_industry_close_price,
                    diff_four_top1_industry_close_price,
                    diff_three_top1_industry_close_price,
                    diff_two_top1_industry_close_price,
                    diff_one_top1_industry_close_price,
                    mean_top1_industry_close_price,
                    std_top1_industry_close_price,
                    more_avg_num_top1_industry_close_price,
                    diff_more_avg_num_top1_industry_close_price,
                    null_top1_industry_high_price,
                    diff_four_top1_industry_high_price,
                    diff_three_top1_industry_high_price,
                    diff_two_top1_industry_high_price,
                    diff_one_top1_industry_high_price,
                    mean_top1_industry_high_price,
                    std_top1_industry_high_price,
                    more_avg_num_top1_industry_high_price,
                    diff_more_avg_num_top1_industry_high_price,
                    null_top1_industry_low_price,
                    diff_four_top1_industry_low_price,
                    diff_three_top1_industry_low_price,
                    diff_two_top1_industry_low_price,
                    diff_one_top1_industry_low_price,
                    mean_top1_industry_low_price,
                    std_top1_industry_low_price,
                    more_avg_num_top1_industry_low_price,
                    diff_more_avg_num_top1_industry_low_price,
                    null_top1_industry_amount,
                    diff_four_top1_industry_amount,
                    diff_three_top1_industry_amount,
                    diff_two_top1_industry_amount,
                    diff_one_top1_industry_amount,
                    mean_top1_industry_amount,
                    std_top1_industry_amount,
                    more_avg_num_top1_industry_amount,
                    diff_more_avg_num_top1_industry_amount,
                    null_top1_industry_volume,
                    diff_four_top1_industry_volume,
                    diff_three_top1_industry_volume,
                    diff_two_top1_industry_volume,
                    diff_one_top1_industry_volume,
                    mean_top1_industry_volume,
                    std_top1_industry_volume,
                    more_avg_num_top1_industry_volume,
                    diff_more_avg_num_top1_industry_volume,
                    null_top1_industry_change_rate,
                    diff_four_top1_industry_change_rate,
                    diff_three_top1_industry_change_rate,
                    diff_two_top1_industry_change_rate,
                    diff_one_top1_industry_change_rate,
                    mean_top1_industry_change_rate,
                    std_top1_industry_change_rate,
                    more_avg_num_top1_industry_change_rate,
                    diff_more_avg_num_top1_industry_change_rate,
                    null_top2_industry_open_price,
                    diff_four_top2_industry_open_price,
                    diff_three_top2_industry_open_price,
                    diff_two_top2_industry_open_price,
                    diff_one_top2_industry_open_price,
                    mean_top2_industry_open_price,
                    std_top2_industry_open_price,
                    more_avg_num_top2_industry_open_price,
                    diff_more_avg_num_top2_industry_open_price,
                    null_top2_industry_close_price,
                    diff_four_top2_industry_close_price,
                    diff_three_top2_industry_close_price,
                    diff_two_top2_industry_close_price,
                    diff_one_top2_industry_close_price,
                    mean_top2_industry_close_price,
                    std_top2_industry_close_price,
                    more_avg_num_top2_industry_close_price,
                    diff_more_avg_num_top2_industry_close_price,
                    null_top2_industry_high_price,
                    diff_four_top2_industry_high_price,
                    diff_three_top2_industry_high_price,
                    diff_two_top2_industry_high_price,
                    diff_one_top2_industry_high_price,
                    mean_top2_industry_high_price,
                    std_top2_industry_high_price,
                    more_avg_num_top2_industry_high_price,
                    diff_more_avg_num_top2_industry_high_price,
                    null_top2_industry_low_price,
                    diff_four_top2_industry_low_price,
                    diff_three_top2_industry_low_price,
                    diff_two_top2_industry_low_price,
                    diff_one_top2_industry_low_price,
                    mean_top2_industry_low_price,
                    std_top2_industry_low_price,
                    more_avg_num_top2_industry_low_price,
                    diff_more_avg_num_top2_industry_low_price,
                    null_top2_industry_amount,
                    diff_four_top2_industry_amount,
                    diff_three_top2_industry_amount,
                    diff_two_top2_industry_amount,
                    diff_one_top2_industry_amount,
                    mean_top2_industry_amount,
                    std_top2_industry_amount,
                    more_avg_num_top2_industry_amount,
                    diff_more_avg_num_top2_industry_amount,
                    null_top2_industry_volume,
                    diff_four_top2_industry_volume,
                    diff_three_top2_industry_volume,
                    diff_two_top2_industry_volume,
                    diff_one_top2_industry_volume,
                    mean_top2_industry_volume,
                    std_top2_industry_volume,
                    more_avg_num_top2_industry_volume,
                    diff_more_avg_num_top2_industry_volume,
                    null_top2_industry_change_rate,
                    diff_four_top2_industry_change_rate,
                    diff_three_top2_industry_change_rate,
                    diff_two_top2_industry_change_rate,
                    diff_one_top2_industry_change_rate,
                    mean_top2_industry_change_rate,
                    std_top2_industry_change_rate,
                    more_avg_num_top2_industry_change_rate,
                    diff_more_avg_num_top2_industry_change_rate
                    '''+is_label+'''
            from stock_active_customer_label a 
            left join top1_open_price_info b 
            on a.customer_no =b.customer_no and a.op_date = b.op_date
            left join top1_close_price_info c 
            on a.customer_no = c.customer_no and a.op_date = c.op_date
            left join top1_high_price_info d
            on a.customer_no = d.customer_no and a.op_date = d.op_date
            left join top1_low_price_info e 
            on a.customer_no = e.customer_no  and a.op_date = e.op_date
            left join top1_amount_info f 
            on a.customer_no = f.customer_no and a.op_date =f.op_date 
            left join top1_volume_info g 
            on a.customer_no = g.customer_no and a.op_date = g.op_date
            left join top2_open_price_info h 
            on a.customer_no = h.customer_no and a.op_date = h.op_date
            left join top2_close_price_info i 
            on a.customer_no = i.customer_no and  a.op_date = i.op_date
            left join top2_high_price_info j
            on a.customer_no = j.customer_no and a.op_date = j.op_date
            left join top2_low_price_info k 
            on a.customer_no = k.customer_no  and a.op_date = k.op_date
            left join top2_amount_info l
            on a.customer_no = l.customer_no and a.op_date =l.op_date
            left join top2_volume_info m
            on a.customer_no = m.customer_no and a.op_date = m.op_date
            left join top1_change_rate_info n
            on a.customer_no = n.customer_no and a.op_date = n.op_date
            left join top2_change_rate_info o
            on a.customer_no = o.customer_no and a.op_date = o.op_date
        ''')
    print('write cust_mining.' +save_table + run_date + ' to hive Successfully')
except:
    print('write cust_mining.'+ save_table + run_date + ' to hive Failed !!!')

sc.stop()

