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
        save_table = 'c3_top2_stock_feature_train'
    else:
        save_table = 'c3_top2_stock_feature_test'
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


#1.2近一天持股集中度TOP2股票的近一周情况
stock_concentrate_top2_info = hql.sql('''
   select customer_no,op_date, secucode as stock_no,
        openprice,closeprice,highprice,lowprice,
        turnoverrate,turnovervalue,turnovervolume,
        changepct,
        row_number()over(partition by customer_no,op_date order by tradingdate  ) as date_rank
   from
   (select *
    from stock_concentrate_top2) aa 
   join
   (select *
   from ctprod.schema_stock_market     
   )bb
   on aa.stock_no = bb.secucode
   and bb.tradingdate >= aa.op_before_week_date
   and bb.tradingdate <= aa.op_before_one_date
    
''').toPandas()

workday_list = ['five_','four_','three_','two_','one_']

### 开盘价
stock_concentrate_top2_openprice =  pd.pivot_table(stock_concentrate_top2_info,index=['customer_no','op_date'],columns=['date_rank'],values=['openprice'])
stock_concentrate_top2_openprice .columns = [x + 'top2_stock_openprice' for x in workday_list ]
stock_concentrate_top2_openprice = stock_concentrate_top2_openprice.reset_index()
stock_concentrate_top2_openprice = add_statistics(stock_concentrate_top2_openprice,'top2_stock_openprice')
stock_concentrate_top2_openprice = hql.createDataFrame(stock_concentrate_top2_openprice)
stock_concentrate_top2_openprice.registerTempTable('stock_concentrate_top2_openprice')


### 收盘价
stock_concentrate_top2_closeprice =  pd.pivot_table(stock_concentrate_top2_info,index=['customer_no','op_date'],columns=['date_rank'],values=['closeprice'])
stock_concentrate_top2_closeprice .columns = [x + 'top2_stock_closeprice' for x in workday_list ]
stock_concentrate_top2_closeprice = stock_concentrate_top2_closeprice.reset_index()
stock_concentrate_top2_closeprice = add_statistics(stock_concentrate_top2_closeprice,'top2_stock_closeprice')
stock_concentrate_top2_closeprice = hql.createDataFrame(stock_concentrate_top2_closeprice)
stock_concentrate_top2_closeprice.registerTempTable('stock_concentrate_top2_closeprice')

### 最高价
stock_concentrate_top2_highprice =  pd.pivot_table(stock_concentrate_top2_info,index=['customer_no','op_date'],columns=['date_rank'],values=['highprice'])
stock_concentrate_top2_highprice .columns = [x + 'top2_stock_highprice' for x in workday_list ]
stock_concentrate_top2_highprice = stock_concentrate_top2_highprice.reset_index()
stock_concentrate_top2_highprice = add_statistics(stock_concentrate_top2_highprice,'top2_stock_highprice')
stock_concentrate_top2_highprice = hql.createDataFrame(stock_concentrate_top2_highprice)
stock_concentrate_top2_highprice.registerTempTable('stock_concentrate_top2_highprice')

### 最低价
stock_concentrate_top2_lowprice =  pd.pivot_table(stock_concentrate_top2_info,index=['customer_no','op_date'],columns=['date_rank'],values=['lowprice'])
stock_concentrate_top2_lowprice .columns = [x + 'top2_stock_lowprice' for x in workday_list ]
stock_concentrate_top2_lowprice = stock_concentrate_top2_lowprice.reset_index()
stock_concentrate_top2_lowprice = add_statistics(stock_concentrate_top2_lowprice,'top2_stock_lowprice')
stock_concentrate_top2_lowprice = hql.createDataFrame(stock_concentrate_top2_lowprice)
stock_concentrate_top2_lowprice.registerTempTable('stock_concentrate_top2_lowprice')

### 成交量
stock_concentrate_top2_turnovervolume =  pd.pivot_table(stock_concentrate_top2_info,index=['customer_no','op_date'],columns=['date_rank'],values=['turnovervolume'])
stock_concentrate_top2_turnovervolume .columns = [x + 'top2_stock_turnovervolume' for x in workday_list ]
stock_concentrate_top2_turnovervolume = stock_concentrate_top2_turnovervolume.reset_index()
stock_concentrate_top2_turnovervolume = add_statistics(stock_concentrate_top2_turnovervolume,'top2_stock_turnovervolume')
stock_concentrate_top2_turnovervolume = hql.createDataFrame(stock_concentrate_top2_turnovervolume)
stock_concentrate_top2_turnovervolume.registerTempTable('stock_concentrate_top2_turnovervolume')

### 成交金额
stock_concentrate_top2_turnovervalue =  pd.pivot_table(stock_concentrate_top2_info,index=['customer_no','op_date'],columns=['date_rank'],values=['turnovervalue'])
stock_concentrate_top2_turnovervalue .columns = [x + 'top2_stock_turnovervalue' for x in workday_list ]
stock_concentrate_top2_turnovervalue = stock_concentrate_top2_turnovervalue.reset_index()
stock_concentrate_top2_turnovervalue = add_statistics(stock_concentrate_top2_turnovervalue,'top2_stock_turnovervalue')
stock_concentrate_top2_turnovervalue = hql.createDataFrame(stock_concentrate_top2_turnovervalue)
stock_concentrate_top2_turnovervalue.registerTempTable('stock_concentrate_top2_turnovervalue')
### 涨跌幅
stock_concentrate_top2_changepct =  pd.pivot_table(stock_concentrate_top2_info,index=['customer_no','op_date'],columns=['date_rank'],values=['changepct'])
stock_concentrate_top2_changepct .columns = [x + 'top2_stock_changepct' for x in workday_list ]
stock_concentrate_top2_changepct = stock_concentrate_top2_changepct.reset_index()
stock_concentrate_top2_changepct = add_statistics(stock_concentrate_top2_changepct,'top2_stock_changepct')
stock_concentrate_top2_changepct = hql.createDataFrame(stock_concentrate_top2_changepct)
stock_concentrate_top2_changepct.registerTempTable('stock_concentrate_top2_changepct')

### 换手率
stock_concentrate_top2_turnoverrate =  pd.pivot_table(stock_concentrate_top2_info,index=['customer_no','op_date'],columns=['date_rank'],values=['turnoverrate'])
stock_concentrate_top2_turnoverrate .columns = [x + 'top2_stock_turnoverrate' for x in workday_list ]
stock_concentrate_top2_turnoverrate = stock_concentrate_top2_turnoverrate.reset_index()
stock_concentrate_top2_turnoverrate = add_statistics(stock_concentrate_top2_turnoverrate,'top2_stock_turnoverrate')
stock_concentrate_top2_turnoverrate = hql.createDataFrame(stock_concentrate_top2_turnoverrate)
stock_concentrate_top2_turnoverrate.registerTempTable('stock_concentrate_top2_turnoverrate')


#近一天持股集中度TOP2股票的近一周融资融券情况
stock_concentrate_top2_financing = hql.sql('''
   select customer_no,op_date,secucode as stock_no,
          rzye,rzmre,rzche,rqyl,rqmcl,rqchl,
          row_number()over(partition by customer_no,op_date order by tradingdate   ) as date_rank
   from
   (select *
    from stock_concentrate_top2) aa 
   join
   (select *
   from ctprod.schema_fin_slo_stocks     
   )bb
   on aa.stock_no = bb.secucode
   and bb.tradingdate >= aa.op_before_week_date
   and bb.tradingdate <= aa.op_before_one_date
    
''').toPandas()

### 融资余额
stock_concentrate_top2_rzye =  pd.pivot_table(stock_concentrate_top2_financing,index=['customer_no','op_date'],columns=['date_rank'],values=['rzye'])
stock_concentrate_top2_rzye .columns = [x + 'top2_stock_rzye' for x in workday_list ]
stock_concentrate_top2_rzye = stock_concentrate_top2_rzye.reset_index()
stock_concentrate_top2_rzye = add_statistics(stock_concentrate_top2_rzye,'top2_stock_rzye')
stock_concentrate_top2_rzye = hql.createDataFrame(stock_concentrate_top2_rzye)
stock_concentrate_top2_rzye.registerTempTable('stock_concentrate_top2_rzye')

### 融资买入额
stock_concentrate_top2_rzmre =  pd.pivot_table(stock_concentrate_top2_financing,index=['customer_no','op_date'],columns=['date_rank'],values=['rzmre'])
stock_concentrate_top2_rzmre .columns = [x + 'top2_stock_rzmre' for x in workday_list ]
stock_concentrate_top2_rzmre = stock_concentrate_top2_rzmre.reset_index()
stock_concentrate_top2_rzmre = add_statistics(stock_concentrate_top2_rzmre,'top2_stock_rzmre')
stock_concentrate_top2_rzmre = hql.createDataFrame(stock_concentrate_top2_rzmre)
stock_concentrate_top2_rzmre.registerTempTable('stock_concentrate_top2_rzmre')

### 融资偿还额
stock_concentrate_top2_rzche =  pd.pivot_table(stock_concentrate_top2_financing,index=['customer_no','op_date'],columns=['date_rank'],values=['rzche'])
stock_concentrate_top2_rzche .columns = [x + 'top2_stock_rzche' for x in workday_list ]
stock_concentrate_top2_rzche = stock_concentrate_top2_rzche.reset_index()
stock_concentrate_top2_rzche = add_statistics(stock_concentrate_top2_rzche,'top2_stock_rzche')
stock_concentrate_top2_rzche = hql.createDataFrame(stock_concentrate_top2_rzche)
stock_concentrate_top2_rzche.registerTempTable('stock_concentrate_top2_rzche')

### 融券余量
stock_concentrate_top2_rqyl =  pd.pivot_table(stock_concentrate_top2_financing,index=['customer_no','op_date'],columns=['date_rank'],values=['rqyl'])
stock_concentrate_top2_rqyl .columns = [x + 'top2_stock_rqyl' for x in workday_list ]
stock_concentrate_top2_rqyl = stock_concentrate_top2_rqyl.reset_index()
stock_concentrate_top2_rqyl = add_statistics(stock_concentrate_top2_rqyl,'top2_stock_rqyl')
stock_concentrate_top2_rqyl = hql.createDataFrame(stock_concentrate_top2_rqyl)
stock_concentrate_top2_rqyl.registerTempTable('stock_concentrate_top2_rqyl')


### 融券卖出量
stock_concentrate_top2_rqmcl =  pd.pivot_table(stock_concentrate_top2_financing,index=['customer_no','op_date'],columns=['date_rank'],values=['rqmcl'])
stock_concentrate_top2_rqmcl .columns = [x + 'top2_stock_rqmcl' for x in workday_list ]
stock_concentrate_top2_rqmcl = stock_concentrate_top2_rqmcl.reset_index()
stock_concentrate_top2_rqmcl = add_statistics(stock_concentrate_top2_rqmcl,'top2_stock_rqmcl')
stock_concentrate_top2_rqmcl = hql.createDataFrame(stock_concentrate_top2_rqmcl)
stock_concentrate_top2_rqmcl.registerTempTable('stock_concentrate_top2_rqmcl')

### 融券偿还量
stock_concentrate_top2_rqchl =  pd.pivot_table(stock_concentrate_top2_financing,index=['customer_no','op_date'],columns=['date_rank'],values=['rqchl'])
stock_concentrate_top2_rqchl .columns = [x + 'top2_stock_rqchl' for x in workday_list ]
stock_concentrate_top2_rqchl = stock_concentrate_top2_rqchl.reset_index()
stock_concentrate_top2_rqchl = add_statistics(stock_concentrate_top2_rqchl,'top2_stock_rqchl')
stock_concentrate_top2_rqchl = hql.createDataFrame(stock_concentrate_top2_rqchl)
stock_concentrate_top2_rqchl.registerTempTable('stock_concentrate_top2_rqchl')


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
                    null_top2_stock_openprice,
                    diff_four_top2_stock_openprice,
                    diff_three_top2_stock_openprice,
                    diff_two_top2_stock_openprice,
                    diff_one_top2_stock_openprice,
                    mean_top2_stock_openprice,
                    std_top2_stock_openprice,
                    more_avg_num_top2_stock_openprice,
                    diff_more_avg_num_top2_stock_openprice,
                    null_top2_stock_closeprice,
                    diff_four_top2_stock_closeprice,
                    diff_three_top2_stock_closeprice,
                    diff_two_top2_stock_closeprice,
                    diff_one_top2_stock_closeprice,
                    mean_top2_stock_closeprice,
                    std_top2_stock_closeprice,
                    more_avg_num_top2_stock_closeprice,
                    diff_more_avg_num_top2_stock_closeprice,
                    null_top2_stock_highprice,
                    diff_four_top2_stock_highprice,
                    diff_three_top2_stock_highprice,
                    diff_two_top2_stock_highprice,
                    diff_one_top2_stock_highprice,
                    mean_top2_stock_highprice,
                    std_top2_stock_highprice,
                    more_avg_num_top2_stock_highprice,
                    diff_more_avg_num_top2_stock_highprice,
                    null_top2_stock_lowprice,
                    diff_four_top2_stock_lowprice,
                    diff_three_top2_stock_lowprice,
                    diff_two_top2_stock_lowprice,
                    diff_one_top2_stock_lowprice,
                    mean_top2_stock_lowprice,
                    std_top2_stock_lowprice,
                    more_avg_num_top2_stock_lowprice,
                    diff_more_avg_num_top2_stock_lowprice,
                    null_top2_stock_turnovervolume,
                    diff_four_top2_stock_turnovervolume,
                    diff_three_top2_stock_turnovervolume,
                    diff_two_top2_stock_turnovervolume,
                    diff_one_top2_stock_turnovervolume,
                    mean_top2_stock_turnovervolume,
                    std_top2_stock_turnovervolume,
                    more_avg_num_top2_stock_turnovervolume,
                    diff_more_avg_num_top2_stock_turnovervolume,
                    null_top2_stock_turnovervalue,
                    diff_four_top2_stock_turnovervalue,
                    diff_three_top2_stock_turnovervalue,
                    diff_two_top2_stock_turnovervalue,
                    diff_one_top2_stock_turnovervalue,
                    mean_top2_stock_turnovervalue,
                    std_top2_stock_turnovervalue,
                    more_avg_num_top2_stock_turnovervalue,
                    diff_more_avg_num_top2_stock_turnovervalue,
                    null_top2_stock_changepct,
                    diff_four_top2_stock_changepct,
                    diff_three_top2_stock_changepct,
                    diff_two_top2_stock_changepct,
                    diff_one_top2_stock_changepct,
                    mean_top2_stock_changepct,
                    std_top2_stock_changepct,
                    more_avg_num_top2_stock_changepct,
                    diff_more_avg_num_top2_stock_changepct,
                    null_top2_stock_turnoverrate,
                    diff_four_top2_stock_turnoverrate,
                    diff_three_top2_stock_turnoverrate,
                    diff_two_top2_stock_turnoverrate,
                    diff_one_top2_stock_turnoverrate,
                    mean_top2_stock_turnoverrate,
                    std_top2_stock_turnoverrate,
                    more_avg_num_top2_stock_turnoverrate,
                    diff_more_avg_num_top2_stock_turnoverrate,
                    null_top2_stock_rzye,
                    diff_four_top2_stock_rzye,
                    diff_three_top2_stock_rzye,
                    diff_two_top2_stock_rzye,
                    diff_one_top2_stock_rzye,
                    mean_top2_stock_rzye,
                    std_top2_stock_rzye,
                    more_avg_num_top2_stock_rzye,
                    diff_more_avg_num_top2_stock_rzye,
                    null_top2_stock_rzmre,
                    diff_four_top2_stock_rzmre,
                    diff_three_top2_stock_rzmre,
                    diff_two_top2_stock_rzmre,
                    diff_one_top2_stock_rzmre,
                    mean_top2_stock_rzmre,
                    std_top2_stock_rzmre,
                    more_avg_num_top2_stock_rzmre,
                    diff_more_avg_num_top2_stock_rzmre,
                    null_top2_stock_rzche,
                    diff_four_top2_stock_rzche,
                    diff_three_top2_stock_rzche,
                    diff_two_top2_stock_rzche,
                    diff_one_top2_stock_rzche,
                    mean_top2_stock_rzche,
                    std_top2_stock_rzche,
                    more_avg_num_top2_stock_rzche,
                    diff_more_avg_num_top2_stock_rzche,
                    null_top2_stock_rqyl,
                    diff_four_top2_stock_rqyl,
                    diff_three_top2_stock_rqyl,
                    diff_two_top2_stock_rqyl,
                    diff_one_top2_stock_rqyl,
                    mean_top2_stock_rqyl,
                    std_top2_stock_rqyl,
                    more_avg_num_top2_stock_rqyl,
                    diff_more_avg_num_top2_stock_rqyl,
                    null_top2_stock_rqmcl,
                    diff_four_top2_stock_rqmcl,
                    diff_three_top2_stock_rqmcl,
                    diff_two_top2_stock_rqmcl,
                    diff_one_top2_stock_rqmcl,
                    mean_top2_stock_rqmcl,
                    std_top2_stock_rqmcl,
                    more_avg_num_top2_stock_rqmcl,
                    diff_more_avg_num_top2_stock_rqmcl,
                    null_top2_stock_rqchl,
                    diff_four_top2_stock_rqchl,
                    diff_three_top2_stock_rqchl,
                    diff_two_top2_stock_rqchl,
                    diff_one_top2_stock_rqchl,
                    mean_top2_stock_rqchl,
                    std_top2_stock_rqchl,
                    more_avg_num_top2_stock_rqchl,
                    diff_more_avg_num_top2_stock_rqchl
                    '''+is_label+'''
            from stock_active_customer_label a 
            left join stock_concentrate_top2_openprice d 
            on a.customer_no = d.customer_no and a.op_date = d.op_date
            left join stock_concentrate_top2_closeprice e 
            on a.customer_no = e.customer_no and a.op_date = e.op_date 
            left join stock_concentrate_top2_highprice f 
            on a.customer_no = f.customer_no and a.op_date = f.op_date 
            left join stock_concentrate_top2_lowprice g 
            on a.customer_no = g.customer_no and a.op_date = g.op_date 
            left join stock_concentrate_top2_turnovervolume h
            on a.customer_no = h.customer_no and a.op_date = h.op_date 
            left join stock_concentrate_top2_turnovervalue i
            on a.customer_no = i.customer_no and a.op_date = i.op_date
            left join stock_concentrate_top2_changepct j
            on a.customer_no = j.customer_no and a.op_date = j.op_date
            left join stock_concentrate_top2_turnoverrate k
            on a.customer_no = k.customer_no and a.op_date = k.op_date
            left join stock_concentrate_top2_rzye l
            on a.customer_no = l.customer_no and a.op_date = l.op_date
            left join stock_concentrate_top2_rzmre m
            on a.customer_no = m.customer_no and a.op_date = m.op_date
            left join stock_concentrate_top2_rzche n
            on a.customer_no = n.customer_no and a.op_date = n.op_date
            left join stock_concentrate_top2_rqyl o
            on a.customer_no = o.customer_no and a.op_date = o.op_date
            left join stock_concentrate_top2_rqmcl p
            on a.customer_no = p.customer_no and a.op_date = p.op_date
            left join stock_concentrate_top2_rqchl q
            on a.customer_no = q.customer_no and a.op_date = q.op_date


        ''')
    print('write cust_mining2.' +save_table + run_date + ' to hive Successfully')
except:
    print('write cust_mining2.'+ save_table + run_date + ' to hive Failed !!!')

sc.stop()


