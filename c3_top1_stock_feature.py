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
        save_table = 'c3_top1_stock_feature_train'
    else:
        save_table = 'c3_top1_stock_feature_test'
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



#获取工作日
agg_cust_stock_workday = hql.sql('''
    select *
    from 
    (select *
    from ctprod.agg_cust_stock)a 
    join
    (select distinct jyr
    from ctprod.sys_calendar)b
    on a.init_date = b.jyr
    ''')
agg_cust_stock_workday.registerTempTable('agg_cust_stock_workday')

#1.持股特征
#1.1 近一周各天持股数量及总金额
stock_info = hql.sql('''
    select customer_no,op_date,date_rank,nvl(stock_num,0) as stock_num,nvl(stock_amount,0) as stock_amount 
    from 
        (select a.customer_no,op_date,init_date,stock_num,stock_amount,
                rank()over(partition by a.customer_no,op_date order by init_date ) as date_rank
        from 
            (select customer_no,sum(current_qty) as stock_num,
            sum(current_amount) as stock_amount,
            concat(substr(init_date, 0,4), '-', substr(init_date, 5,2),'-', substr(init_date, 7,2)) as init_date
            from agg_cust_stock_workday
            group by customer_no,init_date)a
            join 
            (select customer_no,op_date,op_before_one_date,op_before_week_date
            from cust_mining.'''+ tablename +''')b 
            on a.customer_no = b.customer_no
            and a.init_date >= b.op_before_week_date
            and a.init_date <= b.op_before_one_date
       )aa
        
    ''').toPandas()

workday_list = ['five_','four_','three_','two_','one_']

#持股数量
stock_num =  pd.pivot_table(stock_info,index=['customer_no','op_date'],columns=['date_rank'],values=['stock_num'])
stock_num.columns = [x + 'stock_num' for x in workday_list ]
stock_num = stock_num.reset_index()
stock_num = add_statistics(stock_num,'stock_num')
stock_num = hql.createDataFrame(stock_num)
stock_num.registerTempTable('stock_num')


#持股金额
stock_sum =  pd.pivot_table(stock_info,index=['customer_no','op_date'],columns=['date_rank'],values=['stock_amount'])
stock_sum.columns = [x + 'stock_sum' for x in workday_list ]
stock_sum = stock_sum.reset_index()
stock_sum = add_statistics(stock_sum,'stock_sum')
stock_sum = hql.createDataFrame(stock_sum)
stock_sum.registerTempTable('stock_sum')


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


#1.2近一天持股集中度TOP1股票的近一周情况
stock_concentrate_top1_info = hql.sql('''
   select customer_no,op_date, secucode as stock_no,
        openprice,closeprice,highprice,lowprice,
        turnoverrate,turnovervalue,turnovervolume,
        changepct,
        row_number()over(partition by customer_no,op_date order by tradingdate  ) as date_rank
   from
   (select *
    from stock_concentrate_top1) aa 
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
stock_concentrate_top1_openprice =  pd.pivot_table(stock_concentrate_top1_info,index=['customer_no','op_date'],columns=['date_rank'],values=['openprice'])
stock_concentrate_top1_openprice .columns = [x + 'top1_stock_openprice' for x in workday_list ]
stock_concentrate_top1_openprice = stock_concentrate_top1_openprice.reset_index()
stock_concentrate_top1_openprice = add_statistics(stock_concentrate_top1_openprice,'top1_stock_openprice')
stock_concentrate_top1_openprice = hql.createDataFrame(stock_concentrate_top1_openprice)
stock_concentrate_top1_openprice.registerTempTable('stock_concentrate_top1_openprice')



### 收盘价
stock_concentrate_top1_closeprice =  pd.pivot_table(stock_concentrate_top1_info,index=['customer_no','op_date'],columns=['date_rank'],values=['closeprice'])
stock_concentrate_top1_closeprice .columns = [x + 'top1_stock_closeprice' for x in workday_list ]
stock_concentrate_top1_closeprice = stock_concentrate_top1_closeprice.reset_index()
stock_concentrate_top1_closeprice = add_statistics(stock_concentrate_top1_closeprice,'top1_stock_closeprice')
stock_concentrate_top1_closeprice = hql.createDataFrame(stock_concentrate_top1_closeprice)
stock_concentrate_top1_closeprice.registerTempTable('stock_concentrate_top1_closeprice')

### 最高价
stock_concentrate_top1_highprice =  pd.pivot_table(stock_concentrate_top1_info,index=['customer_no','op_date'],columns=['date_rank'],values=['highprice'])
stock_concentrate_top1_highprice .columns = [x + 'top1_stock_highprice' for x in workday_list ]
stock_concentrate_top1_highprice = stock_concentrate_top1_highprice.reset_index()
stock_concentrate_top1_highprice = add_statistics(stock_concentrate_top1_highprice,'top1_stock_highprice')
stock_concentrate_top1_highprice = hql.createDataFrame(stock_concentrate_top1_highprice)
stock_concentrate_top1_highprice.registerTempTable('stock_concentrate_top1_highprice')

### 最低价
stock_concentrate_top1_lowprice =  pd.pivot_table(stock_concentrate_top1_info,index=['customer_no','op_date'],columns=['date_rank'],values=['lowprice'])
stock_concentrate_top1_lowprice .columns = [x + 'top1_stock_lowprice' for x in workday_list ]
stock_concentrate_top1_lowprice = stock_concentrate_top1_lowprice.reset_index()
stock_concentrate_top1_lowprice = add_statistics(stock_concentrate_top1_lowprice,'top1_stock_lowprice')
stock_concentrate_top1_lowprice = hql.createDataFrame(stock_concentrate_top1_lowprice)
stock_concentrate_top1_lowprice.registerTempTable('stock_concentrate_top1_lowprice')

### 成交量
stock_concentrate_top1_turnovervolume =  pd.pivot_table(stock_concentrate_top1_info,index=['customer_no','op_date'],columns=['date_rank'],values=['turnovervolume'])
stock_concentrate_top1_turnovervolume .columns = [x + 'top1_stock_turnovervolume' for x in workday_list ]
stock_concentrate_top1_turnovervolume = stock_concentrate_top1_turnovervolume.reset_index()
stock_concentrate_top1_turnovervolume = add_statistics(stock_concentrate_top1_turnovervolume,'top1_stock_turnovervolume')
stock_concentrate_top1_turnovervolume = hql.createDataFrame(stock_concentrate_top1_turnovervolume)
stock_concentrate_top1_turnovervolume.registerTempTable('stock_concentrate_top1_turnovervolume')

### 成交金额
stock_concentrate_top1_turnovervalue =  pd.pivot_table(stock_concentrate_top1_info,index=['customer_no','op_date'],columns=['date_rank'],values=['turnovervalue'])
stock_concentrate_top1_turnovervalue .columns = [x + 'top1_stock_turnovervalue' for x in workday_list ]
stock_concentrate_top1_turnovervalue = stock_concentrate_top1_turnovervalue.reset_index()
stock_concentrate_top1_turnovervalue = add_statistics(stock_concentrate_top1_turnovervalue,'top1_stock_turnovervalue')
stock_concentrate_top1_turnovervalue = hql.createDataFrame(stock_concentrate_top1_turnovervalue)
stock_concentrate_top1_turnovervalue.registerTempTable('stock_concentrate_top1_turnovervalue')
### 涨跌幅
stock_concentrate_top1_changepct =  pd.pivot_table(stock_concentrate_top1_info,index=['customer_no','op_date'],columns=['date_rank'],values=['changepct'])
stock_concentrate_top1_changepct .columns = [x + 'top1_stock_changepct' for x in workday_list ]
stock_concentrate_top1_changepct = stock_concentrate_top1_changepct.reset_index()
stock_concentrate_top1_changepct = add_statistics(stock_concentrate_top1_changepct,'top1_stock_changepct')
stock_concentrate_top1_changepct = hql.createDataFrame(stock_concentrate_top1_changepct)
stock_concentrate_top1_changepct.registerTempTable('stock_concentrate_top1_changepct')

### 换手率
stock_concentrate_top1_turnoverrate =  pd.pivot_table(stock_concentrate_top1_info,index=['customer_no','op_date'],columns=['date_rank'],values=['turnoverrate'])
stock_concentrate_top1_turnoverrate .columns = [x + 'top1_stock_turnoverrate' for x in workday_list ]
stock_concentrate_top1_turnoverrate = stock_concentrate_top1_turnoverrate.reset_index()
stock_concentrate_top1_turnoverrate = add_statistics(stock_concentrate_top1_turnoverrate,'top1_stock_turnoverrate')
stock_concentrate_top1_turnoverrate = hql.createDataFrame(stock_concentrate_top1_turnoverrate)
stock_concentrate_top1_turnoverrate.registerTempTable('stock_concentrate_top1_turnoverrate')


#近一天持股集中度TOP1股票的近一周融资融券情况
stock_concentrate_top1_financing = hql.sql('''
   select customer_no,op_date,secucode as stock_no,
          rzye,rzmre,rzche,rqyl,rqmcl,rqchl,
          row_number()over(partition by customer_no,op_date order by tradingdate  ) as date_rank
   from
   (select *
    from stock_concentrate_top1) aa 
   join
   (select *
   from ctprod.schema_fin_slo_stocks     
   )bb
   on aa.stock_no = bb.secucode
   and bb.tradingdate >= aa.op_before_week_date
   and bb.tradingdate <= aa.op_before_one_date
    
''').toPandas()

### 融资余额
stock_concentrate_top1_rzye =  pd.pivot_table(stock_concentrate_top1_financing,index=['customer_no','op_date'],columns=['date_rank'],values=['rzye'])
stock_concentrate_top1_rzye .columns = [x + 'top1_stock_rzye' for x in workday_list ]
stock_concentrate_top1_rzye = stock_concentrate_top1_rzye.reset_index()
stock_concentrate_top1_rzye = add_statistics(stock_concentrate_top1_rzye,'top1_stock_rzye')
stock_concentrate_top1_rzye = hql.createDataFrame(stock_concentrate_top1_rzye)
stock_concentrate_top1_rzye.registerTempTable('stock_concentrate_top1_rzye')

### 融资买入额
stock_concentrate_top1_rzmre =  pd.pivot_table(stock_concentrate_top1_financing,index=['customer_no','op_date'],columns=['date_rank'],values=['rzmre'])
stock_concentrate_top1_rzmre .columns = [x + 'top1_stock_rzmre' for x in workday_list ]
stock_concentrate_top1_rzmre = stock_concentrate_top1_rzmre.reset_index()
stock_concentrate_top1_rzmre = add_statistics(stock_concentrate_top1_rzmre,'top1_stock_rzmre')
stock_concentrate_top1_rzmre = hql.createDataFrame(stock_concentrate_top1_rzmre)
stock_concentrate_top1_rzmre.registerTempTable('stock_concentrate_top1_rzmre')

### 融资偿还额
stock_concentrate_top1_rzche =  pd.pivot_table(stock_concentrate_top1_financing,index=['customer_no','op_date'],columns=['date_rank'],values=['rzche'])
stock_concentrate_top1_rzche .columns = [x + 'top1_stock_rzche' for x in workday_list ]
stock_concentrate_top1_rzche = stock_concentrate_top1_rzche.reset_index()
stock_concentrate_top1_rzche = add_statistics(stock_concentrate_top1_rzche,'top1_stock_rzche')
stock_concentrate_top1_rzche = hql.createDataFrame(stock_concentrate_top1_rzche)
stock_concentrate_top1_rzche.registerTempTable('stock_concentrate_top1_rzche')

### 融券余量
stock_concentrate_top1_rqyl =  pd.pivot_table(stock_concentrate_top1_financing,index=['customer_no','op_date'],columns=['date_rank'],values=['rqyl'])
stock_concentrate_top1_rqyl .columns = [x + 'top1_stock_rqyl' for x in workday_list ]
stock_concentrate_top1_rqyl = stock_concentrate_top1_rqyl.reset_index()
stock_concentrate_top1_rqyl = add_statistics(stock_concentrate_top1_rqyl,'top1_stock_rqyl')
stock_concentrate_top1_rqyl = hql.createDataFrame(stock_concentrate_top1_rqyl)
stock_concentrate_top1_rqyl.registerTempTable('stock_concentrate_top1_rqyl')


### 融券卖出量
stock_concentrate_top1_rqmcl =  pd.pivot_table(stock_concentrate_top1_financing,index=['customer_no','op_date'],columns=['date_rank'],values=['rqmcl'])
stock_concentrate_top1_rqmcl .columns = [x + 'top1_stock_rqmcl' for x in workday_list ]
stock_concentrate_top1_rqmcl = stock_concentrate_top1_rqmcl.reset_index()
stock_concentrate_top1_rqmcl = add_statistics(stock_concentrate_top1_rqmcl,'top1_stock_rqmcl')
stock_concentrate_top1_rqmcl = hql.createDataFrame(stock_concentrate_top1_rqmcl)
stock_concentrate_top1_rqmcl.registerTempTable('stock_concentrate_top1_rqmcl')

### 融券偿还量
stock_concentrate_top1_rqchl =  pd.pivot_table(stock_concentrate_top1_financing,index=['customer_no','op_date'],columns=['date_rank'],values=['rqchl'])
stock_concentrate_top1_rqchl .columns = [x + 'top1_stock_rqchl' for x in workday_list ]
stock_concentrate_top1_rqchl = stock_concentrate_top1_rqchl.reset_index()
stock_concentrate_top1_rqchl = add_statistics(stock_concentrate_top1_rqchl,'top1_stock_rqchl')
stock_concentrate_top1_rqchl = hql.createDataFrame(stock_concentrate_top1_rqchl)
stock_concentrate_top1_rqchl.registerTempTable('stock_concentrate_top1_rqchl')


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
            select  a.customer_no ,
                    a.op_date ,  
                    null_stock_num,
                    diff_four_stock_num,
                    diff_three_stock_num,
                    diff_two_stock_num,
                    diff_one_stock_num,
                    mean_stock_num,
                    std_stock_num,
                    more_avg_num_stock_num,
                    diff_more_avg_num_stock_num,
                    null_stock_sum,
                    diff_four_stock_sum,
                    diff_three_stock_sum,
                    diff_two_stock_sum,
                    diff_one_stock_sum,
                    mean_stock_sum,
                    std_stock_sum,
                    more_avg_num_stock_sum,
                    diff_more_avg_num_stock_sum,
                    null_top1_stock_openprice,
                    diff_four_top1_stock_openprice,
                    diff_three_top1_stock_openprice,
                    diff_two_top1_stock_openprice,
                    diff_one_top1_stock_openprice,
                    mean_top1_stock_openprice,
                    std_top1_stock_openprice,
                    more_avg_num_top1_stock_openprice,
                    diff_more_avg_num_top1_stock_openprice,
                    null_top1_stock_closeprice,
                    diff_four_top1_stock_closeprice,
                    diff_three_top1_stock_closeprice,
                    diff_two_top1_stock_closeprice,
                    diff_one_top1_stock_closeprice,
                    mean_top1_stock_closeprice,
                    std_top1_stock_closeprice,
                    more_avg_num_top1_stock_closeprice,
                    diff_more_avg_num_top1_stock_closeprice,
                    null_top1_stock_highprice,
                    diff_four_top1_stock_highprice,
                    diff_three_top1_stock_highprice,
                    diff_two_top1_stock_highprice,
                    diff_one_top1_stock_highprice,
                    mean_top1_stock_highprice,
                    std_top1_stock_highprice,
                    more_avg_num_top1_stock_highprice,
                    diff_more_avg_num_top1_stock_highprice,
                    null_top1_stock_lowprice,
                    diff_four_top1_stock_lowprice,
                    diff_three_top1_stock_lowprice,
                    diff_two_top1_stock_lowprice,
                    diff_one_top1_stock_lowprice,
                    mean_top1_stock_lowprice,
                    std_top1_stock_lowprice,
                    more_avg_num_top1_stock_lowprice,
                    diff_more_avg_num_top1_stock_lowprice,
                    null_top1_stock_turnovervolume,
                    diff_four_top1_stock_turnovervolume,
                    diff_three_top1_stock_turnovervolume,
                    diff_two_top1_stock_turnovervolume,
                    diff_one_top1_stock_turnovervolume,
                    mean_top1_stock_turnovervolume,
                    std_top1_stock_turnovervolume,
                    more_avg_num_top1_stock_turnovervolume,
                    diff_more_avg_num_top1_stock_turnovervolume,
                    null_top1_stock_turnovervalue,
                    diff_four_top1_stock_turnovervalue,
                    diff_three_top1_stock_turnovervalue,
                    diff_two_top1_stock_turnovervalue,
                    diff_one_top1_stock_turnovervalue,
                    mean_top1_stock_turnovervalue,
                    std_top1_stock_turnovervalue,
                    more_avg_num_top1_stock_turnovervalue,
                    diff_more_avg_num_top1_stock_turnovervalue,
                    null_top1_stock_changepct,
                    diff_four_top1_stock_changepct,
                    diff_three_top1_stock_changepct,
                    diff_two_top1_stock_changepct,
                    diff_one_top1_stock_changepct,
                    mean_top1_stock_changepct,
                    std_top1_stock_changepct,
                    more_avg_num_top1_stock_changepct,
                    diff_more_avg_num_top1_stock_changepct,
                    null_top1_stock_turnoverrate,
                    diff_four_top1_stock_turnoverrate,
                    diff_three_top1_stock_turnoverrate,
                    diff_two_top1_stock_turnoverrate,
                    diff_one_top1_stock_turnoverrate,
                    mean_top1_stock_turnoverrate,
                    std_top1_stock_turnoverrate,
                    more_avg_num_top1_stock_turnoverrate,
                    diff_more_avg_num_top1_stock_turnoverrate,
                    null_top1_stock_rzye,
                    diff_four_top1_stock_rzye,
                    diff_three_top1_stock_rzye,
                    diff_two_top1_stock_rzye,
                    diff_one_top1_stock_rzye,
                    mean_top1_stock_rzye,
                    std_top1_stock_rzye,
                    more_avg_num_top1_stock_rzye,
                    diff_more_avg_num_top1_stock_rzye,
                    null_top1_stock_rzmre,
                    diff_four_top1_stock_rzmre,
                    diff_three_top1_stock_rzmre,
                    diff_two_top1_stock_rzmre,
                    diff_one_top1_stock_rzmre,
                    mean_top1_stock_rzmre,
                    std_top1_stock_rzmre,
                    more_avg_num_top1_stock_rzmre,
                    diff_more_avg_num_top1_stock_rzmre,
                    null_top1_stock_rzche,
                    diff_four_top1_stock_rzche,
                    diff_three_top1_stock_rzche,
                    diff_two_top1_stock_rzche,
                    diff_one_top1_stock_rzche,
                    mean_top1_stock_rzche,
                    std_top1_stock_rzche,
                    more_avg_num_top1_stock_rzche,
                    diff_more_avg_num_top1_stock_rzche,
                    null_top1_stock_rqyl,
                    diff_four_top1_stock_rqyl,
                    diff_three_top1_stock_rqyl,
                    diff_two_top1_stock_rqyl,
                    diff_one_top1_stock_rqyl,
                    mean_top1_stock_rqyl,
                    std_top1_stock_rqyl,
                    more_avg_num_top1_stock_rqyl,
                    diff_more_avg_num_top1_stock_rqyl,
                    null_top1_stock_rqmcl,
                    diff_four_top1_stock_rqmcl,
                    diff_three_top1_stock_rqmcl,
                    diff_two_top1_stock_rqmcl,
                    diff_one_top1_stock_rqmcl,
                    mean_top1_stock_rqmcl,
                    std_top1_stock_rqmcl,
                    more_avg_num_top1_stock_rqmcl,
                    diff_more_avg_num_top1_stock_rqmcl,
                    null_top1_stock_rqchl,
                    diff_four_top1_stock_rqchl,
                    diff_three_top1_stock_rqchl,
                    diff_two_top1_stock_rqchl,
                    diff_one_top1_stock_rqchl,
                    mean_top1_stock_rqchl,
                    std_top1_stock_rqchl,
                    more_avg_num_top1_stock_rqchl,
                    diff_more_avg_num_top1_stock_rqchl
                    '''+is_label+'''
            from stock_active_customer_label a 
            left join stock_num b 
            on a.customer_no =b.customer_no and a.op_date = b.op_date
            left join stock_sum c 
            on a.customer_no = c.customer_no and a.op_date = c.op_date
            left join stock_concentrate_top1_openprice d 
            on a.customer_no = d.customer_no and a.op_date = d.op_date
            left join stock_concentrate_top1_closeprice e 
            on a.customer_no = e.customer_no and a.op_date = e.op_date 
            left join stock_concentrate_top1_highprice f 
            on a.customer_no = f.customer_no and a.op_date = f.op_date 
            left join stock_concentrate_top1_lowprice g 
            on a.customer_no = g.customer_no and a.op_date = g.op_date 
            left join stock_concentrate_top1_turnovervolume h
            on a.customer_no = h.customer_no and a.op_date = h.op_date 
            left join stock_concentrate_top1_turnovervalue i
            on a.customer_no = i.customer_no and a.op_date = i.op_date
            left join stock_concentrate_top1_changepct j
            on a.customer_no = j.customer_no and a.op_date = j.op_date
            left join stock_concentrate_top1_turnoverrate k
            on a.customer_no = k.customer_no and a.op_date = k.op_date
            left join stock_concentrate_top1_rzye l
            on a.customer_no = l.customer_no and a.op_date = l.op_date
            left join stock_concentrate_top1_rzmre m
            on a.customer_no = m.customer_no and a.op_date = m.op_date
            left join stock_concentrate_top1_rzche n
            on a.customer_no = n.customer_no and a.op_date = n.op_date
            left join stock_concentrate_top1_rqyl o
            on a.customer_no = o.customer_no and a.op_date = o.op_date
            left join stock_concentrate_top1_rqmcl p
            on a.customer_no = p.customer_no and a.op_date = p.op_date
            left join stock_concentrate_top1_rqchl q
            on a.customer_no = q.customer_no and a.op_date = q.op_date


        ''')
    print('write cust_mining2.' +save_table + run_date + ' to hive Successfully')
except:
    print('write cust_mining2.'+ save_table + run_date + ' to hive Failed !!!')

sc.stop()


