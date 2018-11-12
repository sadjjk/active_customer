# coding:utf-8
from pyspark import SparkConf, SparkContext, HiveContext
import pandas as pd
import datetime
import sys 

conf = SparkConf()
sc = SparkContext()
hql = HiveContext(sc)


def get_save_table(flag):
    if int(flag) == 0:
        save_table = 'c6_company_feature_train'
    else:
        save_table = 'c6_company_feature_test'
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




top1_company_info = hql.sql('''
    select customer_no,op_date,stock_no,
            top1_basiceps,
            top1_netassetps,
            top1_cashflowps,
            top1_netprofit,
            top1_pe,
            top1_pb,
            top1_roe,
            top1_roa,
            top1_grossincomeratio,
            top1_operatingrevenuegrowrate,
            top1_netprofitgrowrate,
            top1_netassetgrowrate,
            top1_basicepsyoy,
            top1_operatingnitotp,
            top1_opercashintoasset,
            top1_currentratio
    from 
        (select customer_no,op_date,stock_no,
            basiceps as top1_basiceps,
            netassetps as top1_netassetps,
            cashflowps as top1_cashflowps,
            netprofit as top1_netprofit,
            pe as top1_pe,
            pb as top1_pb,
            roe as top1_roe,
            roa as top1_roa,
            grossincomeratio as top1_grossincomeratio,
            operatingrevenuegrowrate as top1_operatingrevenuegrowrate,
            netprofitgrowrate as top1_netprofitgrowrate,
            netassetgrowrate as top1_netassetgrowrate,
            basicepsyoy as top1_basicepsyoy,
            operatingnitotp as top1_operatingnitotp,
            opercashintoasset as top1_opercashintoasset,
            currentratio as top1_currentratio,tradingdate,
            rank()over(partition by customer_no,op_date order by tradingdate desc) as date_rank
            from 
            (select *
            from stock_concentrate_top1 )a 
            join
            (select *
            from ctprod.schema_company_fundamentals)b 
        on a.stock_no = b.secucode
        and b.tradingdate>= a.op_before_week_date
        and b.tradingdate <= a.op_before_one_date)aaa
    where date_rank = 1

    ''')
top1_company_info.registerTempTable('top1_company_info')


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

top2_company_info = hql.sql('''
    select customer_no,op_date,stock_no ,
            top2_basiceps,
            top2_netassetps,
            top2_cashflowps,
            top2_netprofit,
            top2_pe,
            top2_pb,
            top2_roe,
            top2_roa,
            top2_grossincomeratio,
            top2_operatingrevenuegrowrate,
            top2_netprofitgrowrate,
            top2_netassetgrowrate,
            top2_basicepsyoy,
            top2_operatingnitotp,
            top2_opercashintoasset,
            top2_currentratio
    from 
        (select customer_no,op_date,stock_no ,
            basiceps as top2_basiceps,
            netassetps as top2_netassetps,
            cashflowps as top2_cashflowps,
            netprofit as top2_netprofit,
            pe as top2_pe,
            pb as top2_pb,
            roe as top2_roe,
            roa as top2_roa,
            grossincomeratio as top2_grossincomeratio,
            operatingrevenuegrowrate as top2_operatingrevenuegrowrate,
            netprofitgrowrate as top2_netprofitgrowrate,
            netassetgrowrate as top2_netassetgrowrate,
            basicepsyoy as top2_basicepsyoy,
            operatingnitotp as top2_operatingnitotp,
            opercashintoasset as top2_opercashintoasset,
            currentratio as top2_currentratio,tradingdate,
                rank()over(partition by customer_no,op_date order by tradingdate desc) as date_rank
            from 
            (select *
            from stock_concentrate_top2 )a 
            join
            (select *
            from ctprod.schema_company_fundamentals)b 
        on a.stock_no = b.secucode
        and b.tradingdate>= a.op_before_week_date
        and b.tradingdate <= a.op_before_one_date)aaa
    where date_rank = 2

    ''')
top2_company_info.registerTempTable('top2_company_info')


try: 
    hql.sql('''
            insert overwrite table cust_mining2.'''+save_table+'''
                partition (part_date=''' + run_date + ''')
                select  a.customer_no,
                        a.op_date,
                        top1_basiceps,
                        top1_netassetps,
                        top1_cashflowps,
                        top1_netprofit,
                        top1_pe,
                        top1_pb,
                        top1_roe,
                        top1_roa,
                        top1_grossincomeratio,
                        top1_operatingrevenuegrowrate,
                        top1_netprofitgrowrate,
                        top1_netassetgrowrate,
                        top1_basicepsyoy,
                        top1_operatingnitotp,
                        top1_opercashintoasset,
                        top1_currentratio,
                        top2_basiceps,
                        top2_netassetps,
                        top2_cashflowps,
                        top2_netprofit,
                        top2_pe,
                        top2_pb,
                        top2_roe,
                        top2_roa,
                        top2_grossincomeratio,
                        top2_operatingrevenuegrowrate,
                        top2_netprofitgrowrate,
                        top2_netassetgrowrate,
                        top2_basicepsyoy,
                        top2_operatingnitotp,
                        top2_opercashintoasset,
                        top2_currentratio
                        '''+is_label+'''
                from stock_active_customer_label a 
                left join top1_company_info b 
                on a.customer_no = b.customer_no
                and a.op_date = b.op_date
                left join top2_company_info c  
                on a.customer_no = c.customer_no
                and a.op_date = c.op_date  
               

            ''')

    print('write cust_mining.' +save_table + run_date + ' to hive Successfully')
except:
    print('write cust_mining.'+ save_table + run_date + ' to hive Failed !!!')

sc.stop()
