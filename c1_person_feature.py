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
        save_table = 'c1_person_feature_train'
    else:
        save_table = 'c1_person_feature_test'
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



person_info = hql.sql('''
    select customer_no,customer_gender,
       substr(CURRENT_DATE, 0,4) - substr(open_date, 0,4) as open_age ,
       degree_code,profession_code,
       substr(CURRENT_DATE, 0,4) - substr(birthday, 0,4) as age ,
       income,
       industry_type,aml_risk_level,
       corp_risk_level
    from ctprod.dim_customer
    ''')
person_info.registerTempTable('person_info')



try: 
    hql.sql('''
            insert overwrite table cust_mining2.'''+save_table+'''
                partition (part_date=''' + run_date + ''')
                select  a.customer_no ,
                        a.op_date  ,
                        customer_gender,
                        open_age,
                        degree_code,  
                        profession_code,
                        age,
                        income,
                        industry_type,
                        aml_risk_level,
                        corp_risk_level
                        '''+is_label+'''
                from stock_active_customer_label a 
                left join person_info b 
                on a.customer_no = b.customer_no 
               

            ''')

    print('write cust_mining.' +save_table + run_date + ' to hive Successfully')
except:
    print('write cust_mining.'+ save_table + run_date + ' to hive Failed !!!')

sc.stop()





