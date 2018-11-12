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

    save_table = 'c99_all_feature_train'
    tablename = 'stock_active_customer' 

    is_label = ',label'
    is_a_label = ',a.label'

else:
    table1 = 'c1_person_feature_test'
    table2 = 'c2_funds_feature_test'
    table3_1 = 'c3_top1_stock_feature_test'
    table3_2 = 'c3_top2_stock_feature_test'
    table4 = 'c4_industry_feature_test'
    table5 = 'c5_market_feature_test'
    table6 = 'c6_company_feature_test'

    save_table = 'c99_all_feature_test'
    tablename = 'stock_active_customer_pred' 

    is_label = ''
    is_a_label = ''


df = hql.sql('''
    select  *
    from cust_mining.'''+tablename+''' a 
    left join
    (select * from cust_mining.'''+table1+''' 
    where part_date = '''+run_date +''')b 
    on a.customer_no = b.customer_no and a.op_date = b.c1_op_date
    left join
    (select * from cust_mining.'''+table2+''' 
    where part_date = '''+run_date +''')c 
    on a.customer_no = c.c2_customer_no and a.op_date = c.c2_op_date
    left join
    (select * from cust_mining.'''+table3_1+''' 
    where part_date = '''+run_date +''')d1 
    on a.customer_no = d1.c3_top1_customer_no and a.op_date = d1.c3_top1_op_date
    left join
    (select * from cust_mining.'''+table3_2+''' 
    where part_date = '''+run_date +''')d2 
    on a.customer_no = d2.c3_top2_customer_no and a.op_date = d2.c3_top2_op_date
    left join
    (select * from cust_mining.'''+table4+''' 
    where part_date = '''+run_date +''')e 
    on a.customer_no = e.c4_customer_no and a.op_date = e.c4_op_date
    left join
    (select * from cust_mining.'''+table5+''' 
    where part_date = '''+run_date +''')f 
    on a.customer_no = f.c5_customer_no and a.op_date = f.c5_op_date
    left join
    (select * from cust_mining.'''+table6+''' 
    where part_date = '''+run_date +''')g 
    on a.customer_no = g.c6_customer_no and a.op_date = g.c6_op_date
    ''').toPandas()

df_part_date = df['part_date'][[0]]
df.drop('part_date',axis=1,inplace=True)
df = pd.concat([df,df_part_date],axis=1)
df = pd.DataFrame(df)
# df_id = df[['customer_no','op_date','part_date']]

# if int(flag) == 0:
#     df_label = df[['label']]
#     df.drop(['customer_no','op_date','label','part_date'],axis = 1,inplace=True)
# else:
#     df.drop(['customer_no','op_date','part_date'],axis = 1,inplace=True)


# #空值特征
# df['null'] = df.isnull().sum(axis = 1)
# df['null_level'] = df['null']
# df['null_level'][df.null  <= 5] = 1
# df['null_level'][(df.null > 5) & (df.null <=10)] = 2 
# df['null_level'][(df.null >10 ) & (df.null <= 15)] = 3
# df['null_level'][(df.null >15 ) & (df.null <= 20)] = 4
# df['null_level'][(df.null >20 ) & (df.null <= 30)] = 5
# df['null_level'][df.null >30 ] = 6



#构造近5天的特征工程
def diff_std_5(df,every_day_name):
    every_day_5_list = ['one_'+every_day_name,'two_'+every_day_name,
                     'three_'+every_day_name,'four_'+every_day_name,
                     'five_'+every_day_name]
    df_week = df[every_day_5_list]

    #空值特征
    df_null = pd.DataFrame(df_week.isnull().sum(axis = 1 ).tolist())
    df_null.columns = ['null_'+every_day_name]

    #差分特征
    df_diff =  df_week.diff(axis = 1)
    df_diff.columns = 'diff_'+ df_diff.columns


    #标准差特征
    df_std = df_week.std(axis = 1).reset_index()
    df_std.drop('index',axis=1,inplace=True)
    df_std.columns = ['std_' + every_day_name]

    #自定义特征(大于平均数的个数)
    df_more_avg_num = df_week[df_week.apply(lambda x: x >=np.mean(x) , axis=1)].notnull().sum(axis=1)
    df_more_avg_num.columns = ['more_avg_num_' + every_day_name]

    df_diff_more_avg_num = df_diff[df_diff.apply(lambda x: x >=np.mean(x) , axis=1)].notnull().sum(axis=1)
    df_diff_more_avg_num.columns = ['diff_more_avg_num_' + every_day_name]


    #合并
    df = pd.concat([df,df_null,df_diff,df_std,df_more_avg_num,df_diff_more_avg_num],axis=1)
    return df


# df = diff_std_5(df,'top1_stock_price_change_rate')
# df = diff_std_5(df,'top1_stock_avg_price_5ago')
# df = diff_std_5(df,'top1_stock_volume')
# df = diff_std_5(df,'top2_stock_volume')
# df = diff_std_5(df,'top2_stock_price_change_rate')
# df = diff_std_5(df,'top2_stock_avg_price_5ago')
df = diff_std_5(df,'stock_num')
df = diff_std_5(df,'stock_sum')
# df = diff_std_5(df,'sh_market_fin_balance')
# df = diff_std_5(df,'sh_market_fin_buy')
# df = diff_std_5(df,'sz_market_fin_balance')
# df = diff_std_5(df,'sz_market_fin_buy')
# df = diff_std_5(df,'deposit_rate')
# df = diff_std_5(df,'top1_industry_open_price')
# df = diff_std_5(df,'top1_industry_close_price')
# df = diff_std_5(df,'top1_industry_high_price')
# df = diff_std_5(df,'top1_industry_low_price')
# df = diff_std_5(df,'top1_industry_amount')
# df = diff_std_5(df,'top1_industry_volume')
# df = diff_std_5(df,'top1_industry_change_rate')
# df = diff_std_5(df,'top2_industry_open_price')
# df = diff_std_5(df,'top2_industry_close_price')
# df = diff_std_5(df,'top2_industry_high_price')
# df = diff_std_5(df,'top2_industry_low_price')
# df = diff_std_5(df,'top2_industry_amount')
# df = diff_std_5(df,'top2_industry_volume')
# df = diff_std_5(df,'top2_industry_change_rate')


# if int(flag) == 0:
#     df = pd.concat([df_id,df,df_label],axis = 1)
# else:
#     df = pd.concat([df_id,df],axis = 1)


spark_df = hql.createDataFrame(df)

try:
    if int(flag) == 0:
        spark_df.write.mode('overwrite').saveAsTable('cust_mining.c99_feature_engine_train_{}'.format(run_date))
        print('write cust_mining.c99_feature_engine_train_' + run_date + ' to hive Successfully')
    else:
        spark_df.write.mode('overwrite').saveAsTable('cust_mining.c99_feature_engine_test_{}'.format(run_date))
        print('write cust_mining.c99_feature_engine_test_' + run_date + ' to hive Successfully')
except:
    print('write ' + run_date + ' to hive Failed !!!')

sc.stop()