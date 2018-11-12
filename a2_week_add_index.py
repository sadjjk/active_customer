# coding:utf-8
import pandas as  pd
import numpy as np

#近一周的相关特征的泛化特征函数
def add_statistics(raw_df,every_day_name):
    workday_list = ['five_','four_','three_','two_','one_']
    df_week = raw_df[[x + every_day_name for x in workday_list ]]

    #空值特征
    df_null = pd.DataFrame(df_week.isnull().sum(axis = 1 ).tolist())
    df_null.columns = ['null_'+every_day_name]

    #差分特征
    df_diff =  df_week.diff(axis = 1)
    df_diff =  df_diff.dropna(how='all', axis=1)  
    df_diff.columns = 'diff_'+ df_diff.columns

    #近一周平均值
    df_mean =  df_week.mean(axis = 1).reset_index()
    df_mean.drop('index',axis=1,inplace=True)
    df_mean.columns = ['mean_' + every_day_name]

    #标准差特征
    df_std = df_week.std(axis = 1).reset_index()
    df_std.drop('index',axis=1,inplace=True)
    df_std.columns = ['std_' + every_day_name]

    #自定义特征(大于平均数的个数)
    df_more_avg_num = df_week[df_week.apply(lambda x: x >=np.mean(x) , axis=1)].notnull().sum(axis=1).reset_index()
    df_more_avg_num.drop('index',axis=1,inplace=True)
    df_more_avg_num.columns = ['more_avg_num_' + every_day_name]

    df_diff_more_avg_num = df_diff[df_diff.apply(lambda x: x >=np.mean(x) , axis=1)].notnull().sum(axis=1).reset_index()
    df_diff_more_avg_num.drop('index',axis=1,inplace=True)
    df_diff_more_avg_num.columns = ['diff_more_avg_num_' + every_day_name]


    #合并
    df = pd.concat([df_null,df_diff,df_mean,df_std,df_more_avg_num,df_diff_more_avg_num],axis=1)
    return df


