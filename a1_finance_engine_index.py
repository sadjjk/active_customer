# coding:utf-8
from __future__ import division
from pyspark import SparkConf, SparkContext, HiveContext
from math import sqrt
import numpy as np
import pandas as pd
import datetime

def get_stock_data(stock_no,market_no,start_date,end_date):

#     stock_no:股票代码，如“000672”
#     market_no:指数代码 作为基准线  如“000001” 
#         可供选择指数  000001  上证指数
#                       399001  深圳指数
#                       399005  中小板指数
#                       399006  创业板指数
                      
#     start_date:开始日期 如：2018-01-01
#     end_date:结束日期 如：2018-01-10
    
    conf = SparkConf()
    sc = SparkContext()
    hql = HiveContext(sc)

    stock_info = hql.sql('''
        select tradingdate as date,secucode as stock_no,changepct as change,closeprice as close_price
        from ctprod.schema_stock_market
    ''').toPandas()
    stock_info['date'] = pd.to_datetime(stock_info['date'])

    market_info = hql.sql('''
        select tradingdate as date,secucode as market_no,changepct as change,closeprice as close_price
    from ctprod.schema_grail_market
    ''').toPandas()
    market_info['date'] = pd.to_datetime(market_info['date'])

    sc.stop()

    stock_data = stock_info[stock_info['stock_no'] == stock_no ]
    benchmark_data = market_info[market_info['market_no'] == market_no ]

    date = pd.date_range(start_date,end_date)

    # 理应选择后复价 比较科学 暂无 前复权价代替
    stock_data = stock_data.loc[stock_data['date'].isin(date),['date','close_price','change']] 
    stock_data.sort_values(by='date',inplace = True)

    date_list = list(stock_data['date'])
    benchmark_data = benchmark_data.loc[benchmark_data['date'].isin(date_list),['date','close_price','change']]
    benchmark_data.sort_values(by='date',inplace = True)

    date_line = list(benchmark_data['date'].dt.strftime('%Y-%m-%d')) #日期序列
    capital_line = list(stock_data['close_price']) #股票价格序列
    return_line = list(stock_data['change']) #股票变化率序列
    indexreturn_line = list(benchmark_data['change']) #基准线指数变化率序列
    index_line = list(benchmark_data['close_price']) #基准线指数价格序列
    
    return date_line,capital_line,return_line,index_line,indexreturn_line



#计算年化收益率
def annual_return(date_line,capital_line):    
#     date_line : 日期序列
#     capital_line:股票变化率序列
#     return:在指定期间内的年华收益率

    df = pd.DataFrame({'date':date_line,'capital':capital_line})
    df.sort_values(by = 'date',inplace = True)
    
    annual = pow(df.loc[len(df.index) -1 ,'capital'] / df.loc[0,'capital'],250/df.shape[0]) -1
    
    return annual 

#计算最大回撤
def max_drawdown(date_line,capital_line):
    df = pd.DataFrame({'date':date_line,'capital':capital_line})
    df.sort_values(by = 'date',inplace=True)
    
    df['max'] = df['capital'].expanding().max()  ## 计算当日股票价格之前的最大价
    df['retreat'] =  df['capital'] / df['max'] -1 
    
    max_retreat = df.sort_values(by = 'retreat').iloc[0][['retreat']].values[0] 
    
    return max_retreat    

# 计算平均涨幅
def average_change(date_line,return_line):

    df = pd.DataFrame({'date':date_line,'rtn':return_line})
    avg = df['rtn'].mean()
    
    return avg


# 计算上涨概率
def prob_up(date_line, return_line):

    df = pd.DataFrame({'date': date_line, 'rtn': return_line})
    df.loc[df['rtn'] > 0, 'rtn'] = 1  # 收益率大于0的记为1
    df.loc[df['rtn'] <= 0, 'rtn'] = 0  # 收益率小于等于0的记为0
    # 统计1和0各出现的次数
    count = df['rtn'].value_counts()
    p_up = count.loc[1] / len(df.index)
    
    return p_up    


    # 计算最大连续上涨天数和最大连续下跌天数
def max_successive_up(date_line, return_line):

    df = pd.DataFrame({'date': date_line, 'rtn': return_line})
    # 新建一个全为空值的series,并作为dataframe新的一列
    s = pd.Series(np.nan, index=df.index)
    s.name = 'up'
    df = pd.concat([df, s], axis=1)

    # 当收益率大于0时，up取1，小于0时，up取0，等于0时采用前向差值
    df.loc[df['rtn'] > 0, 'up'] = 1
    df.loc[df['rtn'] < 0, 'up'] = 0
    df['up'].fillna(method='ffill', inplace=True)

    # 根据up这一列计算到某天为止连续上涨下跌的天数
    rtn_list = list(df['up'])
    successive_up_list = []
    num = 1
    for i in range(len(rtn_list)):
        if i == 0:
            successive_up_list.append(num)
        else:
            if (rtn_list[i] == rtn_list[i - 1] == 1) or (rtn_list[i] == rtn_list[i - 1] == 0):
                num += 1
            else:
                num = 1
            successive_up_list.append(num)
    # 将计算结果赋给新的一列'successive_up'
    df['successive_up'] = successive_up_list
    # 分别在上涨和下跌的两个dataframe里按照'successive_up'的值排序并取最大值
    max_successive_up = df[df['up'] == 1].sort_values(by='successive_up', ascending=False)['successive_up'].iloc[0]
    max_successive_down = df[df['up'] == 0].sort_values(by='successive_up', ascending=False)['successive_up'].iloc[0]
    
    return max_successive_up,max_successive_down

# 计算收益波动率的函数
def volatility(date_line, return_line):
   
    df = pd.DataFrame({'date': date_line, 'rtn': return_line})
    # 计算波动率
    vol = df['rtn'].std() * sqrt(250)
    
    return vol


# 计算贝塔的函数
def beta(date_line, return_line, indexreturn_line):

    df = pd.DataFrame({'date': date_line, 'rtn': return_line, 'benchmark_rtn': indexreturn_line})
    # 账户收益和基准收益的协方差除以基准收益的方差
    b = df['rtn'].cov(df['benchmark_rtn']) / df['benchmark_rtn'].var()
    
    return b


# 计算alpha的函数
def alpha(date_line, capital_line, index_line, return_line, indexreturn_line):

    # 将数据序列合并成dataframe并按日期排序
    df = pd.DataFrame({'date': date_line, 'capital': capital_line, 'benchmark': index_line, 'rtn': return_line,
                       'benchmark_rtn': indexreturn_line})
    df.sort_values(by='date', inplace=True)
    df.reset_index(drop=True, inplace=True)
    rng = pd.period_range(df['date'].iloc[0], df['date'].iloc[-1], freq='D')
    rf = 0.0284  # 无风险利率取10年期国债的到期年化收益率

    annual_stock = pow(df.loc[len(df.index) - 1, 'capital'] / df.loc[0, 'capital'], 250 / len(rng)) - 1  # 账户年化收益
    annual_index = pow(df.loc[len(df.index) - 1, 'benchmark'] / df.loc[0, 'benchmark'], 250 / len(rng)) - 1  # 基准年化收益

    beta = df['rtn'].cov(df['benchmark_rtn']) / df['benchmark_rtn'].var()  # 计算贝塔值
    a = (annual_stock - rf) - beta * (annual_index - rf)  # 计算alpha值
   
    return a 

# 计算夏普比函数
def sharpe_ratio(date_line, capital_line, return_line):

    from math import sqrt
    # 将数据序列合并为一个dataframe并按日期排序
    df = pd.DataFrame({'date': date_line, 'capital': capital_line, 'rtn': return_line})
    df.sort_values(by='date', inplace=True)
    df.reset_index(drop=True, inplace=True)
    rng = pd.period_range(df['date'].iloc[0], df['date'].iloc[-1], freq='D')
    rf = 0.0284  # 无风险利率取10年期国债的到期年化收益率
    # 账户年化收益
    annual_stock = pow(df.loc[len(df.index) - 1, 'capital'] / df.loc[0, 'capital'], 250 / len(rng)) - 1
    # 计算收益波动率
    volatility = df['rtn'].std() * sqrt(250)
    # 计算夏普比
    sharpe = (annual_stock - rf) / volatility
    
    return sharpe

# 计算信息比率函数
def info_ratio(date_line, return_line, indexreturn_line):

    df = pd.DataFrame({'date': date_line, 'rtn': return_line, 'benchmark_rtn': indexreturn_line})
    df['diff'] = df['rtn'] - df['benchmark_rtn']
    annual_mean = df['diff'].mean() * 250
    annual_std = df['diff'].std() * sqrt(250)
    info = annual_mean / annual_std
    return info


def get_finance_index(end_date,stock_no):

    start_date = (datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
    
    # date_line,capital_line,return_line,index_line,indexreturn_line =  get_stock_data('601878','000001','2017-01-01','2017-12-31')
    date_line,capital_line,return_line,index_line,indexreturn_line =  get_stock_data(stock_no,'000001',start_date,end_date)
    

    if date_line == []:
        annual =  None
        max_retreat = None

    # 年化收益率
    annual =  annual_return(date_line, capital_line)
    # 最大回撤
    max_retreat =  max_drawdown(date_line, capital_line)
    # 平均涨幅
    avg = average_change(date_line, return_line)
    # 上涨概率
    p_up = prob_up(date_line, return_line)
    # 最大连续上涨天数和最大连续下跌天数
    max_successive_up,max_successive_down = max_successive_up(date_line, return_line)
    # 收益波动率
    vol = volatility(date_line, return_line)
    # beta值
    b = beta(date_line, return_line, indexreturn_line)
    # alpha值
    a = alpha(date_line, capital_line, index_line, return_line, indexreturn_line)
    # 夏普比率
    sharpe = sharpe_ratio(date_line, capital_line, return_line)
    # 信息比率
    info = info_ratio(date_line, return_line, indexreturn_line)