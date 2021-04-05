from jqdatasdk import *
import pandas as pd

auth("13246765319", 'HuiHong1982128')

def chooseStock(date):
    tradeList = get_trade_days(end_date=date,count=31)
    #print(tradeList)
    pre_30 = tradeList[-8]
    pre_3 = tradeList[-4]
    pre_1 = tradeList[-2]
    now = tradeList[-1]

    df_30=finance.run_query(query(finance.STK_HK_HOLD_INFO).filter(finance.STK_HK_HOLD_INFO.day==pre_30 , finance.STK_HK_HOLD_INFO.link_id.in_(['310001','310002'])))
    df_30 = df_30[['code','name','share_number']]
    df_30 = df_30.set_index('code')
    df_3=finance.run_query(query(finance.STK_HK_HOLD_INFO).filter(finance.STK_HK_HOLD_INFO.day==pre_3 ,finance.STK_HK_HOLD_INFO.link_id.in_(['310001','310002'])))
    df_3 = df_3[['code','name','share_number']]
    df_3 = df_3.set_index('code')
    df_1=finance.run_query(query(finance.STK_HK_HOLD_INFO).filter(finance.STK_HK_HOLD_INFO.day==pre_1,finance.STK_HK_HOLD_INFO.link_id.in_(['310001','310002'])))
    df_1 = df_1[['code','name','share_number']]
    df_1 = df_1.set_index('code')
    df=finance.run_query(query(finance.STK_HK_HOLD_INFO).filter(finance.STK_HK_HOLD_INFO.day==now,finance.STK_HK_HOLD_INFO.link_id.in_(['310001','310002'])))
    df = df[(df['link_id'] == 310001) | (df['link_id'] == 310002)]
    df = df[['code','name','share_number','share_ratio']]
    df = df.set_index('code')
    df = df.join(df_1, rsuffix='_1')
    df = df.join(df_3, rsuffix='_3')
    df = df.join(df_3, rsuffix='_30')
    capDf = get_fundamentals(query(valuation),now)[['code','capitalization','circulating_cap','market_cap']]
    capDf = capDf.set_index('code')
    df = df.join(capDf)

    df['share_chg_30'] = df['share_number'] - df['share_number_30']
    df['share_chg_rank_30'] = df['share_chg_30'].rank(method='max', ascending=False)
    df['share_chg_ratio_1'] = (df['share_number'] - df['share_number_1']) / df['capitalization'] / 100
    df['cicr_share_chg_ratio_1'] = (df['share_number'] - df['share_number_1']) / df['circulating_cap'] / 100
    #print(df[['name','share_chg_rank_30','share_chg_ratio_1','market_cap','share_number','share_number_1','share_chg_30']])
    df['stock_id'] = df.index
    df['stock_id'] = df['stock_id'].apply(lambda x:x[0:6])

    #df = df[(df['share_chg_ratio_1'] > 0.06)]
    print(df)
    #result = df.index.tolist()
    return df

if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.expand_frame_repr', False)
    chooseStock('2017-05-09')