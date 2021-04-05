from jqdatasdk import *

auth("13246765319", 'HuiHong1982128')

def getStockInfo(trade_date):
    df = get_all_securities(types=['stock'], date=trade_date)
    df['code'] = df.index
    df['trade_date'] = trade_date
    sz50 = get_index_stocks('000016.XSHG',date=trade_date)
    sh300 = get_index_stocks('000300.XSHG', date=trade_date)
    zz500 = get_index_stocks('000905.XSHG',date=trade_date)
    df['is_sz50'] = df['code'].apply(lambda x: x in sz50)
    df['is_sh300'] = df['code'].apply(lambda x: x in sh300)
    df['is_zz500'] = df['code'].apply(lambda x: x in zz500)
    df = buildIndex(df)
    return df

def getMtss(stockList,trade_date):
    df = get_mtss(stockList, start_date = trade_date, end_date = trade_date, fields=None)
    df = df.rename(columns={'sec_code':'code','date':'trade_date'})
    df = buildIndex(df)
    return df

def getHkStockHoldInfo(trade_date):
    df = finance.run_query(query(finance.STK_HK_HOLD_INFO).filter(finance.STK_HK_HOLD_INFO.day == trade_date,finance.STK_HK_HOLD_INFO.link_id.in_(['310001','310002'])))
    df = df.rename(columns={'day':'trade_date'})
    return df

def getMoneyFlow(stockList,trade_date):
    df = get_money_flow(stockList,start_date=trade_date, end_date=trade_date, fields=None)
    df = df.rename(columns={'sec_code':'code','date':'trade_date'})
    df = buildIndex(df)
    return df

def buildIndex(df):
    df['id'] = df.apply(lambda x: '%s_%s' % (x['code'],x['trade_date']),axis=1)
    df = df.reset_index(drop=True)
    df = df.set_index('id')
    return df


if __name__ == '__main__':
    trade_date = '2017-05-08'
    print(getStockInfo(trade_date))

    '''
    df = getHkStockHoldInfo(trade_date)
    stockList = df['code'].tolist()
    moneyFlow = getMoneyFlow(stockList,trade_date)
    print(df)
    print(moneyFlow)
    '''
