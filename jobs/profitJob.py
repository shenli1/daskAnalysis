from data.fileReader import getDfFromParquet,saveDfToParquet
from dask.distributed import Client

if __name__ == '__main__':
    client = Client()

    df = getDfFromParquet('joinquant','jq_stock_quotation_daily')

    diff_list = [2,3,4,5,6,7]
    df = df.map_partitions(lambda x: x.sort_index())
    df['pre_code_30'] = df['code'].shift(-7)
    df['future_open_1'] = df['open'].shift(-1)
    df['future_open_limit'] = df['high_limit'].shift(-1)
    df['future_pre_close'] = df['pre_close'].shift(-1)
    # 前N天持仓,N天增仓
    for x in range(2,8):
        futureOpenCol = 'future_open_%d' % (x)
        df[futureOpenCol] = df['open'].shift(-x)
        profitCol = 'profit_%d' % (x-1)
        df[profitCol] = ( df[futureOpenCol] - df['future_open_1'] ) / df['future_open_1'] * 100

    df['is_open_limit'] = df.apply(lambda x: 1 if x['future_open_limit'] == x['future_open_1'] else 0,axis=1,meta=('is_open_limit','i4'))
    df = df[df['code'] == df['pre_code_30']]

    saveDfToParquet(df,'vars','var_profit')




