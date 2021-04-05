from stragety.chooseStock import chooseStock
from data.fileReader import getDfFromParquet
from utils.daskUtils import joinFeature,getJoinFeatureDf
import dask.dataframe as dd
from utils.timeUtils import time
from dask.distributed import Client
from model.autoScoreCard import autoBinning
import pandas as pd
from datetime import datetime

def compareResult(db,table,trade_date,leftCol,rightCol):
    df1 = getDfFromParquet(db,table).compute()
    df1 = df1.reset_index(drop=True)
    df1 = df1.set_index('code')
    df1 = df1[(df1['trade_date'] == datetime.strptime(trade_date,'%Y-%m-%d').date())]
    df1 = df1[[leftCol]]
    print(df1)
    df2 = chooseStock(trade_date)
    df2 = df2.reset_index(drop=True)
    df2['code'] = df2['stock_id']
    df2 = df2.set_index('code')
    df2 = df2[[rightCol]]
    print(df2)
    result = df1.join(df2,how='outer',rsuffix='_')
    result = result.fillna(0)
    if rightCol == leftCol:
        rightCol = rightCol + '_'
    result = result[abs(result[leftCol] - result[rightCol]) > 0.0001]
    print(result)
    return result

if __name__ == '__main__':
    result = compareResult('joinquant','jq_stock_hk_hold','2017-07-10','share_number','share_number')

    print(result)
