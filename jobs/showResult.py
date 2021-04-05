from data.fileReader import getDfFromParquet
from utils.daskUtils import joinFeature,getJoinFeatureDf
import dask.dataframe as dd
from utils.timeUtils import time
from dask.distributed import Client
from model.autoScoreCard import autoBinning
import pandas as pd

if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', 100)
    pd.set_option('display.width', 1000)

    client = Client()
    profit = getDfFromParquet('vars','var_profit')
    profit = profit[profit['future_open_1'] != profit['future_open_limit']][['code','trade_date','profit_1','future_open_1']]
    features = getDfFromParquet('vars','var_hk_hold')
    df = getJoinFeatureDf(features,profit,'profit_1')
    df = df[(df['share_chg_ratio_1'] > 0.06) & (df['share_chg_7_rank'] < 16.5)]
    df = df[['trade_date','code','future_open_1','profit_1','share_chg_ratio_1','share_chg_30_rank','market_cap','share_chg_30','share_number','pre_share_num_1','pre_share_num_30']]
    df = df.sort_values(by=['trade_date', 'code'])
    print(df.head(100))