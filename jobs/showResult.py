from data.fileReader import getDfFromParquet
from utils.daskUtils import joinFeature,getJoinFeatureDf
import dask.dataframe as dd
from utils.timeUtils import time
from dask.distributed import Client
from model.autoScoreCard import autoBinning
import pandas as pd


pd.set_option('display.max_columns', 200)
pd.set_option('display.max_rows', 100)
pd.set_option('display.min_rows', 100)
pd.set_option('display.expand_frame_repr', True)

if __name__ == '__main__':

    client = Client()
    profit = getDfFromParquet('vars','var_profit')
    profit = profit[profit['future_open_1'] != profit['future_open_limit']][['code','trade_date','profit_1','future_open_1']]
    features = getDfFromParquet('vars','var_hk_hold')
    df = getJoinFeatureDf(features,profit,'profit_1')
    df = df[(df['share_ratio'] < 0.81) & (df['share_chg_ratio_3'] > 0.05) & (df['circ_share_chg_ratio_1'] > 0.21)]
    df = df[['trade_date','code','future_open_1','profit_1']]
    df = df.sort_values(by=['trade_date', 'code'])
    print(df)