from data.fileReader import getDfFromParquet
from utils.daskUtils import joinFeature,getJoinFeatureDf
import dask.dataframe as dd
from utils.timeUtils import time
from dask.distributed import Client
from model.autoScoreCard import autoBinning

if __name__ == '__main__':
    client = Client()
    profit = getDfFromParquet('vars','var_profit')
    profit = profit[profit['open'] != profit['high_limit']][['code','trade_date','profit_1']]
    features = getDfFromParquet('vars','var_hk_hold')
    result = getJoinFeatureDf(features,profit,'profit_1')
    result = result[result['paused'] == 0]
    result = result[result['circ_share_chg_ratio_3'] >= 0.17]
    selectFeatures = autoBinning(result,'profit_1')

