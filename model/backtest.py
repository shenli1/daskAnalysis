import alphalens
import dask.dataframe as dd
import pandas as pd
import numpy as np

def runBackTest(df,featureCol,splits,profitCols = 'profit_1'):
    bins = [-np.inf]
    for x in splits:
        bins.append(x)
    bins.append(np.inf)
    if isinstance(df,dd.DataFrame):
        df = df.compute()

    assert isinstance(df,pd.DataFrame)
    df = df.rename(columns={'trade_date':'date','code':'asset'})
    df = df.set_index(['date','asset'])
    factors = pd.Series(df[featureCol],index=df.index)
    forwardRetrun = pd.DataFrame(df[profitCols] / 100 - 0.001,index=df.index)
    forwardRetrun = forwardRetrun.rename(columns={profitCols:'1D'})
    factorData = alphalens.utils.get_clean_factor(factors,forwardRetrun,quantiles=None,bins=bins,max_loss=1)
    alphalens.tears.create_full_tear_sheet(factorData,long_short=False)