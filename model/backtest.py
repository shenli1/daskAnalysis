import alphalens_adj as alphalens
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
    forwardRetrun = pd.DataFrame(df[profitCols] / 100 - 0.002,index=df.index)
    forwardRetrun = forwardRetrun.rename(columns={profitCols:'1D'})
    factorData = alphalens.utils.get_clean_factor(factors,forwardRetrun,quantiles=None,bins=bins,max_loss=1)

    '''
    binReturn,binStd = alphalens.performance.mean_return_by_quantile(factorData,by_date=True,demeaned=False)
    binReturn = binReturn['1D']
    ret_wide = binReturn.unstack('factor_quantile')
    cum_ret = ret_wide.apply(alphalens.performance.cumulative_returns)
    maxCumReturn = -1000
    maxCumCol = None
    for x in cum_ret.columns:
        tmp = cum_ret[x].values[-1]
        if tmp > maxCumReturn:
            maxCumCol = x
            maxCumReturn = tmp
    maxReturnSeries = cum_ret[maxCumCol]
    alphalens.plotting.plot_cumulative_returns(ret_wide[maxCumCol],period="1D",title='max cumsum bin')
    '''

    alphalens.tears.create_full_tear_sheet(factorData,long_short=False)

