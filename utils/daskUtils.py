import dask.dataframe as dd
from utils.timeUtils import time

def rebuildCode(df):
    if 'code' in df.columns:
        df['code'] = df['code'].apply(lambda x:x[0:6])
        return df
    for other in ['jq_code','stock_id']:
        if other in df.columns:
            df['code'] = df[other].apply(lambda x:x[0:6])
    return df

@time
def buildIndex(df):
    if not isinstance(df,dd.DataFrame):
        df = dd.from_pandas(df,10)
    if 'id' not in df.columns:
        df['id'] = df.apply(lambda x: '%s_%s' % (x['code'],x['trade_date']),axis=1,meta=('id','U32'))
    df.set_index('id')
    return df

@time
def joinFeature(sample,features,labelCol):
    assert isinstance(sample,dd.DataFrame)
    sample.rename(columns={labelCol:'label'})
    result = sample.merge(features,how='left',suffixes=('_l','_r'))
    return result

@time
def getJoinFeatureDf(sample,features,labelCol):
    return joinFeature(sample,features,labelCol).compute()

