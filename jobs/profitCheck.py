from data.fileReader import getDfFromParquet,saveDfToParquet
from dask.distributed import Client

if __name__ == '__main__':
    client = Client()

    df = getDfFromParquet('vars','var_profit')
    df = df[df['close'] != df['future_pre_close']]
    print(df.compute()[['close','future_pre_close']])