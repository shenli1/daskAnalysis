from data.mysqlReader import getDfFromSql,getDfFromTable
import dask.dataframe as dd
from utils.timeUtils import time
from utils.daskUtils import buildIndex
import pandas as pd
pd.set_option('display.max_columns', None)   # 显示完整的列
pd.set_option('display.max_rows', 10)  # 显示完整的行
pd.set_option('display.expand_frame_repr', False)  # 设置不折叠数据

basePath = 'E:\\data\\%s\\%s\\'


def getParquetPath(db,table):
    return basePath % (db,table)

@time
def getDfFromParquet(db,table):
    path = getParquetPath(db,table)
    result = dd.read_parquet(path)
    if 'id' in result.columns:
        result = result.set_index('id')
    return result

@time
def saveDfToParquet(df,db,table):
    #df = buildIndex(df)
    if isinstance(df,pd.DataFrame):
        df = dd.from_pandas(df,npartitions=10)
    path = getParquetPath(db,table)
    df.to_parquet(path)

if __name__ == '__main__':
    db = 'vars'
    table = 'var_hk_hold'
    path = getParquetPath(db,table)
    df = getDfFromParquet(db,table)
    print(df.count())

