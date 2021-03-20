from data.mysqlReader import getDfFromTable,showTables,getDfFromSql
from data.fileReader import saveDfToParquet
from utils.timeUtils import time
from utils.daskUtils import buildIndex,rebuildCode

@time
def convertTableToParquet(db,table):
    df = getDfFromTable(db,table)
    df = rebuildCode(df)
    df = buildIndex(df)
    saveDfToParquet(df,db,table)

def convertDataBaseToParquet(db,tableList):
    for table in tableList:
        print('----------saving %s--------------' % (table))
        convertTableToParquet(db,table)

def convertSqlToParquet(db,sql,table):
    df = getDfFromSql(db,sql)
    df = rebuildCode(df)
    df = buildIndex(df)
    saveDfToParquet(df,db,table)

if __name__ == '__main__':
    convertTableToParquet('vars','var_hk_hold')