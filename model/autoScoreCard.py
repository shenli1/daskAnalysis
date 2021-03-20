from optbinning import ContinuousOptimalBinning,OptimalBinning
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

from sklearn.linear_model import HuberRegressor

from optbinning import BinningProcess
from optbinning import Scorecard

def isCategray(df):
    valueCounts = len(pd.unique(df))
    if valueCounts < 10:
        return True
    return False

def autoBinning(df,yCol,isContinue = True):
    features = []
    toDropColumns = []
    for col in df.columns:
        # print(modelInputs[col].dtype.kind)
        if df[col].dtype.kind == 'f' or df[col].dtype.kind == 'i':
            continue
        toDropColumns.append(col)

    print(toDropColumns)
    df = df.drop(columns=toDropColumns)
    ivList = []
    for col in df.columns:
        print(col)
        if(col == yCol):
            continue
        try:
            x = df[col].values
            y = df[yCol].values
            dtype = "numerical"
            if isCategray(df[col]):
                dtype = "categorical"
            optb = OptimalBinning(name=col, dtype=dtype, solver="cp")
            if isContinue:
                optb = ContinuousOptimalBinning(name=col, dtype=dtype)
            optb.fit(x, y)
            #print(optb.status)
            binning_table = optb.binning_table
            binning_result = binning_table.build()
            print(binning_result)
            iv = binning_table.iv
            if iv > 0.02:
                ivList.append([col,iv])
                features.append(col)
            print(iv)
        except Exception as e:
            print(e)
            pass
    ivList.sort(key=lambda x:x[1],reverse=True)
    print(ivList)
    return features

def buildScoreCard(df,features,labelCol):
    binning_process = BinningProcess(features)
    estimator = HuberRegressor(max_iter=200)
    scorecard = Scorecard(binning_process=binning_process, target=labelCol,
                          estimator=estimator, scaling_method=None,
                          scaling_method_params={"min": 0, "max": 100},
                          reverse_scorecard=True)
    scorecard.verbose = True
    scorecard.fit(df, check_input=False)
    scorecard.information(print_level=2)
    print(scorecard.table(style="summary"))
    score = scorecard.score(df)
    y_pred = scorecard.predict(df)
    plt.scatter(score, df[labelCol], alpha=0.01, label="Average profit")
    plt.plot(score, y_pred, label="Huber regression", linewidth=2, color="orange")
    plt.ylabel("Average profit value (unit=100,000)")
    plt.xlabel("Score")
    plt.legend()
    plt.show()



