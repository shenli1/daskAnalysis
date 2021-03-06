import matplotlib.pyplot as plt
import pandas as pd
from optbinning import BinningProcess
from optbinning import ContinuousOptimalBinning, OptimalBinning
from optbinning import Scorecard
from sklearn.linear_model import HuberRegressor

from model.backtest import runBackTest


def isCategray(df):
    valueCounts = len(pd.unique(df))
    if valueCounts < 10:
        return True
    return False


def autoBinning(orginDf, yCol, isContinue=True):
    features = []
    toDropColumns = []
    for col in orginDf.columns:
        # print(modelInputs[col].dtype.kind)
        if orginDf[col].dtype.kind == 'f' or orginDf[col].dtype.kind == 'i':
            continue
        toDropColumns.append(col)

    print(toDropColumns)
    df = orginDf.drop(columns=toDropColumns)
    ivList = []
    for col in df.columns:
        try:
            print(col)
            if (col == yCol):
                continue

            x = df[col].values
            y = df[yCol].values
            dtype = "numerical"
            if isCategray(df[col]):
                dtype = "categorical"
            optb = OptimalBinning(name=col, dtype=dtype, solver="cp")
            if isContinue:
                optb = ContinuousOptimalBinning(name=col, dtype=dtype,monotonic_trend='auto_asc_desc')
            optb.fit(x, y)
            # print(optb.status)
            binning_table = optb.binning_table
            binning_result = binning_table.build()
            print(binning_result)
            iv = binning_table.iv
            if iv > 0.02:
                ivList.append([col, iv])
                features.append(col)
                runBackTest(orginDf, col, optb.splits)
            print(iv)
        except Exception as e:
            print(e)

    ivList.sort(key=lambda x: x[1], reverse=True)
    print(ivList)
    return features


def buildScoreCard(df, features, labelCol):
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
