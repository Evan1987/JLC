
# coding: utf-8

import numpy as np
import pandas as pd
import scipy.stats as stats
import math

# 基础参数
daySpan = 5
conf_level = 0.97
p = daySpan/30
alpha = 0.7368063
trialsInMonth = 30
meanSuccess = trialsInMonth*p
sdSuccess = trialsInMonth*p*(1-p)**(0.5)
path = "F:/Code projects/Python/Common/20170825wage_invest_mode_recognition/"

invest_log = pd.read_csv(path+"validInvestLog.csv")
invest_log["date"] = invest_log["date"].astype("datetime64[D]")
invest_log = invest_log.sort_values(["userid", "date"], ascending=True)
timeCal = lambda x: x.year*100+x.month
invest_log["month"] = invest_log["date"].apply(timeCal)

invest_month_summary = invest_log.groupby(["userid", "month"], as_index=False).agg({"invest_amount": sum})

# 月份补全函数
def seqMonth(start, end, yearSpan=[2015, 2016, 2017], monthExcept=[1, 2, 10]):
    m = np.arange(1, 13)
    m = m[np.isin(m, monthExcept)==False]
    monthSeq = np.array([m+100*x for x in yearSpan]).ravel()
    return monthSeq[(monthSeq >= start) & (monthSeq <= end)]

# 时间集中度的假设检验
# 输出可信度
def timeConcentrate(x, df, monthSeq, r, N, prob = p):
    y = df.loc[(df.day >= x) & (df.day <= (x+4))].month.unique()
    judge = sum(np.isin(monthSeq, y)*r)
    return 1-stats.binom_test(np.floor(judge), N, p=prob, alternative="greater")

# 金额集中度的假设检验
# 输出金额时间集中度的可信度、平均金额和金额波动性评价分数
def moneyConcentrate(x, df, dfMonthSummary, r, N, trialsInMonth, mean, sd, conf_level):
    focus = df.loc[(df.day >= x) & (df.day <= (x+4))]
    focusLog = focus.groupby("month", as_index=False).agg({"invest_amount": sum}).\
        join(dfMonthSummary.set_index("month"), on="month", how="right", rsuffix="_total").\
        sort_values("month").reset_index(drop=True)
    focusLog.invest_amount = focusLog.invest_amount.fillna(0)
    focusLog["conf"] = stats.norm.cdf((focusLog.invest_amount*trialsInMonth/(focusLog.invest_amount_total+0.01)-mean)/sd)
    judge = sum(focusLog.conf*r)
    concentrateConf = round(stats.norm.cdf((judge-0.5*N)/(N/12)**0.5)*100, 2)
    
    selectAmount = focusLog.invest_amount[focusLog.conf >= conf_level]
    if len(selectAmount) < 3:
        meanAmount = 0.0
        stableConf = 0.0
    else:
        meanAmount = np.median(selectAmount)
        y = np.var(selectAmount)**0.5/np.mean(selectAmount)
        stableConf = round(120/(1+math.exp(8.047*(y-0.2))), 2)
    return x, concentrateConf, meanAmount, stableConf


# 辅助函数：识别连续数字并分组；返回分组信息
def findGroup(x):
    x = list(x)
    size = len(x)
    index = np.zeros(size)
    r = 0
    before = x[0]
    for i in range(1, size):
        now = x[i]
        if now-before == 1:
            index[i] = r
        else:
            r += 1
            index[i] = r
        before = now
    return index

users = invest_month_summary.userid.unique()
result = pd.DataFrame(columns=['start',
                               'concentrateConf',
                               'meanAmount',
                               'stableConf',
                               'userid',
                               'totalScore',
                               'end'])


# 对全部用户进行相同的挖掘处理
for user in users:
    # 用户基础数据
    user_invest_log = invest_log.loc[invest_log.userid == user, :]
    user_invest_summary = invest_month_summary.loc[invest_month_summary.userid == user, :]
    # 补齐用户投资月份
    minMonth = user_invest_summary.month.min()
    maxMonth = user_invest_summary.month.max()
    monthSeq = seqMonth(minMonth, maxMonth)
    fullMonth = pd.DataFrame({"month": monthSeq}).set_index("month")
    user_invest_summary = user_invest_summary.join(fullMonth,
                                                   on=["month"],
                                                   how="right").reset_index(drop=True)
    user_invest_summary.invest_amount = user_invest_summary.invest_amount.fillna(0)
    # 设定用户时间衰减参数
    N = len(monthSeq)
    rseq = np.array(alpha**(user_invest_summary.index))[::-1]
    r = rseq*N/sum(rseq)
    
    # 生成一系列待检验的时间区间
    minDay = user_invest_log.day.min()
    maxDay = user_invest_log.day.max()
    end = max(minDay, maxDay-(daySpan-1))
    timeLineStart = np.arange(minDay, end+1)
    
    # 检验1： 对这些时间区间进行时间集中度检验
    timeFocusJudge = np.array([timeConcentrate(x,
                                           df=user_invest_log,
                                           monthSeq=monthSeq,
                                           r=r,
                                           N=N,
                                           prob=p) for x in timeLineStart])
    # 通过时间集中度检验的时间区间
    validTimeLineStart = timeLineStart[timeFocusJudge >= conf_level]
    if len(validTimeLineStart) == 0:
        continue
    
    # 检验2： 进行金额集中度检验
    z=[moneyConcentrate(x,
                        df=user_invest_log,
                        dfMonthSummary=user_invest_summary,
                        r=r,
                        N=N,
                        trialsInMonth=trialsInMonth,
                        mean=meanSuccess,
                        sd=sdSuccess,
                        conf_level=conf_level) 
       for x in validTimeLineStart]
    tmp = pd.DataFrame(z, columns=["start", "concentrateConf", "meanAmount", "stableConf"])
    tmp = tmp.loc[(tmp.stableConf >= 70) & (tmp.concentrateConf >= 85)].sort_values("start")
    if len(tmp) == 0:
        continue
    else:
        # 补全字段，并筛除多余行
        tmp["userid"] = user
        tmp["totalScore"] = tmp.concentrateConf + tmp.stableConf
        tmp = tmp.sort_values("start")
        tmp["label"] = findGroup(tmp.start)
        tmp = tmp.sort_values(["label", "totalScore"], ascending=[True, False]).groupby("label").first()
        tmp["end"] = tmp.start+4
    result = pd.concat([result, tmp], axis=0, ignore_index=True)

result.to_csv(path+"result.csv", index=False)