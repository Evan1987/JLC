
# coding: utf-8
import pandas as pd
import numpy as np
import time
import math
import re

path = "F:/Code projects/Python/Common/20170816user_regular_potential/"

# 0 数据读取并处理
user_premium = pd.read_csv(path+"user_premium.csv")
# 0.1 存量表的时间格式转换
f = lambda x: time.strftime("%Y-%m-%d", time.strptime(str(x), "%Y%m%d"))
user_premium.log_day = user_premium.log_day.apply(func=f).astype("datetime64[D]")
# 0.2 排序
user_premium = user_premium.sort_values(["userid", "log_day"], ascending=True).reset_index(drop=True)
# 0.3 获取待处理的用户
users = user_premium.userid.unique()

# func0: 日期数据补全函数，缺失的日期，premium_cur字段填为0
def completeDF(df,dateVar="log_day",addVar="premium_cur"):
    dateCol = df[dateVar]
    minDate,maxDate = min(dateCol), max(dateCol)
    if (maxDate-minDate).days+1 == len(dateCol):
        return df
    else:
        timeLine = np.arange(minDate, maxDate+np.timedelta64(1, "D"),
                             dtype='datetime64[D]')
        timeHoles = timeLine[np.where(np.isin(timeLine, dateCol) == False)[0]]
        dfAd = pd.DataFrame({dateVar: timeHoles, addVar: 0.0})
        df = df.append(dfAd).sort_values(dateVar).reset_index(drop=True)
    return df

# func1: 字符串正则匹配全量查找函数：在string中找到所有满足pattern的字符位置
def searchAll(string, pattern):
    x = re.search(string=string, pattern=pattern)
    start = 0
    startList = []
    endList = []
    while (x is not None) and len(string)>0:
        span = x.span()
        startList.append(span[0]+start)
        endList.append(span[1]+start)
        start = span[1]
        string = string[start:]
        x = re.search(string=string, pattern=pattern)
    return startList, endList

# func2: 本项目的最主要函数——潜力区间搜索函数：
# 采用二分法进行查找
def findAvailSec(df,
                 judgeVar="premium_cur",# 判断字段
                 dateVar="log_day",# 时间字段
                 minPremium=1000,# 最小水面高度
                 minStep=1000,# 水面变化的最小步长
                 minLen=30,# 满足条件的最小区间长度
                 maxStepNum=10):# 最大loop数
    # 正则pattern
    pattern = "1{"+str(minLen)+",}"
    # x：水面高度，从最小高度起始
    x = minPremium
    log_day = df[dateVar]
    judge = df[judgeVar]
    # 
    judgeVec = "".join(np.int_(judge>=x).astype(str))
    startList, endList = searchAll(judgeVec, pattern)
    # 如果在最小高度下有潜力，则进入二分法查找最大潜力
    if len(startList)>0:
        ###### 二分法初始设置
        finalStartList, finalEndList = startList, endList
        # 以当前高度为下限
        finalX = down = x
        # 以最大水面高度为上限
        up = max(judge)
        # 新测试高度选取
        newX = math.floor(0.5*(up+down)/minStep)*minStep
        # 变化步长及loop数
        step = abs(newX - x)
        x = newX
        stepNum = 1
        
        while stepNum <= maxStepNum and step >= minStep:
            judgeVec = "".join(np.int_(judge >= x).astype(str))
            startList,endList = searchAll(judgeVec, pattern)
            if len(startList) > 0:
                finalStartList, finalEndList = startList, endList
                finalX = down = x
                newX = math.floor(0.5*(up+down)/minStep)*minStep
                step = abs(newX - x)
                x = newX
            else:
                up = x
                newX = math.floor(0.5*(up+down)/minStep)*minStep
                step = abs(newX - x)
                x = newX
            stepNum += 1
        result = pd.DataFrame({"days": minLen,
                               "from": log_day.iloc[finalStartList].tolist(),
                               "to": log_day.iloc[list(np.array(finalEndList)+1-minLen)].tolist(),
                               "amount": finalX})
        return result
    # 如果在最小高度下无潜力，则不再搜索，返回空列表
    else:
        return []

# 1 主要处理部分
# 全部结果数据
result = pd.DataFrame(columns=["userid", "from", "to", "days", "amount"])
# 按用户逐个处理，相互独立
for user in users:
    userTmp = user_premium.loc[user_premium.userid==user]
    # 补全用户数据
    userTmp = completeDF(userTmp)
    # 该用户的结果数据
    resultTmpI = pd.DataFrame(columns=["userid", "from", "to", "days", "amount"])
    # 按加息计划长度分别处理，相互独立
    for j in range(1, 13):
        # 该用户该加息计划类型的结果数据
        resultTmpJ = findAvailSec(userTmp, minLen=30*j)
        if len(resultTmpJ) > 0:
            resultTmpJ["userid"] = user
            resultTmpI = pd.concat([resultTmpI, resultTmpJ], axis=0, ignore_index=True)
    if len(resultTmpI) > 0:
        result = pd.concat([result, resultTmpI], axis=0, ignore_index=True)


print(result)
