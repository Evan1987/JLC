
# coding: utf-8
# 基于利率的用户资产置换
import pandas as pd
import numpy as np
import math
import pymysql
from sqlalchemy import create_engine
from pandas.io.sql import read_sql_query
from operator import itemgetter

# 用户分产品利率现状输出函数
def getRateSummary(match_record, asset_data, regular_info, rate_dict):
    exp_current_rate = rate_dict.get("current")
    exp_tPlus_rate = rate_dict.get("tPlus")
    regular_info["value"] = regular_info.num * regular_info.amount
    regular_summary = regular_info.groupby(["userid"], as_index=False).agg({"value": sum, "amount": sum})
    regular_summary["num"] = round(regular_summary.value / (0.5 * regular_summary.amount)) * 0.5
    regular_summary = pd.merge(regular_summary[["userid", "num"]], regular_rate, on="num", how="left")

    match_record = pd.merge(match_record, asset_data[["asset_id", "rate"]], on="asset_id", how="left")
    match_record["daily_interest"] = match_record.rate / 36500 * match_record.amount
    match_record["isfix"] = 0
    type_dict = {1: "current", 2: "tPlus", 3: "regular"}
    match_summary = match_record.groupby(["userid", "type"], as_index=False).agg({"amount": sum, "daily_interest": sum})
    match_summary["general_rate"] = round(match_summary.daily_interest * 365 * 100 / match_summary.amount, 2)
    f = itemgetter(*match_summary.type)
    match_summary["type"] = list(f(type_dict))

    var = ["userid", "type"]
    regular_summary["type"] = "regular"
    regular_summary = regular_summary.set_index(var)
    regular_summary = regular_summary[["exp_rate"]]
    match_summary = match_summary.set_index(var)
    res = match_summary.loc[:, match_summary.columns.union(regular_summary.columns)]
    res.update(regular_summary)
    res.reset_index(inplace=True)
    res.loc[res.type == "current", "exp_rate"] = exp_current_rate
    res.loc[res.type == "tPlus", "exp_rate"] = exp_tPlus_rate
    res = res[np.isnan(res.exp_rate) == False]
    res["diff_rate"] = round(res.general_rate - res.exp_rate, 2)

    group = res.groupby("userid")
    user_summary = pd.DataFrame(
        {"general_rate": group.apply(lambda x: round(sum(x.daily_interest) * 365 * 100 / sum(x.amount), 2)),
         "exp_rate": group.apply(lambda x: round(sum(x.amount * x.exp_rate) / sum(x.amount), 2)),
         "type": "total"}).reset_index()
    user_summary["type"] = "total"
    user_summary["diff_rate"] = round(user_summary.general_rate - user_summary.exp_rate, 2)

    res2 = pd.concat([res.loc[:, user_summary.columns].reset_index(drop=True), user_summary.reset_index(drop=True)],
                     axis=0)
    result = res2.pivot_table(index="userid", columns="type").reset_index()
    return result

# 向上调整队列选择函数
def wannaUpSelect(user_rate_summary,
                  match_record,
                  asset_data,
                  lowDiffRegRate = -0.5,
                  lowDiffTotalRate = -0.3,
                  lowAmount = 100,
                  account_type = 3):
    r1 = user_rate_summary[("diff_rate", "regular")]<=lowDiffRegRate
    r2 = user_rate_summary[("diff_rate", "total")]<=lowDiffTotalRate
    up_users = user_rate_summary.loc[r1 & r2, ("userid", "")]
    wanna_up = match_record[(match_record.userid.isin(up_users)) &
                            (match_record.type == account_type) &
                            (match_record.amount >= lowAmount)]
    wanna_up = pd.merge(wanna_up,
                        asset_data[["asset_id", "rate"]],
                        on="asset_id",
                        how="left")
    user_exp = user_rate_summary[[("userid", ""), ("exp_rate", "total")]]
    user_exp.columns = ["userid", "exp_rate"]
    wanna_up = pd.merge(wanna_up, user_exp, on="userid", how="left")
    wanna_up["diff_rate"] = wanna_up.rate - wanna_up.exp_rate
    wanna_up = wanna_up.sort_values(by=["diff_rate", "exp_rate", "amount"],
                                    ascending=[True, False, False]).reset_index(drop=True)
    return {"up_users": up_users, "wanna_up": wanna_up}

# 向下调整队列选择函数
def wannaDownSelect(user_rate_summary,
                    match_record,
                    asset_data,
                    elimUsers,
                    lowDiffCurRate=1,
                    lowAmount=100,
                    account_type=[1, 2],
                    lowRate=6.1
                    ):
    r1 = user_rate_summary[("diff_rate", "current")] >= lowDiffCurRate
    r2 = user_rate_summary[("diff_rate", "total")] > 0
    r3 = np.isnan(user_rate_summary[("exp_rate", "regular")])
    r4 = user_rate_summary.userid.isin(elimUsers) == False
    if (type(account_type) == int):
        account_type = [account_type]
    type_dict = {1: "current", 2: "tPlus", 3: "regular"}
    f = lambda x: [("exp_rate", type_dict.get(_)) for _ in x]

    down_data = user_rate_summary.loc[r1 & r2 & r3 & r4,
                                      [("userid", "")] + f(account_type)]

    down_data = down_data.set_index("userid")
    down_users = list(down_data.index)
    user_exp = down_data.stack(level=-1)
    user_exp = user_exp.reset_index()
    inv_type_dict = dict(zip(type_dict.values(), type_dict.keys()))
    g = itemgetter(*user_exp.type)
    user_exp["type"] = g(inv_type_dict)

    wanna_down = match_record[(match_record.userid.isin(down_users)) &
                              (match_record.type.isin(account_type)) &
                              (match_record.amount >= lowAmount)]
    wanna_down = pd.merge(wanna_down, user_exp, on=["userid", "type"], how="inner")
    wanna_down = pd.merge(wanna_down,
                          asset_data[["asset_id", "rate"]],
                          on="asset_id",
                          how="left")
    wanna_down = wanna_down.loc[wanna_down.rate >= lowRate]
    return {"down_users": down_users, "wanna_down": wanna_down}

# 双队列双向选择的匹配评价函数
def match_penalty(wait_trans,wanna_down_list):
    match_up = np.where(wanna_down_list.rate>=wait_trans.exp_rate.iloc[0],
                        0.5*(wanna_down_list.rate-wait_trans.exp_rate.iloc[0]),
                        2*(wait_trans.exp_rate.iloc[0]-wanna_down_list.rate)
                       )
    match_down = 0.5*abs(wanna_down_list.exp_rate-wait_trans.rate.iloc[0])
    match_amount = abs(wanna_down_list.amount-wait_trans.amount.iloc[0])/wait_trans.amount.iloc[0]
    penalty = match_up+match_down+7*match_amount
    return penalty

# 对任意向上调整单元，向下调整单元的匹配输出函数
def doTrans(wait_trans, wanna_down_list):
    if (wait_trans.avail_num.iloc[0] == 0):
        wanna_down_list = wanna_down_list.loc[wanna_down_list.amount >= wait_trans.amount.iloc[0]]
    wanna_down_list["fake_avail_num"] = np.where(wanna_down_list.total_amount >= wait_trans.amount.iloc[0],
                                                 wanna_down_list.avail_num,
                                                 wanna_down_list.avail_num + 1)
    wanna_down_list = wanna_down_list.loc[wanna_down_list.fake_avail_num > 0]
    if (len(wanna_down_list) == 0):
        result = pd.DataFrame()
    else:
        wanna_down_target = wanna_down_list.iloc[[0]]
        trans_amount = round(min(wanna_down_target.amount.iloc[0], wait_trans.amount.iloc[0]), 2)

        # trans_temp：便于计算的输出结果
        var = ["userid", "asset_id", "type"]
        down = wanna_down_target.loc[:, var].reset_index(drop=True)
        up = wait_trans.loc[:, var].reset_index(drop=True)
        remove = pd.concat([up, down], axis=0)
        remove["amount"] = -trans_amount
        add = remove.copy(deep=True)
        add["asset_id"] = add.asset_id[::-1]
        add["amount"] = trans_amount
        trans_temp = pd.concat([remove, add], axis=0)

        # log_temp：便于写库的输出结果
        down_var = list(map(lambda x: "down_" + x, var))
        up_var = list(map(lambda x: "up_" + x, var))
        down = down.rename(columns=dict(zip(var, down_var), inplace=True))
        up = up.rename(columns=dict(zip(var, up_var), inplace=True))
        log_temp = pd.concat([down, up], axis=1)
        log_temp["amount"] = trans_amount

        result = {"trans_temp": trans_temp, "log_temp": log_temp}
    return result

# 资产端数据库连接信息
asset_con = create_engine("mysql+pymysql://datacenter_read:Zjy-yinker20150309@120.27.167.74:80/"
                          "jianlc_asset?charset=utf8")
# 资金端数据库连接信息
cash_con = create_engine("mysql+pymysql://xmanread:LtLUGkNbr84UWXglBFYe4GuMX8EJXeIG@120.55.176.18:5306/"
                          "product?charset=utf8")
# 写表输出连接信息
test_con = create_engine("mysql+pymysql://zhangjiayi:Zjy@yinker20150309@10.1.5.220:3306/test?charset=utf8")

z0 = "select user_id as userid,asset_id,money_account_type as type,sum(hold_amount) as amount from ast_matched_record " \
     "where status=1 and yn=0  group by user_id,asset_id,money_account_type"
z1 = "select id as asset_id,corpusAmount,aunualInterestRate as rate from ast_loan_asset " \
     "WHERE yn = 0 AND STATUS IN(400,600) and corpusAmount>0"
z2 = "select user_id as userid,amount,`time` as num from regular_info where yn=0 and status=10"

match_record = read_sql_query(sql=z0, con=asset_con)
asset_data = read_sql_query(sql=z1, con=asset_con)
regular_info = read_sql_query(sql=z2, con=cash_con)

print("sql finished!")
# path = "F:/20170714data/"
# match_record = pd.read_csv(path+"match_record.csv")
# asset_data = pd.read_csv(path+"asset_data.csv")
# regular_info = pd.read_csv(path+"regular_info.csv")

# 创建囊括所有资金类型的预期利率词典
num = np.arange(1,12.5,0.5)
exp_rate = [6.1, 6.1, 6.1, 6.15, 6.2, 6.25, 6.3, 6.35, 6.4, 6.45, 6.5,
            6.55, 6.6, 6.65, 6.7, 6.75, 6.8, 6.85, 6.9, 6.9, 6.9, 6.9, 6.9]
regular_rate = pd.DataFrame({"num": num, "exp_rate": exp_rate})
n = 200
rate_dict = {"current": 5, "tPlus": 6, "regular": regular_rate}

# 输出用户利率现状
user_rate_summary = getRateSummary(match_record, asset_data, regular_info, rate_dict)

# 选择需要向上调整的资金队列
up_result = wannaUpSelect(user_rate_summary, match_record, asset_data)
up_users = up_result.get("up_users")
wanna_up = up_result.get("wanna_up")

# 选择需要向下调整的资金队列
down_result = wannaDownSelect(user_rate_summary,
                              match_record,
                              asset_data,
                              elimUsers=up_users,
                              account_type=[1])
wanna_down = down_result.get("wanna_down")
print("data all prepared!")

adjust_num = wanna_up.shape[0]

for i in range(0,adjust_num):
    wait_trans = wanna_up.iloc[[i]]
    group1 = match_record.groupby(["asset_id"])
    group2 = match_record.groupby(["asset_id", "userid"])
    asset_match_status = pd.DataFrame({"avail_num":
                                       n-group1.userid.nunique()}).reset_index()
    user_match_status = pd.DataFrame({"total_amount":
                                      group2.amount.sum()}).reset_index()
    wait_trans = wait_trans.merge(asset_match_status,
                                  on="asset_id",
                                  how="left").merge(user_match_status,
                                                    on=["userid", "asset_id"],
                                                    how="left")
    if((wait_trans.avail_num.iloc[0]==0) & (wait_trans.amount.iloc[0]<wait_trans.total_amount.iloc[0])):
        continue
    if(len(wanna_down)==0):
        break

    wanna_down_list = wanna_down[wanna_down.rate>wait_trans.rate.iloc[0]]

    if(len(wanna_down_list)==0):
        continue

    wanna_down_list["penalty"] = match_penalty(wait_trans, wanna_down_list)
    wanna_down_list = wanna_down_list.merge(asset_match_status,
                                            on="asset_id",
                                            how="left").merge(user_match_status,
                                                              on=["userid", "asset_id"],
                                                              how="left").sort_values(by="penalty")
    result = doTrans(wait_trans, wanna_down_list)
    log_temp = result.get("log_temp")
    trans_temp = result.get("trans_temp")
    #*************************** ！结果输出 **************************
    log_temp.to_sql(name="asset_trans_log", con=test_con, if_exists="append", index=False)

    # 结果应用于队列和match_record
    wanna_down.loc[(wanna_down.userid == log_temp.down_userid.iloc[0])&
                   (wanna_down.asset_id == log_temp.down_asset_id.iloc[0])&
                   (wanna_down.type == log_temp.down_type.iloc[0]), "amount"] = \
        round(wanna_down.amount-log_temp.amount.iloc[0], 2)

    remove = pd.merge(match_record,
                      trans_temp.loc[trans_temp.amount < 0],
                      how="left",
                      on=["userid", "asset_id", "type"],
                      suffixes=("", "_y"))
    remove.loc[remove.amount_y.isnull() == False, "amount"] = round(remove.amount+remove.amount_y, 2)
    remove = remove.loc[remove.amount > 0]
    remove.drop('amount_y', axis=1, inplace=True)
    add = trans_temp.loc[trans_temp.amount > 0]
    match_record = pd.concat([remove, add], axis=0)
    print("index: %d finished!"%i)

