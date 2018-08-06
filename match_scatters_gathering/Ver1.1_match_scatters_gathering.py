
# coding: utf-8
import pandas as pd
import numpy as np
import math
import pymysql
from sqlalchemy import create_engine
from pandas.io.sql import read_sql_query

# 定义数据框列拼接
def cbind(x,y):
    def repeatDF(df, times):
        result = pd.concat([df]*times, axis=0)
        return result
    xrows = x.shape[0]
    yrows = y.shape[0]
    if(xrows < yrows):
        times = math.ceil(yrows/xrows)
        x = repeatDF(x, times)
        x = x[:yrows]
    elif(xrows > yrows):
        times = math.ceil(xrows/yrows)
        y = repeatDF(y, times)
        y = y[:xrows]
    else:
        pass
    return pd.concat([x.reset_index(drop=True), y.reset_index(drop=True)], axis=1)

# 定义交换记录输出函数
def move_info(target_user, mover, avail_replacer):
    x = match_summary[(match_summary.asset_id.isin(avail_replacer)) &
                      (match_summary.userid == target_user)].sort_values(by="amount")
    x['cum_amount'] = np.cumsum(x.amount)
    x['rest'] = round(x.cum_amount-mover.amount.iloc[0], 2).apply(lambda z: max(z, 0))
    x['trans_amount'] = round(x.amount-x.rest, 2)
    result = x.loc[x.trans_amount>0,["asset_id", "userid", "trans_amount"]]
    result.rename(columns={"trans_amount": "amount"}, inplace=True)
    # 便于使用的结果
    mover.amount = sum(result.amount)
    a = pd.concat([mover, result], axis=0)
    a.amount = -a.amount
    b1 = result.copy()
    b1.userid = mover.userid.iloc[0]
    b2 = result.copy()
    b2.asset_id = mover.asset_id.iloc[0]
    b = pd.concat([b1, b2], axis=0)
    trans_result = pd.concat([a, b], axis=0)
    
	# 便于交给研发的结果
    varnames = ["asset_id", "userid"]
    move_varnames = list(map(lambda z: "mover_"+z, varnames))
    replace_varnames = list(map(lambda z: "replacer_"+z, varnames))
    a = mover.loc[:, varnames]
    a.rename(columns=dict(zip(varnames, move_varnames)), inplace=True)
    b = result.copy()
    b.rename(columns=dict(zip(varnames, replace_varnames)), inplace=True)
    log_result = cbind(a, b)
    return [trans_result, log_result]

################# 1 基础数据收集	#################
asset_con = create_engine("mysql+pymysql://datacenter_read:Zjy-yinker20150309@120.27.167.74:80/"
                          "jianlc_asset?charset=utf8")

test_con = create_engine("mysql+pymysql://zhangjiayi:Zjy@yinker20150309@10.1.5.220:3306/test?charset=utf8")

z0 = "select user_id as userid,asset_id,hold_amount as amount from ast_matched_record where yn=0 and status=1"
z1 = "select a.id as asset_id, a.aunualInterestRate as rate,b.asset_type from ast_loan_asset a " \
     "left join ast_matching_asset_group b on a.id=b.asset_id where a.yn=0 and a.status in(400,600)"

final_match_record = read_sql_query(sql=z0, con=asset_con)
asset_info = pd.read_sql_query(sql=z1, con=asset_con)

full_asset = final_match_record.groupby("asset_id", as_index=False).agg({"amount": sum, "userid": pd.Series.nunique})
full_asset = pd.merge(full_asset, asset_info[['asset_id', 'rate', "asset_type"]], how="left", on="asset_id", copy=False)
full_asset.rename(columns={"userid": "num"}, inplace=True)
full_asset = full_asset[(full_asset.amount >= 100000) & (full_asset.asset_type == 1)]
print("files prepared!")

################# 2 资产分群	#################
rate_set = np.unique(full_asset['rate'])
set_num = len(rate_set)
#final_match = pd.DataFrame(columns=final_match_record.columns)

# 2.1 资产群及相关数据选择
i = 0
asset_set = full_asset[full_asset.rate == rate_set[i]]
match_record = final_match_record[final_match_record.asset_id.isin(asset_set.asset_id)]
match_summary = match_record.groupby(['asset_id', 'userid'], as_index=False).agg({'amount':sum})

user_match_status = match_summary.groupby(['userid']).size().reset_index(name='num')
user_match_status = user_match_status[user_match_status.num > 1].sort_values(by='num', ascending=False).reset_index(drop=True)
duplicate_user = user_match_status['userid']
user_num = len(duplicate_user)

# 2.2 逐一按用户进行整合
for j in range(user_num):
	# 选择 target_user
    target_user = duplicate_user.iloc[j]
    asset_list = match_summary[match_summary.userid == target_user]
    asset_num = asset_list.shape[0]

    if(asset_num < 2):
        continue
	
    container_list = asset_list.sort_values(by="amount", ascending=False).asset_id.reset_index(drop=True)
    while len(container_list) > 1:
        # 选择container
        container = container_list[0]
        # 选择replacer
        replacer_list = container_list[1:]
        replacer_total_summary = match_summary[match_summary.asset_id.isin(replacer_list)]
        # 列出mover
        mover_list = match_summary[(match_summary.asset_id == container) &
                                   (match_summary.userid != target_user) &
                                   (match_summary.userid.isin(replacer_total_summary.userid))].sort_values(by="amount")
        mover_num = mover_list.shape[0]
        if(mover_num > 0):
			# 逐一对mover进行替换
            for k in range(mover_num):
                mover = mover_list.iloc[[k]]
                pre_avail_list = replacer_total_summary[replacer_total_summary.userid==mover.userid.iloc[0]].asset_id
                avail_replacer = match_summary[(match_summary.asset_id.isin(pre_avail_list))&(match_summary.userid==target_user)].asset_id

                if(len(avail_replacer) == 0):
                    continue
                else:
					# 获得替换结果
                    result = move_info(target_user=target_user, mover=mover, avail_replacer=avail_replacer)
                    trans_temp = result[0]
                    log_temp = result[1]
                    #### 结果输出 ####
                    log_temp.to_sql(name="trans_log", con=test_con, if_exists="append", index=False)
					
					# 根据交换结果模拟现有数据的更新
                    a = match_summary[(match_summary.asset_id.isin(trans_temp.asset_id)==False)]
                    b1 = match_summary[match_summary.asset_id.isin(trans_temp.asset_id)]
                    b2 = pd.concat([b1, trans_temp], axis=0)
                    b = b2.groupby(["asset_id", "userid"], as_index=False).agg({"amount": sum})
                    b["amount"] = round(b.amount, 2)
                    b = b[b.amount > 0]
                    match_summary = pd.concat([a.reset_index(drop=True), b.reset_index(drop=True)])
                    print("user_num: %d finished!" % k)
                    
		# 重新筛选container集合
        container_list_pre = match_summary[(match_summary.userid == target_user) & (match_summary.asset_id.isin(replacer_list))].sort_values(by="amount", ascending=False)
        container_list = container_list_pre.asset_id.reset_index(drop=True)

