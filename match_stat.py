
# coding: utf-8

import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine
from pandas.io.sql import read_sql_query

# 数据提取
asset_con = create_engine("mysql+pymysql://datacenter_read:Zjy-yinker20150309@120.27.167.74:80/"
                          "jianlc_asset?charset=utf8")

z0 = "select user_id as userid,account_type as type,unmatched_amount,target_match_amount from ast_money_account " \
     "where yn=0 and unmatched_amount>0"
z1 = "select COUNT(1) as unmatched_avail_num,SUM(unmatched_amount) as unmatched_amount FROM ast_matching_asset_group " \
     "WHERE yn = 0 AND STATUS IN(1)"

cash_status = read_sql_query(sql=z0, con=asset_con)
# 0.当前未匹资产情况
asset_status = read_sql_query(sql=z1, con=asset_con)

# 1.当前未匹资金情况
type_dict = {1: "current", 2: "tPlus", 3: "regular"}
f = lambda x: ("unmatched_"+type_dict.get(x), round(cash_status.loc[cash_status.type == x, "unmatched_amount"].sum(), 2))
unmatched_dict = dict(list(map(f, list(type_dict.keys()))))


# 2.当前匹配能力和最大匹配能力
h1 = lambda x: list(cash_status.loc[cash_status.type == x, "target_match_amount" if x == 1 else "unmatched_amount"].sort_values(ascending=False)[:200])
h2 = lambda x: list(cash_status.loc[cash_status.type == x, "unmatched_amount"].sort_values(ascending=False)[:200])

now_match_seq = np.concatenate(list(map(h1, list(type_dict.keys()))), axis=0)
max_match_seq = np.concatenate(list(map(h2, list(type_dict.keys()))), axis=0)

now_match_ability = round(-np.sort(-now_match_seq)[:200].sum())
max_match_ability = round(-np.sort(-max_match_seq)[:200].sum())


# 3.未匹活期的金额分布
breaks = [0, 100, 200, 300, 400, 500, 700, 1000, 2000, 3000, 5000, 7000, 10000, 20000, float("inf")]
df = cash_status.loc[(cash_status.type == 1) & (cash_status.unmatched_amount > 0)]
df["label"] = pd.cut(df.unmatched_amount, bins=breaks, right=True)
group = df.groupby("label")
df_summary = pd.DataFrame({"total_amount": round(group.unmatched_amount.sum(), 2),
                           "user_num": group.userid.nunique()}).reset_index()
df_summary["label"] = df_summary.label.astype(str)

