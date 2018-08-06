
def wiseSlimByAmount(targetCashAmount,tol=5e+6):
    import pandas as pd
    from pymysql import connect
    from pandas.io.sql import read_sql
    asset_con = connect(host='120.27.167.74',
                        port=80,
                        user='datacenter_read',
                        password='Zjy-yinker20150309',
                        db='jianlc_asset')
    cash_con = connect(host='120.55.176.18',
                       port=5306,
                       user='xmanread',
                       password='LtLUGkNbr84UWXglBFYe4GuMX8EJXeIG',
                       db='product')
    z0 = "SELECT user_id as userid,(un_match_amount+match_amount+asset_out_amount) as premium " \
         "FROM user_account_info  WHERE (match_amount + un_match_amount + asset_out_amount) > 0"
    user_premium_data = read_sql(sql=z0, con=cash_con)
    cash_con.commit()
    cash_con.close()

    z1 = "select user_id as userid,unmatched_amount " \
         "from ast_money_account where yn=0 and unmatched_amount>0 and account_type=1"
    user_match_data = read_sql(sql=z1, con=asset_con)
    asset_con.commit()
    asset_con.close()

    user_list = pd.merge(user_match_data,
                         user_premium_data,
                         how="left",
                         left_on="userid",
                         right_on="userid")

    user_list['exp_match_amount']=user_list['premium']-user_list['unmatched_amount']


    def nowAmount(user_list,target_ratio):
        user_list['target_amount']=user_list['premium']*target_ratio-user_list['exp_match_amount']
        user_list['target_amount']=user_list['target_amount'].apply(lambda x: max(x, 0))
        return round(sum(user_list['target_amount']))

    target_ratio = 1
    down = 0
    max_cash_amount = nowAmount(user_list, target_ratio)

    if(max_cash_amount > targetCashAmount+tol):
        stepnum = 0
        up = target_ratio
        step = abs(1/2*(up+down)-target_ratio)
        target_ratio = 1/2*(up+down)
        while stepnum <= 4 and step >= 0.0005:
            now_cash_amount = nowAmount(user_list, target_ratio)
            if(now_cash_amount > targetCashAmount+tol):
                up = target_ratio
                step = abs(1/2*(up+down)-target_ratio)
                target_ratio = 1/2*(up+down)
            elif(now_cash_amount<targetCashAmount-tol):
                down = target_ratio
                step = abs(1/2*(up+down)-target_ratio)
                target_ratio = 1/2*(up+down)
            else:
                break
            stepnum += 1
        final_ratio = target_ratio
    else:
        final_ratio = 1

    return final_ratio

result = wiseSlimByAmount(80000000)
print(result)

