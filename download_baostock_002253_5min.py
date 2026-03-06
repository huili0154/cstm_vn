import baostock as bs
import pandas as pd
import os

def download_002253_5min():
    # 登录系统
    lg = bs.login()
    print('login respond error_code:'+lg.error_code)
    print('login respond  error_msg:'+lg.error_msg)

    code = "sz.002253" # 川大智胜
    start_date = "2025-01-01"
    end_date = "2025-12-31"
    frequency = "5"
    adjustflag = "3" # 3: 不复权

    print(f"尝试下载 {code} {frequency}分钟数据 ({start_date} 至 {end_date})...")

    # 获取沪深A股历史K线数据
    rs = bs.query_history_k_data_plus(code,
        "date,time,code,open,high,low,close,volume,amount,adjustflag",
        start_date=start_date, end_date=end_date,
        frequency=frequency, adjustflag=adjustflag)
    
    print('query_history_k_data_plus respond error_code:'+rs.error_code)
    print('query_history_k_data_plus respond  error_msg:'+rs.error_msg)

    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    
    if len(data_list) > 0:
        result = pd.DataFrame(data_list, columns=rs.fields)
        filename = f"{code}_{start_date}_{end_date}_5min.csv"
        result.to_csv(filename, index=False)
        print(f"成功！已保存 {len(result)} 条记录到 {filename}")
        print(result.head())
    else:
        print("未找到数据。可能原因：该品种不支持分钟线，或时间范围内无数据。")

    bs.logout()

if __name__ == "__main__":
    download_002253_5min()
