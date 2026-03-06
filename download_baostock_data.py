import baostock as bs
import pandas as pd
import os

def download_stock_data(code="sh.600000", start_date="2025-01-01", end_date="2025-12-31"):
    #### 登陆系统 ####
    lg = bs.login()
    # 显示登陆返回信息
    print('login respond error_code:'+lg.error_code)
    print('login respond  error_msg:'+lg.error_msg)

    #### 获取沪深A股历史K线数据 ####
    # 详细指标参数，参见“历史行情指标参数”章节；“分钟线”参数与“日线”参数不同。
    # 分钟线指标：date,time,code,open,high,low,close,volume,amount,adjustflag
    # 频率：d=日k线、w=周、m=月、5=5分钟、15=15分钟、30=30分钟、60=60分钟，默认为d
    # 暂时用日线或者60分钟线测试，因为分钟数据量大
    rs = bs.query_history_k_data_plus(code,
        "date,time,code,open,high,low,close,volume,amount,adjustflag",
        start_date=start_date, end_date=end_date,
        frequency="5", adjustflag="3")
    print('query_history_k_data_plus respond error_code:'+rs.error_code)
    print('query_history_k_data_plus respond  error_msg:'+rs.error_msg)

    #### 打印结果集 ####
    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    
    result = pd.DataFrame(data_list, columns=rs.fields)

    #### 结果集输出到csv文件 ####   
    filename = f"{code}_{start_date}_{end_date}.csv"
    result.to_csv(filename, index=False)
    print(f"Result saved to {filename}")

    #### 登出系统 ####
    bs.logout()
    return filename

if __name__ == '__main__':
    # 下载浦发银行(sh.600000) 2025年1月至今的数据
    download_stock_data(code="sh.600000", start_date="2025-01-01", end_date="2025-03-01")
