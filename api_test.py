# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/5/18 17:29
# @Author  : MAYA
import requests
import json
import pandas as pd
import random
from datetime import datetime, timedelta

API_HOST = {"local": "http://localhost:8000", "cloud": "http://cdqrmi.com:8000"}

API_TYPE = {"common": "/common/", "cona": "/cona/", "kamba": "/kamba/"}


def get_params_data(block, base_data, random_date=False):
    time_range = json.loads(test_api("common", {"key": "real_time", "block": block}))
    if random_date:
        start, end = map(lambda x: datetime.strptime(x, "%Y/%m/%d %H:%M:%S"), [time_range["start_limit"], time_range["end_limit"]])
        dates = pd.date_range(start, end, freq="1D")
        start = random.choice(dates)
        end = start + timedelta(days=7)
        base_data["start"] = start.strftime("%Y/%m/%d") + " 00:00:00"
        base_data["end"] = end.strftime("%Y/%m/%d") + " 23:59:59"
    else:
        base_data["start"] = time_range["start"]
        base_data["end"] = time_range["end"]
    return base_data


def test_api(u_type, data, is_local=True):
    """ 测试接口，向云接口或本地发起POST请求

    Args:
        u_type: 请求对象，common(公用)、cona(错那)、kamba(岗巴)
        data: 请求参数，字典
        is_local: 是否本地，默认为True，访问本地数据库，否则或向云数据库发起请求

    Returns:
        接口返回json格式的数据
    """
    url = API_HOST["local"] + API_TYPE[u_type] if is_local else API_HOST["cloud"] + API_TYPE[u_type]
    r = requests.post(url, data=data)
    return r.text

# ********************************************** 测试 /common 接口 START ************************************************
# 1. 账号密码登录
# res = test_api("common", {"u_name": "tes", "pwd": "test2", "key": "login"})

# 2. token验证
# res = test_api("common", {"token": "d4a68b48afd923ca02ed5d6940f9f62d9385887d1decbe7717c77f315", "key": "token_check"})

# 3. 某板块时间范围
# res = test_api("common", {"key": "real_time", "block": "cona"})

# ************************* ******************** 测试 /common 接口 END **************************************************
# **********************************************************************************************************************


# *********************************************** 测试 /cona 接口 START **************************************************
block = "cona"
# 1. 面板数据
# res = test_api(block, get_params_data(block, {"key": "panel_data"}))

# 2. 供水温度与气温关系
# res = test_api(block, get_params_data(block, {"key": "water_supply_with_temp", "by": "d"}))

# 3. 地热井可提供热量
# res = test_api(block, get_params_data(block, {"key": "geothermal_wells_provide_heat", "by": "d"}))

# 4. 系统COP
# res = test_api(block, get_params_data(block, {"key": "com_cop", "by": "h"}, True))

# 5. 节省供暖费用
# res = test_api(block, get_params_data(block, {"key": "cost_saving", "by": "d"}, True))

# 6. 高低温节省费用
# res = test_api(block, get_params_data(block, {"key": "types_cost_saving", "by": "d"}, True))

# 7. 平均供回水温度与温差
res = test_api(block, get_params_data(block, {"key": "water_supply_and_return_temp", "by": "d"}, True))





# r = requests.post(
#     "http://localhost:8000/cona/", data={
#         "key": "geothermal_wells_provide_heat",
#         "start": "2021-02-01 00:00:00",
#         "end": "2021-02-07 23:59:59",
#         "by": "h"
#     }
# )

print(res)