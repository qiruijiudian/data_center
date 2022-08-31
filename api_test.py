# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/5/18 17:29
# @Author  : MAYA
from itertools import chain

import requests
import json
import pandas as pd
import random
from datetime import datetime, timedelta

API_HOST = {"local": "http://localhost:8000", "cloud": "http://cdqrmi.com:8000"}

API_TYPE = {"common": "/common/", "cona": "/cona/", "kamba": "/kamba/", "tianjin": "/tianjin/", "custom": "/custom/"}


def get_params_data(block, base_data, random_date=False):
    if base_data["key"] not in ["config_file", "custom_interface"]:

        time_range = json.loads(test_api("common", {"key": "real_time", "block": block}))
        if random_date:
            start, end = map(lambda x: datetime.strptime(x, "%Y/%m/%d %H:%M:%S"),
                             [time_range["start_limit"], time_range["end_limit"]])
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
# res = test_api("common", {"u_name": "tianjin", "pwd": "123456", "key": "login"})

# 2. token验证
# res = test_api("common", {"token": "fea2700a631cb000e5b244b8112ee43560e5b67e3b8723f8c83fcc90fb9545b8", "key": "token_check", "block": "tianjin"})

# 3. 某板块时间范围
# res = test_api("common", {"key": "real_time", "block": "tianjin"})

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
# res = test_api(block, get_params_data(block, {"key": "com_cop", "by": "h"}, False))

# 5. 节省供暖费用
# res = test_api(block, get_params_data(block, {"key": "cost_saving", "by": "d"}, True))

# 6. 高低温节省费用
# res = test_api(block, get_params_data(block, {"key": "types_cost_saving", "by": "d"}, True))

# 7. 平均供回水温度与温差
# res = test_api(block, get_params_data(block, {"key": "water_supply_and_return_temp", "by": "d"}))

# 8. 热力井供热与气温关系
# res = test_api(block, get_params_data(block, {"key": "heat_well_heating_with_temp", "by": "d"}))

# 9. 热力管网供热与气温关系
# res = test_api(block, get_params_data(block, {"key": "heat_pipe_network_heating_with_temp", "by": "d"}))

# 8. 水源热泵制热量与气温关系
# res = test_api(block, get_params_data(block, {"key": "water_heat_pump_heat_with_temp", "by": "d"}))

# 8. 高温板换制热量与气温关系
# res = test_api(block, get_params_data(block, {"key": "high_temp_plate_exchange_heat_with_temp", "by": "d"}))

# 9. 负荷
# res = test_api(block, get_params_data(block, {"key": "load", "by": "d"}))

# 10. 补水量
# res = test_api(block, get_params_data(block, {"key": "water_replenishment", "by": "h"}))

# ************************* ******************** 测试 /cona 接口 END **************************************************
# **********************************************************************************************************************


# *********************************************** 测试 /kamba 接口 START **************************************************
block = "kamba"
# 1. 面板数据
# res = test_api(block, get_params_data(block, {"key": "panel_data"}))

# 2. 蓄热水池高低温热量
# res = test_api(block, get_params_data(block, {"key": "heat_storage_tank_heating", "by": "d"}))

# 3. 电锅炉可替换供热天数
# res = test_api(block, get_params_data(block, {"key": "alternative_heating_days", "by": "d"}))

# 4. 系统综合COP
# res = test_api(block, get_params_data(block, {"key": "com_cop", "by": "h"}, True))

# 3. 水源热泵COP
# res = test_api(block, get_params_data(block, {"key": "wshp_cop", "by": "h"}, True))

# 4. 水池温度热力图
# res = test_api(block, get_params_data(block, {"key": "pool_temperature_heatmap", "by": "h"}))

# 5. 太阳能集热量
# res = test_api(block, get_params_data(block, {"key": "solar_collector", "by": "h"}))

# 6. 太阳能矩阵供回水温度
# res = test_api(block, get_params_data(block, {"key": "solar_matrix_water_temperature", "by": "h"}))

# 7. 蓄热水池补水量
# res = test_api(block, get_params_data(block, {"key": "heat_storage_tank_replenishment", "by": "h"}))

# 8. 供热端补水量
# res = test_api(block, get_params_data(block, {"key": "heat_replenishment", "by": "h"}))

# 9. 太阳能测补水量
res = test_api(block, get_params_data(block, {"key": "solar_side_replenishment", "by": "h"}))

# 10. 负荷量
# res = test_api(block, get_params_data(block, {"key": "load", "by": "d"}))

# 11. 负荷量
# res = test_api(block, get_params_data(block, {"key": "load", "by": "h"}))


# 12. 末端供水与气温关系
# res = test_api(block, get_params_data(block, {"key": "end_water_supply_with_temp", "by": "d"}))

# 13. 末端回水与气温关系
# res = test_api(block, get_params_data(block, {"key": "end_water_return_with_temp", "by": "d"}))

# 14. 末端工供回水温差与气温关系
# res = test_api(block, get_params_data(block, {"key": "end_water_diff_with_temp", "by": "d"}))

# 15. 末端供回水温度对照分析
# res = test_api(block, get_params_data(block, {"key": "end_water_temperature_compare", "by": "d"}, True))

# 16. 太阳能集热分析
# res = test_api(block, get_params_data(block, {"key": "solar_collector_analysis", "by": "d"}))

# 17. 供热分析
# res = test_api(block, get_params_data(block, {"key": "heating_analysis", "by": "d"}))

# 18. 供热量情况
# res = test_api(block, get_params_data(block, {"key": "heat_production", "by": "d"}))

# 19. 高温板换制热功率
# res = test_api(block, get_params_data(block, {"key": "high_temperature_plate_exchange_heat_rate", "by": "h"}))

# 20. 节省供暖费用
# res = test_api(block, get_params_data(block, {"key": "cost_saving", "by": "d"}))


# 21. 末端供回水温度
# res = test_api(block, get_params_data(block, {"key": "end_water_temperature", "by": "d"}))

# ************************* ******************** 测试 /kamba 接口 END **************************************************
# **********************************************************************************************************************


# *********************************************** 测试 /tianjin 接口 START ***********************************************
block = "tianjin"

# MAU 数据板块
# 1. 面板数据
res = test_api(block, get_params_data(block, {"key": "panel_data", "block": "MAU"}))

# 1. MAU风机频率
# res = test_api(block, get_params_data(block, {"key": "mau_fan_frequency"}))

# 2. MAU冷热水阀开度201
# res = test_api(block, get_params_data(block, {"key": "mau_water_valve_201"}))

# 3. MAU冷热水阀开度202
# res = test_api(block, get_params_data(block, {"key": "mau_water_valve_202"}))

# 4. MAU冷热水阀开度203
# res = test_api(block, get_params_data(block, {"key": "mau_water_valve_203"}))

# 5. MAU冷热水阀开度301
# res = test_api(block, get_params_data(block, {"key": "mau_water_valve_301"}))

# 6. MAU冷热水阀开度401
# res = test_api(block, get_params_data(block, {"key": "mau_water_valve_401"}))

# 7. MAU送风温度与湿度201
# res = test_api(block, get_params_data(block, {"key": "mau_air_supply_temp_and_humidity_201"}))

# 8. MAU冷热水阀开度202
# res = test_api(block, get_params_data(block, {"key": "mau_air_supply_temp_and_humidity_202"}))

# 9. MAU冷热水阀开度203
# res = test_api(block, get_params_data(block, {"key": "mau_air_supply_temp_and_humidity_203"}))

# 10. MAU冷热水阀开度301
# res = test_api(block, get_params_data(block, {"key": "mau_air_supply_temp_and_humidity_301"}))

# 11. MAU冷热水阀开度401
# res = test_api(block, get_params_data(block, {"key": "mau_air_supply_temp_and_humidity_401"}))

# 12. MAU送风压力
# res = test_api(block, get_params_data(block, {"key": "mau_air_supply_pressure"}))

# 13. 室外空气温湿度
# res = test_api(block, get_params_data(block, {"key": "air_temperature_and_humidity"}))

# 14. item 与温度关系
# res = test_api(block, get_params_data(block, {"key": "mau_data_with_temp", "item": "sat"}))

# 15. item 设定值与温度关系
# res = test_api(block, get_params_data(block, {"key": "mau_set_point_with_temp", "item": "air_supply_temperature_201"}))



# ************************* ******************** 测试 /tianjin 接口 END **************************************************
# **********************************************************************************************************************


# *********************************************** 测试 /custom 接口 START ***********************************************
# block = "custom"
# res = test_api(block, get_params_data(block, {"key": "config_file"}), False)

# res = test_api(
#     block,
#     {
#         "key": "custom_interface",
#         "series": json.dumps(
#             {
#                 "高温板换制热功率": {"type": "bar", "data": "high_temp_plate_exchange_heat_production", "stack": "制热量"},
#                 "高温板换制热量": {"type": "bar", "data": "water_heat_pump_heat_production", "stack": "制热量"},
#                 "地热井可提供高温热量": {"type": "line", "data": "geothermal_wells_high_heat_provide"},
#                 "地热井可提供低温热量": {"type": "line", "data": "geothermal_wells_low_heat_provide"}
#             }
#         ),
#         "block": "cona",
#         "by": "d",
#         "chart_type": "time",
#         "start": "2021-05-08 00:00:00",
#         "end": "2021-05-14 23:59:59"
#     }
# )

print(res)