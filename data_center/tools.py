# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/5/18 17:23
# @Author  : MAYA
import collections

import pandas as pd
from datetime import datetime, timedelta
import pymysql
import platform

from sqlalchemy import create_engine

from data_center.settings import DATABASE, DB_USER, DB_PASSWORD, DB_HOST, DB_ORIGIN, DB_DC, TIME_DATA_INDEX, DB_NAME, \
    POINT_NAME, VALUE_NAME
import operator


def gen_time_range(df):
    res = {
        "start": df.index[0].strftime("%Y/%m/%d"),
        "end": df.index[-1].strftime("%Y/%m/%d")
    }
    return res


def get_common_response(df, by, is_timing=True):
    """获取常用返回值（开始、结束、每列值）

    :param df:
    :param time_index:
    :param by:
    :param is_timing: 是否为时序图，如果为时序图则会按照时序格式（包含字典的列表 ）
    :return:
    """
    res = {}
    # df = df.round(2).fillna("")
    df = df.round(2).fillna(0)

    res["start"] = df.index[0].strftime("%Y/%m/%d")
    res["end"] = df.index[-1].strftime("%Y/%m/%d")
    if is_timing:
        time_format = "%Y/%m/%d" if by == "d" else "%Y/%m/%d %H:%M:%S"
        time_data = list(map(lambda x: x.strftime(time_format), df.index))

        for column in df.columns:

            items = list(zip(time_data, df[column].values))
            res[column] = [{"value": item} for item in items]
    else:
        for column in df.columns:
            res[column] = df[column].values
    return res


def get_compare_with_item(df, with_column):
    res = {}
    df = df.round(2).fillna("")
    with_values = df[with_column].values
    res["start"] = df.index[0].strftime("%Y/%m/%d")
    res["end"] = df.index[-1].strftime("%Y/%m/%d")
    for column in df.columns:
        if column != with_column:
            res[column] = list(zip(with_values, df[column].values))
    return res


def get_custom_response(df, by, chart_type, x_data):
    res = {}
    df = df.round(2).fillna("")

    res["start"] = df.index[0].strftime("%Y/%m/%d")
    res["end"] = df.index[-1].strftime("%Y/%m/%d")
    if chart_type == "timing":
        if not x_data:
            time_format = "%Y/%m/%d" if by == "d" else "%Y/%m/%d %H:%M:%S"
            time_data = [item.strftime(time_format) for item in df.index]
            for column in df.columns:

                items = list(zip(time_data, df[column].values))
                res[column] = [{"value": item} for item in items]

        else:
            for column in df.columns:
                res[column] = df[column].values
    else:
        x_data_values = df[x_data].values
        for column in df.columns:
            if column != x_data:
                res[column] = list(zip(x_data_values, df[column].values))

    return res


def get_conn_by_db(is_origin=True):
    """返回数据库连接，如果is_origin为True则返回原始数据库，否则返回计算值数据库

    :param is_origin: bool 是否访问原始数据库连接
    :return: 数据库连接
    """
    db = DB_ORIGIN if is_origin else DB_DC
    return create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(DB_USER, DB_PASSWORD, DB_HOST, db))


def is_day(df):
    if len(df) > 1:
        start, end = 0, 1
        times = pd.to_datetime(df.index)
        delta = times[end] - times[start]
        while delta.days != 1:
            if delta.days < 1:
                return False
            start += 1
            end += 1
        return True

    else:
        value = pd.to_datetime(df.index[0])

        return value.hour == value.minute == 0


def abnormal_data_handling(df, params):

    if "heat_supply" in df.columns:
        conformity_cols = [col for col in df.columns if col in [
            "cop", "wshp_cop", "max_load", "min_load", "avg_load", "high_temperature_plate_exchange_heat",
            "high_temperature_plate_exchange_heat_rate", "wshp_heat", "heating_guarantee_rate", "heat_supply_rate",
            "wshp_power_consume_bk", "cost_saving", "power_consumption", "wshp_power_consume",
            "co2_emission_reduction", "co2_equal_num"
        ]
                           ]

        df.loc[df["heat_supply"] < 500, conformity_cols] = 0
        if "heat_supply" not in params:
            return df.loc[:, params]

    if "WSHP_HeatLoad" in df.columns:
        if is_day(df):

            df["WSHP_HeatLoad"][df["WSHP_HeatLoad"] < 43200] = 0
        else:
            df["WSHP_HeatLoad"][df["WSHP_HeatLoad"] < 1800] = 0
    return df


# def get_common_sql(params, db, start, end, time_key, deal=True):
#     q_params = params
#
#     if deal:
#         if "heat_supply" not in params:
#             q_params = params + ["heat_supply"]
#
#     if len(q_params) == 1:
#         common_sql = "select * from {} where pointname = '{}' and {} between '{}' and '{}'".format(
#             db, q_params[0], time_key, start, end
#         )
#     else:
#         common_sql = "select * from {} where pointname in {} and {} between '{}' and '{}'".format(
#             db, tuple(q_params), time_key, start, end
#         )
#     return common_sql

def get_common_sql(params, db, start, end, time_key, deal=True):
    q_params = params

    if deal:
        if "heat_supply" not in params:
            q_params = params + ["heat_supply"]

    if len(q_params) == 1:
        common_sql = "select * from {} where point_name = '{}' and {} between '{}' and '{}'".format(
            db, q_params[0], time_key, start, end
        )
    else:
        common_sql = "select * from {} where point_name in {} and {} between '{}' and '{}'".format(
            db, tuple(q_params), time_key, start, end
        )
    return common_sql


def get_common_df(params, db, start, end, time_key, engine, deal=True):
    sql = get_common_sql(params, db, start, end, time_key, deal)
    df = pd.read_sql(sql, con=engine)
    df = df.drop_duplicates(subset=[TIME_DATA_INDEX, POINT_NAME])
    # df = df.reset_index()
    return df.pivot(index=TIME_DATA_INDEX, columns=POINT_NAME, values=VALUE_NAME)

# def get_common_df(params, db, start, end, time_key, engine, deal=True):
#     sql = get_common_sql(params, db, start, end, time_key, deal)
#     df = pd.read_sql(sql, con=engine)
#     return df.pivot(index=time_key, columns="pointname", values="value")


def get_correspondence_with_temp_chart_response(df, last_df, time_range, value_column, temp_column="temp"):
    """获取XX（value_column）与XX(默认为温度)的关系

    :param df:实时数据
    :param last_df: 上周期数据
    :param time_range: 时间周期
    :param value_column: 变量名称
    :param temp_column: 比较变量名称
    :return:
    """
    res = {}
    df, last_df = map(lambda x: x.round(2).fillna(""), [df, last_df])

    # 处理时间参数 将 xxxx/xx/xx xx:xx:xx 后面的时分秒去除
    for k, v in time_range.items():
        if " " in v:
            res[k] = v.split(" ")[0]
        else:
            res[k] = v

    res["values"] = list(zip(df[temp_column].values, df[value_column].values))
    res["last_values"] = list(zip(last_df[temp_column].values, last_df[value_column].values))
    return res


def gen_response(df, time_index, by):
    """把df中每列加入到返回字典中

    :param df: dataframe数据
    :param time_index: 时间列名称，用户格式化字符串
    :param by: 时间跨度
    :return: 返回值字典
    """
    print(df)

    res = {}
    df = df.round(2).fillna("")
    for column in df.columns:
        if column == time_index:
            if by == "d":
                res[column] = [item.strftime("%Y/%m/%d") for item in df[column]]
            elif by == "h":
                res[column] = ["{}时".format(item.strftime("%Y/%m/%d %H")) for item in df[column]]

            res["start"] = df[column].iloc[0].strftime("%Y/%m/%d")
            res["end"] = df[column].iloc[-1].strftime("%Y/%m/%d")
        else:
            res[column] = df[column].values
    return res


def get_block_time_range(block):
    start_limit = {"cona": "2020/12/31 00:00:00", "kamba": "2020/08/17 00:00:00", "tianjin": "2022/03/15 00:00:00"}
    db = DATABASE[platform.system()]["data"]
    res = None
    engine = get_conn_by_db(False)
    conn = engine.connect()

    try:
        if block in ["cona", "kamba"]:
            res = conn.execute(
                f"select {TIME_DATA_INDEX} from {DB_NAME[block]['common']['h']} order by {TIME_DATA_INDEX} desc limit 1"
            )
        else:
            res = conn.execute(
                f"select {TIME_DATA_INDEX} from {DB_NAME[block]['common']} order by {TIME_DATA_INDEX} desc limit 1"
            )
        res = res.fetchone()

    finally:
        conn.close()
        engine.dispose()

    if not res:
        return None

    latest_time = res[0]
    _day = datetime.now() - timedelta(2)
    if latest_time.date() < _day.date():
        _day = latest_time

    start_date = _day - timedelta(6)
    start, end = start_date.strftime('%Y/%m/%d') + ' 00:00:00', _day.strftime('%Y/%m/%d') + ' 23:59:59'
    last_month_date = (_day - timedelta(29)).strftime('%Y/%m/%d') + ' 00:00:00'

    return {
        "start": start, "end": end, "last_month_date": last_month_date, "start_limit":
            start_limit[block], "end_limit": latest_time.strftime("%Y/%m/%d %H:%M:%S")
    }


def get_last_time_range(start, end):
    time_start, time_end = map(lambda x: datetime.strptime(x, "%Y/%m/%d %H:%M:%S") if "/" in x else datetime.strptime(x, "%Y-%m-%d %H:%M:%S"), [start, end])
    last_end = time_start - timedelta(days=1)
    delta = time_end - time_start
    if delta < timedelta(days=8):
        hint = "上月"
        last_start = last_end - timedelta(days=30)
    elif delta <= timedelta(days=30):
        hint = "上季度"
        last_start = last_end - timedelta(days=90)
    else:
        hint = "去年"
        last_start = last_end - timedelta(days=365)
    return {
        "hint": hint,
        "start": start,
        "end": end,
        "last_start": last_start.strftime("%Y/%m/%d %H:%M:%S"),
        "last_end": last_end.strftime("%Y/%m/%d %H:%M:%S")
    }


def get_last_time_by_delta(now, operate, delta, unit):
    if "-" in now:
        now = datetime.strptime(now, "%Y-%m-%d %H:%M:%S")
    else:
        now = datetime.strptime(now, "%Y/%m/%d %H:%M:%S")
    end = now - timedelta(days=1)
    days = {"y": 365, "m": 30, "d": 1}[unit] * delta
    start = {"+": operator.add, "-": operator.sub}[operate](end, timedelta(days=days))
    return {"start": start.strftime("%Y/%m/%d") + " 00:00:00", "end": end.strftime("%Y/%m/%d") + " 23:59:59"}


def file_iterator(filename, chunk_size=512):
    with open(filename, 'rb') as f:
        while True:
            c = f.read(chunk_size)
            if c:
                yield c
            else:
                break


def get_custom_variables_mapping(block):

    context = {
        "cona": {'日期': {'h': 'time_data', 'd': 'time_data'}, '高温版换制热量': {'h': 'high_temp_plate_exchange_heat_production', 'd': 'high_temp_plate_exchange_heat_production'}, '水源热泵制热量': {'h': 'water_heat_pump_heat_production', 'd': 'water_heat_pump_heat_production'}, '地热井可提供高温热量': {'h': 'geothermal_wells_high_heat_provide', 'd': 'geothermal_wells_high_heat_provide'}, '地热井可提供低温热量': {'h': 'geothermal_wells_low_heat_provide', 'd': 'geothermal_wells_low_heat_provide'}, 'COP能效': {'h': 'com_cop', 'd': 'com_cop'}, '节省供暖费用': {'h': 'cost_saving', 'd': 'cost_saving'}, '高温供暖费用': {'h': 'high_temp_charge', 'd': 'high_temp_charge'}, '低温供暖费用': {'h': 'low_temp_charge', 'd': 'low_temp_charge'}, '热力井供热量': {'h': 'heat_well_heating', 'd': 'heat_well_heating'}, '热力管网供热量': {'h': 'heat_pipe_network_heating', 'd': 'heat_pipe_network_heating'}, '水源热泵供热量': {'h': 'water_heat_pump_heat_production', 'd': 'water_heat_pump_heat_production'}, '高温板换供热量': {'h': 'high_temp_plate_exchange_heat_production', 'd': 'high_temp_plate_exchange_heat_production'}, '最大负荷': {'h': 'max_load', 'd': 'max_load'}, '最小负荷': {'h': 'min_load', 'd': 'min_load'}, '平均负荷': {'h': 'avg_load', 'd': 'avg_load'}, '供水温度': {'h': 'water_supply_temperature', 'd': 'water_supply_temperature'}, '回水温度': {'h': 'return_water_temperature', 'd': 'return_water_temperature'}, '供回水温差': {'h': 'supply_return_water_temp_diff', 'd': 'supply_return_water_temp_diff'}, '补水量': {'h': 'water_replenishment', 'd': 'water_replenishment'}, '补水量限值': {'h': 'water_replenishment_limit', 'd': 'water_replenishment_limit'}, '2号机房综合COP': {'h': 'f2_cop', 'd': 'f2_cop'}, '3号机房综合COP': {'h': 'f3_cop', 'd': 'f3_cop'}, '4号机房综合COP': {'h': 'f4_cop', 'd': 'f4_cop'}, '5号机房综合COP': {'h': 'f5_cop', 'd': 'f5_cop'}, '2号机房水源热泵COP': {'h': 'f2_whp_cop', 'd': 'f2_whp_cop'}, '3号机房水源热泵COP': {'h': 'f3_whp_cop', 'd': 'f3_whp_cop'}, '4号机房水源热泵COP': {'h': 'f4_whp_cop', 'd': 'f4_whp_cop'}, '5号机房水源热泵COP': {'h': 'f5_whp_cop', 'd': 'f5_whp_cop'}, '2号机房支路1供水温度': {'h': 'f2_HHWLoop001_ST', 'd': 'f2_HHWLoop001_ST'}, '3号机房支路1供水温度': {'h': 'f3_HHWLoop001_ST', 'd': 'f3_HHWLoop001_ST'}, '3号机房支路2供水温度': {'h': 'f3_HHWLoop002_ST', 'd': 'f3_HHWLoop002_ST'}, '3号机房支路3供水温度': {'h': 'f3_HHWLoop003_ST', 'd': 'f3_HHWLoop003_ST'}, '4号机房支路1供水温度': {'h': 'f4_HHWLoop001_ST', 'd': 'f4_HHWLoop001_ST'}, '5号机房支路1供水温度': {'h': 'f5_HHWLoop001_ST', 'd': 'f5_HHWLoop001_ST'}, '气温': {'d': 'temp'}},
        "kamba": {'日期': {'h': 'time_data', 'd': 'time_data'}, '蓄热水池低温热量': {'h': 'low_heat_of_storage', 'd': 'low_heat_of_storage'}, '蓄热水池高温热量': {'h': 'high_heat_of_storage', 'd': 'high_heat_of_storage'}, '电锅炉可替代供热天数': {'h': 'heat_supply_days', 'd': 'heat_supply_days'}, '水池平均温度': {'h': 'avg_pool_temperature', 'd': 'avg_pool_temperature'}, '系统综合COP': {'h': 'cop', 'd': 'cop'}, '水源热泵COP': {'h': 'wshp_cop', 'd': 'wshp_cop'}, '太阳能矩阵供水温度': {'h': 'solar_matrix_supply_water_temp', 'd': 'solar_matrix_supply_water_temp'}, '太阳能矩阵回水温度': {'h': 'solar_matrix_return_water_temp', 'd': 'solar_matrix_return_water_temp'}, '最大负荷': {'h': 'max_load', 'd': 'max_load'}, '最小负荷': {'h': 'min_load', 'd': 'min_load'}, '平均负荷': {'h': 'avg_load', 'd': 'avg_load'}, '末端供水温度': {'h': 'end_supply_water_temp', 'd': 'end_supply_water_temp'}, '末端回水温度': {'h': 'end_return_water_temp', 'd': 'end_return_water_temp'}, '末端供回水温差': {'h': 'end_return_water_temp_diff', 'd': 'end_return_water_temp_diff'}, '气温': {'h': 'temp', 'd': 'temp'}, '高温板换制热量': {'h': 'high_temperature_plate_exchange_heat', 'd': 'high_temperature_plate_exchange_heat'}, '水源热泵制热量': {'h': 'wshp_heat', 'd': 'wshp_heat'}, '高温板换制热功率': {'h': 'high_temperature_plate_exchange_heat_rate'}, '太阳能集热量': {'h': 'solar_collector', 'd': 'solar_collector'}, '太阳能辐射量': {'h': 'solar_radiation', 'd': 'solar_radiation'}, '总太阳能辐射量': {'h': 'total_solar_radiation', 'd': 'total_solar_radiation'}, '末端供热量': {'h': 'heat_supply', 'd': 'heat_supply'}, '集热系统流量': {'h': 'flow_rate', 'd': 'flow_rate'}, '集热系统供水温度': {'h': 'heat_collection_system_water_supply_temperature', 'd': 'heat_collection_system_water_supply_temperature'}, '集热系统回水温度': {'h': 'heat_collection_system_water_return_temperature', 'd': 'heat_collection_system_water_return_temperature'}, '供热率': {'h': 'heat_supply_rate', 'd': 'heat_supply_rate'}, '水源热泵耗电量': {'h': 'power_consume', 'd': 'power_consume'}, '节省供暖费用': {'h': 'cost_saving', 'd': 'cost_saving'}, '耗电量': {'h': 'power_consumption', 'd': 'power_consumption'}, 'CO2减排耗电量': {'h': 'co2_power_consume', 'd': 'co2_power_consume'}, 'CO2减排量': {'h': 'co2_emission_reduction', 'd': 'co2_emission_reduction'}, '等效种植数目数量': {'h': 'co2_emission_reduction', 'd': 'co2_equal_num'}, '供热端补水量': {'h': 'heat_water_replenishment', 'd': 'heat_water_replenishment'}, '供热端补水量限值': {'h': 'heat_water_replenishment_limit', 'd': 'heat_water_replenishment_limit'}, '蓄热水池补水量': {'h': 'heat_storage_tank_replenishment', 'd': 'heat_storage_tank_replenishment'}, '太阳能侧补水量': {'h': 'solar_side_replenishment', 'd': 'solar_side_replenishment'}, '太阳能侧补水量限值': {'h': 'solar_side_replenishment_limit', 'd': 'solar_side_replenishment_limit'}, '供暖保证率': {'d': 'heating_guarantee_rate'}, '集热效率': {'d': 'heat_collection_efficiency'}},
        "tianjin": {'日期': 'time_data', 'MAU201风机频率': 'fan_frequency_201', 'MAU202风机频率': 'fan_frequency_202', 'MAU203风机频率': 'fan_frequency_203', 'MAU301风机频率': 'fan_frequency_301', 'MAU401风机频率': 'fan_frequency_401', 'MAU201冷水阀开度': 'cold_water_valve_201', 'MAU202冷水阀开度': 'cold_water_valve_202', 'MAU203冷水阀开度': 'cold_water_valve_203', 'MAU301冷水阀开度': 'cold_water_valve_301', 'MAU401冷水阀开度': 'cold_water_valve_401', 'MAU201热水阀开度': 'hot_water_valve_201', 'MAU202热水阀开度': 'hot_water_valve_202', 'MAU203热水阀开度': 'hot_water_valve_203', 'MAU301热水阀开度': 'hot_water_valve_301', 'MAU401热水阀开度': 'hot_water_valve_301', 'MAU201送风压力': 'air_supply_pressure_201', 'MAU202送风压力': 'air_supply_pressure_202', 'MAU203送风压力': 'air_supply_pressure_203', 'MAU301送风压力': 'air_supply_pressure_301', 'MAU401送风压力': 'air_supply_pressure_401', 'MAU201送风湿度': 'air_supply_humidity_201', 'MAU202送风湿度': 'air_supply_humidity_202', 'MAU203送风湿度': 'air_supply_humidity_203', 'MAU301送风湿度': 'air_supply_humidity_301', 'MAU401送风湿度': 'air_supply_humidity_401', 'MAU201送风温度': 'air_supply_temperature_201', 'MAU202送风温度': 'air_supply_temperature_202', 'MAU203送风温度': 'air_supply_temperature_203', 'MAU301送风温度': 'air_supply_temperature_301', 'MAU401送风温度': 'air_supply_temperature_401'}
    }
    return context[block]


def check_custom_file(file):
    file_obj = pd.ExcelFile(file, engine="openpyxl")
    line_type_error = "变量类型(line_type)配置异常,line_type需与variables对应且选择规范的图线类型"
    variables_error = "变量(variables)配置异常,请确认对应数据板块下是否存在该变量"
    sheetname_error = "表名称(sheet_name)配置异常,请确认是否设置sheet_name或sheet_name是否包含在规定条目中"
    row_error = "行ID(row_id)配置异常,一行最多容纳两个图表"
    x_name_error = "横轴配置异常(xAxis)，请确认该图表类型是否支持对应横轴设置"

    # 检查sheetname
    if not file_obj.sheet_names:
        return False, sheetname_error

    sheet_name = file_obj.sheet_names[0]
    if sheet_name not in ["cona", "kamba", "tianjin"]:
        return False, sheetname_error

    # 检查字段
    df = file_obj.parse(sheet_name=sheet_name)
    columns = ["title", "x_name", "y_name", "y_unit", "variables", "line_types", "interval", "row_id"]
    df_columns = df.columns
    if len(df_columns) != len(columns):
        return False, variables_error

    for column in columns:
        if column not in df_columns:
            return False, variables_error

    variables = df["variables"].values
    interval = df["interval"].values
    variables_items = [[item, interval[i]] for i in range(len(interval)) for item in variables[i].split("-")]
    variables_mapping = get_custom_variables_mapping(sheet_name)

    for item, by in variables_items:
        if "tianjin" in sheet_name:
            if not variables_mapping.get(item):
                print(item, by, "未通过")
                return False, variables_error
        else:
            if not variables_mapping.get(item):
                print(item, by, "未通过")
                return False, variables_error
            else:
                if not variables_mapping[item].get(by):
                    print(item, by, "未通过")
                    return False, variables_error

    # 检查line_type
    line_types = df["line_types"].values
    for i in range(len(variables)):
        l_types = line_types[i].split("-")
        for item2 in ["heatmap", "pie", "area"]:
            if item2 in l_types:
                return False, line_type_error

        l_vars = variables[i].split("-")
        if len(l_types) != len(l_vars):
            return False, line_type_error

        if "bar" in l_types or "line" in l_types:
            if "scatter" in l_types:
                return False, line_type_error
        elif "scatter" in l_types:
            if "bar" in l_types or "line" in l_types:
                return False, line_type_error

    # 检查row_id
    row_id, num_left = df["row_id"].values, 2
    row_counter = collections.Counter(row_id)
    for item in row_counter.values():
        if item > 2:
            return False, row_error

    # print("row_id通过")

    # scatter x_name只能为气温 时序图 x_name只能为日期
    for c_xname, ctype in zip(df["x_name"], df["line_types"]):
        if "scatter" in ctype and c_xname != "气温":
            return False, x_name_error
        elif "scatter" not in ctype and c_xname != "日期":
            return False, x_name_error
    return True, "验证通过"


def get_latest_data(conn, table):
    res = conn.execute(f"select time_data from {table} order by time_data desc limit 1")
    res = res.fetchone()
    if res:
        return res[0].strftime("%Y/%m/%d 23:59:59")
    return datetime.today().strftime("%Y/%m/%d 23:59:59")

def get_box_data(start, end):
    # 暂时使用 之后重置数据库更新内容
    engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(
        "root",
        "cdqr2008",
        "121.199.48.82",
        # "localhost",
        "data_center_original"
    )
    )

    try:

        sql = f"""select * from kamba where pointname in ('SolarPT1_SEHCM01', 'SolarPT2_SEHCM01', 'SolarPT3_SEHCM01', 
                'SolarPT4_SEHCM01', 'SolarPT5_SEHCM01', 'SolarPT1_SEHCM02', 'SolarPT2_SEHCM02', 'SolarPT3_SEHCM02', 
                'SolarPT4_SEHCM02', 'SolarPT5_SEHCM02', 'SolarPT1_SEHCM03', 'SolarPT2_SEHCM03', 'SolarPT3_SEHCM03', 
                'SolarPT4_SEHCM03', 'SolarPT5_SEHCM03', 'SolarPT1_SEHCM04', 'SolarPT2_SEHCM04', 'SolarPT3_SEHCM04', 
                'SolarPT4_SEHCM04', 'SolarPT5_SEHCM04', 'SolarPT1_SEHCM05', 'SolarPT2_SEHCM05', 'SolarPT3_SEHCM05', 
                'SolarPT4_SEHCM05', 'SolarPT5_SEHCM05', 'SolarPT1_SEHCM06', 'SolarPT2_SEHCM06', 'SolarPT3_SEHCM06', 
                'SolarPT4_SEHCM06', 'SolarPT5_SEHCM06', 'SolarPT1_SEHCM07', 'SolarPT2_SEHCM07', 'SolarPT3_SEHCM07', 
                'SolarPT4_SEHCM07', 'SolarPT5_SEHCM07', 'SolarPT1_SEHCM08', 'SolarPT2_SEHCM08', 'SolarPT3_SEHCM08', 
                'SolarPT4_SEHCM08', 'SolarPT5_SEHCM08') and Timestamp between '{start}' and '{end}'"""

        result_df = pd.read_sql(sql, con=engine).pivot(index='Timestamp', columns='pointname', values='value')
        res = pd.Series(dtype="float64")
        if "Timestamp" in result_df.index:
            result_df = result_df.set_index("Timestamp")

        for column in result_df.columns:
            res = res.append(result_df[column])

        res_df = pd.DataFrame({"Timestamp": res.index, "value": res.values})
        res_df = res_df.set_index("Timestamp")
        res_df = res_df.dropna(how="all")
        res_df = res_df.resample("h")
        x, y = [], []
        for k, v in res_df:
            x.append(k.strftime("%Y/%m/%d %H:%M"))
            y.append(v["value"].tolist())
        return x, y

    finally:

        engine.dispose()