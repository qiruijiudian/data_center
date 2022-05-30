# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/5/18 17:23
# @Author  : MAYA
from datetime import datetime, timedelta
import pymysql
import platform
from data_center.settings import DATABASE


def gen_time_range(df, time_index):
    res = {
        "start": df[time_index].iloc[0].strftime("%Y/%m/%d"),
        "end": df[time_index].iloc[-1].strftime("%Y/%m/%d")
    }
    return res


def get_common_response(df, time_index, by, is_timing=True):
    res = {}
    df = df.round(2).fillna("")

    res["start"] = df[time_index].iloc[0].strftime("%Y/%m/%d")
    res["end"] = df[time_index].iloc[-1].strftime("%Y/%m/%d")
    if is_timing:
        time_format = "%Y/%m/%d" if by == "d" else "%Y/%m/%d %H:%M:%S"
        time_data = df[time_index].apply(lambda x: x.strftime(time_format))

        for column in df.columns:
            if column != time_index:
                items = list(zip(time_data.values, df[column].values))
                res[column] = [{"value": item} for item in items]

    return res


def get_common_sql(params, db, start, end, time_key):
    common_sql = "select {} from {} where {} between '{}' and '{}'"
    return common_sql.format(",".join(params), db, time_key, start, end)


def get_correspondence_with_temp_chart_response(df, last_df, time_range, value_column, temp_column="temp"):
    res = {}
    df, last_df = map(lambda x: x.round(2).fillna(""), [df, last_df])
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
    start_limit = {"cona": "2020/12/31 00:00:00", "kamba": "2020/08/17 00:00:00", "tianjin": ""}
    db = DATABASE[platform.system()]
    res = None
    with pymysql.connect(host=db["host"], user=db["user"], password=db["password"], database=db["database"]) as conn:
        cur = conn.cursor()
        cur.execute("select time_data from {}_hours_data order by time_data desc limit 1".format(block))
        res = cur.fetchone()
        cur.close()

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
