# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/7/6 11:02
# @Author  : MAYA
from sqlalchemy import create_engine
import pymysql
from data_center.tools import get_common_sql

import pandas as pd


engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(
                    "root",
                    "299521",
                    "localhost",
                    "data_center_statistical"
                )
        )

df = pd.read_sql(
    """
    select * from cona_days_data where pointname in ("high_temp_plate_exchange_heat_production", "water_heat_pump_heat_production", 
    "geothermal_wells_high_heat_provide", "geothermal_wells_low_heat_provide") and time_data between '2021-05-08 00:00:00' and '2021-05-14 23:59:59'
    """, con=engine
).pivot_table(index="time_data", columns="pointname", values="value")
print(df)
time_format = "%Y/%m/%d"
time_data = list(map(lambda x: x.strftime(time_format), df.index))
print(time_data)
engine.dispose()
