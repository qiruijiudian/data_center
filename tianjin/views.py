from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.status import HTTP_200_OK, HTTP_500_INTERNAL_SERVER_ERROR, HTTP_404_NOT_FOUND
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from data_center.settings import DATABASE, DB_NAME, TIME_DATA_INDEX
from data_center.tools import gen_response, gen_time_range, get_common_response, get_last_time_range, \
    get_correspondence_with_temp_chart_response, get_common_sql, gen_time_range, get_last_time_by_delta, \
    get_compare_with_item, get_common_df, get_conn_by_db
import platform


class TianjinView(APIView):
    def post(self, request):
        by = "h"
        data = {}

        # 获取参数
        key = request.data.get('key', None)
        start = request.data.get('start', None)
        end = request.data.get('end', None)
        block = request.data.get('block', None)

        if not all([key, start, end]):
            return Response({"msg": "params error"}, status=HTTP_404_NOT_FOUND)

        db = DB_NAME["tianjin"]["common"]

        engine = get_conn_by_db(False)
        try:

            if key in ["panel_data"]:
                if not block:
                    return Response({"msg": "params error"}, status=HTTP_404_NOT_FOUND)
                block = block.lower()

            if key == "panel_data":

                if block == "mau":
                    params = ["air_supply_pressure", "air_supply_humidity", "air_supply_temperature"]
                    nums = ["201", "202", "203", "301", "401"]

                else:
                    params = ["air_supply_pressure", "return_air_humidity", "return_air_temperature"]
                    nums = ["101", "102", "103", "104", "105", "106", "108", "109", "110", "201", "202", "203",  "204",
                            "205", "206", "207-1", "207-2", "207-3", "207-4", "207-5", "207-6", "207-7", "207-8", "209",
                            "301", "302", "303", "401", "402-1", "402-2", "402-3", "402-4"]

                params = [f"{block}_{param}_{num}" for param in params for num in nums]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                df = df.round(2)
                data.update(
                    {
                        "max_temperature": "{} ℃".format(df.loc[:, [item for item in df.columns if "temperature" in item]].max().values.max()),
                        "max_humidity": "{} %".format(df.loc[:, [item for item in df.columns if "humidity" in item]].max().values.max()),
                        "max_pressure": "{} Pa".format(df.loc[:, [item for item in df.columns if "pressure" in item]].max().values.max())
                    }
                )
            elif key == "mau_fan_frequency":
                params = ["mau_fan_frequency_201", "mau_fan_frequency_202", "mau_fan_frequency_203", "mau_fan_frequency_301", "mau_fan_frequency_401"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                for column in params:
                    df[column] = df[column] * 100
                data.update(get_common_response(df, TIME_DATA_INDEX, by))
            elif key == "mau_water_valve_201":
                params = ["time_data", "cold_water_valve_201", "hot_water_valve_201"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                data.update(get_common_response(df, by))
            elif key == "mau_water_valve_202":
                params = ["cold_water_valve_202", "hot_water_valve_202"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                data.update(get_common_response(df, by))
            elif key == "mau_water_valve_203":
                params = ["cold_water_valve_203", "hot_water_valve_203"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                data.update(get_common_response(df, by))
            elif key == "mau_water_valve_301":
                params = ["cold_water_valve_301", "hot_water_valve_301"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                data.update(get_common_response(df, by))
            elif key == "mau_water_valve_401":
                params = ["cold_water_valve_401", "hot_water_valve_401"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                data.update(get_common_response(df, by))
            elif key == "mau_air_supply_temp_and_humidity_201":
                params = ["air_supply_temperature_201", "air_supply_humidity_201"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                data.update(get_common_response(df, by))
            elif key == "mau_air_supply_temp_and_humidity_202":
                params = ["air_supply_temperature_202", "air_supply_humidity_202"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                data.update(get_common_response(df, by))
            elif key == "mau_air_supply_temp_and_humidity_203":
                params = ["air_supply_temperature_203", "air_supply_humidity_203"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                data.update(get_common_response(df, by))
            elif key == "mau_air_supply_temp_and_humidity_301":
                params = ["air_supply_temperature_301", "air_supply_humidity_301"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                data.update(get_common_response(df, by))
            elif key == "mau_air_supply_temp_and_humidity_401":
                params = ["air_supply_temperature_401", "air_supply_humidity_401"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                data.update(get_common_response(df, by))
            elif key == "mau_air_supply_temperature":

                params = ["air_supply_temperature_201", "air_supply_temperature_202", "air_supply_temperature_203",
                          "air_supply_temperature_301", "air_supply_temperature_401"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                data.update(get_common_response(df, by))
            elif key == "mau_air_supply_temperature_specify":
                temp_id = request.data.get("id")
                if not id:
                    return Response({"msg": "params error"}, status=HTTP_404_NOT_FOUND)
                params = ["air_supply_temperature_" + temp_id]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                data.update(get_common_response(df, by))
            elif key == "mau_air_supply_humidity":
                params = ["air_supply_humidity_201", "air_supply_humidity_202",
                          "air_supply_humidity_203",
                          "air_supply_humidity_301", "air_supply_humidity_401"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                data.update(get_common_response(df, by))
            elif key == "mau_air_supply_humidity_specify":
                hum_id = request.data.get("id")
                if not id:
                    return Response({"msg": "params error"}, status=HTTP_404_NOT_FOUND)
                params = ["air_supply_humidity_" + hum_id]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                data.update(get_common_response(df, by))
            elif key == "mau_air_supply_pressure":
                params = ["air_supply_pressure_201", "air_supply_pressure_202", "air_supply_pressure_203", "air_supply_pressure_301", "air_supply_pressure_401"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                data.update(get_common_response(df, by))
            elif key == "air_temperature_and_humidity":

                params = ["air_temperature", "air_humidity"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                data.update(get_common_response(df, by))
            elif key == "mau_data_with_temp":
                params = ["air_temperature"]
                item = request.data.get("item")
                if item == "sat":
                    params.extend(["mau_air_supply_temperature_201", "mau_air_supply_temperature_202", "mau_air_supply_temperature_203", "mau_air_supply_temperature_301", "mau_air_supply_temperature_401"])
                elif item == "sap":
                    params.extend(["mau_air_supply_pressure_201", "mau_air_supply_pressure_202", "mau_air_supply_pressure_203", "mau_air_supply_pressure_301", "mau_air_supply_pressure_401"])
                elif item == "sah":
                    params.extend(["mau_air_supply_humidity_201", "mau_air_supply_humidity_202", "mau_air_supply_humidity_203", "mau_air_supply_humidity_301", "mau_air_supply_humidity_401"])

                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                if item == "sat":
                    df["set_point"] = 8.2
                elif item == "sap":
                    df["set_point"] = 10.2
                else:
                    df["set_point"] = 99.9
                data.update(get_compare_with_item(df, "air_temperature"))
            elif key == "mau_set_point_with_temp":

                params = ["air_temperature"]
                item = request.data.get("item")

                if "temperature" in item:
                    set_point = 8.2
                elif "pressure" in item:
                    set_point = 10.2
                else:
                    set_point = 99.9

                params.append(item)

                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                df["set_point"] = set_point
                data.update(get_compare_with_item(df, "air_temperature"))
            elif key == "combined_data":
                item_id = request.data.get("id")
                if not item_id:
                    return Response({"msg": "params error"}, status=HTTP_404_NOT_FOUND)
                params = [item.format(item_id) for item in [
                    "mau_fan_frequency_{}", "mau_cold_water_valve_{}", "mau_hot_water_valve_{}", "mau_air_supply_humidity_{}",
                    "mau_air_supply_temperature_{}"]
                          ]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                for column in df.columns:
                    if "fan_frequency" in column:
                        df[column] = np.floor(df[column] * 100)
                data.update(get_common_response(df, by))

        except Exception as e:
            # print("异常", e)
            import traceback
            traceback.print_exc()
            engine.dispose()
        finally:
            engine.dispose()
            if data:
                return Response(data, status=HTTP_200_OK)
            else:
                return Response({"msg": "data error"}, status=HTTP_500_INTERNAL_SERVER_ERROR)






