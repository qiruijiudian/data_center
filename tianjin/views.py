from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.status import HTTP_200_OK, HTTP_500_INTERNAL_SERVER_ERROR, HTTP_404_NOT_FOUND
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from data_center.settings import DATABASE
from data_center.tools import gen_response, gen_time_range, get_common_response, get_last_time_range, \
    get_correspondence_with_temp_chart_response, get_common_sql, gen_time_range, get_last_time_by_delta, \
    get_compare_with_item
import platform


class TianjinView(APIView):
    def post(self, request):
        plate_form = platform.system()
        time_index = "time_data"
        by = "h"
        data = {}

        # 获取参数
        key = request.data.get('key', None)
        start = request.data.get('start', None)
        end = request.data.get('end', None)

        if not key:
            return Response({"msg": "params error"}, status=HTTP_404_NOT_FOUND)

        if not all([key, start, end]):
            return Response({"msg": "params error"}, status=HTTP_404_NOT_FOUND)

        db = "tianjin_commons_data"

        engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(
                    DATABASE[plate_form]["data"]["user"],
                    DATABASE[plate_form]["data"]["password"],
                    DATABASE[plate_form]["data"]["host"],
                    DATABASE[plate_form]["data"]["database"]
                )
        )
        try:

            if key == "panel_data":
                params = [
                    "air_supply_pressure_201", "air_supply_pressure_202", "air_supply_pressure_203", "air_supply_pressure_301", "air_supply_pressure_401",
                    "air_supply_humidity_201", "air_supply_humidity_202", "air_supply_humidity_203", "air_supply_humidity_301", "air_supply_humidity_401",
                    "air_supply_temperature_201", "air_supply_temperature_202", "air_supply_temperature_203", "air_supply_temperature_301", "air_supply_temperature_401"
                ]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                df = df.round(2)
                data.update(
                    {
                        "max_temperature": "{} ℃".format(df.loc[:, ["air_supply_temperature_201", "air_supply_temperature_202", "air_supply_temperature_203", "air_supply_temperature_301", "air_supply_temperature_401"]].max().values.max()),
                        "max_humidity": "{} %".format(df.loc[:, ["air_supply_humidity_201", "air_supply_humidity_202", "air_supply_humidity_203", "air_supply_humidity_301", "air_supply_humidity_401"]].max().values.max()),
                        "max_pressure": "{} Pa".format(df.loc[:, ["air_supply_pressure_202", "air_supply_pressure_203", "air_supply_pressure_301", "air_supply_pressure_401"]].max().values.max())
                    }
                )
            elif key == "mau_fan_frequency":
                params = ["time_data", "fan_frequency_201", "fan_frequency_202", "fan_frequency_203", "fan_frequency_301", "fan_frequency_401"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                for column in params[1:]:
                    df[column] = df[column] * 100
                data.update(get_common_response(df, time_index, by))
            elif key == "mau_water_valve_201":
                params = ["time_data", "cold_water_valve_201", "hot_water_valve_201"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                data.update(get_common_response(df, time_index, by))
            elif key == "mau_water_valve_202":
                params = ["time_data", "cold_water_valve_202", "hot_water_valve_202"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                data.update(get_common_response(df, time_index, by))
            elif key == "mau_water_valve_203":
                params = ["time_data", "cold_water_valve_203", "hot_water_valve_203"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                data.update(get_common_response(df, time_index, by))
            elif key == "mau_water_valve_301":
                params = ["time_data", "cold_water_valve_301", "hot_water_valve_301"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                data.update(get_common_response(df, time_index, by))
            elif key == "mau_water_valve_401":
                params = ["time_data", "cold_water_valve_401", "hot_water_valve_401"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                data.update(get_common_response(df, time_index, by))
            elif key == "mau_air_supply_temp_and_humidity_201":
                params = ["time_data", "air_supply_temperature_201", "air_supply_humidity_201"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                data.update(get_common_response(df, time_index, by))
            elif key == "mau_air_supply_temp_and_humidity_202":
                params = ["time_data", "air_supply_temperature_202", "air_supply_humidity_202"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                data.update(get_common_response(df, time_index, by))
            elif key == "mau_air_supply_temp_and_humidity_203":
                params = ["time_data", "air_supply_temperature_203", "air_supply_humidity_203"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                data.update(get_common_response(df, time_index, by))
            elif key == "mau_air_supply_temp_and_humidity_301":
                params = ["time_data", "air_supply_temperature_301", "air_supply_humidity_301"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                data.update(get_common_response(df, time_index, by))
            elif key == "mau_air_supply_temp_and_humidity_401":
                params = ["time_data", "air_supply_temperature_401", "air_supply_humidity_401"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                data.update(get_common_response(df, time_index, by))
            elif key == "mau_air_supply_temperature":

                params = ["time_data", "air_supply_temperature_201", "air_supply_temperature_202", "air_supply_temperature_203",
                          "air_supply_temperature_301", "air_supply_temperature_401"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                data.update(get_common_response(df, time_index, by))
            elif key == "mau_air_supply_humidity":
                params = ["time_data", "air_supply_humidity_201", "air_supply_humidity_202",
                          "air_supply_humidity_203",
                          "air_supply_humidity_301", "air_supply_humidity_401"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                data.update(get_common_response(df, time_index, by))
            elif key == "mau_air_supply_pressure":
                params = ["time_data", "air_supply_pressure_201", "air_supply_pressure_202", "air_supply_pressure_203", "air_supply_pressure_301", "air_supply_pressure_401"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                data.update(get_common_response(df, time_index, by))
            elif key == "air_temperature_and_humidity":

                params = ["time_data", "air_temperature", "air_humidity"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                data.update(get_common_response(df, time_index, by))

            elif key == "mau_data_with_temp":
                params = ["time_data", "air_temperature"]
                item = request.data.get("item")
                if item == "sat":
                    params.extend(["air_supply_temperature_201", "air_supply_temperature_202", "air_supply_temperature_203", "air_supply_temperature_301", "air_supply_temperature_401"])
                elif item == "sap":
                    params.extend(["air_supply_pressure_201", "air_supply_pressure_202", "air_supply_pressure_203", "air_supply_pressure_301", "air_supply_pressure_401"])
                elif item == "sah":
                    params.extend(["air_supply_humidity_201", "air_supply_humidity_202", "air_supply_humidity_203", "air_supply_humidity_301", "air_supply_humidity_401"])

                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                if item == "sat":
                    df["set_point"] = 8.2
                elif item == "sap":
                    df["set_point"] = 10.2
                else:
                    df["set_point"] = 99.9
                data.update(get_compare_with_item(df, time_index, "air_temperature"))
            elif key == "mau_set_point_with_temp":

                params = ["time_data", "air_temperature"]
                item = request.data.get("item")

                if "temperature" in item:
                    set_point = 8.2
                elif "pressure" in item:
                    set_point = 10.2
                else:
                    set_point = 99.9

                params.append(item)

                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                df["set_point"] = set_point
                data.update(get_compare_with_item(df, time_index, "air_temperature"))
            elif key == "mau_air_supply_temperature_specify":
                temp_id = request.data.get("id")
                if not id:
                    return Response({"msg": "params error"}, status=HTTP_404_NOT_FOUND)
                params = ["time_data", "air_supply_temperature_" + temp_id]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                data.update(get_common_response(df, time_index, by))

            elif key == "mau_air_supply_humidity_specify":
                hum_id = request.data.get("id")
                if not id:
                    return Response({"msg": "params error"}, status=HTTP_404_NOT_FOUND)
                params = ["time_data", "air_supply_humidity_" + hum_id]

                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                data.update(get_common_response(df, time_index, by))


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






