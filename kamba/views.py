from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.status import HTTP_200_OK, HTTP_500_INTERNAL_SERVER_ERROR, HTTP_404_NOT_FOUND
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from data_center.settings import DATABASE
from data_center.tools import gen_response, gen_time_range, get_common_response, get_last_time_range, \
    get_correspondence_with_temp_chart_response, get_common_sql, gen_time_range, get_last_time_by_delta
import platform


class KambaView(APIView):
    def post(self, request):
        plate_form = platform.system()
        time_index = "time_data"
        data = {}

        # 获取参数
        key = request.data.get('key', None)
        start = request.data.get('start', None)
        end = request.data.get('end', None)
        by = request.data.get('by', None)

        if not key:
            return Response({"msg": "params error"}, status=HTTP_404_NOT_FOUND)

        if not all([key, start, end]):
            return Response({"msg": "params error"}, status=HTTP_404_NOT_FOUND)

        db = "kamba_hours_data" if by and by.strip() == "h" else "kamba_days_data"

        engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(
                    DATABASE[plate_form]["user"],
                    DATABASE[plate_form]["password"],
                    DATABASE[plate_form]["host"],
                    DATABASE[plate_form]["database"]
                )
        )
        try:
            if key == "geothermal_wells_provide_heat":
                params = ["time_data", "high_temp_plate_exchange_heat_production", "water_heat_pump_heat_production",
                          "geothermal_wells_high_heat_provide", "geothermal_wells_low_heat_provide"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                data.update(get_common_response(df, time_index, by))
            elif key == "panel_data":
                params, db = ["max_load", "min_load", "cost_saving"], "kamba_days_data"
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                # TODO 小于0异常值处理
                for param in params:
                    df[param] = df[param].apply(lambda x: x if x >= 0 else 0)
                cost_saving_sum = df["cost_saving"].sum()
                cs_sum = cost_saving_sum / 10000
                if cs_sum >= 1:
                    if cs_sum > 10:
                        cost_saving_sum = "{} 万元".format(round(cs_sum))
                    else:
                        cost_saving_sum = "{} 万元".format(round(cs_sum, 1))
                else:
                    cost_saving_sum = "{} 元".format(round(cost_saving_sum, 2))

                data.update(
                    {
                        "max_load": "{} KW".format(df["max_load"].max().round(2)),
                        "min_load": "{} KW".format(df["min_load"].min().round(2)),
                        "cost_saving_total": cost_saving_sum
                    }
                )

            elif key == "heat_storage_tank_heating":
                params = ["time_data", "high_heat_of_storage", "low_heat_of_storage"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                data.update(get_common_response(df, time_index, by))
            elif key == "alternative_heating_days":
                params = ["time_data", "high_heat_of_storage", "heat_supply_days"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                data.update(get_common_response(df, time_index, by))
            elif key == "com_cop":
                df = pd.read_sql(get_common_sql(["time_data", "cop"], db, start, end, time_index), con=engine)
                df["Target Minimum"] = 6
                df["Low Threshold"] = 4
                data.update(get_common_response(df, time_index, by))
                data["status"] = "数据异常" if ("" in df["cop"].values or None in df["cop"].values) else "正常"
            elif key == "wshp_cop":
                df = pd.read_sql(get_common_sql(["time_data", "wshp_cop"], db, start, end, time_index), con=engine)
                df["Target Minimum"] = 6
                df["Low Threshold"] = 4
                data.update(get_common_response(df, time_index, by))
                data["status"] = "数据异常" if ("" in df["wshp_cop"].values or None in df["wshp_cop"].values) else "正常"
            elif key == "pool_temperature_heatmap":
                db = "kamba_hours_pool_data"
                if not end:
                    return Response({"msg": "params error"}, status=HTTP_404_NOT_FOUND)
                day = end.split(" ")[0]
                start = "{} 00:00:00".format(day)
                data["date"] = day

                params = ['Timestamp', 'Pit_LT01_0m00cm', 'Pit_LT02_0m20cm', 'Pit_LT03_0m40cm', 'Pit_LT04_0m60cm', 'Pit_LT05_0m80cm', 'Pit_LT06_1m00cm', 'Pit_LT07_1m20cm', 'Pit_LT08_1m40cm', 'Pit_LT09_1m60cm', 'Pit_LT10_1m80cm', 'Pit_LT11_2m00cm', 'Pit_LT12_2m20cm', 'Pit_LT13_2m40cm', 'Pit_LT14_2m60cm', 'Pit_LT15_2m80cm', 'Pit_LT16_3m00cm', 'Pit_MT01_3m20cm', 'Pit_MT02_3m40cm', 'Pit_MT03_3m60cm', 'Pit_MT04_3m80cm', 'Pit_MT05_4m00cm', 'Pit_MT06_4m20cm', 'Pit_MT07_4m40cm', 'Pit_MT08_4m60cm', 'Pit_MT09_4m80cm', 'Pit_MT10_5m00cm', 'Pit_MT11_5m20cm', 'Pit_MT12_5m40cm', 'Pit_HT01_5m73cm', 'Pit_HT02_6m06cm', 'Pit_HT03_6m39cm', 'Pit_HT04_6m72cm', 'Pit_HT05_7m05cm', 'Pit_HT06_7m38cm', 'Pit_HT07_7m71cm', 'Pit_HT08_8m04cm', 'Pit_HT09_8m37cm', 'Pit_HT10_8m70cm', 'Pit_HT11_9m03cm', 'Pit_HT12_9m36cm']
                height = ['0', '0.2', '0.4', '0.6', '0.8', '1', '1.2', '1.4', '1.6', '1.8', '2', '2.2', '2.4',
                          '2.6', '2.8', '3', '3.2', '3.4', '3.6', '3.8', '4', '4.2', '4.4', '4.6', '4.8', '5', '5.2',
                          '5.4', '5.73', '6.06', '6.39', '6.72', '7.05', '7.38', '7.71', '8.04', '8.37', '8.7', '9.03',
                          '9.36']

                df = pd.read_sql(get_common_sql(params, db, start, end, "Timestamp"), con=engine)
                df = df.round(2).fillna("")
                res = {"0-4": {},  "4-8": {},  "8-12": {},  "12-16": {}, "16-20": {}, "20-24": {}}
                max_num, min_num, values = -np.inf, np.inf, []
                for column_index, column in enumerate(params[1:]):
                    for index in df.index:
                        hour = df.loc[index, "Timestamp"].hour
                        value = df.loc[index, column]
                        if 0 <= hour < 4:
                            hour_key = "0-4"
                        elif 4 <= hour < 8:
                            hour_key = "4-8"
                        elif 8 <= hour < 12:
                            hour_key = "8-12"
                        elif 12 <= hour < 16:
                            hour_key = "12-16"
                        elif 16 <= hour < 20:
                            hour_key = "16-20"
                        else:
                            hour_key = "20-24"

                        if height[column_index] not in res[hour_key]:
                            res[hour_key][height[column_index]] = [value]
                        else:
                            res[hour_key][height[column_index]].append(value)

                        if isinstance(value, float) and value > max_num:
                            max_num = value
                        if isinstance(value, float) and value < min_num:
                            min_num = value
                for hour_item, value_item in res.items():
                    for height_item, value_item2 in value_item.items():
                        heat_value = [hour_item, height_item, round(sum(value_item2)/len(value_item2), 2)]
                        if heat_value not in values:
                            values.append(heat_value)
                data["values"] = values
                data["max"] = max_num
                data["min"] = min_num
                data["sizes"] = height
                data["time"] = ["0-4", "4-8", "8-12", "12-16", "16-20", "20-24"]

            elif key == "solar_collector":
                df = pd.read_sql(get_common_sql(["time_data", "solar_collector"], db, start, end, time_index), con=engine)

                data.update(get_common_response(df, time_index, by))
            elif key == "solar_matrix_water_temperature":
                params = ["time_data", "solar_matrix_supply_water_temp", "solar_matrix_return_water_temp"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)

                data.update(get_common_response(df, time_index, by))
            elif key == "load":
                params = ["time_data", "max_load", "min_load", "avg_load"]

                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                # TODO 小于0异常值处理
                for param in params[1:]:
                    df[param] = df[param].apply(lambda x: x if x >= 0 else 0)

                data.update(get_common_response(df, time_index, by))
            elif key == "end_water_supply_with_temp":
                params = ["time_data", "end_supply_water_temp", "temp"]

                time_range = get_last_time_range(start, end)
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)

                last_df = pd.read_sql(
                    get_common_sql(params, db, time_range["last_start"], time_range["last_end"], time_index), con=engine
                )

                data.update(get_correspondence_with_temp_chart_response(df, last_df, time_range, "end_supply_water_temp"))
            elif key == "end_water_return_with_temp":
                params = ["time_data", "end_return_water_temp", "temp"]

                time_range = get_last_time_range(start, end)
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)

                last_df = pd.read_sql(
                    get_common_sql(params, db, time_range["last_start"], time_range["last_end"], time_index), con=engine
                )

                data.update(
                    get_correspondence_with_temp_chart_response(df, last_df, time_range, "end_return_water_temp"))
            elif key == "end_water_diff_with_temp":
                params = ["time_data", "end_return_water_temp_diff", "temp"]

                time_range = get_last_time_range(start, end)
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)

                last_df = pd.read_sql(
                    get_common_sql(params, db, time_range["last_start"], time_range["last_end"], time_index), con=engine
                )

                data.update(
                    get_correspondence_with_temp_chart_response(df, last_df, time_range, "end_return_water_temp_diff"))
            elif key == "end_water_temperature_compare":
                params = ["time_data", "end_supply_water_temp", "end_return_water_temp", "end_return_water_temp_diff", "temp"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                last_time = get_last_time_by_delta(start, "-", 1, "y")

                last_df = pd.read_sql(
                    get_common_sql(params, db, last_time["start"], last_time["end"], time_index), con=engine
                )

                df, last_df = df.round(2).fillna(""), last_df.round(2).fillna("")
                temp, last_temp = df["temp"].values, last_df["temp"].values
                for column in ["end_supply_water_temp", "end_return_water_temp", "end_return_water_temp_diff"]:
                    data[column] = list(zip(temp, df[column].values))
                    data["last_" + column] = list(zip(last_temp, last_df[column].values))
                df_start, df_end = df.iloc[0]["time_data"], df.iloc[-1]["time_data"]
                last_df_start, last_df_end = last_df.iloc[0]["time_data"], last_df.iloc[-1]["time_data"]
                data["start"] = df_start.strftime("%Y/%m/%d")
                data["end"] = df_end.strftime("%Y/%m/%d")
                data["last_year_start"] = last_df_start.strftime("%Y/%m/%d")
                data["last_year_end"] = last_df_end.strftime("%Y/%m/%d")
            elif key == "solar_collector_analysis":
                if by == "h":
                    return Response({"msg": "params error"}, status=HTTP_404_NOT_FOUND)
                df = pd.read_sql(get_common_sql(["time_data", "heat_supply", "solar_collector", "heating_guarantee_rate"], db, start, end, time_index),
                                 con=engine)
                df["heating_guarantee_rate"] = df["heating_guarantee_rate"] * 100

                # TODO 小于0异常值处理
                for param in ["heat_supply", "solar_collector", "heating_guarantee_rate"]:
                    df[param] = df[param].apply(lambda x: x if x >= 0 else 0)

                data.update(get_common_response(df, time_index, by))
            elif key == "heating_analysis":
                params = ["time_data", "high_temperature_plate_exchange_heat", "wshp_heat"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                # TODO 小于0异常值处理
                for param in params[1:]:
                    df[param] = df[param].apply(lambda x: x if x >= 0 else 0)

                data.update(get_common_response(df, time_index, by))
            elif key == "heat_production":
                params = ["time_data", "heat_supply", "power_consume", "heat_supply_rate"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                df["heat_supply_rate"] = df["heat_supply_rate"] * 100
                # TODO 小于0异常值处理
                for param in params[1:]:
                    df[param] = df[param].apply(lambda x: x if x >= 0 else 0)
                data.update(get_common_response(df, time_index, by))
            elif key == "high_temperature_plate_exchange_heat_rate":
                if by == "d":
                    return Response({"msg": "params error"}, status=HTTP_404_NOT_FOUND)
                params = ["time_data", "high_temperature_plate_exchange_heat_rate"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                data.update(get_common_response(df, time_index, by))
            elif key == "cost_saving":
                params = ["time_data", "cost_saving", "power_consumption"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                # TODO 异常值 < 0
                df["cost_saving"] = df["cost_saving"].apply(lambda x: x if x >= 0 else 0)
                data.update(get_common_response(df, time_index, by))
            elif key == "end_water_temperature":
                params = ["time_data", "end_supply_water_temp", "end_return_water_temp", "end_return_water_temp_diff"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                data.update(get_common_response(df, time_index, by))
            elif key == "heat_storage_tank_replenishment":
                df = pd.read_sql(get_common_sql(["time_data", "heat_storage_tank_replenishment"], db, start, end, time_index), con=engine)
                data.update(get_common_response(df, time_index, by))
            elif key == "heat_replenishment":
                params = ["time_data", "heat_water_replenishment", "heat_water_replenishment_limit"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                data.update(get_common_response(df, time_index, by))
            elif key == "solar_side_replenishment":
                params = ["time_data", "solar_side_replenishment", "solar_side_replenishment_limit"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                data.update(get_common_response(df, time_index, by))
            elif key == "emission_reduction":
                params = ["time_data", "co2_emission_reduction", "co2_power_consume"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                df["co2_emission_reduction"] = df["co2_emission_reduction"].cumsum()
                df["co2_power_consume"] = df["co2_power_consume"].cumsum()
                data.update(get_common_response(df, time_index, by))
            elif key == "co2_equal_data":
                params = ["time_data", "co2_emission_reduction", "co2_equal_num"]
                df = pd.read_sql(get_common_sql(params, db, start, end, time_index), con=engine)
                data.update(get_common_response(df, time_index, by))

        except Exception as e:
            print("异常", e)
            import traceback
            traceback.print_exc()
            engine.dispose()
        finally:
            engine.dispose()
            if data:
                return Response(data, status=HTTP_200_OK)
            else:
                return Response({"msg": "data error"}, status=HTTP_500_INTERNAL_SERVER_ERROR)






