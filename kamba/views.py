from datetime import datetime, timedelta
import platform
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.status import HTTP_200_OK, HTTP_500_INTERNAL_SERVER_ERROR, HTTP_404_NOT_FOUND
import numpy as np
from data_center.settings import START_DATE, DB_NAME, TIME_DATA_INDEX, DATABASE, TIMESTAMP, TESTOPTION
from data_center.tools import get_common_response, get_last_time_range, get_correspondence_with_temp_chart_response, \
    get_last_time_by_delta, get_common_df, abnormal_data_handling, get_box_data, get_latest_data, \
    get_conn_by_db, convert_str_to_datetime


class KambaView(APIView):
    def post(self, request):
        block = "kamba"
        data = {}

        # 获取参数
        key = request.data.get('key', None)
        start = request.data.get('start', None)
        end = request.data.get('end', None)
        by = request.data.get('by', None)
        print(start, end, by, key)

        if not all([key, start, end]):
            return Response({"msg": "params error"}, status=HTTP_404_NOT_FOUND)
        
        db = DB_NAME[block]["common"]["d" if not by else by.strip()]

        # 区分测试与现网环境
        if TESTOPTION is True:
            engine = get_conn_by_db(False, DATABASE['Windows']['data']['database'])
        else:
            engine = get_conn_by_db(False)

        try:

            if key == "panel_data":
                params, db = ["avg_load", "co2_emission_reduction", "cost_saving"], DB_NAME[block]["common"]["d"]
                end = get_latest_data(engine, db)
                start = START_DATE[block]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                df = abnormal_data_handling(df, params)
                co2 = df["co2_emission_reduction"].interpolate(
                    method="index"
                ).interpolate(method="nearest").bfill().ffill().sum()
                cost = df["cost_saving"].sum()
                co2_sum = co2 * 1.964 / 1000
                co2 = f"{int(np.floor(co2_sum))} 吨" if co2_sum >= 100 else f"{int(np.floor(co2))} Kg"
                cs_sum = cost / 10000
                cost = "{} 万元".format(int(np.floor(cs_sum))) if cs_sum >= 10 else "{} 元".format(int(np.floor(cost)))
                data.update({"cost": cost, "avg_load": f'{int(np.floor(df["avg_load"].mean()))} kW', "co2": co2})

            elif key == "heat_storage_tank_heating":
                params = ["high_heat_of_storage", "low_heat_of_storage"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine, False)
                data.update(get_common_response(df, by))
            elif key == "alternative_heating_days":
                params = ["high_heat_of_storage", "heat_supply_days"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine, False)
                data.update(get_common_response(df, by))
            elif key == "com_cop":
                df = get_common_df(["cop"], db, start, end, TIME_DATA_INDEX, engine)
                df = abnormal_data_handling(df, ["cop"])
                df["Target Minimum"] = 6
                df["Low Threshold"] = 4
                if not df.empty:
                    data.update(get_common_response(df, by))
                data["status"] = "数据异常" if ("" in df["cop"].values or None in df["cop"].values) else "正常"
            elif key == "wshp_cop":
                df = get_common_df(["wshp_cop"], db, start, end, TIME_DATA_INDEX, engine)
                df = abnormal_data_handling(df, ["wshp_cop"])
                df["Target Minimum"] = 6
                df["Low Threshold"] = 4
                data.update(get_common_response(df, by))
                data["status"] = "数据异常" if (("" in df["wshp_cop"].values) or (None in df["wshp_cop"].values)) else "正常"
            elif key == "pool_temperature_heatmap":
                t = convert_str_to_datetime(end) - timedelta(days=1)
                db = DB_NAME[block]["pool"]["h"]
                day = t.strftime("%Y/%m/%d")
                start = "{} 00:00:00".format(day)
                end = "{} 23:59:59".format(day)
                data["date"] = day

                params = ['Pit_LT01_0m00cm', 'Pit_LT02_0m20cm', 'Pit_LT03_0m40cm', 'Pit_LT04_0m60cm', 'Pit_LT05_0m80cm',
                          'Pit_LT06_1m00cm', 'Pit_LT07_1m20cm', 'Pit_LT08_1m40cm', 'Pit_LT09_1m60cm', 'Pit_LT10_1m80cm',
                          'Pit_LT11_2m00cm', 'Pit_LT12_2m20cm', 'Pit_LT13_2m40cm', 'Pit_LT14_2m60cm', 'Pit_LT15_2m80cm',
                          'Pit_LT16_3m00cm', 'Pit_MT01_3m20cm', 'Pit_MT02_3m40cm', 'Pit_MT03_3m60cm', 'Pit_MT04_3m80cm',
                          'Pit_MT05_4m00cm', 'Pit_MT06_4m20cm', 'Pit_MT07_4m40cm', 'Pit_MT08_4m60cm', 'Pit_MT09_4m80cm',
                          'Pit_MT10_5m00cm', 'Pit_MT11_5m20cm', 'Pit_MT12_5m40cm', 'Pit_HT01_5m73cm', 'Pit_HT02_6m06cm',
                          'Pit_HT03_6m39cm', 'Pit_HT04_6m72cm', 'Pit_HT05_7m05cm', 'Pit_HT06_7m38cm', 'Pit_HT07_7m71cm',
                          'Pit_HT08_8m04cm', 'Pit_HT09_8m37cm', 'Pit_HT10_8m70cm', 'Pit_HT11_9m03cm', 'Pit_HT12_9m36cm']
                height = ['0', '0.2', '0.4', '0.6', '0.8', '1', '1.2', '1.4', '1.6', '1.8', '2', '2.2', '2.4', '2.6',
                          '2.8', '3', '3.2', '3.4', '3.6', '3.8', '4', '4.2', '4.4', '4.6', '4.8', '5', '5.2', '5.4',
                          '5.73', '6.06', '6.39', '6.72', '7.05', '7.38', '7.71', '8.04', '8.37', '8.7', '9.03', '9.36']

                df = get_common_df(params, db, start, end, TIMESTAMP, engine, False)
                # df = df.round(2).fillna("")
                df = df.round(2).dropna(how="any")
                res = {"0-4": {},  "4-8": {},  "8-12": {},  "12-16": {}, "16-20": {}, "20-24": {}}
                max_num, min_num, values = -np.inf, np.inf, []
                for column_index, column in enumerate(params[1:]):
                    for index in df.index:
                        hour = index.hour
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
                df = get_common_df(["solar_collector"], db, start, end, TIME_DATA_INDEX, engine, False)
                data.update(get_common_response(df, by))
            elif key == "solar_radiation":
                df = get_common_df(["solar_radiation"], db, start, end, TIME_DATA_INDEX, engine, False)

                start, end = "2022/01/01 00:00:00", "2022/01/31 23:59:59"
                data.update(get_common_response(df, by))
            elif key == "solar_collector_efficiency":
                df = get_common_df(["heat_collection_efficiency"], db, start, end, TIME_DATA_INDEX, engine, False)
                df["heat_collection_efficiency"] = df["heat_collection_efficiency"] * 100
                df["heat_collection_efficiency"] = df["heat_collection_efficiency"].apply(np.floor)
                data.update(get_common_response(df, by))
            elif key == "solar_matrix_water_temperature":
                params = ["time_data", "solar_matrix_supply_water_temp", "solar_matrix_return_water_temp"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine, False)

                data.update(get_common_response(df, by))
            elif key == "load":
                params = ["max_load", "min_load", "avg_load"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                df = abnormal_data_handling(df, params)

                data.update(get_common_response(df, by))
            elif key == "end_water_supply_with_temp":
                params = ["end_supply_water_temp", "temp"]

                time_range = get_last_time_range(start, end)
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine, False)
                last_df = get_common_df(
                    params, db, time_range["last_start"], time_range["last_end"], TIME_DATA_INDEX, engine, False
                )

                data.update(
                    get_correspondence_with_temp_chart_response(df, last_df, time_range, "end_supply_water_temp")
                )
            elif key == "end_water_return_with_temp":
                params = ["end_return_water_temp", "temp"]

                time_range = get_last_time_range(start, end)
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine, False)
                last_df = get_common_df(
                    params, db, time_range["last_start"], time_range["last_end"], TIME_DATA_INDEX, engine, False
                )

                data.update(
                    get_correspondence_with_temp_chart_response(df, last_df, time_range, "end_return_water_temp"))
            elif key == "end_water_diff_with_temp":
                params = ["time_data", "end_return_water_temp_diff", "temp"]

                time_range = get_last_time_range(start, end)
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine, False)
                last_df = get_common_df(
                    params, db, time_range["last_start"], time_range["last_end"], TIME_DATA_INDEX, engine, False)

                data.update(
                    get_correspondence_with_temp_chart_response(df, last_df, time_range, "end_return_water_temp_diff")
                )
            elif key == "end_water_temperature_compare":
                params = ["end_supply_water_temp", "end_return_water_temp", "end_return_water_temp_diff", "temp"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine, False)
                last_time = get_last_time_by_delta(start, "-", 1, "y")
                last_df = get_common_df(params, db, last_time["start"], last_time["end"], TIME_DATA_INDEX, engine, False)
                df, last_df = df.round(2).fillna(""), last_df.round(2).fillna("")
                temp, last_temp = df["temp"].values, last_df["temp"].values
                for column in ["end_supply_water_temp", "end_return_water_temp", "end_return_water_temp_diff"]:
                    data[column] = list(zip(temp, df[column].values))
                    data["last_" + column] = list(zip(last_temp, last_df[column].values))
                data["start"] = df.index[0].strftime("%Y/%m/%d")
                data["end"] = df.index[-1].strftime("%Y/%m/%d")
                data["last_year_start"] = last_df.index[0].strftime("%Y/%m/%d")
                data["last_year_end"] = last_df.index[-1].strftime("%Y/%m/%d")
            elif key == "heat_supply_with_temperature_compare":

                params = ["heat_supply", "temp"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine, False)
                last_time = get_last_time_by_delta(start, "-", 1, "y")
                last_df = get_common_df(params, db, last_time["start"], last_time["end"], TIME_DATA_INDEX, engine,
                                        False)
                df[df["heat_supply"] < 0] = 0
                last_df[last_df["heat_supply"] < 0] = 0
                df, last_df = df.round(2).fillna(0), last_df.round(2).fillna(0)

                temp, last_temp = df["temp"].values, last_df["temp"].values
                for column in ["heat_supply"]:
                    data[column] = list(zip(temp, df[column].values))
                    data["last_" + column] = list(zip(last_temp, last_df[column].values))
                data["start"] = df.index[0].strftime("%Y/%m/%d")
                data["end"] = df.index[-1].strftime("%Y/%m/%d")
                data["last_year_start"] = last_df.index[0].strftime("%Y/%m/%d")
                data["last_year_end"] = last_df.index[-1].strftime("%Y/%m/%d")

            elif key == "solar_collector_analysis":
                if by == "h":
                    return Response({"msg": "params error"}, status=HTTP_404_NOT_FOUND)

                df = get_common_df(
                    [
                        "heat_supply", "solar_collector", "heating_guarantee_rate"
                    ], db, start, end, TIME_DATA_INDEX, engine
                )
                df = abnormal_data_handling(df, ["heat_supply", "solar_collector", "heating_guarantee_rate"])

                df["heating_guarantee_rate"] = np.floor(df["heating_guarantee_rate"] * 100)

                data.update(get_common_response(df, TIME_DATA_INDEX, by))
            elif key == "heating_analysis":
                params = ["high_temperature_plate_exchange_heat", "wshp_heat"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                # TODO 临时处理
                df.loc[df["wshp_heat"] < 0, "wshp_heat"] = 0
                df = abnormal_data_handling(df, params)

                data.update(get_common_response(df, by))
            elif key == "heat_production":
                params = ["heat_supply", "wshp_power_consume", "heat_supply_rate"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                df["heat_supply_rate"] = np.floor(df["heat_supply_rate"] * 100)
                df = abnormal_data_handling(df, params)
                data.update(get_common_response(df, by))
            elif key == "high_temperature_plate_exchange_heat_rate":
                params = ["high_temperature_plate_exchange_heat_rate"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                df = abnormal_data_handling(df, params)
                data.update(get_common_response(df, by))
            elif key == "cost_saving":
                params = ["cost_saving", "power_consumption"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                df = abnormal_data_handling(df, params)
                # TODO 临时处理耗电量
                df["power_consumption"][df["power_consumption"] < 0] = 0
                data.update(get_common_response(df, by))
            elif key == "end_water_temperature":
                params = ["end_supply_water_temp", "end_return_water_temp", "end_return_water_temp_diff"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine, False)
                data.update(get_common_response(df, by))
            elif key == "heat_storage_tank_replenishment":
                df = get_common_df(["heat_storage_tank_replenishment"], db, start, end, TIME_DATA_INDEX, engine, False)
                data.update(get_common_response(df, by))
            elif key == "heat_replenishment":
                params = ["heat_water_replenishment", "heat_water_replenishment_limit"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine, False)
                data.update(get_common_response(df, by))
            elif key == "solar_side_replenishment":
                params = ["solar_side_replenishment", "solar_side_replenishment_limit"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine, False)
                data.update(get_common_response(df, by))
            elif key == "emission_reduction":
                params = ["co2_emission_reduction", "power_consumption"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                df = abnormal_data_handling(df, params)
                df["co2_emission_reduction"] = df["co2_emission_reduction"].cumsum()
                df["power_consumption"] = df["power_consumption"].cumsum()

                data.update(get_common_response(df, by))
            elif key == "co2_equal_data":
                params = ["co2_emission_reduction", "co2_equal_num"]
                df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                df = abnormal_data_handling(df, params)
                data.update(get_common_response(df, by))
            elif key == "box_data":
                x, y = get_box_data(start, end)
                data["time_data"] = x
                data["box_data"] = y
                data["start"] = start.split(" ")[0]
                data["end"] = end.split(" ")[0]
            elif key == "cumulative_data":
                item = request.data.get("item")
                if request.data.get("initial"):
                    start, end = START_DATE["kamba"], datetime.today().strftime("%Y/%m/%d 00:00:00")
                if item.lower() == "co2":

                    df = get_common_df(["co2_emission_reduction"], db, start, end, TIME_DATA_INDEX, engine)
                    df = abnormal_data_handling(df, ["co2_emission_reduction"])
                    df["co2_emission_reduction"] = df["co2_emission_reduction"].interpolate(
                        method="index"
                    ).interpolate(method="nearest").bfill().ffill()
                    df["co2_emission_reduction"] = (df["co2_emission_reduction"].cumsum() * 1.964 / 1000).round()
                    data.update(get_common_response(df, by))
                else:
                    # 节省电费
                    params = ["cost_saving"]
                    df = get_common_df(params, db, start, end, TIME_DATA_INDEX, engine)
                    df = abnormal_data_handling(df, params)
                    df["cost_saving"] = (df["cost_saving"].cumsum() / 10000).round()
                    data.update(get_common_response(df, by))

        except Exception as e:
            import traceback
            traceback.print_exc()
            engine.dispose()
        finally:
            engine.dispose()
            if data:
                return Response(data, status=HTTP_200_OK)
            else:
                return Response({"msg": "data error"}, status=HTTP_500_INTERNAL_SERVER_ERROR)






