from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.status import HTTP_200_OK, HTTP_500_INTERNAL_SERVER_ERROR, HTTP_404_NOT_FOUND
from sqlalchemy import create_engine
import pandas as pd
from data_center.settings import DATABASE
from data_center.tools import get_common_response, get_last_time_range, get_correspondence_with_temp_chart_response, \
    get_common_sql, gen_time_range, get_common_df
import platform


class ConaView(APIView):
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

        db = "cona_hours_data" if by and by.strip() == "h" else "cona_days_data"

        engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(
                    DATABASE[plate_form]["data"]["user"],
                    DATABASE[plate_form]["data"]["password"],
                    DATABASE[plate_form]["data"]["host"],
                    DATABASE[plate_form]["data"]["database"]
                )
        )
        try:
            if key == "geothermal_wells_provide_heat":
                params = ["high_temp_plate_exchange_heat_production", "water_heat_pump_heat_production",
                          "geothermal_wells_high_heat_provide", "geothermal_wells_low_heat_provide"]
                df = get_common_df(params, db, start, end, time_index, engine)
                data.update(get_common_response(df, by))
            elif key == "panel_data":
                params, db = ["max_load", "min_load", "cost_saving"], "cona_days_data"
                df = get_common_df(params, db, start, end, time_index, engine)
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

            elif key == "water_supply_with_temp":
                if by == "h":
                    return Response({"msg": "params error"}, status=HTTP_404_NOT_FOUND)
                params = ["water_supply_temperature", "temp"]

                time_range = get_last_time_range(start, end)
                df = get_common_df(params, db, start, end, time_index, engine)
                last_df = get_common_df(params, db, time_range["last_start"], time_range["last_end"], time_index, engine)
                data.update(get_correspondence_with_temp_chart_response(df, last_df, time_range, "water_supply_temperature"))
            elif key == "com_cop":
                # df = pd.read_sql(get_common_sql(["time_data", "com_cop"], db, start, end, time_index), con=engine)
                df = get_common_df(["com_cop"], db, start, end, time_index, engine)
                df["Target Minimum"] = 6
                df["Low Threshold"] = 4
                data.update(get_common_response(df, by))
                data["status"] = "数据异常" if ("" in df["com_cop"].values or None in df["com_cop"].values) else "正常"
            elif key == "cost_saving":
                df = get_common_df(["cost_saving"], db, start, end, time_index, engine)

                data.update(get_common_response(df, by))
            elif key == "types_cost_saving":
                df = get_common_df(["high_temp_charge", "low_temp_charge"], db, start, end, time_index, engine)
                data.update(gen_time_range(df))
                data.update(
                    {
                        "high": {"name": "节省高温供暖费用", "value": df["high_temp_charge"].sum().round(2)},
                        "low": {"name": "节省低温供暖费用", "value": df["low_temp_charge"].sum().round(2)},
                    }
                )
            elif key == "water_supply_and_return_temp":
                params = ["water_supply_temperature", "return_water_temperature", "supply_return_water_temp_diff"]
                df = get_common_df(params, db, start, end, time_index, engine)
                data.update(get_common_response(df, by))
            elif key == "heat_well_heating_with_temp":
                params = ["time_data", "heat_well_heating", "temp"]

                time_range = get_last_time_range(start, end)
                df = get_common_df(params, db, start, end, time_index, engine)
                last_df = get_common_df(params, db, time_range["last_start"], time_range["last_end"], time_index, engine)

                data.update(get_correspondence_with_temp_chart_response(df, last_df, time_range, "heat_well_heating"))
            elif key == "heat_pipe_network_heating_with_temp":
                params = ["time_data", "heat_pipe_network_heating", "temp"]

                time_range = get_last_time_range(start, end)
                df = get_common_df(params, db, start, end, time_index, engine)
                last_df = get_common_df(params, db, time_range["last_start"], time_range["last_end"], time_index,
                                        engine)

                data.update(get_correspondence_with_temp_chart_response(df, last_df, time_range, "heat_pipe_network_heating"))
            elif key == "water_heat_pump_heat_with_temp":
                params = ["time_data", "water_heat_pump_heat_production", "temp"]

                time_range = get_last_time_range(start, end)

                df = get_common_df(params, db, start, end, time_index, engine)
                last_df = get_common_df(params, db, time_range["last_start"], time_range["last_end"], time_index,
                                        engine)

                data.update(get_correspondence_with_temp_chart_response(df, last_df, time_range, "water_heat_pump_heat_production"))
            elif key == "high_temp_plate_exchange_heat_with_temp":
                params = ["time_data", "high_temp_plate_exchange_heat_production", "temp"]

                time_range = get_last_time_range(start, end)

                df = get_common_df(params, db, start, end, time_index, engine)
                last_df = get_common_df(params, db, time_range["last_start"], time_range["last_end"], time_index,
                                        engine)

                data.update(get_correspondence_with_temp_chart_response(df, last_df, time_range, "high_temp_plate_exchange_heat_production"))
            elif key == "load":
                params = ["min_load", "avg_load", "max_load"]
                df = get_common_df(params, db, start, end, time_index, engine)
                data.update(get_common_response(df, by))
            elif key == "load_compare":
                params = ["min_load", "avg_load", "max_load"]
                df = get_common_df(params, db, start, end, time_index, engine)
                data.update(gen_time_range(df))
                data.update(
                    {
                        "max_load": {"name": "最大负荷", "value": df["max_load"].max().round(2)},
                        "avg_load": {"name": "平均负荷", "value": df["avg_load"].mean().round(2)},
                        "min_load": {"name": "最小负荷", "value": df["min_load"].min().round(2)}
                    }
                )
            elif key == "water_replenishment":
                params = ["water_replenishment", "water_replenishment_limit"]
                df = get_common_df(params, db, start, end, time_index, engine)
                data.update(get_common_response(df, time_index, by))
            elif key == "sub_com_cop":
                cop_id = request.data.get("id")
                context = {"2": "f2_cop", "3": "f3_cop", "4": "f4_cop", "5": "f5_cop"}
                params = [context[cop_id]]
                df = get_common_df(params, db, start, end, time_index, engine)
                df["Target Minimum"] = 6
                df["Low Threshold"] = 4
                data.update(get_common_response(df, time_index, by))
                data["status"] = "数据异常" if ("" in df[params[-1]].values or None in df[params[-1]].values) else "正常"

            elif key == "sub_wshp_cop":
                cop_id = request.data.get("id")
                context = {"2": "f2_whp_cop", "3": "f3_whp_cop", "4": "f4_whp_cop", "5": "f5_whp_cop"}
                params = [context[cop_id]]
                df = get_common_df(params, db, start, end, time_index, engine)
                df["Target Minimum"] = 6
                df["Low Threshold"] = 4
                data.update(get_common_response(df, time_index, by))
                data["status"] = "数据异常" if ("" in df[params[-1]].values or None in df[params[-1]].values) else "正常"
            elif key == "room_network_water_temperature":
                item_name = request.data.get("id")
                params = ["temp"] + [item_name]
                time_range = get_last_time_range(start, end)

                df = get_common_df(params, db, start, end, time_index, engine)

                last_df = get_common_df(params, db, time_range["last_start"], time_range["last_end"], time_index, engine)

                data.update(get_correspondence_with_temp_chart_response(df, last_df, time_range, params[-1]))

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






