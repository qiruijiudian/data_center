from django.http import FileResponse, StreamingHttpResponse
from django.utils.encoding import escape_uri_path
from django.views.decorators.csrf import csrf_exempt
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.status import HTTP_200_OK, HTTP_500_INTERNAL_SERVER_ERROR, HTTP_404_NOT_FOUND
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import json
from data_center.settings import DATABASE, UPLOAD, DOWNLOAD
from data_center.tools import gen_response, gen_time_range, get_common_response, get_last_time_range, \
    get_correspondence_with_temp_chart_response, get_common_sql, gen_time_range, get_last_time_by_delta, file_iterator, \
    check_custom_file, get_custom_variables_mapping, get_custom_response
import platform
import os





class CustomizedView(APIView):

    @csrf_exempt
    def get(self, request):
        context = {"cona": "cona.xlsx", "kamba": "kamba.xlsx", "tianjin": "tianjin.xlsx", "custom": "custom.xlsx"}
        file_name = request.GET.get("file_name")
        key = request.GET.get("key")
        if key == "params_file_download":
            f_name = context[file_name]
            file = os.path.join(DOWNLOAD, f_name)
            response = StreamingHttpResponse(file_iterator(os.path.join(file)))
            response['Content-Type'] = 'application/octet-stream'  # #设定文件头，这种设定可以让任意文件都能正确下载，而且已知文本文件不是本地打开
            response['Content-Disposition'] = "attachment; filename*=utf-8''{}".format(
                escape_uri_path(f_name))  # 正确写法
            return response

    def post(self, request):
        plate_form = platform.system()
        data = {}

        # 获取参数
        key = request.data.get('key', None)

        if not key:
            return Response({"msg": "params error"}, status=HTTP_404_NOT_FOUND)

        db = "tianjin_commons_data"
        if key == "upload_file":

            file = request.FILES.get("my_file")

            file_path = os.path.join(UPLOAD, "custom.xlsx")
            try:
                success, msg = check_custom_file(file)
                if success:
                    destination = open(file_path, 'wb+')  # 打开特定的文件进行二进制的写操作

                    for chunk in file.chunks():  # 分块写入文件
                        destination.write(chunk)
                    destination.close()

                    return Response({"status": "success"}, status=HTTP_200_OK)
                else:
                    return Response({"status": "failure", "msg": msg}, status=HTTP_200_OK)
            except Exception as e:
                return Response({"status": "failure"}, status=HTTP_200_OK)
        elif key == "config_file":
            data["charts"] = []
            file_name = "custom_init.xlsx"
            if os.path.exists(os.path.join(UPLOAD, "custom.xlsx")):
                file_name = "custom.xlsx"
            file_obj = pd.ExcelFile(os.path.join(UPLOAD, file_name), engine="openpyxl")
            sheet_names = file_obj.sheet_names
            sheet_name = sheet_names[0]  # sheet name
            init_id = 1
            df = file_obj.parse(sheet_name=sheet_name)
            variables_mapping = get_custom_variables_mapping(sheet_name)    # 变量对应字段名称

            for index in df.index:
                interval = df.loc[index, "interval"]
                tmp = {
                    "chart_id": "chart_{}".format(init_id),
                    "time_id": "time_{}".format(init_id),
                    "line_id": df.loc[index, "row_id"],
                    "title": df.loc[index, "title"],
                }
                x_name = df.loc[index, "x_name"]
                # 确定x轴单位
                if x_name == "日期":
                    tmp.update({"xAxis": [{"name": "日期"}]})
                else:
                    if x_name == "气温":
                        tmp.update({"xAxis": [{"name": "气温", "unit": "℃", "data": "temp"}]})
                    elif x_name in variables_mapping.keys():
                        if "热量" in x_name or "耗电量" in x_name:
                            unit = "kWh"
                        elif "温度" in x_name or "温差" in x_name:
                            unit = "℃"
                        elif "负荷" in x_name or "功率" in x_name or x_name == "总太阳能辐射量":
                            unit = "kW"
                        elif "补水" in x_name:
                            unit = "m³"
                        elif "费用" in x_name:
                            unit = "元"
                        elif "COP" in x_name:
                            unit = ""
                        elif "天数" in x_name:
                            unit = "天"
                        elif "树木数量" in x_name:
                            unit = "棵"
                        elif x_name == "太阳能辐射量":
                            unit = "W/m³"
                        elif "流量" in x_name:
                            unit = "m³/h"
                        elif "风机频率" in x_name or "开度" in x_name or "湿度" in x_name:
                            unit = "%"
                        elif "送风压力" in x_name:
                            unit = "Pa"
                        else:
                            unit = ""
                        tmp.update(
                            {"xAxis": [{"name": x_name, "unit": unit, "data": variables_mapping[x_name][interval]}]}
                        )

                # 处理二进制解析导致的字符乱码（y轴单位）
                y_unit = df.loc[index, "y_unit"]
                if "m3" in y_unit:
                    tmp.update({"yAxis": [{"name": df.loc[index, "y_name"], "unit": "m³"}]})
                else:
                    tmp.update({"yAxis": [{"name": df.loc[index, "y_name"], "unit": y_unit}]})

                # series构建
                series = {}
                legend = df.loc[index, "variables"].split("-")
                tmp["legend"] = legend
                line_types = df.loc[index, "line_types"].split("-")

                if sheet_name != "tianjin":

                    tmp["request_data"] = {"by": interval}
                    for legend_item, line_item in zip(legend, line_types):
                        series.update(
                            {legend_item: {"type": line_item, "data": variables_mapping[legend_item][interval]}}
                        )
                else:
                    tmp["request_data"] = {}
                    for legend_item, line_item in zip(legend, line_types):
                        series.update({legend_item: {"type": line_item, "data": variables_mapping[legend_item]}})
                tmp["series"] = series
                data["charts"].append(tmp)
                init_id += 1
            data["block"] = sheet_name
            return Response(data, status=HTTP_200_OK)
        elif key == "custom_interface":
            start = request.data.get('start', None)
            end = request.data.get('end', None)
            if not (start and end):
                return Response({"msg": "params error"}, status=HTTP_404_NOT_FOUND)

            by = request.data.get("by")
            block = request.data.get("block")
            x_data = request.data.get("x_data")
            db = "tianjin_commons_data"
            chart_type = request.data.get("chart_type")

            if by:
                db = {
                    "cona": {"h": "cona_hours_data", "d": "cona_days_data"},
                    "kamba": {"h": "kamba_hours_data", "d": "kamba_days_data"},
                }[block][by]
            series = json.loads(request.data.get("series"))

            engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(
                        DATABASE[plate_form]["user"],
                        DATABASE[plate_form]["password"],
                        DATABASE[plate_form]["host"],
                        DATABASE[plate_form]["database"]
                    )
            )
            try:
                params = [item["data"] for item in series.values()]
                if x_data:
                    params = [x_data] + params

                sql = "select time_data,{} from {} where time_data between '{}' and '{}'".format(
                    ",".join(params), db, start, end
                )
                df = pd.read_sql(sql, con=engine)
                for column in df.columns:
                    if "load" in column or "heat_supply" in column or "solar_collector" in column \
                            or "heating_guarantee_rate" in column or "high_temperature_plate_exchange_heat" in column \
                            or "wshp_heat" in column or "power_consume" in column or "heat_supply_rate" in column \
                            or "cost_saving" in column or "power_consumption" in column:
                        df[column] = df[column].apply(lambda x: x if x >=0 else 0)

                data.update(get_custom_response(df, "time_data", by, chart_type, x_data))

            except Exception as e:
                # print("异常", e)
                # import traceback
                # traceback.print_exc()
                engine.dispose()
            finally:
                engine.dispose()
                if data:
                    return Response(data, status=HTTP_200_OK)
                else:
                    return Response({"msg": "data error"}, status=HTTP_500_INTERNAL_SERVER_ERROR)

