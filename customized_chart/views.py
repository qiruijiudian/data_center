from django.http import FileResponse, StreamingHttpResponse
from django.utils.encoding import escape_uri_path
from django.views.decorators.csrf import csrf_exempt
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.status import HTTP_200_OK, HTTP_500_INTERNAL_SERVER_ERROR, HTTP_404_NOT_FOUND
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from data_center.settings import DATABASE, UPLOAD, DOWNLOAD
from data_center.tools import gen_response, gen_time_range, get_common_response, get_last_time_range, \
    get_correspondence_with_temp_chart_response, get_common_sql, gen_time_range, get_last_time_by_delta, file_iterator, \
    check_custom_file
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
        time_index = "time_data"
        by = "h"
        data = {}

        # 获取参数
        key = request.data.get('key', None)
        start = request.data.get('start', None)
        end = request.data.get('end', None)

        if not key:
            return Response({"msg": "params error"}, status=HTTP_404_NOT_FOUND)

        db = "tianjin_commons_data"
        if key == "upload_file":
            file = request.FILES.get("my_file")

            file_path = os.path.join(UPLOAD, file.name)
            try:
                success, msg = check_custom_file(file)
                if success:
                    destination = open(file_path, 'wb+')  # 打开特定的文件进行二进制的写操作

                    for chunk in file.chunks():  # 分块写入文件
                        destination.write(chunk)
                    destination.close()

                    return Response({"status": "successful"}, status=HTTP_200_OK)
                else:
                    return Response({"status": "failure", "msg": msg}, status=HTTP_200_OK)
            except Exception as e:
                return Response({"status": "failure"}, status=HTTP_200_OK)

        # engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(
        #             DATABASE[plate_form]["user"],
        #             DATABASE[plate_form]["password"],
        #             DATABASE[plate_form]["host"],
        #             DATABASE[plate_form]["database"]
        #         )
        # )
        # try:
        #
        #     if key == "params_file_download":
        #         file = request.data.get("file_name")
        #         file_name = "custom.csv" if file == "kamba" else "cona.csv"
        #         # file_path = os.path.join(UPLOAD, file_name)
        #         response = FileResponse(open(os.path.join(UPLOAD, file_name)))
        #         print(response)
        #         response['content_type'] = "application/octet-stream"
        #         response['Content-Disposition'] = 'attachment; filename=' + os.path.basename(file_name)
        #         return response
        #
        # except Exception as e:
        #     print("异常", e)
        #     import traceback
        #     traceback.print_exc()
        #     # engine.dispose()
        # finally:
        #     # engine.dispose()
        #     if data:
        #         return Response(data, status=HTTP_200_OK)
        #     else:
        #         return Response({"msg": "data error"}, status=HTTP_500_INTERNAL_SERVER_ERROR)






