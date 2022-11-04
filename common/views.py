from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.status import HTTP_200_OK, HTTP_500_INTERNAL_SERVER_ERROR, HTTP_404_NOT_FOUND, HTTP_403_FORBIDDEN
import pymysql
from data_center.settings import DATABASE
from datetime import datetime, timedelta
from data_center.tools import gen_response, gen_time_range, get_block_time_range
import platform
from hashlib import sha256


class CommonView(APIView):
    def post(self, request):
        plate_form = platform.system()
        db = DATABASE[plate_form]["user"]
        data = {}
        key = request.data.get("key")

        header = {
            "Access-Control-Allow-Origin" : "*",
            "Access-Control-Allow-Methods" : "POST, GET, OPTIONS",
            "Access-Control-Allow-Headers" : "*"
        }

        if not key:
            return Response({'msg': 'params error'}, status=HTTP_404_NOT_FOUND)

        with pymysql.connect(host=db["host"], user=db["user"], password=db["password"], database=db["database"]) as conn:
            cur = conn.cursor()
            try:
                if key == "login":
                    # 获取参数
                    u_name = request.data.get("u_name")
                    pwd = request.data.get("pwd")

                    if not all([u_name, pwd]):
                        return Response({'msg': 'params error'}, status=HTTP_404_NOT_FOUND)

                    cur.execute(
                        "select username, password, level from account where username='{}' and password='{}'".format(u_name, pwd)
                    )
                    cur_res = cur.fetchone()

                    if not cur_res:
                        data = Response({'msg': '账号或密码错误，请修改输入信息后重试'}, status=HTTP_403_FORBIDDEN)
                    else:
                        # 生成令牌
                        login_expires = datetime.now() + timedelta(days=30)
                        auth_token = sha256('{} {}'.format(u_name, login_expires).encode('utf-8')).hexdigest()
                        cur.execute("update account set auth_token='{}',login_expires='{}' where username='{}'".format(
                            auth_token, login_expires, u_name)
                        )
                        conn.commit()
                        level = cur_res[-1]
                        if level == 0:
                            index_url = "cona"
                        elif level == 2:
                            index_url = "tianjin"
                        else:
                            index_url = "kamba"

                        data = Response({"status": "success", "token": auth_token, "index": index_url}, status=HTTP_200_OK)
                elif key == "token_check":
                    token = request.data.get("token")
                    block = request.data.get("block")
                    if not all([token, block]) and block not in ["cona", "kamba", "tianjin", "custom"]:
                        return Response({'msg': 'params error'}, status=HTTP_404_NOT_FOUND)

                    allow_level = [3]

                    if block == "cona":
                        allow_level.append(0),
                    elif block == "kamba":
                        allow_level.append(1)
                    elif block == "tianjin":
                        allow_level.append(2)

                    cur.execute(
                        'select username, level from account where auth_token="{}" and login_expires >= now()'.format(token)
                    )
                    res = cur.fetchone()

                    if not res:
                        data = Response({"status": "failure", "msg": "login"}, status=HTTP_200_OK)
                    else:
                        level = res[-1]
                        if level not in allow_level:
                            data = Response({"status": "failure", "msg": "permission"}, status=HTTP_200_OK)
                        else:
                            data = Response({"status": "success", 'user': res[0]}, status=HTTP_200_OK)
                elif key == "real_time":
                    block = request.data.get("block")
                    time_range = get_block_time_range(block)
                    data = Response(time_range, status=HTTP_200_OK)

                else:
                    data = None

            except Exception as e:
                print("异常", e)
            finally:
                cur.close()

            if data:
                return data
            else:
                return Response({"msg": "data error"}, status=HTTP_500_INTERNAL_SERVER_ERROR, headers=header)

    def get(self):
        return Response({"msg": "OK"}, status=HTTP_200_OK)






