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
        db = DATABASE[plate_form]
        data = {}
        key = request.data.get("key")

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
                        "select username, password from user where username='{}' and password='{}'".format(u_name, pwd)
                    )
                    if not cur.fetchone():
                        data = Response({'msg': 'No such user'}, status=HTTP_403_FORBIDDEN)
                    else:
                        # 生成令牌
                        login_expires = datetime.now() + timedelta(days=30)
                        auth_token = sha256('{} {}'.format(u_name, login_expires).encode('utf-8')).hexdigest()
                        cur.execute("update user set auth_token='{}',login_expires='{}' where username='{}'".format(
                            auth_token, login_expires, u_name)
                        )
                        conn.commit()
                        data = Response({"status": "success", "token": auth_token}, status=HTTP_200_OK)

                elif key == "token_check":
                    token = request.data.get("token")
                    cur.execute(
                        'select username from user where auth_token="{}" and login_expires >= now()'.format(token)
                    )
                    res = cur.fetchone()
                    if not res:
                        data = Response({'status': 'failure'}, status=HTTP_403_FORBIDDEN)
                    else:
                        data = Response({'status': 'success', 'user': res[0]}, status=HTTP_200_OK)
                elif key == "real_time":
                    block = request.data.get("block")
                    time_range = get_block_time_range(block)
                    print(time_range)
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
                return Response({"msg": "data error"}, status=HTTP_500_INTERNAL_SERVER_ERROR)






