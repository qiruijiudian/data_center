
# -*- coding: UTF-8 -*-
import sched
import time
import json
import requests
import urllib3
import datetime
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class Kamba_tools():
    API_HOST = {"local": "http://localhost:8000", "cloud": "http://cdqrmi.com:8000"}
    API_TYPE = {"common": "/common/", "cona": "/cona/", "kamba": "/kamba/", "tianjin": "/tianjin/", "custom": "/custom/"}
    # 定义间隔时间（24h）
    interval = 24 * 1

    # 创建调度程序对象
    scheduler = sched.scheduler(time.time, time.sleep)

    def test_api(self, u_type, data, is_local=True):
        """ 测试接口，向云接口或本地发起POST请求

        Args:
            u_type: 请求对象，common(公用)、cona(错那)、kamba(岗巴)
            data: 请求参数，字典
            is_local: 是否本地，默认为True，访问本地数据库，否则或向云数据库发起请求

        Returns:
            接口返回json格式的数据
        """
        url = self.API_HOST["local"] + self.API_TYPE[u_type] if is_local else self.API_HOST["cloud"] + self.API_TYPE[u_type]
        r = requests.post(url, data=data, timeout= 90)
        return r.text

    def write_json(self):
        try:
            time_range = json.loads(self.test_api("common", {"key": "real_time", "block": 'kamba'}, False))
            with open("./time_range.json", "w", encoding='utf-8') as json_file:
                json.dump(time_range, json_file)
        except Exception as e:
            now = datetime.datetime.now()
            formatted_date = now.strftime("%Y-%m-%d %H:%M:%S")
            print(f'{formatted_date} error : {e}')

    def run_script(self):
        # 在此处插入原始脚本中的代码
        self.write_json()
        now = datetime.datetime.now()
        formatted_date = now.strftime("%Y-%m-%d %H:%M:%S")
        print(f'{formatted_date} to update time_range')
        # 安排下一次运行
        self.scheduler.enter(self.interval, 1, self.run_script)

    def start_scheduler(self):
        # 安排第一次运行
        self.scheduler.enter(self.interval, 1, self.run_script)
        # 启动调度程序
        self.scheduler.run()


if __name__ == '__main__':
    func_doer = Kamba_tools()
    func_doer.start_scheduler()