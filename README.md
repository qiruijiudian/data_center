

<h1 align="center">data_center: 数据分析平台</h1>


[TOC]

### 1. 项目运行方式

- 运行项目需要安装项目依赖以及`Mysql`数据环境。

~~~
pip install -r requirements.txt 
python manage.py runserver 
访问 http://127.0.0.1:8000 下对应路由进行测试
~~~



### 2. 项目结构

![data_center文件结构.png](http://tva1.sinaimg.cn/large/bf776e91ly1h336dh4bhpj20w609g76t.jpg)



### 3. 项目文件概述

文件结构方面各接口内容一致（`kamba、cona、tianjin、customized_chart`），主函数均位于`views.py`函数内，路由通过`data_center`目录的`urls.py`统一进行设定，其他文件诸如`README.md、requirements.txt等`均为相关配置文件，陈列一些使用说明或者环境内容。



### 4. 接口函数说明

#### 4.1 `CommonView`

公共接口板块，示例登录校验和实时时间数据获取,参数详情如下：

| 接口说明      | key         |
| ------------- | ----------- |
| 账号密码登录  | login       |
| token登录校验 | token_check |
| 实时时间获取  | real_time   |



#### 4.2 `ConaView、KambaView`

错那和岗巴接口板块，包含错那所有数据的统计接口，数据表为：`cona_hours_data(时数据)、cona_days_data(日数据)、kamba_hours_data（时数据）、kamba_days_data(日数据)`，按照用户传入的by参数确定所查询的数据表选项，各数据表所包含字段内容可参照`数据解析文档`或者前往数据库查看，`ConaView、KambaView`接口均为配合相关页面的固定返回数据(需要配合前端内容使用)



#### 4.3 `CustomizedView`

![自定义模板解析.png](http://tva1.sinaimg.cn/large/bf776e91ly1h33a0367puj20qq07g74t.jpg)

自定义模板接口，区别于此前的内容，此接口可以自定义数据渲染结构，按照所上传的文件返回配置文件进行图表渲染，`CustomizedView`接口中，GET请求处理文件下载（错那变量文件、岗巴变量文件、天津变量文件、自定义示例文件），在自定义图表格式的时候，**变量需要与各变量文件中的内容所对应**，否则无法通过文件校验过程,文件以excel文件格式存在，除了文件内各列所述参数外，还需要指定`sheetname`，设置详情可以参考[自定义文件设置示例](http://cdqrmi.com/DataCenter/custom_file.html)



![自定义模板示例.png](http://tva1.sinaimg.cn/large/bf776e91ly1h337dy1q45j21810cotf8.jpg)





### 5. 接口测试

根目录下的`api_test.py`文件可用作测试接口，文件中定义了所有定义的接口以及相关参数，也可以自行编写代码进行调用测试



#### 5.1 `api_test.py`

- `get_params_data(block, base_data, random_date=False)`：构建完整的请求参数字典，在已有参数的基础上添加其他参数（时间等）

  - `block`：数据源，`cona/kamba/tianjin`
  - `base_data`：基础请求参数，包含`key、by`参数
  - `ramdom_date`：是否生成随机日期

- `test_api(u_type, data, is_local=True)`

  - `u_type`：数据源，`cona/kamba/tianjin`
  - `data`：请求参数
  - `is_local`：是否调用本地接口，为False则访问服务器接口

- 调用示例：

  - ~~~
    block = "kamba"
    # 实时负荷量（日数据）
    print(test_api(block, get_params_data(block, {"key": "load", "by": "d"})))
    ~~~



#### 5.2 自定义代码测试

~~~
import requests

url = "http://localhost:8000/kamba/"	# 定义访问路由
data = {"start": "2022-05-08 00:00:00", "end": "2022-05-08 00:00:00", "key": "load", "by": "d"}	# 定义请求参数
# 负荷量（日数据）
r = requests.post(url, data=data)	# 发起POST请求
print(r.text)
~~~



### 6. 本地测试

网页功能测试需要后端代码配合上前端的页面代码，且需要提前部署本地数据库，这里的测试案例使用云服务器的数据库来做演示

#### 6.1 第一步：克隆或拉取最新代码

- 后端代码方面：测试版本的代码放在`github`的test分支下，如果第一次克隆代码使用`git clone -b test https://github.com/qiruijiudian/data_center.git`,如果此前克隆过代码可使用`git pull`更新最新代码`git pull origin test:test`
- 前端代码和其他板块代码都只保留主分支，直接克隆或者拉取即可

~~~
1. 拉取/克隆后端代码

git clone -b test https://github.com/qiruijiudian/data_center.git

2. 拉取/克隆前端代码
git clone https://github.com/qiruijiudian/data_center_web.git
~~~



**执行完代码准备阶段，此时的代码结构如下：**

├─data_center
└─data_center_web



#### 6.2 第二步：安装后端代码环境依赖库

~~~
cd data_center

pip install -r requirements.txt
~~~



#### 6.3 第三步：开启后端服务

```
cd data_center(如果已在data_center根目录下可忽略)
python manage.py runserver

```



#### 6.4 第四步：用服务器模式访问网页

- 保证目前的路径在项目根路径下，即当前目录为
  - ├─data_center
    └─data_center_web
- 分别用`cmd`在当前目录下依次执行以下指令：

```
python -m http.server 8080
start http://localhost:8080/data_center_web/kamba.html

```

