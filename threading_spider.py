import datetime
import time

import pymysql
import requests
from lxml import etree
import json
from queue import Queue
import threading
from requests.exceptions import RequestException

from code_l import js225_l





'''
1. 创建 URL队列, 响应队列, 数据队列 在init方法中
2. 在生成URL列表中方法中,把URL添加URL队列中
3. 在请求页面的方法中,从URL队列中取出URL执行,把获取到的响应数据添加响应队列中
4. 在处理数据的方法中,从响应队列中取出页面内容进行解析, 把解析结果存储数据队列中
5. 在保存数据的方法中, 从数据队列中取出数据,进行保存
6. 开启几个线程来执行上面的方法
'''

def run_forever(func):
    def wrapper(obj):
        while True:
            func(obj)
    return wrapper


def insertDB(content):
    connection = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='123456', db='JS225_JS400',
                                 charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

    cursor = connection.cursor()

    try:
        js225 = 'J9984,J9983,J9766,J9735,J9613,J9602,J9532,J9531,J9503,J9502,J9501,J9437,J9433,J9432,J9412,J9301,J9202,J9107,J9104,J9101,J9064,J9062,J9022,J9021,J9020,J9009,J9008,J9007,J9005,J9001,J8830,J8804,J8802,J8801,J8795,J8766,J8750,J8729,J8725,J8630,J8628,J8604,J8601,J8411,J8355,J8354,J8331,J8316,J8309,J8308,J8306,J8304,J8303,J8267,J8253,J8252,J8233,J8058,J8053,J8035,J8031,J8028,J8015,J8002,J8001,J7951,J7912,J7911,J7832,J7762,J7752,J7751,J7735,J7733,J7731,J7272,J7270,J7269,J7267,J7261,J7211,J7205,J7203,J7202,J7201,J7186,J7013,J7012,J7011,J7004,J7003,J6988,J6976,J6971,J6954,J6952,J6902,J6857,J6841,J6770,J6762,J6758,J6752,J6724,J6703,J6702,J6701,J6674,J6645,J6506,J6504,J6503,J6501,J6479,J6473,J6472,J6471,J6367,J6361,J6326,J6305,J6302,J6301,J6178,J6113,J6103,J6098,J5901,J5803,J5802,J5801,J5714,J5713,J5711,J5707,J5706,J5703,J5631,J5541,J5411,J5406,J5401,J5333,J5332,J5301,J5233,J5232,J5214,J5202,J5201,J5108,J5101,J5020,J5019,J4911,J4902,J4901,J4755,J4751,J4704,J4689,J4631,J4578,J4568,J4543,J4523,J4519,J4507,J4506,J4503,J4502,J4452,J4324,J4272,J4208,J4188,J4183,J4151,J4063,J4061,J4043,J4042,J4021,J4005,J4004,J3863,J3861,J3436,J3407,J3405,J3402,J3401,J3382,J3289,J3105,J3103,J3101,J3099,J3086,J2914,J2871,J2802,J2801,J2768,J2531,J2503,J2502,J2501,J2432,J2413,J2282,J2269,J2002,J1963,J1928,J1925,J1812,J1808,J1803,J1802,J1801,J1721,J1605,J1333,J1332'

        f_225 = "%s," * 225
        cursor.executemany('insert into js225_s ({0}) values ({1})'.format(js225, f_225[:-1]), content)
        connection.commit()
        connection.commit()
        connection.close()
        print('向MySQL中添加数据成功！')
    except TypeError:
        pass
def RemoveDot(item):
    f_l = []
    for it in item:

        f_str = "".join(it.split(","))
        ff_str = f_str +"00"
        f_l.append(ff_str)

    return f_l

def remove_douhao(num):
    num1 = "".join(num.split(","))
    f_num = str(num1)
    return f_num
def remove_block(items):
    new_items = []
    for it in items:
        f = "".join(it.split())
        new_items.append(f)
    return new_items
class QiubaiSpider(object):

    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36'
        }
        self.url_pattern = "https://minkabu.jp/stock/{0}/analysis?order=1&sc_id=4&tc_id=ya0j0001"
        # url 队列
        self.url_queue = Queue()
        # 响应队列
        self.page_queue = Queue()
        # 数据队列
        self.data_queue = Queue()


    def add_url_to_queue(self):
        # 把URL添加url队列中
        for i in js225_l:
            self.url_queue.put(self.url_pattern.format(i))

    @run_forever
    def add_page_to_queue(self):
        ''' 发送请求获取数据 '''
        url = self.url_queue.get()
        # print(url)
        response = requests.get(url, headers=self.headers)
        print(response.status_code)
        if response.status_code != 200:
            self.url_queue.put(url)
        else:
            self.page_queue.put(response.content)
        # 完成当前URL任务
        self.url_queue.task_done()

    @run_forever
    def add_dz_to_queue(self):
        '''根据页面内容使用lxml解析数据, 获取段子列表'''
        page = self.page_queue.get()
        element = etree.HTML(page)

        now_price = element.xpath(
            '//*[@id="layout"]/div[2]/div[3]/div[2]/div/div[1]/div/div/div[1]/div[2]/div/div[2]/div/text()')
        f_price = RemoveDot(remove_block(now_price))
        big_list.append(f_price[0])


        # self.data_queue.put(f_price)
        self.page_queue.task_done()

    def get_first_element(self, list):
        '''获取列表中第一个元素,如果是空列表就返回None'''
        return list[0] if len(list) != 0 else None







    def run_use_more_task(self, func, count=1):
        '''把func放到线程中执行, count:开启多少线程执行'''
        for i in range(0, count):
            t = threading.Thread(target=func)
            t.setDaemon(True)
            t.start()

    def run(self):
        # 开启线程执行上面的几个方法
        url_t = threading.Thread(target=self.add_url_to_queue)
        # url_t.setDaemon(True)
        url_t.start()

        self.run_use_more_task(self.add_page_to_queue, 10)
        self.run_use_more_task(self.add_dz_to_queue, 9)


        # 使用队列join方法,等待队列任务都完成了才结束
        self.url_queue.join()
        self.page_queue.join()







if __name__ == '__main__':


    big_list = []
    ff_l = []

    qbs = QiubaiSpider()
    qbs.run()

    f_tup = tuple(big_list)
    ff_l.append((f_tup))
    insertDB(ff_l)


#
#
#
# # # create table js225_s(id int not null primary key auto_increment,J9984 float,J9983 float,J9766 float,J9735 float,J9613 float,J9602 float,J9532 float,J9531 float,J9503 float,J9502 float,J9501 float,J9437 float,J9433 float,J9432 float,J9412 float,J9301 float,J9202 float,J9107 float,J9104 float,J9101 float,J9064 float,J9062 float,J9022 float,J9021 float,J9020 float,J9009 float,J9008 float,J9007 float,J9005 float,J9001 float,J8830 float,J8804 float,J8802 float,J8801 float,J8795 float,J8766 float,J8750 float,J8729 float,J8725 float,J8630 float,J8628 float,J8604 float,J8601 float,J8411 float,J8355 float,J8354 float,J8331 float,J8316 float,J8309 float,J8308 float,J8306 float,J8304 float,J8303 float,J8267 float,J8253 float,J8252 float,J8233 float,J8058 float,J8053 float,J8035 float,J8031 float,J8028 float,J8015 float,J8002 float,J8001 float,J7951 float,J7912 float,J7911 float,J7832 float,J7762 float,J7752 float,J7751 float,J7735 float,J7733 float,J7731 float,J7272 float,J7270 float,J7269 float,J7267 float,J7261 float,J7211 float,J7205 float,J7203 float,J7202 float,J7201 float,J7186 float,J7013 float,J7012 float,J7011 float,J7004 float,J7003 float,J6988 float,J6976 float,J6971 float,J6954 float,J6952 float,J6902 float,J6857 float,J6841 float,J6770 float,J6762 float,J6758 float,J6752 float,J6724 float,J6703 float,J6702 float,J6701 float,J6674 float,J6645 float,J6506 float,J6504 float,J6503 float,J6501 float,J6479 float,J6473 float,J6472 float,J6471 float,J6367 float,J6361 float,J6326 float,J6305 float,J6302 float,J6301 float,J6178 float,J6113 float,J6103 float,J6098 float,J5901 float,J5803 float,J5802 float,J5801 float,J5714 float,J5713 float,J5711 float,J5707 float,J5706 float,J5703 float,J5631 float,J5541 float,J5411 float,J5406 float,J5401 float,J5333 float,J5332 float,J5301 float,J5233 float,J5232 float,J5214 float,J5202 float,J5201 float,J5108 float,J5101 float,J5020 float,J5019 float,J4911 float,J4902 float,J4901 float,J4755 float,J4751 float,J4704 float,J4689 float,J4631 float,J4578 float,J4568 float,J4543 float,J4523 float,J4519 float,J4507 float,J4506 float,J4503 float,J4502 float,J4452 float,J4324 float,J4272 float,J4208 float,J4188 float,J4183 float,J4151 float,J4063 float,J4061 float,J4043 float,J4042 float,J4021 float,J4005 float,J4004 float,J3863 float,J3861 float,J3436 float,J3407 float,J3405 float,J3402 float,J3401 float,J3382 float,J3289 float,J3105 float,J3103 float,J3101 float,J3099 float,J3086 float,J2914 float,J2871 float,J2802 float,J2801 float,J2768 float,J2531 float,J2503 float,J2502 float,J2501 float,J2432 float,J2413 float,J2282 float,J2269 float,J2002 float,J1963 float,J1928 float,J1925 float,J1812 float,J1808 float,J1803 float,J1802 float,J1801 float,J1721 float,J1605 float,J1333 float,J1332 float) engine=InnoDB  charset=utf8;
# #
# #
# # # drop table js225_s;
# #
# #
# # # mei
# # #*/3 * * * * /home/w/pyenv/bin/python /home/w/SP500_Nasdap100/SP500.py
# #
# # ##s
# #
#
#
#
#
#
# #


def remove_douhao(num):
    num1 = "".join(num.split(","))
    f_num = str(num1)
    return f_num



def call_page(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.text
        return None
    except RequestException:
        return None





def RemoveDot(item):
    f_l = []
    for it in item:

        f_str = "".join(it.split(","))
        ff_str = f_str +"00"
        f_l.append(ff_str)

    return f_l









# # create table js225_s(id int not null primary key auto_increment,J9984 float,J9983 float,J9766 float,J9735 float,J9613 float,J9602 float,J9532 float,J9531 float,J9503 float,J9502 float,J9501 float,J9437 float,J9433 float,J9432 float,J9412 float,J9301 float,J9202 float,J9107 float,J9104 float,J9101 float,J9064 float,J9062 float,J9022 float,J9021 float,J9020 float,J9009 float,J9008 float,J9007 float,J9005 float,J9001 float,J8830 float,J8804 float,J8802 float,J8801 float,J8795 float,J8766 float,J8750 float,J8729 float,J8725 float,J8630 float,J8628 float,J8604 float,J8601 float,J8411 float,J8355 float,J8354 float,J8331 float,J8316 float,J8309 float,J8308 float,J8306 float,J8304 float,J8303 float,J8267 float,J8253 float,J8252 float,J8233 float,J8058 float,J8053 float,J8035 float,J8031 float,J8028 float,J8015 float,J8002 float,J8001 float,J7951 float,J7912 float,J7911 float,J7832 float,J7762 float,J7752 float,J7751 float,J7735 float,J7733 float,J7731 float,J7272 float,J7270 float,J7269 float,J7267 float,J7261 float,J7211 float,J7205 float,J7203 float,J7202 float,J7201 float,J7186 float,J7013 float,J7012 float,J7011 float,J7004 float,J7003 float,J6988 float,J6976 float,J6971 float,J6954 float,J6952 float,J6902 float,J6857 float,J6841 float,J6770 float,J6762 float,J6758 float,J6752 float,J6724 float,J6703 float,J6702 float,J6701 float,J6674 float,J6645 float,J6506 float,J6504 float,J6503 float,J6501 float,J6479 float,J6473 float,J6472 float,J6471 float,J6367 float,J6361 float,J6326 float,J6305 float,J6302 float,J6301 float,J6178 float,J6113 float,J6103 float,J6098 float,J5901 float,J5803 float,J5802 float,J5801 float,J5714 float,J5713 float,J5711 float,J5707 float,J5706 float,J5703 float,J5631 float,J5541 float,J5411 float,J5406 float,J5401 float,J5333 float,J5332 float,J5301 float,J5233 float,J5232 float,J5214 float,J5202 float,J5201 float,J5108 float,J5101 float,J5020 float,J5019 float,J4911 float,J4902 float,J4901 float,J4755 float,J4751 float,J4704 float,J4689 float,J4631 float,J4578 float,J4568 float,J4543 float,J4523 float,J4519 float,J4507 float,J4506 float,J4503 float,J4502 float,J4452 float,J4324 float,J4272 float,J4208 float,J4188 float,J4183 float,J4151 float,J4063 float,J4061 float,J4043 float,J4042 float,J4021 float,J4005 float,J4004 float,J3863 float,J3861 float,J3436 float,J3407 float,J3405 float,J3402 float,J3401 float,J3382 float,J3289 float,J3105 float,J3103 float,J3101 float,J3099 float,J3086 float,J2914 float,J2871 float,J2802 float,J2801 float,J2768 float,J2531 float,J2503 float,J2502 float,J2501 float,J2432 float,J2413 float,J2282 float,J2269 float,J2002 float,J1963 float,J1928 float,J1925 float,J1812 float,J1808 float,J1803 float,J1802 float,J1801 float,J1721 float,J1605 float,J1333 float,J1332 float) engine=InnoDB  charset=utf8;
