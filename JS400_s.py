

import datetime

import pymysql
import requests
from lxml import etree
import json
from queue import Queue
import threading
from requests.exceptions import RequestException

from code_l import js400_l






def insertDB(content):
    connection = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='123456', db='JS225_JS400',
                                 charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

    cursor = connection.cursor()

    try:
        js400 ='J9989,J9984,J9983,J9962,J9843,J9810,J9766,J9744,J9735,J9719,J9706,J9697,J9684,J9678,J9627,J9613,J9602,J9532,J9531,J9513,J9508,J9506,J9503,J9502,J9437,J9435,J9433,J9432,J9404,J9202,J9201,J9086,J9065,J9064,J9062,J9048,J9045,J9042,J9041,J9024,J9022,J9021,J9020,J9009,J9008,J9007,J9005,J9003,J9001,J8905,J8876,J8850,J8830,J8804,J8802,J8801,J8795,J8766,J8750,J8729,J8725,J8697,J8630,J8628,J8604,J8601,J8593,J8591,J8585,J8570,J8473,J8439,J8424,J8411,J8410,J8331,J8316,J8309,J8308,J8306,J8304,J8303,J8283,J8282,J8279,J8273,J8267,J8252,J8227,J8113,J8111,J8088,J8058,J8056,J8053,J8036,J8035,J8031,J8028,J8020,J8015,J8002,J8001,J7988,J7974,J7956,J7951,J7867,J7846,J7832,J7751,J7747,J7741,J7735,J7733,J7731,J7729,J7717,J7701,J7649,J7606,J7575,J7564,J7550,J7532,J7459,J7458,J7453,J7419,J7313,J7309,J7282,J7276,J7272,J7270,J7269,J7267,J7261,J7259,J7205,J7203,J7202,J7186,J7167,J7164,J7148,J7013,J7012,J7011,J6988,J6981,J6976,J6971,J6965,J6954,J6952,J6923,J6920,J6902,J6877,J6869,J6861,J6857,J6856,J6849,J6845,J6841,J6806,J6770,J6762,J6758,J6755,J6752,J6750,J6728,J6724,J6723,J6702,J6701,J6645,J6641,J6594,J6588,J6586,J6506,J6504,J6503,J6501,J6481,J6479,J6473,J6471,J6463,J6448,J6432,J6383,J6367,J6326,J6324,J6305,J6302,J6301,J6273,J6269,J6268,J6201,J6146,J6141,J6136,J6113,J6098,J6028,J5975,J5947,J5929,J5802,J5801,J5714,J5713,J5703,J5486,J5411,J5401,J5393,J5334,J5333,J5332,J5301,J5233,J5201,J5110,J5108,J5101,J5021,J5020,J5019,J4967,J4927,J4922,J4912,J4911,J4902,J4849,J4819,J4768,J4755,J4751,J4739,J4732,J4716,J4704,J4689,J4684,J4681,J4666,J4661,J4631,J4613,J4612,J4587,J4578,J4568,J4555,J4543,J4536,J4530,J4528,J4523,J4521,J4519,J4516,J4507,J4506,J4503,J4502,J4452,J4403,J4324,J4307,J4246,J4217,J4208,J4206,J4204,J4202,J4188,J4185,J4183,J4182,J4151,J4091,J4088,J4063,J4061,J4043,J4042,J4021,J4005,J4004,J3932,J3861,J3769,J3765,J3738,J3668,J3659,J3626,J3549,J3543,J3436,J3407,J3405,J3402,J3401,J3391,J3382,J3360,J3349,J3291,J3289,J3288,J3254,J3231,J3197,J3167,J3148,J3141,J3116,J3107,J3092,J3088,J3086,J3064,J3048,J3038,J3003,J2914,J2897,J2875,J2871,J2815,J2809,J2802,J2801,J2784,J2782,J2768,J2702,J2670,J2651,J2587,J2503,J2502,J2433,J2432,J2427,J2413,J2412,J2379,J2371,J2337,J2331,J2327,J2282,J2269,J2267,J2229,J2206,J2201,J2181,J2175,J2146,J2127,J2124,J2121,J1959,J1951,J1928,J1925,J1911,J1893,J1881,J1878,J1861,J1860,J1824,J1821,J1820,J1812,J1808,J1803,J1802,J1801,J1721,J1720,J1719,J1605,J1333,J1332'


        f_400 = "%s," * 399
        cursor.executemany('insert into js400_s ({0}) values ({1})'.format(js400, f_400[:-1]), content)
        connection.commit()
        connection.commit()
        connection.close()
        print('向MySQL中添加数据成功！')
    except TypeError:
        pass









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
        for i in js400_l:
            self.url_queue.put(self.url_pattern.format(i))

    @run_forever
    def add_page_to_queue(self):
        ''' 发送请求获取数据 '''
        url = self.url_queue.get()
        # print(url)
        response = requests.get(url, headers=self.headers)
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

        self.run_use_more_task(self.add_page_to_queue, 6)
        self.run_use_more_task(self.add_dz_to_queue, 5)


        # 使用队列join方法,等待队列任务都完成了才结束
        self.url_queue.join()
        self.page_queue.join()







if __name__ == '__main__':
    print(datetime.datetime.now())
    s = datetime.datetime.now()


    big_list = []
    ff_l = []

    qbs = QiubaiSpider()
    qbs.run()

    f_tup = tuple(big_list)
    ff_l.append((f_tup))
    insertDB(ff_l)
    print(datetime.datetime.now())

    e = datetime.datetime.now()
    f = e-s








def RemoveDot(item):
    f_l = []
    for it in item:

        f_str = "".join(it.split(","))
        ff_str = f_str +"00"
        f_l.append(ff_str)

    return f_l







# create table js400_s(id int not null primary key auto_increment,J9989 float,J9984 float,J9983 float,J9962 float,J9843 float,J9810 float,J9766 float,J9744 float,J9735 float,J9719 float,J9706 float,J9697 float,J9684 float,J9678 float,J9627 float,J9613 float,J9602 float,J9532 float,J9531 float,J9513 float,J9508 float,J9506 float,J9503 float,J9502 float,J9437 float,J9435 float,J9433 float,J9432 float,J9404 float,J9202 float,J9201 float,J9086 float,J9065 float,J9064 float,J9062 float,J9048 float,J9045 float,J9042 float,J9041 float,J9024 float,J9022 float,J9021 float,J9020 float,J9009 float,J9008 float,J9007 float,J9005 float,J9003 float,J9001 float,J8905 float,J8876 float,J8850 float,J8830 float,J8804 float,J8802 float,J8801 float,J8795 float,J8766 float,J8750 float,J8729 float,J8725 float,J8697 float,J8630 float,J8628 float,J8604 float,J8601 float,J8593 float,J8591 float,J8585 float,J8570 float,J8473 float,J8439 float,J8424 float,J8411 float,J8410 float,J8331 float,J8316 float,J8309 float,J8308 float,J8306 float,J8304 float,J8303 float,J8283 float,J8282 float,J8279 float,J8273 float,J8267 float,J8252 float,J8227 float,J8113 float,J8111 float,J8088 float,J8058 float,J8056 float,J8053 float,J8036 float,J8035 float,J8031 float,J8028 float,J8020 float,J8015 float,J8002 float,J8001 float,J7988 float,J7974 float,J7956 float,J7951 float,J7867 float,J7846 float,J7832 float,J7751 float,J7747 float,J7741 float,J7735 float,J7733 float,J7731 float,J7729 float,J7717 float,J7701 float,J7649 float,J7606 float,J7575 float,J7564 float,J7550 float,J7532 float,J7459 float,J7458 float,J7453 float,J7419 float,J7313 float,J7309 float,J7282 float,J7276 float,J7272 float,J7270 float,J7269 float,J7267 float,J7261 float,J7259 float,J7205 float,J7203 float,J7202 float,J7186 float,J7167 float,J7164 float,J7148 float,J7013 float,J7012 float,J7011 float,J6988 float,J6981 float,J6976 float,J6971 float,J6965 float,J6954 float,J6952 float,J6923 float,J6920 float,J6902 float,J6877 float,J6869 float,J6861 float,J6857 float,J6856 float,J6849 float,J6845 float,J6841 float,J6806 float,J6770 float,J6762 float,J6758 float,J6755 float,J6752 float,J6750 float,J6728 float,J6724 float,J6723 float,J6702 float,J6701 float,J6645 float,J6641 float,J6594 float,J6588 float,J6586 float,J6506 float,J6504 float,J6503 float,J6501 float,J6481 float,J6479 float,J6473 float,J6471 float,J6463 float,J6448 float,J6432 float,J6383 float,J6367 float,J6326 float,J6324 float,J6305 float,J6302 float,J6301 float,J6273 float,J6269 float,J6268 float,J6201 float,J6146 float,J6141 float,J6136 float,J6113 float,J6098 float,J6028 float,J5975 float,J5947 float,J5929 float,J5802 float,J5801 float,J5714 float,J5713 float,J5703 float,J5486 float,J5411 float,J5401 float,J5393 float,J5334 float,J5333 float,J5332 float,J5301 float,J5233 float,J5201 float,J5110 float,J5108 float,J5101 float,J5021 float,J5020 float,J5019 float,J4967 float,J4927 float,J4922 float,J4912 float,J4911 float,J4902 float,J4849 float,J4819 float,J4768 float,J4755 float,J4751 float,J4739 float,J4732 float,J4716 float,J4704 float,J4689 float,J4684 float,J4681 float,J4666 float,J4661 float,J4631 float,J4613 float,J4612 float,J4587 float,J4578 float,J4568 float,J4555 float,J4543 float,J4536 float,J4530 float,J4528 float,J4523 float,J4521 float,J4519 float,J4516 float,J4507 float,J4506 float,J4503 float,J4502 float,J4452 float,J4403 float,J4324 float,J4307 float,J4246 float,J4217 float,J4208 float,J4206 float,J4204 float,J4202 float,J4188 float,J4185 float,J4183 float,J4182 float,J4151 float,J4091 float,J4088 float,J4063 float,J4061 float,J4043 float,J4042 float,J4021 float,J4005 float,J4004 float,J3932 float,J3861 float,J3769 float,J3765 float,J3738 float,J3668 float,J3659 float,J3626 float,J3549 float,J3543 float,J3436 float,J3407 float,J3405 float,J3402 float,J3401 float,J3391 float,J3382 float,J3360 float,J3349 float,J3291 float,J3289 float,J3288 float,J3254 float,J3231 float,J3197 float,J3167 float,J3148 float,J3141 float,J3116 float,J3107 float,J3092 float,J3088 float,J3086 float,J3064 float,J3048 float,J3038 float,J3003 float,J2914 float,J2897 float,J2875 float,J2871 float,J2815 float,J2809 float,J2802 float,J2801 float,J2784 float,J2782 float,J2768 float,J2702 float,J2670 float,J2651 float,J2587 float,J2503 float,J2502 float,J2433 float,J2432 float,J2427 float,J2413 float,J2412 float,J2379 float,J2371 float,J2337 float,J2331 float,J2327 float,J2282 float,J2269 float,J2267 float,J2229 float,J2206 float,J2201 float,J2181 float,J2175 float,J2146 float,J2127 float,J2124 float,J2121 float,J1959 float,J1951 float,J1928 float,J1925 float,J1911 float,J1893 float,J1881 float,J1878 float,J1861 float,J1860 float,J1824 float,J1821 float,J1820 float,J1812 float,J1808 float,J1803 float,J1802 float,J1801 float,J1721 float,J1720 float,J1719 float,J1605 float,J1333 float,J1332 float) engine=InnoDB  charset=utf8;
