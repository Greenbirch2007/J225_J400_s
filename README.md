# J225_J400_s

2020.2.26


1. 尝试使用多线程来写爬虫(多线程还是很厉害!)

2. https://indexes.nikkei.co.jp/nkave/index/component?idx=jpxnk400


3. 也弄一个计划任务


4. 修改数据源,多线程发威啦!

多线程爬虫



计划任务就是每周1到周5,早8点每隔30分钟执行一次

分钟  小时 天  周  月

0,30  0-6 * * 1-5   /usr/local/bin/python3.6 /root/J225_J400_s/JS225_s.py
0,30  0-6 * * 1-5   /usr/local/bin/python3.6 /root/J225_J400_s/JS400_s.py
