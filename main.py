from itertools import count
import json
import requests
import time
from bs4 import BeautifulSoup
import enum


# 区划代码首页地址
URL_BASE = 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/index.html'
# 超时
HTTP_TIME_OUT = 2
# 休眠
HTTP_SLEEP = 60
# 区划代码发布日期字典
RELEASE_DATE_DICT = {}


class AreaType(enum.Enum):
    Province = 1 << 48
    City = 1 << 36
    Country = 1 << 24
    Town = 1 << 12
    Village = 1


def main():
    """程序入口"""
    init_date_dict()
    # print(RELEASE_DATE_DICT)
    # init_table()
    # for k in RELEASE_DATE_DICT:
    #     # out(f'{trim_right(URL_BASE)}{k}/index.html')
    #     read_data(f'{trim_right(URL_BASE)}{k}/index.html', None, 2021)
    read_data(f'{trim_right(URL_BASE)}{2021}/index.html', None, 2021)
    # out(trim_right(URL_BASE))


def init_table():
    out('init table ...')


def read_data(url, parent, year):
    """
    该程序的关键函数
    该方法为递归爬取数据 url必须是全路径
    """
    html = BeautifulSoup(http_get(url), 'html.parser')
    # 将数据从html中抽离出来
    data = area_type(html)
    # 转化数据为数据库对象
    infos = []
    urls = []
    if(data[0] == AreaType.Village):
        for i in range(0, len(data[1])):
            info = {}
            e = data[1][i].contents
            info['id'] = data[0].value * (i+1) + parent['id']
            info['number'] = e[0].text
            info['name'] = e[2].text
            info['full_name'] = f"{parent['full_name']}/{e[2].text}"
            info['type'] = e[1].text
            info['level'] = level(data[0])
            info['year'] = year
            # 这个地方涉及深拷贝浅拷贝的指针问题
            parents_id = parent['parents_id'].copy()
            parents_id.append(parent['id'])
            info['parents_id'] = parents_id
            info['release_date'] = RELEASE_DATE_DICT[year]
            info['create_time'] = time.localtime()
            infos.append(info)
    elif(data[0] == AreaType.Province):
        for i in range(0, len(data[1])):
            info = {}
            e = data[1][i]
            info['id'] = data[0].value * (i+1)
            href = e.attrs['href']
            info['number'] = href[0:2].ljust(12, '0')
            info['name'] = e.text
            info['full_name'] = e.text
            info['type'] = None
            info['level'] = level(data[0])
            info['year'] = year
            info['parents_id'] = []
            info['release_date'] = RELEASE_DATE_DICT[year]
            info['create_time'] = time.localtime()
            infos.append(info)
            urls.append(href)
    else:
        for i in range(0, len(data[1])):
            info = {}
            e = data[1][i].contents
            info['id'] = data[0].value * (i+1) + parent['id']
            info['number'] = e[0].text
            info['name'] = e[1].text
            out(parent['full_name'])
            info['full_name'] = f"{parent['full_name']}/{e[1].text}"
            info['type'] = None
            info['level'] = level(data[0])
            info['year'] = year
            # 这个地方涉及深拷贝浅拷贝的指针问题
            parents_id = parent['parents_id'].copy()
            parents_id.append(parent['id'])
            info['parents_id'] = parents_id
            info['release_date'] = RELEASE_DATE_DICT[year]
            info['create_time'] = time.localtime()
            infos.append(info)
            a = e[0].find('a')
            if(a):
                urls.append(a.attrs['href'])
            else:
                urls.append(None)
    out(infos)
    for i in range(len(urls)):
        if(urls[i]):
            read_data(trim_right(url)+urls[i], infos[i], year)


def level(type):
    if(type == AreaType.Village):
        return 5
    elif(type == AreaType.Town):
        return 4
    elif(type == AreaType.Country):
        return 3
    elif(type == AreaType.City):
        return 2
    else:
        return 1


def trim_right(str):
    """移除该字符创从右往左数第一个'/'后的字符"""
    return str[:str.rfind('/')+1]


def area_type(html):
    """这个版本升级的亮点：使用区划代码获取区划等级，达到100%正确率，性能比前两个版本判断区划编码和判断链接的方法得到显著提升"""
    list = html.select('tr.villagetr')
    if(len(list) > 0):
        return (AreaType.Village, list)
    list = html.select('tr.towntr')
    if(len(list) > 0):
        return (AreaType.Town, list)
    list = html.select('tr.towntr')
    if(len(list) > 0):
        return (AreaType.Town, list)
    list = html.select('tr.countytr')
    if(len(list) > 0):
        return (AreaType.Country, list)
    list = html.select('tr.citytr')
    if(len(list) > 0):
        return (AreaType.City, list)
    return (AreaType.Province, html.select('tr.provincetr a'))


def init_date_dict():
    """初始化数据发布日期字典"""
    html = BeautifulSoup(http_get(URL_BASE), 'html.parser')
    for i in html.select('ul.center_list_contlist span.cont_tit'):
        date = i.select('font')
        RELEASE_DATE_DICT[int(date[0].text.replace('年', ''))] = date[1].text
    out('区域数据发布日期初始化完成')


def read_file(filename):
    """根据目录读文件"""
    file = open(filename)
    data = file.read()
    file.close()
    return data


def http_get(url):
    """封装requests的get请求，页面转码和超时重试"""
    try:
        result = requests.get(url, timeout=HTTP_TIME_OUT)
        if(result.text.find('gb2312', 100, 300) >= 0):
            result.encoding = 'gb2312'
        else:
            result.encoding = 'utf-8'
        return result.text
    except requests.exceptions.Timeout:
        out(f'休息{HTTP_SLEEP}秒')
        time.sleep(HTTP_SLEEP)
        return http_get(url)


def out(output):
    """将时间和内容输出到控制台"""
    print(f"{time.strftime('[%H:%M:%S]', time.localtime())} {output}")


if __name__ == '__main__':
    main()
