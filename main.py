import asyncio
import asyncpg
import enum
import requests
import time
from bs4 import BeautifulSoup
from sqlalchemy import Table


# 区划代码首页地址
URL_BASE = 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/index.html'
# 超时
HTTP_TIME_OUT = 2
# 休眠
HTTP_SLEEP = 60
# 区划代码发布日期字典
RELEASE_DATE_DICT = {}
# 全局变量 全局数据库连接池
global POOL


class AreaType(enum.Enum):
    """地区类型与划分的int64二进制区段的枚举"""
    Province = 1 << 48
    City = 1 << 36
    Country = 1 << 24
    Town = 1 << 12
    Village = 1


async def main():
    """程序入口"""
    await init_pool()
    await init_table()
    # init_date_dict()
    await save(None)
    # print(RELEASE_DATE_DICT)
    # for k in RELEASE_DATE_DICT:
    #     # out(f'{trim_right(URL_BASE)}{k}/index.html')
    #     read_data(f'{trim_right(URL_BASE)}{k}/index.html', None, 2021)
    # await read_data(f'{trim_right(URL_BASE)}{2021}/index.html', None, 2021, None)
    # out(trim_right(URL_BASE))


async def init_pool():
    global POOL
    # 数据库连接字符串
    # postgres://user:password@host:port/database?option=value
    # pool连接池使用默认的参数就可
    POOL = await asyncpg.create_pool(read_file('ODBC.txt'))


async def init_table():
    """初始化连接字符串 初始化数据表"""
    global CONNECTING_STRING
    # 通过异步上下文管理器的方式创建, 会自动帮我们关闭引擎
    async with POOL.acquire() as conn:
        await conn.execute(read_file('table.sql'))
    out('数据表初始化完成')


async def save(infos):
    async with POOL.acquire() as conn:
        # 使用 executemany 加事务的方式
        # 因为这里只关注性能和是否插入成功
        async with conn.transaction():
            await conn.execute("insert into china_area values (1,'1',  '1',  '1',      null,1,    1,   array[1],  to_date('2018-03-12','yyyy-MM-dd'),now());")


async def read_data(url, parent, year, parents_id):
    """
    该程序的关键函数
    该方法为递归爬取数据 
    url必须是全路径
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
            info['full_name'] = f"{parent['full_name']}/{e[1].text}"
            info['type'] = None
            info['level'] = level(data[0])
            info['year'] = year
            info['parents_id'] = parents_id
            info['release_date'] = RELEASE_DATE_DICT[year]
            info['create_time'] = time.localtime()
            infos.append(info)
            a = e[0].find('a')
            if(a):
                urls.append(a.attrs['href'])
            else:
                urls.append(None)
    await save(infos)
    for i in range(len(urls)):
        if(urls[i]):
            # parents_id 涉及浅拷贝和深拷贝的指针问题
            # 使用list.copy() 比较耗费性能 因此父级统一计算 再传递给子级
            # 这样做可以解决计算的指数级增长问题
            info = infos[i]
            ids = info['parents_id'].copy()
            ids.append(info['id'])
            await read_data(trim_right(url)+urls[i], info, year, ids)


def level(type):
    """level与type的映射"""
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
    """移除该字符串从右往左数第一个'/'右边的字符"""
    return str[:str.rfind('/')+1]


def area_type(html):
    """这个函数是当前版本升级的亮点：使用css类名获取区划等级，达到100%正确率，性能比前两个版本判断区划编码和判断链接的方法得到显著提升"""
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
    """根据路径读文件内容"""
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
    asyncio.run(main())
