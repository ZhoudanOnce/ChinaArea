import asyncio
import json
import asyncpg
import enum
import requests
import time
from bs4 import BeautifulSoup


# 超时
HTTP_TIME_OUT = 3
# 休眠
HTTP_SLEEP = 20

# 区划代码发布日期字典
RELEASE_DATE_DICT = {}
# 全局变量 全局数据库信息 => config.json
global SQL_INFO
# 全局变量 全局数据库连接池
global POOL


# 区划代码首页地址
URL_BASE = 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/index.html'
# headers
HTTP_HEADERS = {
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 Edg/96.0.1054.62', 'referer': URL_BASE}


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
    init_date_dict()
    for k in SQL_INFO['Year']:
        if(k in RELEASE_DATE_DICT):
            await read_data(f'{trim_right(URL_BASE)}{k}/index.html', None, k, [])
        else:
            out(f'未找到{k}年数据')


async def init_pool():
    global SQL_INFO
    SQL_INFO = json.loads(read_file('config.json'))
    global POOL
    # 数据库连接字符串
    # postgres://user:password@host:port/database?option=value
    # pool连接池使用默认的参数就可
    POOL = await asyncpg.create_pool(SQL_INFO['ODBC'])


async def init_table():
    """初始化连接字符串 初始化数据表"""
    # 通过异步上下文管理器的方式创建, 会自动帮我们关闭引擎
    async with POOL.acquire() as conn:
        await conn.execute(read_file('table.sql'))
    out('数据表初始化完成')


async def save(infos):
    async with POOL.acquire() as conn:
        # 使用 executemany 加事务的方式
        # 因为这里只关注性能和是否插入成功
        async with conn.transaction():
            await conn.executemany(SQL_INFO['InsertSQL'], infos)
    out(f'已插入{len(infos)}条数据')


async def read_data(url, parent, year, parents_id):
    """
    该程序的关键函数
    该方法为递归爬取数据
    url必须是全路径
    """
    html = BeautifulSoup(http_get(url), 'html.parser', from_encoding='gb18030')
    # 将数据从html中抽离出来
    data = area_type(html)
    # 转化数据为数据库对象
    infos = []
    urls = []
    url = trim_right(url)
    if(data[0] == AreaType.Village):
        for i in range(len(data[1])):
            info = []
            e = data[1][i].find_all('td')
            info.append(data[0].value * (i+1) + parent[0])
            info.append(e[0].text)
            info.append(e[2].text)
            info.append(f"{parent[3]}/{e[2].text}")
            info.append(int(e[1].text))
            info.append(level(data[0]))
            info.append(year)
            info.append(parents_id)
            info.append(RELEASE_DATE_DICT[year])
            infos.append(tuple(info))
    elif(data[0] == AreaType.Province):
        # 这个地方无法解析 台湾省 香港特别行政区 澳门特别行政区
        # 因为这三个地区没有行政区划代码
        for i in range(len(data[1])):
            info = []
            e = data[1][i]
            info.append(data[0].value * (i+1))
            href = e.attrs['href']
            info.append(href[0: 2].ljust(12, '0'))
            info.append(e.text)
            info.append(e.text)
            info.append(None)
            info.append(level(data[0]))
            info.append(year)
            info.append(parents_id)
            info.append(RELEASE_DATE_DICT[year])
            infos.append(tuple(info))
            urls.append(f'{url}{href}')
    else:
        for i in range(len(data[1])):
            info = []
            # 不可用.contents 和 .children，只能用find_all
            # 因为这两个方法会将 '\n' 等字符输出
            e = data[1][i].find_all('td')
            info.append(data[0].value * (i+1) + parent[0])
            info.append(e[0].text)
            info.append(e[1].text)
            info.append(f'{parent[3]}/{e[1].text}')
            info.append(None)
            info.append(level(data[0]))
            info.append(year)
            info.append(parents_id)
            info.append(RELEASE_DATE_DICT[year])
            infos.append(tuple(info))
            a = e[0].find('a')
            if(a):
                urls.append(f"{url}{a.attrs['href']}")
            else:
                urls.append(None)
    await save(infos)
    for i in range(len(urls)):
        if(urls[i]):
            # parents_id 涉及浅拷贝和深拷贝的指针问题
            # 使用list.copy() 比较耗费性能 因此父级统一计算 再传递给子级
            # url也放在组装元组之前计算 并且url在组装元组是就计算完成
            # 这样做可以解决计算的指数级增长问题
            info = infos[i]
            ids = info[7].copy()
            ids.append(info[0])
            await read_data(urls[i], info, year, ids)


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
    return str[: str.rfind('/')+1]


def area_type(html):
    """
    这个函数是当前版本升级的亮点：使用css类名获取区划等级，达到100%正确率
    性能比前两个版本判断区划编码和判断链接的方法得到显著提升
    """
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
    html = BeautifulSoup(http_get(URL_BASE), 'html.parser',
                         from_encoding='gb18030')
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
        # 我直接裂开 这个地方经测试不需要判断文本编码 最新bs解决了编码问题 只需要传入参数
        # 由于不用二进制转字符串以及截取字符串查找charset 因此这个地方优化了性能
        result = requests.get(url, timeout=HTTP_TIME_OUT, headers=HTTP_HEADERS)
        # 使用二进制数据 性能再次质的提升
        return result.content
    except requests.exceptions.Timeout:
        out(f'休息{HTTP_SLEEP}秒')
        time.sleep(HTTP_SLEEP)
        return http_get(url)


def out(output):
    """将时间和内容输出到控制台"""
    print(f"{time.strftime('[%H:%M:%S]', time.localtime())} {output}")


if __name__ == '__main__':
    asyncio.run(main())
