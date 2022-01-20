import asyncio
import json
import asyncpg
import enum
import time
from bs4 import BeautifulSoup
from bs4.element import Tag, ResultSet
import aiofiles
import aiohttp
from aiohttp import ClientSession, BaseConnector, ClientTimeout
from asyncpg import Pool


# 超时
HTTP_TIME_OUT: ClientTimeout = aiohttp.ClientTimeout(total=2)
# 休眠
HTTP_SLEEP: int = 2
# 区划代码发布日期字典
RELEASE_DATE_DICT: dict[int, str] = None
# 全局数据库信息 => config.json
SQL_INFO = None
# 全局数据库连接池
POOL: Pool = None
# 全局http连接配置
CONNECTOR: BaseConnector = None
# 区划代码首页地址
URL_BASE: str = 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/index.html'
# headers
HTTP_HEADERS: dict[str, str] = {
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 Edg/96.0.1054.62', 'referer': URL_BASE}


class AreaType(enum.Enum):
    """地区类型与划分的int64二进制区段的枚举"""
    Province = 1 << 48
    City = 1 << 36
    Country = 1 << 24
    Town = 1 << 12
    Village = 1


async def main() -> None:
    """程序入口"""
    global SQL_INFO, HTTP_TIME_OUT, HTTP_HEADERS, CONNECTOR, RELEASE_DATE_DICT, URL_BASE
    await init_pool()
    await init_table()
    async with aiohttp.ClientSession(timeout=HTTP_TIME_OUT, headers=HTTP_HEADERS, connector=CONNECTOR) as session:
        await init_date_dict(session)
        for k in SQL_INFO['Year']:
            if(k in RELEASE_DATE_DICT):
                await read_data(f'{trim_right(URL_BASE)}{k}/index.html', None, k, [], session)
            else:
                out(f'未找到{k}年数据')


async def init_pool() -> None:
    """初始化数据库连接池"""
    global SQL_INFO, POOL
    config = await read_file('config.json')
    SQL_INFO = json.loads(config)
    # 数据库连接字符串
    # postgres://user:password@host:port/database?option=value
    # pool连接池使用默认的参数就可
    POOL = await asyncpg.create_pool(SQL_INFO['ODBC'])


async def init_table() -> None:
    """初始化连接字符串 初始化数据表"""
    global POOL
    # 通过异步上下文管理器的方式创建, 会自动帮我们关闭引擎
    async with POOL.acquire() as conn:
        sql = await read_file('table.sql')
        await conn.execute(sql)
    out('数据表初始化完成')


async def save(infos: list[tuple]) -> None:
    global SQL_INFO, POOL
    async with POOL.acquire() as conn:
        # 使用 executemany 加事务的方式
        # 因为这里只关注性能和是否插入成功
        async with conn.transaction():
            await conn.executemany(SQL_INFO['InsertSQL'], infos)
    out(f'已插入{len(infos)}条数据')


async def read_data(url: str, parent: tuple, year: int, parents_id: list[int], session: ClientSession) -> None:
    """
    该程序的关键函数
    该方法为递归爬取数据
    url必须是全路径
    """
    body = await http_get(url, session)
    html = BeautifulSoup(body, 'html.parser', from_encoding='gb18030')
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
            info.append(f'{parent[3]}/{e[2].text}')
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
            await read_data(urls[i], info, year, ids, session)


def level(type: AreaType) -> int:
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


def trim_right(str: str) -> str:
    """移除该字符串从右往左数第一个'/'右边的字符"""
    return str[: str.rfind('/')+1]


def area_type(html: BeautifulSoup) -> tuple[AreaType, ResultSet[Tag]]:
    """
    这个函数是当前版本升级的亮点：使用css类名获取区划等级，达到100%正确率
    性能比前两个版本判断区划编码和判断链接的方法得到显著提升
    """
    rows = html.select('tr.villagetr')
    if(len(rows) > 0):
        return (AreaType.Village, rows)
    rows = html.select('tr.towntr')
    if(len(rows) > 0):
        return (AreaType.Town, rows)
    rows = html.select('tr.towntr')
    if(len(rows) > 0):
        return (AreaType.Town, rows)
    rows = html.select('tr.countytr')
    if(len(rows) > 0):
        return (AreaType.Country, rows)
    rows = html.select('tr.citytr')
    if(len(rows) > 0):
        return (AreaType.City, rows)
    return (AreaType.Province, html.select('tr.provincetr a'))


async def init_date_dict(session: ClientSession):
    """初始化数据发布日期字典"""
    global RELEASE_DATE_DICT, URL_BASE
    body = await http_get(URL_BASE, session)
    html = BeautifulSoup(body, 'html.parser', from_encoding='gb18030')
    date_dict: dict[int, str] = {}
    for i in html.select('ul.center_list_contlist span.cont_tit'):
        date = i.select('font')
        date_dict[int(date[0].text.replace('年', ''))] = date[1].text
    RELEASE_DATE_DICT = date_dict
    out('区域数据发布日期初始化完成')


async def http_get(url: str, session: ClientSession) -> bytes:
    """异步封装的get请求 加对异常的捕获 使用共用一个session处理并发 优化性能"""
    global HTTP_SLEEP
    try:
        async with session.get(url) as resp:
            return await resp.content.read()
    except (asyncio.exceptions.TimeoutError, asyncio.exceptions.InvalidStateError):
        out(f'休息{HTTP_SLEEP}秒')
        time.sleep(HTTP_SLEEP)
        return await http_get(url, session)


async def read_file(file_name: str) -> str:
    async with aiofiles.open(file_name) as file:
        return await file.read()


def out(output: any) -> None:
    """将时间和内容输出到控制台"""
    print(f"{time.strftime('[%H:%M:%S]', time.localtime())} {output}")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        out('程序 ctrl + c 中止')
