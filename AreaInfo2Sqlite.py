import asyncio
import json
import asyncpg
import enum
import time
from bs4 import BeautifulSoup
from bs4.element import Tag, ResultSet
import aiofiles
from aiohttp import ClientSession, TCPConnector, ClientTimeout, ServerDisconnectedError, ClientConnectorError
from asyncpg import Pool


# 全局数据库信息 = > config.json
CONFIG: dict = None
# 全局数据库连接池
POOL: Pool = None
# 全局http回话
SESSION: ClientSession = None
# 区划代码首页地址
URL_BASE: str = 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/index.html'
# 区划代码发布日期字典
DATE_DICT: dict[int, str] = None
# 休眠
TIME_SLEEP: int = 0.5
# 数据缓存
DATA_TEMP: list[tuple[int, str, str, str, int, int, int, list[int], str]] = []
# 城市缓存
DATA_CITY: list[tuple[str, int, list[int], str]] = []
# 当前年
CONTEXT_YEAR: int = None


class AreaType(enum.Enum):
    Province = 1 << 48
    City = 1 << 36
    Country = 1 << 24
    Town = 1 << 12
    Village = 1


async def main() -> None:
    start_time = time.time()
    await init()
    await start()
    await SESSION.close()
    out('main', f'程序共耗时{time_use(start_time)}s')


async def init() -> None:
    # 1 初始化配置信息
    await init_config()
    # 2 初始化数据库
    await init_sql()
    # 3 初始化table
    await init_table()
    # 4 初始化session
    await init_session()
    # 5 初始化日期字典
    await init_date()


async def init_config() -> None:
    global CONFIG
    info = await read_file('config.json')
    out('init_config', info)
    CONFIG = json.loads(info)
    out('init_config', '初始化配置信息成功')


async def init_sql() -> None:
    global POOL
    conn: str = CONFIG['ODBC']
    out('init_sql', f'正在连接 > {conn}')
    POOL = await asyncpg.create_pool(conn)
    out('init_sql', '数据库连接成功')


async def init_table() -> None:
    sql = await read_file('table.sql')
    out('init_table', f'初始化表 > {sql}')
    out('init_table', f'插入语句 > {CONFIG["InsertSQL"]}')
    async with POOL.acquire() as conn:
        await conn.execute(sql)
    out('init_table', '数据表初始化完成')


async def init_session() -> None:
    out('init_session', '初始化Session')
    time_out: int = 1
    conn_limit: int = 50
    header_dic: dict[str, str] = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 Edg/96.0.1054.62',
        'referer': URL_BASE}
    global SESSION
    SESSION = ClientSession(
        timeout=ClientTimeout(total=time_out),
        headers=header_dic,
        connector=TCPConnector(limit=conn_limit, force_close=True))
    out('init_session', f'Session初始化成功 > 超时:{time_out} 限制连接数:{conn_limit}')


async def init_date() -> None:
    body = await get_data(URL_BASE)
    html = BeautifulSoup(body, 'html.parser', from_encoding='gb18030')
    date_dict: dict[int, str] = {}
    for i in html.select('ul.center_list_contlist span.cont_tit'):
        date = i.select('font')
        date_dict[int(date[0].text.replace('年', ''))] = date[1].text
    global DATE_DICT
    DATE_DICT = date_dict
    out('init_date', '区域数据发布日期初始化完成')
    out('init_date', DATE_DICT)


async def start() -> None:
    for year in CONFIG['Year']:
        if(year in DATE_DICT):
            global CONTEXT_YEAR
            CONTEXT_YEAR = year
            out('start', f'开始下载{year}年数据')
            await make_data()
        else:
            out('start', f'未找到{year}年数据')


async def make_data() -> None:
    """组装数据 核心函数大变样 以市为分界线进行分区读取 增加模块的专一性
    拆分功能职责 将通用部分声明称公共变量"""
    # 加载市以上的行政单位 包括城市
    url = f'{trim_right(URL_BASE)}{CONTEXT_YEAR}/index.html'
    body = await get_data(url)
    html = BeautifulSoup(body, 'html.parser', from_encoding='gb18030')
    page_rows: tuple[AreaType, ResultSet[Tag]] = read_data(html)
    provinces = build_data(data=page_rows[1],
                           type=page_rows[0],
                           page_url=url)
    out('make_data', '加载城市数据')
    global DATA_CITY
    for p in provinces:
        ps = p[2].copy()
        ps.append(p[1])
        p_body = await get_data(p[0])
        p_html = BeautifulSoup(p_body, 'html.parser', from_encoding='gb18030')
        p_page_rows: tuple[AreaType, ResultSet[Tag]] = read_data(p_html)
        DATA_CITY += build_data(data=p_page_rows[1],
                                type=p_page_rows[0],
                                page_url=p[0],
                                parent_id=p[1],
                                parents_id=ps,
                                parent_full_name=p[3])
    for c in DATA_CITY:
        out('make_data', f'执行下载数据 >> {c[3]}')
        start_time = time.time()
        await next_down(c)
        out('make_data', f'{c[3]} 数据下载成功 用时{time_use(start_time)}s')
        await save_data()
    # 完成一年的数据加载后清除城市缓存信息
    DATA_CITY.clear()


async def next_down(info: tuple[str, int, list[int], str], is_rec: bool = True) -> None:
    """加载市以下的行政单位 不包括城市"""
    ps = info[2].copy()
    ps.append(info[1])
    body = await get_data(info[0])
    html = BeautifulSoup(body, 'html.parser', from_encoding='gb18030')
    next_page_rows: tuple[AreaType, ResultSet[Tag]] = read_data(html)
    if(next_page_rows == None):
        out('next_down', f'奇奇怪怪日志 url >> {info[0]}')
        return
    next_infos = build_data(data=next_page_rows[1],
                            type=next_page_rows[0],
                            page_url=info[0],
                            parent_id=info[1],
                            parents_id=ps,
                            parent_full_name=info[3])
    if(next_infos and is_rec):
        for ni in next_infos:
            await next_down(ni)


async def save_data() -> None:
    if(DATA_TEMP):
        start_time = time.time()
        async with POOL.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(CONFIG['InsertSQL'], DATA_TEMP)
        out('save_data', f'已插入数据{len(DATA_TEMP)}条 耗时{time_use(start_time)}s')
        DATA_TEMP.clear()


def read_data(html: BeautifulSoup) -> tuple[AreaType, ResultSet[Tag]]:
    """读取数据 全新改版 核心思想不变 增加异常数据报错 为空时是大胡同街道场景"""
    rows = html.select('tr.villagetr')
    if(rows):
        return (AreaType.Village, rows)
    rows = html.select('tr.towntr')
    if(rows):
        return (AreaType.Town, rows)
    rows = html.select('tr.countytr')
    if(rows):
        return (AreaType.Country, rows)
    rows = html.select('tr.citytr')
    if(rows):
        return (AreaType.City, rows)
    rows = html.select('tr.provincetr a')
    if(rows):
        return (AreaType.Province, rows)
    if(html.select('a.STYLE3')):
        out('read_data', f'注意奇奇怪怪发生啦 {html.prettify()}')
        return None
    else:
        out('read_data', html.prettify())
        raise Exception('捕获到异常数据')


def build_data(data: ResultSet[Tag],
               type: AreaType,
               page_url: str,
               parent_id: int = None,
               parents_id: list[int] = [],
               parent_full_name: str = '') -> list[tuple[str, int, list[int], str]]:
    """构建数据对象 这个地方应该放回下级对象的所需的本方法所有参数 以满足递归调用"""
    next_base_url = trim_right(page_url)
    loop = len(data)
    if(type == AreaType.Village):
        for i in range(loop):
            e: ResultSet[Tag] = data[i].find_all('td')
            name: str = e[2].text
            model = (type.value * (i+1) + parent_id, e[0].text, name,
                     f'{parent_full_name}/{name}', int(e[1].text),
                     level(type), CONTEXT_YEAR, parents_id, DATE_DICT[CONTEXT_YEAR])
            DATA_TEMP.append(model)
        return []
    elif(type == AreaType.Province):
        next_infos: list = []
        for i in range(loop):
            id = type.value * (i+1)
            e: Tag = data[i]
            href: str = e.attrs['href']
            name: str = e.text
            model = (id, href[0: 2].ljust(12, '0'), name, name,
                     None, level(type), CONTEXT_YEAR, [], DATE_DICT[CONTEXT_YEAR])
            DATA_TEMP.append(model)
            next_info = (f'{next_base_url}{href}', id, parents_id, name)
            next_infos.append(next_info)
        return next_infos
    else:
        next_infos: list = []
        for i in range(loop):
            id = type.value * (i+1) + parent_id
            e: ResultSet[Tag] = data[i].find_all('td')
            name: str = e[1].text
            full_name = f'{parent_full_name}/{name}'
            model = (id, e[0].text, name, full_name, None, level(type),
                     CONTEXT_YEAR, parents_id, DATE_DICT[CONTEXT_YEAR])
            DATA_TEMP.append(model)
            a: Tag = e[0].find('a')
            if(a):
                url = f"{next_base_url}{a.attrs['href']}"
                next_info = (url, id, parents_id, full_name)
                next_infos.append(next_info)
        return next_infos


def level(type: AreaType) -> int:
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


async def get_data(url: str) -> bytes:
    """当404网站出现时 该方法会返回None 使用该方法需要判断是否为空"""
    try:
        async with SESSION.get(url) as resp:
            if(resp.status == 200):
                return await resp.content.read()
            elif(resp.status == 404):
                body = await resp.text()
                out('get_data', f'404出现了 {url} {body}')
                return None
            elif(resp.status == 502):
                body = await resp.text()
                out('get_data', f'502出现了 {TIME_SLEEP}秒后重试 {url} {body}')
                time.sleep(TIME_SLEEP)
                return await get_data(url)
            else:
                out('get_data', '警告 捕获到未知异常 下面是当前页面请求体')
                out('get_data', await resp.text())
                raise Exception("get_data")
    # TimeoutError类名与urllib类库的超时异常类重名 这里需要指定
    except (asyncio.exceptions.TimeoutError, ServerDisconnectedError, ClientConnectorError):
        out('get_data', f'读取超时 {TIME_SLEEP}秒后重试 {url}')
        time.sleep(TIME_SLEEP)
        return await get_data(url)


async def read_file(file_name: str) -> str:
    async with aiofiles.open(file_name) as file:
        return await file.read()


def get_sql(year_number) -> str:
    return f"""create table if not exists area_info_{year_number}(
                id int8 not null,
                number varchar(20) not null,
                name text not null,
                full_name text not null,
                type int4 null,
                level int4 not null,
                year int4 not null,
                parents_id _int8 not null,
                release_date date not null,
	            create_time timestamp not null default (datetime('now', 'localtime')),
                constraint area_info_pk primary key (id,year)
            );"""


def time_use(start_time: float) -> float:
    return round(time.time()-start_time, 2)


def out(mode: str, desc: any) -> None:
    print(f"{time.strftime('[%H:%M:%S]', time.localtime())} [{mode}] {desc}")


def trim_right(str: str) -> str:
    """移除该字符串从右往左数第一个'/'右边的字符"""
    return str[: str.rfind('/')+1]


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        out('程序 ctrl + c 中止')
