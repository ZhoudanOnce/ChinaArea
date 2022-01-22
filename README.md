# ChinaAreaInfo

中国行政区域信息爬虫 第三代重构版

#### 优化点

- 使用全新的 python 语言
- 优化 Id 字段 将 Id 的运用发挥到极致 只需要对 Id 添加索引就可以进行各种高性能的查询操作 无需其他字段
- 全新的递归逻辑 插入时记录 ParentsId 和 FullName
- 一次加载所有的发布日期 转化为字典
- 极大的减少了之前冗余臃肿的实体类对象 减少了文件与代码
- 增加数据库连接池 极大地提高了大批量数据库插入性能

#### 3.0 更新点

- 修复字节编码错误
- 优化网络交互开支
- 优化指数级运算增长的地址和父 id 的问题

#### 4.0 更新点

- 优化镜像的体积
- 增加 config.json 配置文件 便于爬虫的更多爬取选项

#### 5.0 更新点

- 全部变成异步函数
- 指定函数参数类型

### 6.0 更新点

- 超强纠错 没有读到数据再去请求一遍
- 移除 HTTP 连接限制

### 6.5 更新点

- 解决金门县在 2011、2012 年数据中的无效连接问题
