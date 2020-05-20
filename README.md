# bloom-server

布隆过滤器服务

直接运行或编译运行

示例：`http://localhost:2020/?url=https://www.example.com&prefix=github`

> 其中prefix代表布隆分类，支持多个布隆过滤器
> url是连接地址，返回true或false代表是否存在

每次运行服务前会加载本地布隆文件，当不存在对应分类的布隆文件会新建一个
