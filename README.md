# 黑马点评（Redis 高并发增强版）

一个基于 Spring Boot、MySQL 和 Redis 的本地生活点评项目。项目在基础的店铺查询、用户登录、达人探店、关注 Feed 等业务之上，重点实现了多级缓存、动态热点识别、异步秒杀下单、订单状态并发控制与失败补偿，并提供可重复执行的性能测试脚本和测试报告。

## 项目亮点

- **两级缓存**：使用 `Caffeine → Redis → MySQL` 查询链路，结合 Cache Aside、缓存空值和逻辑过期处理缓存穿透与热点 Key 失效问题。
- **热点识别**：通过 Redis Lua 滑动窗口原子统计访问频率，热点数据自动晋升至 Caffeine；使用 Redis Stream 通知多实例失效本地缓存。
- **缓存重建**：使用 Redisson 分布式锁与有界线程池异步重建缓存，锁竞争失败时优先返回可用旧数据。
- **高并发秒杀**：Redis Lua 原子完成库存校验、库存预扣和一人一单校验，Redis Stream Consumer Group 异步落库。
- **可靠订单消费**：事务成功后 ACK，支持 Pending List 恢复、有限重试、死信 Stream、库存与购买资格补偿。
- **订单状态管理**：SpringTask 定时关闭超时订单，通过 CAS 条件更新解决支付与关单竞争，并保证库存只补偿一次。
- **社交与位置服务**：支持博客点赞、关注 Feed、共同关注、签到统计，以及基于 Redis GEO 的附近店铺查询。

## 技术栈

| 分类 | 技术 |
| --- | --- |
| 后端 | Java 8+、Spring Boot 2.3.12、Spring MVC |
| 数据访问 | MyBatis-Plus 3.4.3、MySQL 8 |
| 缓存与消息 | Redis、Lettuce、Caffeine、Redis Stream |
| 并发控制 | Redis Lua、Redisson、CAS、线程池 |
| 前端 | Vue 2、Element UI、Axios、Nginx |
| 测试 | JUnit、Python HTTP Benchmark |

## 系统架构

```text
Browser
   │
   ▼
Nginx :8080 ── /api/* ──▶ Spring Boot :8081
                              │
                 ┌────────────┴────────────┐
                 ▼                         ▼
          Caffeine / Redis             MySQL :3307
                 │                         ▲
                 └── Redis Stream ─────────┘
                     异步订单 / 缓存失效
```

## 目录结构

```text
.
├── backend/                       # Spring Boot 后端
│   ├── src/main/java/com/hmdp/
│   │   ├── controller/            # HTTP 接口
│   │   ├── service/               # 业务与异步消费者
│   │   ├── mapper/                # MyBatis-Plus Mapper
│   │   ├── config/                # MVC、Redisson、数据库配置
│   │   └── utils/                 # 缓存、锁、ID 生成等工具
│   ├── src/main/resources/
│   │   ├── db/hmdp.sql            # 数据库结构与示例数据
│   │   └── *.lua                  # 秒杀、补偿、热点识别脚本
│   └── benchmarks/                # 压测脚本、结果与报告
├── frontend/                      # Vue 静态页面与 Nginx
└── PROJECT_TARGET_CHECKLIST.md    # 功能实现与验收记录
```

## 快速开始

### 1. 环境要求

- JDK 8 或 11（推荐 JDK 11）
- Maven 3.6+
- MySQL 8.0
- Redis 6.2+
- Nginx（仓库内已包含配置）
- Python 3（仅运行压测时需要）

项目默认连接参数如下：

| 服务 | 地址 | 用户名 / 密码 |
| --- | --- | --- |
| MySQL | `127.0.0.1:3307/hmdp` | `root / 123456` |
| Redis | `127.0.0.1:6380` | 无密码 |
| 后端 | `http://127.0.0.1:8081` | - |
| 前端 | `http://127.0.0.1:8080` | - |

如本地环境不同，请修改 `backend/src/main/resources/application.yaml`。Redisson 的地址目前配置在 `backend/src/main/java/com/hmdp/config/RedissonConfig.java`。

### 2. 启动基础服务

可使用本机服务，也可以用 Docker 快速启动：

```bash
docker run -d --name hmdp-mysql \
  -p 3307:3306 \
  -e MYSQL_ROOT_PASSWORD=123456 \
  -e MYSQL_DATABASE=hmdp \
  mysql:8.0

docker run -d --name hmdp-redis \
  -p 6380:6379 \
  redis:6.2-alpine
```

导入数据库结构和示例数据：

```bash
docker exec -i hmdp-mysql \
  mysql -uroot -p123456 hmdp \
  < backend/src/main/resources/db/hmdp.sql
```

应用启动时还会幂等补充订单唯一索引和超时订单扫描索引。

### 3. 启动后端

```bash
cd backend
mvn spring-boot:run
```

也可以先构建再运行：

```bash
cd backend
mvn clean package
java -jar target/hm-dianping-0.0.1-SNAPSHOT.jar
```

### 4. 启动前端

macOS / Linux：

```bash
nginx -p "$PWD/frontend/" -c conf/nginx.conf
```

Windows 可在 `frontend` 目录运行：

```powershell
.\nginx.exe
```

浏览器访问 `http://localhost:8080`。Nginx 会将 `/api/*` 请求代理到 `http://127.0.0.1:8081`。

> 登录验证码为开发模式：调用发送验证码接口后，验证码会输出在后端控制台。

## 核心流程

### 店铺缓存

```text
请求 → Caffeine → Redis → MySQL
          │          │
          └─ 热点晋升 └─ 空值缓存 / 逻辑过期
                         └─ Redisson 锁 + 异步重建
```

店铺更新成功后删除 Redis 缓存、失效本地缓存，并通过 Stream 向其他实例广播失效事件；删除失败时进入补偿重试流程，TTL 作为最终一致性兜底。

### 秒杀下单

```text
请求
  └─ Lua：校验库存 + 一人一单 + 预扣库存
       └─ XADD stream.orders
            └─ Consumer Group
                 └─ MySQL 条件扣库存 + 创建订单
                      ├─ 成功：ACK
                      └─ 失败：重试 → 死信 → Redis 补偿
```

数据库通过 `(user_id, voucher_id)` 唯一索引兜底防重，库存更新包含 `stock > 0` 条件，消费端使用订单 ID 和唯一索引保证幂等。

## 测试

运行后端测试：

```bash
cd backend
mvn test
```

运行缓存压测：

```bash
cd backend
python3 benchmarks/http_benchmark.py cache \
  --requests 10000 \
  --concurrency 100 \
  --shop-id 1
```

运行秒杀压测前，需要创建有效的测试券、初始化 Redis 库存，并在 `backend/tokens.txt` 中准备不同用户的登录 Token：

```bash
cd backend
python3 benchmarks/http_benchmark.py seckill \
  --requests 1000 \
  --concurrency 100 \
  --voucher-id 90001 \
  --tokens tokens.txt
```

更完整的准备与验收方式见 [压测说明](backend/benchmarks/README.md)。

## 性能结果

以下为本机 macOS、JDK 11、Docker MySQL/Redis 环境下的受控测试结果，不代表生产环境容量。

| 场景 | QPS | P99 |
| --- | ---: | ---: |
| MySQL 直查 | 5,362.81 | 43.88 ms |
| Redis 单级缓存 | 6,313.59 | 36.56 ms |
| Caffeine + Redis | 8,232.77 | 30.31 ms |
| 同步秒杀下单 | 861.45 | 408.81 ms |
| Lua + Stream 异步下单 | 3,668.06 | 68.59 ms |

在 1,000 个不同用户并发抢购 200 份库存的测试中，最终创建 200 个订单，重复订单和超卖订单均为 0，Redis 与 MySQL 剩余库存均为 0。异步版本吞吐量为同步基线的 4.26 倍，P99 降低 83.22%。

原始数据与测试边界见 [性能与正确性报告](backend/benchmarks/PERFORMANCE_REPORT.md)。

## 常用接口

| 功能 | 方法与路径 |
| --- | --- |
| 发送验证码 / 登录 | `POST /user/code`、`POST /user/login` |
| 查询店铺 | `GET /shop/{id}` |
| 附近店铺 | `GET /shop/of/type` |
| 发布 / 查询博客 | `POST /blog`、`GET /blog/hot` |
| 关注与共同关注 | `PUT /follow/{id}/{isFollow}`、`GET /follow/common/{id}` |
| 关注 Feed | `GET /blog/of/follow` |
| 用户签到 | `POST /user/sign` |
| 秒杀下单 | `POST /voucher-order/seckill/{id}` |
| 支付订单 | `POST /voucher-order/{id}/pay` |
| 缓存指标 | `GET /shop/cache/metrics` |
| 订单消费指标 | `GET /voucher-order/metrics` |

通过前端访问时，请在路径前添加 `/api`；直接请求后端 `8081` 端口时无需添加。

## 注意事项

- 当前配置包含开发环境数据库密码，只适合本地学习；部署时应改用环境变量或外部配置。
- `backend/tokens.txt` 是压测用临时 Token 文件，不应提交真实凭证。
- `RedissonConfig` 还定义了 `6381`、`6382` 两个客户端供多节点锁测试使用；正常运行主流程只依赖 `6380`，执行相关 Redisson 测试时需额外启动对应 Redis 实例。
- 上传图片默认写入前端静态资源目录，部署时请改为对象存储或独立文件服务。

## 致谢

项目基于黑马程序员 Redis 实战课程中的“黑马点评”案例扩展，主要用于学习缓存一致性、高并发秒杀、异步消息消费和并发状态管理。
