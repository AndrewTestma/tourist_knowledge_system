## 项目文件结构

```plaintext
tourist_knowledge_system/
├── kg_module/                 # 知识图谱模块
│   ├── __init__.py
│   ├── entity_manager.py      # 实体管理
│   ├── relationship_manager.py # 关系管理
│   └── query_builder.py       # 查询构建器
├── mcp_service/               # MCP服务模块
│   ├── __init__.py
│   ├── data_fetcher.py        # 数据获取
│   ├── data_processor.py      # 数据处理
│   ├── cache_manager.py       # 缓存管理
│   └── sync_service.py        # 数据同步
├── milvus_module/             # Milvus模块
│   ├── __init__.py
│   ├── vector_manager.py      # 向量管理（向量生成、存储与更新）
│   └── search_engine.py       # 检索引擎（实现向量相似度搜索、查询处理及结果排序）
├── app_services/              # 应用服务模块
│   ├── __init__.py
│   ├── route_planner.py       # 路线规划
│   ├── recommender.py         # 推荐服务
│   └── visualizer.py          # 数据可视化
├── common/                    # 公共模块
│   ├── __init__.py
│   ├── config.py              # 配置管理
│   ├── logger.py              # 日志管理
│   └── utils.py               # 工具函数
├── api/                       # API接口
│   ├── __init__.py
│   ├── routes.py              # 路由定义
│   └── schemas.py             # 数据模型
├── tests/                     # 测试目录
│   ├── test_kg.py
│   ├── test_mcp.py
│   └── test_milvus.py
├── main.py                    # 程序入口
├── requirements.txt           # 依赖列表
└── README.md                  # 项目说明
```

## 动态属性实现方案

1. 数据获取层（data_fetcher.py）：
   - 针对不同类型的动态属性（天气、客流、停车场）实现了 API 调用
   - 包含错误处理和日志记录
   - 支持通过配置文件管理 API 端点和密钥
2. 数据处理层（data_processor.py）：
   - 将原始 API 数据转换为标准化的 "实体 - 时间 - 属性值" 结构
   - 统一时间格式，便于时序数据库存储
   - 提取关键属性，过滤无关数据
3. 存储与关联层（relationship_manager.py）：
   - 使用 InfluxDB 存储时序数据
   - 知识图谱中建立实体与动态属性类型的关联
   - 提供动态属性查询接口
4. 同步调度层（sync_service.py）：
   - 使用 APScheduler 实现不同频率的同步任务
   - 天气数据每小时同步一次
   - 客流数据每 10 分钟同步一次
   - 停车场数据每 30 分钟同步一次
   - 支持动态加载需要监控的实体列表
5. 配置层（config.py）：
   - 集中管理所有配置参数
   - 支持通过环境变量或.env 文件配置
   - 包含数据库连接、API 信息和同步频率等配置
