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
依赖库清单
