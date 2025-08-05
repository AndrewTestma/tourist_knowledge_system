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


1. **时间范围**

    - `sday`: 20302
    - `eday`: 20305
2. **总收益汇总**

    - `total_gpu`: 12.82 美元（GPU 租赁总收益）
    - `total_stor`: 2.89 美元（存储服务总收益）
    - `total_bwu`: 0 美元（带宽上传收益）
    - `total_bwd`: 0 美元（带宽下载收益）
3. **用户信息**

    - `username`/`email`/`fullname`: 用户注册邮箱及名称（可能未完全填充）
    - `address1`-`country`: 账单地址信息（均为`null`，表示未填写）
    - `taxinfo`: 税务信息（未提供）
      这些字段用于账户验证和收益结算，但用户可能未完善个人资料12。
4. **当前财务状态**

    - `balance`: 76.10 美元（账户可用余额）
    - `service_fee`: 0 美元（服务费，Vast.ai 目前未收取）
    - `total`: 76.10 美元（总金额，等于余额 + 信用额度，此处信用为 0）
    - `credit`: 0 美元（信用额度，未启用）
5. **机器级收益拆分**

    - `machine_id`: 31340（唯一机器标识符）
    - `gpu_earn`: 12.82 美元（该机器贡献的 GPU 收益）
    - `sto_earn`: 2.89 美元（该机器贡献的存储收益）
    - `bwu_earn`/`bwd_earn`: 0 美元（该机器未产生带宽收益）
6. **每日收益明细**

    - `day`: 20302-20305（日期编码）
    - `gpu_earn`: 单日 GPU 使用小时数（如 20302 日为 19.45 小时）
    - `sto_earn`: 单日存储使用量（如 20302 日为 3.92 GB / 月等效值）
    - `bwu_earn`/`bwd_earn`: 单日带宽流量（如 20302 日下载 0.04 TB）
