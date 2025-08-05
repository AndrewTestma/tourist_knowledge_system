from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from typing import List, Dict, Any
from common.config import DatabaseConfig, load_config
import pandas as pd
import json

class StaticDataFetcher:
    def __init__(self):
        """初始化数据库连接"""
        config = load_config()
        self.engine = create_engine(
            f"mysql+pymysql://{config.db.username}:{config.db.password}@{config.db.host}:{config.db.port}/{config.db.database}"
        )
        self.Session = sessionmaker(bind=self.engine)

    # def fetch_attractions(self, batch_size: int = 100) -> List[Dict[str, Any]]:
    #     """批量获取景点静态数据"""
    #     attractions = []
    #     offset = 0
    #
    #     with self.Session() as session:
    #         while True:
    #             # 分页查询景点基础信息
    #             query = f"""
    #             SELECT id, name, alias, category,location, address,
    #                    area, opening_hours_regular, ticket_price_regular, description, tags,
    #                    official_website, contact,built_year, grade
    #             FROM attractions
    #             LIMIT {batch_size} OFFSET {offset}
    #             """
    #             result = session.execute(query)
    #             batch = result.fetchall()
    #
    #             if not batch:
    #                 break  # 没有更多数据
    #
    #             # 转换为字典格式
    #             columns = result.keys()
    #             for row in batch:
    #                 attraction = dict(zip(columns, row))
    #                 # 处理特殊格式字段
    #                 attraction['alias'] = attraction['alias'].split(',') if attraction['alias'] else []
    #                 attraction['category'] = attraction['category'].split(',') if attraction['category'] else []
    #                 attraction['tags'] = attraction['tags'].split(',') if attraction['tags'] else []
    #                 attraction['location'] = {
    #                     'lon': attraction.pop('lon'),
    #                     'lat': attraction.pop('lat')
    #                 }
    #                 attractions.append(attraction)
    #
    #             offset += batch_size
    #             print(f"已加载 {len(attractions)} 个景点数据")
    #
    #     return attractions
    #
    # def fetch_related_entities(self, entity_type: str, batch_size: int = 100) -> List[Dict[str, Any]]:
    #     """获取相关实体数据（子景点/交通枢纽/周边设施）"""
    #     # 根据实体类型获取对应表的数据
    #     tables = {
    #         'sub_attraction': 'sub_attractions',
    #         'transport_hub': 'transport_hubs',
    #         'facility': 'facilities'
    #     }
    #
    #     if entity_type not in tables:
    #         raise ValueError(f"不支持的实体类型: {entity_type}")
    #
    #     entities = []
    #     offset = 0
    #     table_name = tables[entity_type]
    #
    #     with self.Session() as session:
    #         while True:
    #             query = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
    #             result = session.execute(query)
    #             batch = result.fetchall()
    #
    #             if not batch:
    #                 break
    #
    #             columns = result.keys()
    #             for row in batch:
    #                 entity = dict(zip(columns, row))
    #                 # 处理位置信息
    #                 if 'longitude' in entity and 'latitude' in entity:
    #                     entity['location'] = {
    #                         'longitude': entity.pop('longitude'),
    #                         'latitude': entity.pop('latitude')
    #                     }
    #                 entities.append(entity)
    #
    #             offset += batch_size
    #             print(f"已加载 {len(entities)} 个{entity_type}数据")
    #
    #     return entities

    def fetch_attractions(self):
        """从数据库获取景点数据"""
        with self.Session() as session:
            query = "SELECT * FROM attractions"
            return pd.read_sql(query, session.bind).to_dict(orient='records')

    def fetch_sub_attractions(self):
        """从数据库获取子景点数据"""
        with self.Session() as session:
            query = "SELECT * FROM sub_attractions"
            return pd.read_sql(query, session.bind).to_dict(orient='records')

    def fetch_transport_hubs(self):
        """从数据库获取交通枢纽数据"""
        with self.Session() as session:
            query = "SELECT * FROM transport_hubs"
            return pd.read_sql(query, session.bind).to_dict(orient='records')

    def fetch_facilities(self):
        """从数据库获取周边设施数据"""
        with self.Session() as session:
            query = "SELECT * FROM facilities"
            return pd.read_sql(query, session.bind).to_dict(orient='records')

    def fetch_vectors(self):
        """从数据库获取向量数据"""
        with self.Session() as session:
            # 假设向量数据存储在vectors表，包含entity_id和vector字段
            query = "SELECT id, name, description, tags FROM attractions"
            return pd.read_sql(query, session.bind).to_dict(orient='records')


    def fetch_all_data(self):
        """获取所有类型的数据"""
        return {
            "attractions": self.fetch_attractions(),
            "sub_attractions": self.fetch_sub_attractions(),
            "transport_hubs": self.fetch_transport_hubs(),
            "facilities": self.fetch_facilities(),
            "vectors": self.fetch_vectors()  # 假设向量数据暂时不通过数据库获取，可按需修改
        }
