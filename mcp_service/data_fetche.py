from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from typing import List, Dict, Any

from vast import headers

from common.config import DatabaseConfig, load_config
import pandas as pd
import requests
import time
import json
import logging

class StaticDataFetcher:
    def __init__(self):
        """初始化数据库连接"""
        config = load_config()
        self.engine = create_engine(
            f"mysql+pymysql://{config.db.username}:{config.db.password}@{config.db.host}:{config.db.port}/{config.db.database}"
        )
        self.Session = sessionmaker(bind=self.engine)


    def fetch_attractions(self):
        """从数据库获取景点数据（修正后）"""
        with self.Session() as session:
            # 使用数据库函数将几何字段转为WKT字符串（如MySQL的ST_AsWKT，PostGIS的ST_AsText）
            query = """
            SELECT id, name, alias, category, 
                   ST_AsWKT(location) AS location,  -- 关键修改：将二进制几何转为WKT文本
                   address, area, opening_hours_regular, ticket_price_regular, 
                   description, tags, official_website, contact, built_year, grade 
            FROM attractions
            """
            return pd.read_sql(query, session.bind).to_dict(orient='records')

    def fetch_sub_attractions(self):
        """从数据库获取子景点数据"""
        with self.Session() as session:
            query = "SELECT * FROM sub_attractions"
            return pd.read_sql(query, session.bind).to_dict(orient='records')

    def fetch_transport_hubs(self):
        """从数据库获取交通枢纽数据"""
        with self.Session() as session:
            query = """
            SELECT id,name,type,
                    ST_AsWKT(location) AS location, 
                    distance_to_attraction,`accessible` 
            FROM transport_hubs
            """
            return pd.read_sql(query, session.bind).to_dict(orient='records')

    def fetch_facilities(self):
        """从数据库获取周边设施数据"""
        with self.Session() as session:
            query = """
            SELECT id,name,type,
                    ST_AsWKT(location) AS location, 
                    distance_to_attraction,price_level,business_hours 
            FROM facilities
            """
            return pd.read_sql(query, session.bind).to_dict(orient='records')

    def fetch_vectors(self):
        """从数据库获取向量数据"""
        with self.Session() as session:
            # 假设向量数据存储在vectors表，包含entity_id和vector字段
            query = "SELECT id, name,alias, description, tags FROM attractions"
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

