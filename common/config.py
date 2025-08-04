from dataclasses import dataclass
from typing import List, Dict, Any
import configparser
import os

@dataclass
class DatabaseConfig:
    """关系型数据库配置"""
    host: str
    port: int
    username: str
    password: str
    database: str

@dataclass
class Neo4jConfig:
    """知识图谱配置"""
    uri: str
    username: str
    password: str

@dataclass
class MilvusConfig:
    """Milvus配置"""
    host: str
    port: int
    user: str
    password: str
    collection_name: str
    token: str
    api_url: str

@dataclass
class AppConfig:
    """应用全局配置"""
    db: DatabaseConfig
    neo4j: Neo4jConfig
    milvus: MilvusConfig
    embedding_model: str
    batch_size: int

def load_config() -> AppConfig:
    config = configparser.ConfigParser()
    # 定位到项目根目录下的 config.ini 文件
    config_file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.ini')
    config.read(config_file_path)

    db_config = DatabaseConfig(
        host=config.get('database', 'host'),
        port=config.getint('database', 'port'),
        username=config.get('database', 'username'),
        password=config.get('database', 'password'),
        database=config.get('database', 'database')
    )

    neo4j_config = Neo4jConfig(
        uri=config.get('neo4j', 'uri'),
        username=config.get('neo4j', 'username'),
        password=config.get('neo4j', 'password')
    )

    milvus_config = MilvusConfig(
        host=config.get('milvus', 'host'),
        port=config.getint('milvus', 'port'),
        user=config.get('milvus', 'user'),
        password=config.get('milvus', 'password'),
        collection_name=config.get('milvus', 'collection_name'),
        token=config.get('milvus', 'token'),
        api_url=config.get('milvus', 'api_url')
    )

    app_config = AppConfig(
        db=db_config,
        neo4j=neo4j_config,
        milvus=milvus_config,
        embedding_model=config.get('app', 'embedding_model'),
        batch_size=config.getint('app', 'batch_size')
    )

    return app_config
