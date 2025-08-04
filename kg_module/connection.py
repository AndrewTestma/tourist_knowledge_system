from neo4j import GraphDatabase, exceptions
from dotenv import load_dotenv
from pymysql import connect

from common.config import load_config
import os
import logging


# 加载环境变量
load_dotenv()

class Neo4jConnection:
    def __init__(self):
        """初始化Neo4j连接"""
        config = load_config()
        self.uri = config.neo4j.uri
        self.user = config.neo4j.username
        self.password = config.neo4j.password
        self.driver = None
        self.logger = logging.getLogger("neo4j_connection")

    def connect(self):
        """连接到Neo4j数据库"""
        try:
            self.driver = GraphDatabase.driver(
                self.uri,
                auth=(self.user, self.password)
            )
            with self.driver.session() as session:
                session.run("MATCH (n) RETURN n AS count")
            self.logger.info("成功连接到Neo4j数据库")
            return True
        except exceptions.Neo4jError as e:
            self.logger.error(f"连接到Neo4j数据库失败: {e}")
            return False
    def close(self):
        """关闭Neo4j数据库连接"""
        if self.driver:
            self.driver.close()
            self.logger.info("已关闭Neo4j数据库连接")

    def get_session(self):
        """获取Neo4j数据库会话"""
        if not self.driver:
            self.connect()
        return self.driver.session()
