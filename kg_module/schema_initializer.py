from .connection import Neo4jConnection
import logging


class SchemaInitializer:
    """知识图谱Schema初始化工具"""

    def __init__(self):
        self.connection = Neo4jConnection()
        self.connection.connect()
        self.logger = logging.getLogger("schema_initializer")

    def create_constraints(self):
        """创建唯一性约束，确保实体ID不重复"""
        constraints = [
            # 景点实体约束
            "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Attraction) REQUIRE a.id IS UNIQUE",
            # 子景点实体约束
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:SubAttraction) REQUIRE s.id IS UNIQUE",
            # 交通枢纽实体约束
            "CREATE CONSTRAINT IF NOT EXISTS FOR (t:TransportHub) REQUIRE t.id IS UNIQUE",
            # 周边设施实体约束
            "CREATE CONSTRAINT IF NOT EXISTS FOR (f:Facility) REQUIRE f.id IS UNIQUE",
            # 季节实体约束
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Season) REQUIRE s.name IS UNIQUE"
        ]

        with self.connection.get_session() as session:
            for constraint in constraints:
                try:
                    session.run(constraint)
                    self.logger.info(f"约束创建成功: {constraint[:50]}...")
                except Exception as e:
                    self.logger.error(f"约束创建失败: {str(e)}")

    def create_indexes(self):
        """创建索引提升查询性能"""
        indexes = [
            # 空间索引
            "CREATE INDEX IF NOT EXISTS FOR (a:Attraction) ON (a.location)",
            "CREATE INDEX IF NOT EXISTS FOR (t:TransportHub) ON (t.location)",
            "CREATE INDEX IF NOT EXISTS FOR (f:Facility) ON (f.location)",
            # 类型索引
            "CREATE INDEX IF NOT EXISTS FOR (a:Attraction) ON (a.category)",
            "CREATE INDEX IF NOT EXISTS FOR (t:TransportHub) ON (t.type)",
            "CREATE INDEX IF NOT EXISTS FOR (f:Facility) ON (f.type)"
        ]

        with self.connection.get_session() as session:
            for index in indexes:
                try:
                    session.run(index)
                    self.logger.info(f"索引创建成功: {index[:50]}...")
                except Exception as e:
                    self.logger.error(f"索引创建失败: {str(e)}")

    def initialize_seasons(self):
        """初始化季节实体"""
        seasons = ["春季", "夏季", "秋季", "冬季"]
        with self.connection.get_session() as session:
            for season in seasons:
                session.run(
                    "MERGE (s:Season {name: $name})",
                    name=season
                )
            self.logger.info("季节实体初始化完成")

    def full_initialize(self):
        """完整初始化流程"""
        self.create_constraints()
        self.create_indexes()
        self.initialize_seasons()
        self.logger.info("知识图谱Schema初始化完成")
