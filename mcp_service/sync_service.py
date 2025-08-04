from kg_module.entity_creator import EntityCreator
from common.config import Neo4jConfig, MilvusConfig
from .data_fetche import StaticDataFetcher
from .data_processor import DataProcessor
from milvus_module.vector_manager import VectorManager
import logging

class Neo4jSyncService:
    def __init__(self):
        """初始化 Neo4j 同步服务"""
        self.entity_creator = EntityCreator()
        self.logger = logging.getLogger("neo4j_sync_service")
        self.batch_size = 100

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口，确保连接关闭"""
        if hasattr(self.entity_creator, 'neo4j_conn'):
            self.entity_creator.neo4j_conn.close()

    def sync_data_to_neo4j(self, data):
        """将不同类型的数据同步到 Neo4j"""
        # 获取Neo4j会话
        with self.entity_creator.neo4j_conn.get_session() as session:

            if "attractions" in data:
                for attraction in data["attractions"]:
                    try:
                        # 使用显式事务
                        with session.begin_transaction():
                            entity_id = self.entity_creator.create_attraction(attraction, session)
                        self.logger.info(f"景点数据同步成功: {entity_id}")
                    except Exception as e:
                        self.logger.error(f"景点数据同步失败: {str(e)}")

            if "sub_attractions" in data:
                for sub_attraction in data["sub_attractions"]:
                    try:
                        entity_id = self.entity_creator.create_sub_attraction(sub_attraction,session)
                        self.logger.info(f"子景点数据同步成功: {entity_id}")
                    except Exception as e:
                        self.logger.error(f"子景点数据同步失败: {str(e)}")

            if "transport_hubs" in data:
                for transport_hub in data["transport_hubs"]:
                    try:
                        entity_id = self.entity_creator.create_transport_hub(transport_hub,session)
                        self.logger.info(f"交通枢纽数据同步成功: {entity_id}")
                    except Exception as e:
                        self.logger.error(f"交通枢纽数据同步失败: {str(e)}")

            if "facilities" in data:
                for facility in data["facilities"]:
                    try:
                        entity_id = self.entity_creator.create_facility(facility,session)
                        self.logger.info(f"周边设施数据同步成功: {entity_id}")
                    except Exception as e:
                        self.logger.error(f"周边设施数据同步失败: {str(e)}")

class DataSyncService:
    def __init__(self):
        """初始化数据同步服务"""
        self.data_fetcher = StaticDataFetcher()
        self.neo4j_sync = Neo4jSyncService()
        self.milvus_sync = VectorManager()
        self.data_processor = DataProcessor()
        self.logger = logging.getLogger("data_sync_service")

    def sync_data(self):
        """执行数据同步"""
        raw_data = self.data_fetcher.fetch_all_data()
        processed_data = self.data_processor.process_data(raw_data)  # 获取处理后的数据
        vectors = processed_data.get("vectors", [])

        # 同步到 Neo4j - 修复：启用上下文管理器并确保正确的会话管理
        with self.neo4j_sync:
            self.neo4j_sync.sync_data_to_neo4j(processed_data)

        # 同步到 Milvus
        if vectors:
            self.milvus_sync.generate_and_insert_vectors(vectors)

        self.logger.info("数据同步完成")
