import datetime
import time
from typing import Optional, List, Dict

from kg_module.entity_creator import EntityCreator
from kg_module.relationship_manager import DynamicAttributeManager
from milvus_module.vector_manager import VectorManager
import logging

from .data_fetche import StaticDataFetcher
from .data_processor import DataProcessor
from .weather_fetcher import WeatherDataFetcher
from .weather_processor import WeatherDataProcessor


class Neo4jSyncService:
    def __init__(self):
        """初始化 Neo4j 同步服务"""
        self.entity_creator = EntityCreator()
        self.logger = logging.getLogger("neo4j_sync_service")
        # self.logger.setLevel(logging.INFO)
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
                        with session.begin_transaction() as tx:
                            entity_id = self.entity_creator.create_attraction(attraction, tx)
                        self.logger.info(f"景点数据同步成功: {entity_id}")
                    except Exception as e:
                        self.logger.error(f"景点数据同步失败: {str(e)}")

            if "sub_attractions" in data:
                for sub_attraction in data["sub_attractions"]:
                    try:
                        # 使用显式事务
                        with session.begin_transaction() as tx:
                            entity_id = self.entity_creator.create_sub_attraction(sub_attraction,tx)
                        self.logger.info(f"子景点数据同步成功: {entity_id}")
                    except Exception as e:
                        self.logger.error(f"子景点数据同步失败: {str(e)}")

            if "transport_hubs" in data:
                for transport_hub in data["transport_hubs"]:
                    try:
                        # 使用显式事务
                        with session.begin_transaction() as tx:
                            entity_id = self.entity_creator.create_transport_hub(transport_hub,tx)
                        self.logger.info(f"交通枢纽数据同步成功: {entity_id}")
                    except Exception as e:
                        self.logger.error(f"交通枢纽数据同步失败: {str(e)}")

            if "facilities" in data:
                for facility in data["facilities"]:
                    try:
                        # 使用显式事务
                        with session.begin_transaction() as tx:
                            entity_id = self.entity_creator.create_facility(facility,tx)
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
        # with self.neo4j_sync:
        #     self.neo4j_sync.sync_data_to_neo4j(processed_data)

        # 同步到 Milvus
        if vectors:
            self.milvus_sync.generate_and_insert_vectors(vectors)

        self.logger.info("数据同步完成")

class WeatherSyncService:
    """动态属性同步服务，负责按不同频率同步各类动态数据"""
    def __init__(self):
        self.fetcher = WeatherDataFetcher()
        self.processor = WeatherDataProcessor()
        self.storage = DynamicAttributeManager()
        self.logger = logging.getLogger("weather_sync_service")
        #self.logger.setLevel(logging.INFO)

    def sync_weather_data(self, entity_id: str, longitude: float, latitude: float) -> bool:
        """
        同步指定实体的天气数据（获取->处理->存储）

        :param entity_id: 知识图谱实体ID
        :param longitude: 经度
        :param latitude: 纬度
        :return: 同步成功返回True，否则返回False
        """
        try:
            # 1. 获取原始天气数据
            raw_data = self.fetcher.fetch_weather(entity_id, longitude, latitude)
            self.logger.info(f"获取实体 {entity_id} 的天气数据: {raw_data}")
            if not raw_data:
                self.logger.error(f"无法获取实体 {entity_id} 的天气数据")
                return False

            # 2. 处理原始数据
            processed_data = self.processor.process_weather_data(entity_id, raw_data)
            self.logger.info(f"处理实体 {entity_id} 的天气数据: {processed_data}")
            if not processed_data:
                self.logger.error(f"无法处理实体 {entity_id} 的天气数据")
                return False

            # 3. 存储处理后的数据
            storage_result = self.storage.store_weather_attributes(processed_data)
            return storage_result

        except Exception as e:
            self.logger.error(f"同步天气数据时发生错误: {str(e)}, 实体ID: {entity_id}")
            return False

    def get_entity_weather(self, entity_id: str, attribute_type: Optional[str] = None) -> List[Dict]:
        """
        获取指定实体的天气数据

        :param entity_id: 实体ID
        :param attribute_type: 可选，指定属性类型
        :return: 天气属性数据列表
        """
        return self.storage.get_latest_weather(entity_id, attribute_type)

    def check_and_update_weather(self, entity_id: str, longitude: float, latitude: float,
                                 update_interval: int = 3600) -> List[Dict]:
        """
        检查并更新实体天气数据（自动判断是否需要更新）

        :param entity_id: 实体ID
        :param longitude: 经度
        :param latitude: 纬度
        :param update_interval: 更新间隔（秒），默认1小时
        :return: 最新天气数据列表
        """
        # 1. 获取当前最新数据
        latest_weather = self.get_entity_weather(entity_id)
        self.logger.info(f"检查实体 {entity_id} 的天气数据，最新数据: {latest_weather}")
        # 2. 判断是否需要更新（无数据或超过更新间隔）
        need_update = True
        if latest_weather:
            # 关键修改：检查时间字段类型，若是 datetime 对象则直接使用，否则转换为字符串
            latest_time = latest_weather[0]["time"]
            if isinstance(latest_time, datetime.datetime):
                # 直接使用 datetime 对象计算时间差
                time_diff = datetime.datetime.now(datetime.timezone.utc) - latest_time
            else:
                # 若是字符串则先解析（兼容旧数据）
                latest_time = datetime.datetime.fromisoformat(latest_time)
                time_diff = datetime.datetime.now(datetime.timezone.utc) - latest_time

        # 3. 需要更新时执行同步
        if need_update:
            sync_success = self.sync_weather_data(entity_id, longitude, latitude)
            if not sync_success:
                self.logger.warning(f"天气数据更新失败，实体ID: {entity_id}")

        # 4. 返回最新数据（可能是旧数据或新数据）
        return self.get_entity_weather(entity_id)
