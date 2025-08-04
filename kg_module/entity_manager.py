from .connection import Neo4jConnection
from neo4j import exceptions
from typing import List, Dict, Any, Optional
import logging


class EntityManager:
    """实体管理器，提供实体的完整生命周期管理"""

    def __init__(self):
        """初始化实体管理器"""
        self.neo4j_conn = Neo4jConnection()
        self.neo4j_conn.connect()
        self.logger = logging.getLogger("entity_manager")

    def get_entity_by_id(self, entity_id: str) -> Optional[Dict[str, Any]]:
        """
        通过ID获取实体详情

        :param entity_id: 实体ID（如ATxxx, SAxxx, THxxx等）
        :return: 实体属性字典，不存在时返回None
        """
        # 根据ID前缀判断实体类型
        entity_type = self._get_entity_type(entity_id)
        if not entity_type:
            self.logger.warning(f"无效的实体ID格式: {entity_id}")
            return None

        with self.neo4j_conn.get_session() as session:
            try:
                result = session.run(
                    f"""
                    MATCH (e:{entity_type} {{id: $id}})
                    RETURN e {{.*}} as entity
                    """,
                    id=entity_id
                )
                record = result.single()
                return record["entity"] if record else None
            except exceptions.Neo4jError as e:
                self.logger.error(f"获取实体失败: {str(e)}")
                return None

    def get_entities_by_type(self, entity_type: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        获取指定类型的实体列表

        :param entity_type: 实体类型（Attraction, SubAttraction等）
        :param limit: 结果数量限制
        :return: 实体列表
        """
        valid_types = ["Attraction", "SubAttraction", "TransportHub", "Facility"]
        if entity_type not in valid_types:
            raise ValueError(f"无效实体类型，必须是{valid_types}之一")

        with self.neo4j_conn.get_session() as session:
            try:
                result = session.run(
                    f"""
                    MATCH (e:{entity_type})
                    RETURN e {{.*}} as entity
                    LIMIT $limit
                    """,
                    limit=limit
                )
                return [record["entity"] for record in result]
            except exceptions.Neo4jError as e:
                self.logger.error(f"获取实体列表失败: {str(e)}")
                return []

    def update_entity_properties(self, entity_id: str, properties: Dict[str, Any]) -> bool:
        """
        更新实体属性

        :param entity_id: 实体ID
        :param properties: 待更新的属性字典
        :return: 更新成功返回True，失败返回False
        """
        entity_type = self._get_entity_type(entity_id)
        if not entity_type:
            self.logger.warning(f"无效的实体ID格式: {entity_id}")
            return False

        # 移除ID字段（不允许更新ID）
        properties.pop("id", None)

        with self.neo4j_conn.get_session() as session:
            try:
                session.run(
                    f"""
                    MATCH (e:{entity_type} {{id: $id}})
                    SET e += $properties
                    """,
                    id=entity_id,
                    properties=properties
                )
                self.logger.info(f"实体 {entity_id} 更新成功")
                return True
            except exceptions.Neo4jError as e:
                self.logger.error(f"实体更新失败: {str(e)}")
                return False

    def delete_entity(self, entity_id: str) -> bool:
        """
        删除实体及其所有关系

        :param entity_id: 实体ID
        :return: 删除成功返回True，失败返回False
        """
        entity_type = self._get_entity_type(entity_id)
        if not entity_type:
            self.logger.warning(f"无效的实体ID格式: {entity_id}")
            return False

        with self.neo4j_conn.get_session() as session:
            try:
                # 删除实体及其所有关系
                session.run(
                    """
                    MATCH (e {id: $id})
                    DETACH DELETE e
                    """,
                    id=entity_id
                )
                self.logger.info(f"实体 {entity_id} 删除成功")
                return True
            except exceptions.Neo4jError as e:
                self.logger.error(f"实体删除失败: {str(e)}")
                return False

    def _get_entity_type(self, entity_id: str) -> Optional[str]:
        """根据ID前缀判断实体类型"""
        if entity_id.startswith("AT"):
            return "Attraction"
        elif entity_id.startswith("SA"):
            return "SubAttraction"
        elif entity_id.startswith("TH"):
            return "TransportHub"
        elif entity_id.startswith("FA"):
            return "Facility"
        return None

    def close(self):
        """关闭数据库连接"""
        self.neo4j_conn.close()
