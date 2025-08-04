from .connection import Neo4jConnection
from neo4j.spatial import WGS84Point
import logging

class EntityCreator:
    """实体创建器"""
    def __init__(self):
        """初始化实体创建器"""
        self.neo4j_conn = Neo4jConnection() # 仅保留连接，不主动创建会话
        self.neo4j_conn.connect()
        self.logger = logging.getLogger("entity_creator")

    def create_attraction(self, properties, tx):  # 新增tx参数（事务对象）
        """创建景点实体（修改后）"""
        # 验证ID格式
        if not properties.get("id", "").startswith("AT"):
            raise ValueError("景点ID必须以'AT'为前缀")

        # 处理地理坐标
        if "location" in properties:
            try:
                if isinstance(properties["location"], bytes):
                    # 处理二进制格式的坐标数据
                    import struct
                    # 解析二进制数据 (假设是WKB格式)
                    # 前5字节是头部信息，后面8字节是经度，再8字节是纬度
                    lon, lat = struct.unpack('!2d', properties["location"][9:25])
                    properties["location"] = WGS84Point([lon, lat])
                elif isinstance(properties["location"], str) and properties["location"].startswith("POINT("):
                    # 处理WKT格式的POINT数据
                    coords = properties["location"][6:-1].split()
                    lon = float(coords[0])
                    lat = float(coords[1])
                    properties["location"] = WGS84Point([lon, lat])
                else:
                    # 处理其他格式的坐标数据
                    lat, lon = properties["location"]
                    properties["location"] = WGS84Point([lon, lat])
            except Exception as e:
                self.logger.error(f"处理坐标失败: {str(e)}")
                del properties["location"]

            try:
                result = tx.run(
                    """
                    MERGE (a:Attraction {id: $id})
                    SET a += $properties
                    RETURN a.id AS id
                    """,
                    id=properties["id"],
                    properties={k: v for k, v in properties.items() if k != "id"}
                )
                entity_id = result.single()["id"]
                self.logger.info(f"景点创建成功: {entity_id}")
                return entity_id
            except Exception as e:
                self.logger.error(f"景点创建失败: {str(e)}")
                raise

    def create_sub_attraction(self, properties,tx):
        """创建子景点实体"""
        if not properties.get("id", "").startswith("SA"):
            raise ValueError("子景点ID必须以'SA'为前缀")

        try:
            result = tx.run(
                """
                MERGE (s:SubAttraction {id: $id})
                SET s += $properties
                RETURN s.id AS id
                """,
                id=properties["id"],
                properties={k: v for k, v in properties.items() if k != "id"}
            )
            entity_id = result.single()["id"]
            self.logger.info(f"子景点创建成功: {entity_id}")
            return entity_id
        except Exception as e:
            self.logger.error(f"子景点创建失败: {str(e)}")
            raise

    def create_transport_hub(self, properties,tx):
        """创建交通枢纽实体"""
        if not properties.get("id", "").startswith("TH"):
            raise ValueError("交通枢纽ID必须以'TH'为前缀")

        if "location" in properties:
            try:
                if isinstance(properties["location"], bytes):
                    # 处理二进制格式的坐标数据
                    import struct
                    # 解析二进制数据 (假设是WKB格式)
                    # 前5字节是头部信息，后面8字节是经度，再8字节是纬度
                    lon, lat = struct.unpack('!2d', properties["location"][9:25])
                    properties["location"] = WGS84Point([lon, lat])
                elif isinstance(properties["location"], str) and properties["location"].startswith("POINT("):
                    # 处理WKT格式的POINT数据
                    coords = properties["location"][6:-1].split()
                    lon = float(coords[0])
                    lat = float(coords[1])
                    properties["location"] = WGS84Point([lon, lat])
                else:
                    # 处理其他格式的坐标数据
                    lat, lon = properties["location"]
                    properties["location"] = WGS84Point([lon, lat])
            except Exception as e:
                self.logger.error(f"处理坐标失败: {str(e)}")
                del properties["location"]

            try:
                result = tx.run(
                    """
                    MERGE (t:TransportHub {id: $id})
                    SET t += $properties
                    RETURN t.id AS id
                    """,
                    id=properties["id"],
                    properties={k: v for k, v in properties.items() if k != "id"}
                )
                entity_id = result.single()["id"]
                self.logger.info(f"交通枢纽创建成功: {entity_id}")
                return entity_id
            except Exception as e:
                self.logger.error(f"交通枢纽创建失败: {str(e)}")
                raise

    def create_facility(self, properties,tx):
        """创建周边设施实体"""
        if not properties.get("id", "").startswith("FA"):
            raise ValueError("周边设施ID必须以'FA'为前缀")

        if "location" in properties:
            try:
                if isinstance(properties["location"], bytes):
                    # 处理二进制格式的坐标数据
                    import struct
                    # 解析二进制数据 (假设是WKB格式)
                    # 前5字节是头部信息，后面8字节是经度，再8字节是纬度
                    lon, lat = struct.unpack('!2d', properties["location"][9:25])
                    properties["location"] = WGS84Point([lon, lat])
                elif isinstance(properties["location"], str) and properties["location"].startswith("POINT("):
                    # 处理WKT格式的POINT数据
                    coords = properties["location"][6:-1].split()
                    lon = float(coords[0])
                    lat = float(coords[1])
                    properties["location"] = WGS84Point([lon, lat])
                else:
                    # 处理其他格式的坐标数据
                    lat, lon = properties["location"]
                    properties["location"] = WGS84Point([lon, lat])
            except Exception as e:
                self.logger.error(f"处理坐标失败: {str(e)}")
                del properties["location"]

            try:
                result = tx.run(
                    """
                    MERGE (f:Facility {id: $id})
                    SET f += $properties
                    RETURN f.id AS id
                    """,
                    id=properties["id"],
                    properties={k: v for k, v in properties.items() if k != "id"}
                )
                entity_id = result.single()["id"]
                self.logger.info(f"周边设施创建成功: {entity_id}")
                return entity_id
            except Exception as e:
                self.logger.error(f"周边设施创建失败: {str(e)}")
                raise
