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
                loc_data = properties["location"]
                if isinstance(loc_data, bytes):
                    # 校验WKB最小长度（带SRID的POINT至少需要 1(字节序)+4(几何类型)+4(SRID)+8(X)+8(Y)=25字节）
                    if len(loc_data) < 25:
                        raise ValueError(f"WKB数据长度不足，至少需要25字节，实际{len(loc_data)}字节")

                    import struct

                    # 1. 解析字节序（第0字节）
                    endian = loc_data[0]
                    if endian == 0x00:  # 大端（网络字节序）
                        fmt = '!'
                    elif endian == 0x01:  # 小端
                        fmt = '<'
                    else:
                        raise ValueError(f"无效的字节序标识: {endian}")

                    # 2. 解析几何类型（1-4字节）
                    geom_type_packed = loc_data[1:5]
                    geom_type = struct.unpack(f"{fmt}I", geom_type_packed)[0]
                    is_ewkb = (geom_type & 0x80000000) != 0  # 最高位为1表示EWKB（带SRID）

                    # 3. 解析SRID（若存在）
                    srid = None
                    x_start = 5  # 无SRID时，X坐标从第5字节开始
                    if is_ewkb:
                        srid = struct.unpack(f"{fmt}I", loc_data[5:9])[0]  # SRID在5-8字节
                        x_start = 9  # 有SRID时，X坐标从第9字节开始
                        if srid != 4326:
                            self.logger.warning(f"SRID不是4326，可能坐标系不匹配: {srid}")

                    # 4. 解析坐标（X: x_start~x_start+8，Y: x_start+8~x_start+16）
                    lon = struct.unpack(f"{fmt}d", loc_data[x_start:x_start + 8])[0]  # 经度
                    lat = struct.unpack(f"{fmt}d", loc_data[x_start + 8:x_start + 16])[0]  # 纬度

                    # 5. 校验坐标合理性
                    if not (-180 <= lon <= 180 and -90 <= lat <= 90):
                        raise ValueError(f"坐标超出地球范围: 经度={lon}, 纬度={lat}")

                    properties["location"] = WGS84Point([lon, lat])
                    self.logger.debug(f"成功解析WKB坐标: POINT({lon} {lat})")
                # ... 其他格式处理（如WKT字符串）保持不变 ...
            except Exception as e:
                self.logger.error(f"景点实体处理坐标失败: {str(e)}")
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
                loc_data = properties["location"]
                if isinstance(loc_data, bytes):
                    # 校验WKB最小长度（带SRID的POINT至少需要 1(字节序)+4(几何类型)+4(SRID)+8(X)+8(Y)=25字节）
                    if len(loc_data) < 25:
                        raise ValueError(f"WKB数据长度不足，至少需要25字节，实际{len(loc_data)}字节")

                    import struct

                    # 1. 解析字节序（第0字节）
                    endian = loc_data[0]
                    if endian == 0x00:  # 大端（网络字节序）
                        fmt = '!'
                    elif endian == 0x01:  # 小端
                        fmt = '<'
                    else:
                        raise ValueError(f"无效的字节序标识: {endian}")

                    # 2. 解析几何类型（1-4字节）
                    geom_type_packed = loc_data[1:5]
                    geom_type = struct.unpack(f"{fmt}I", geom_type_packed)[0]
                    is_ewkb = (geom_type & 0x80000000) != 0  # 最高位为1表示EWKB（带SRID）

                    # 3. 解析SRID（若存在）
                    srid = None
                    x_start = 5  # 无SRID时，X坐标从第5字节开始
                    if is_ewkb:
                        srid = struct.unpack(f"{fmt}I", loc_data[5:9])[0]  # SRID在5-8字节
                        x_start = 9  # 有SRID时，X坐标从第9字节开始
                        if srid != 4326:
                            self.logger.warning(f"SRID不是4326，可能坐标系不匹配: {srid}")

                    # 4. 解析坐标（X: x_start~x_start+8，Y: x_start+8~x_start+16）
                    lon = struct.unpack(f"{fmt}d", loc_data[x_start:x_start + 8])[0]  # 经度
                    lat = struct.unpack(f"{fmt}d", loc_data[x_start + 8:x_start + 16])[0]  # 纬度

                    # 5. 校验坐标合理性
                    if not (-180 <= lon <= 180 and -90 <= lat <= 90):
                        raise ValueError(f"坐标超出地球范围: 经度={lon}, 纬度={lat}")

                    properties["location"] = WGS84Point([lon, lat])
                    self.logger.debug(f"成功解析WKB坐标: POINT({lon} {lat})")
                # ... 其他格式处理（如WKT字符串）保持不变 ...
            except Exception as e:
                self.logger.error(f"交通枢纽实体处理坐标失败: {str(e)}")
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
                loc_data = properties["location"]
                if isinstance(loc_data, bytes):
                    # 校验WKB最小长度（带SRID的POINT至少需要 1(字节序)+4(几何类型)+4(SRID)+8(X)+8(Y)=25字节）
                    if len(loc_data) < 25:
                        raise ValueError(f"WKB数据长度不足，至少需要25字节，实际{len(loc_data)}字节")

                    import struct

                    # 1. 解析字节序（第0字节）
                    endian = loc_data[0]
                    if endian == 0x00:  # 大端（网络字节序）
                        fmt = '!'
                    elif endian == 0x01:  # 小端
                        fmt = '<'
                    else:
                        raise ValueError(f"无效的字节序标识: {endian}")

                    # 2. 解析几何类型（1-4字节）
                    geom_type_packed = loc_data[1:5]
                    geom_type = struct.unpack(f"{fmt}I", geom_type_packed)[0]
                    is_ewkb = (geom_type & 0x80000000) != 0  # 最高位为1表示EWKB（带SRID）

                    # 3. 解析SRID（若存在）
                    srid = None
                    x_start = 5  # 无SRID时，X坐标从第5字节开始
                    if is_ewkb:
                        srid = struct.unpack(f"{fmt}I", loc_data[5:9])[0]  # SRID在5-8字节
                        x_start = 9  # 有SRID时，X坐标从第9字节开始
                        if srid != 4326:
                            self.logger.warning(f"SRID不是4326，可能坐标系不匹配: {srid}")

                    # 4. 解析坐标（X: x_start~x_start+8，Y: x_start+8~x_start+16）
                    lon = struct.unpack(f"{fmt}d", loc_data[x_start:x_start + 8])[0]  # 经度
                    lat = struct.unpack(f"{fmt}d", loc_data[x_start + 8:x_start + 16])[0]  # 纬度

                    # 5. 校验坐标合理性
                    if not (-180 <= lon <= 180 and -90 <= lat <= 90):
                        raise ValueError(f"坐标超出地球范围: 经度={lon}, 纬度={lat}")

                    properties["location"] = WGS84Point([lon, lat])
                    self.logger.debug(f"成功解析WKB坐标: POINT({lon} {lat})")
                # ... 其他格式处理（如WKT字符串）保持不变 ...
            except Exception as e:
                self.logger.error(f"周边设施实体处理坐标失败: {str(e)}")
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
