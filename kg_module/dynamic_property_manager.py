from .connection import Neo4jConnection
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import logging
import time


class DynamicPropertyManager:
    """动态属性管理类，处理与时序数据库的关联"""

    def __init__(self):
        self.neo4j_conn = Neo4jConnection()
        self.neo4j_conn.connect()
        # 初始化InfluxDB连接
        self.influx_client = InfluxDBClient(
            url="http://192.168.3.7:8086",
            token="MyInitialAdminToken0==",
            org="tourism-org"
        )
        self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
        self.bucket = "dynamic_properties"
        self.logger = logging.getLogger("dynamic_property_manager")

    def add_dynamic_property(self, entity_id, property_name, value):
        """添加动态属性到时序数据库"""
        point = Point("dynamic_property") \
            .tag("entity_id", entity_id) \
            .field(property_name, value) \
            .time(int(time.time() * 1000))

        self.write_api.write(bucket=self.bucket, record=point)
        self.logger.info(f"动态属性添加成功: {entity_id}.{property_name}={value}")

    def get_latest_dynamic_property(self, entity_id, property_name):
        """获取实体的最新动态属性"""
        query = f'''
        from(bucket: "{self.bucket}")
          |> range(start: -1h)
          |> filter(fn: (r) => r["_measurement"] == "dynamic_property")
          |> filter(fn: (r) => r["entity_id"] == "{entity_id}")
          |> filter(fn: (r) => r["_field"] == "{property_name}")
          |> last()
        '''
        tables = self.influx_client.query_api().query(query=query)
        for table in tables:
            for record in table.records:
                return record.get_value()
        return None

    def get_entity_with_dynamic_properties(self, entity_id):
        """获取实体及其最新动态属性"""
        # 从Neo4j获取基本信息
        with self.neo4j_conn.get_session() as session:
            result = session.run(
                "MATCH (e) WHERE e.id = $id RETURN e",
                id=entity_id
            )
            entity = result.single()["e"]

        # 获取动态属性
        dynamic_props = {}
        # 根据实体类型判断可能的动态属性
        if entity_id.startswith("AT"):
            props = ["real_time_weather", "current_visitor_flow", "ticket_available", "temporary_notice"]
        elif entity_id.startswith("TH"):
            props = ["parking_available", "bus_arrival_time"]
        elif entity_id.startswith("FA"):
            props = ["current_waiting_time", "room_available"]
        else:
            props = []

        for prop in props:
            value = self.get_latest_dynamic_property(entity_id, prop)
            if value is not None:
                dynamic_props[prop] = value

        return {
            "basic_info": dict(entity),
            "dynamic_properties": dynamic_props
        }
