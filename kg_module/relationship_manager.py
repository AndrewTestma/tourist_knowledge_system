import datetime
from typing import List, Dict, Optional
from influxdb_client import InfluxDBClient, Point  # v2客户端
from influxdb_client.client.write_api import SYNCHRONOUS
from common.config import load_config
import logging


class DynamicAttributeManager:
    """动态属性管理器，处理知识图谱与时序数据库的关联（适配InfluxDB v2）"""

    def __init__(self):
        self.config = load_config()
        self.logger = logging.getLogger("dynamic_attribute_manager")
        # 初始化InfluxDB v2客户端
        self.client = InfluxDBClient(
            url=f"http://{self.config.influxdb.host}:{self.config.influxdb.port}",
            token=self.config.influxdb.token,
            org=self.config.influxdb.org
        )
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()
        # 确保桶存在（v2中bucket替代database）
        self._create_bucket_if_not_exists()

    def _create_bucket_if_not_exists(self):
        """检查并创建桶（v2替代v1的create_database）"""
        buckets_api = self.client.buckets_api()
        bucket = buckets_api.find_bucket_by_name(self.config.influxdb.bucket)
        if not bucket:
            # 创建桶（保留时间30天，单位小时）
            buckets_api.create_bucket(
                bucket_name=self.config.influxdb.bucket,
                retention_rules=[{"type": "expire", "everySeconds": 30*24*3600}],
                org=self.config.influxdb.org
            )
            self.logger.info(f"创建InfluxDB桶: {self.config.influxdb.bucket}")
        else:
            self.logger.info(f"InfluxDB桶已存在: {self.config.influxdb.bucket}")

    def store_weather_attributes(self, attributes: List[Dict]) -> bool:
        """存储天气属性（适配v2的写入API）"""
        if not attributes:
            self.logger.warning("没有可存储的天气属性数据")
            return False

        try:
            points = []
            for attr in attributes:
                # 构建v2的数据点（使用Point类）
                point = Point("weather_attributes") \
                    .tag("entity_id", attr["entity_id"]) \
                    .tag("attribute_type", attr["attribute_type"]) \
                    .field("value", attr["value"]) \
                    .time(attr["timestamp"].isoformat())  # 时间字段

                # 添加单位（如果存在）
                if "unit" in attr:
                    point = point.field("unit", attr["unit"])
                points.append(point)

            # 写入数据到指定桶
            self.write_api.write(
                bucket=self.config.influxdb.bucket,
                org=self.config.influxdb.org,
                record=points
            )
            self.logger.info(f"成功存储 {len(points)} 条天气属性数据")
            return True

        except Exception as e:
            self.logger.error(f"存储天气属性到InfluxDB时发生错误: {str(e)}")
            return False

    def get_latest_weather(self, entity_id: str, attribute_type: Optional[str] = None) -> List[Dict]:
        """查询最新天气属性（使用v2的Flux查询语法）"""
        try:
            # 新增：转义特殊字符（Flux中字符串内的单引号用两个单引号转义）
            escaped_entity_id = entity_id.replace("'", "''")
            # 构建Flux查询（v2的查询语言，替代InfluxQL）
            if attribute_type:
                # 按实体ID和属性类型过滤
                # 新增：转义属性类型中的特殊字符
                escaped_attr_type = attribute_type.replace("'", "''")
                flux_query = f'''
                                    from(bucket: "{self.config.influxdb.bucket}")
                                      |> range(start: -30d)  # 查最近30天数据
                                      |> filter(fn: (r) => r._measurement == "weather_attributes")
                                      |> filter(fn: (r) => r.entity_id == '{escaped_entity_id}')  # 改用单引号+转义
                                      |> filter(fn: (r) => r.attribute_type == '{escaped_attr_type}')  # 改用单引号+转义
                                      |> last()  # 取最新一条
                                      |> keep(columns: ["_time", "_value", "entity_id", "attribute_type", "unit"])
                                '''
            else:
                # 按实体ID查询所有属性类型的最新数据
                flux_query = f'''
                                    from(bucket: "{self.config.influxdb.bucket}")
                                      |> range(start: -30d)
                                      |> filter(fn: (r) => r._measurement == "weather_attributes")
                                      |> filter(fn: (r) => r.entity_id == '{escaped_entity_id}')  # 改用单引号+转义
                                      |> group(columns: ["attribute_type"])  # 按属性类型分组
                                      |> last()
                                      |> keep(columns: ["_time", "_value", "entity_id", "attribute_type", "unit"])
                                '''

            # 执行查询
            result = self.query_api.query(flux_query)

            # 处理查询结果（转换为字典格式）
            points = []
            for table in result:
                for record in table.records:
                    points.append({
                        "time": record.get_time(),
                        "entity_id": record["entity_id"],
                        "attribute_type": record["attribute_type"],
                        "value": record.get_value(),
                        "unit": record["unit"]  # 可能为None
                    })
            return points

        except Exception as e:
            self.logger.error(f"查询天气属性时发生错误: {str(e)}")
            return []
