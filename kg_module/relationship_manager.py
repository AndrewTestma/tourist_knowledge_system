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
        #self.logger.setLevel(logging.INFO)
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
        # self._clean_conflict_data()

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

    # 可以在DynamicAttributeManager初始化时添加一次清理逻辑（仅执行一次）
    def _clean_conflict_data(self):
        """清理旧的冲突数据（首次运行后可删除）"""
        try:
            # 删除整个measurement（谨慎使用，会删除所有历史数据）
            delete_api = self.client.delete_api()
            start = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
            stop = datetime.datetime.now(datetime.timezone.utc)
            delete_api.delete(
                start, stop,
                '_measurement="weather_attributes"',
                bucket=self.config.influxdb.bucket,
                org=self.config.influxdb.org
            )
            self.logger.info("已清理旧的天气属性数据，解决类型冲突")
        except Exception as e:
            self.logger.warning(f"清理冲突数据时发生错误: {str(e)}")

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
                    .time(attr["timestamp"].isoformat())

                # 根据值的类型选择不同的field，避免类型冲突
                value = attr["value"]
                if isinstance(value, (int, float)):
                    # 数值类型存储到value_num字段
                    point = point.field("value_num", float(value))
                else:
                    # 字符串类型存储到value_str字段
                    point = point.field("value_str", str(value))

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
            bucket = self.config.influxdb.bucket

            query = f'''
                union(tables: [
                    from(bucket: "{bucket}")
                      |> range(start: -1h)
                      |> filter(fn: (r) => r._measurement == "weather_attributes")
                      |> filter(fn: (r) => r.entity_id == "{escaped_entity_id}")
                      |> filter(fn: (r) => r._field == "value_num")
                      |> group(columns: ["attribute_type"])
                      |> last()
                      |> map(fn: (r) => ({{
                          _time: r._time,
                          entity_id: r.entity_id,
                          attribute_type: r.attribute_type,
                          unit: r.unit,
                          _value: r._value,
                          _type: "number"
                      }})),
                    from(bucket: "{bucket}")
                      |> range(start: -1h)
                      |> filter(fn: (r) => r._measurement == "weather_attributes")
                      |> filter(fn: (r) => r.entity_id == "{escaped_entity_id}")
                      |> filter(fn: (r) => r._field == "value_str")
                      |> group(columns: ["attribute_type"])
                      |> last()
                      |> map(fn: (r) => ({{
                          _time: r._time,
                          entity_id: r.entity_id,
                          attribute_type: r.attribute_type,
                          unit: r.unit,
                          _value: r._value, 
                          _type: "string"
                      }}))
                ])
                |> group(columns: ["attribute_type"])
                |> sort(columns: ["_time"], desc: true)
                |> keep(columns: ["_time", "_value", "entity_id", "attribute_type", "unit"])
            '''

            # 执行查询
            result = self.query_api.query(query)
            self.logger.info(f"执行查询: {query}")

            # 处理查询结果（转换为字典格式）
            points = []
            for table in result:
                for record in table.records:
                    points.append({
                        "time": record.get_time(),
                        "entity_id": record["entity_id"],
                        "attribute_type": record["attribute_type"],
                        "value": record.get_value(),
                        "unit": record.values.get("unit")  # 改为使用get方法，不存在时返回None
                    })
            return points

        except Exception as e:
            self.logger.error(f"查询天气属性时发生错误: {str(e)}")
            return []
