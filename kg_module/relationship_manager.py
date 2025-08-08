import datetime
from typing import List, Dict, Optional

from influxdb import InfluxDBClient
from common.config import DatabaseConfig, load_config
import logging


class DynamicAttributeManager:
    """动态属性管理器，处理知识图谱与时序数据库的关联"""

    def __init__(self):
        self.config =  load_config()
        self.logger = logging.getLogger("dynamic_attribute_manager")
        self.influx_client = InfluxDBClient(
            host=self.config.influxdb.host,
            port=self.config.influxdb.port,
            username=self.config.influxdb.username,
            password=self.config.influxdb.password,
            database=self.config.influxdb.database
        )
        # 确保数据库存在
        self.influx_client.create_database(self.config.influxdb.database)

    def store_weather_attributes(self, attributes: List[Dict]) -> bool:
        """
        将处理后的天气属性存储到时序数据库

        :param attributes: 标准化的天气属性列表
        :return: 存储成功返回True，否则返回False
        """
        if not attributes:
            self.logger.warning("没有可存储的天气属性数据")
            return False

        try:
            # 转换为InfluxDB数据格式
            points = []
            for attr in attributes:
                point = {
                    "measurement": "weather_attributes",
                    "tags": {
                        "entity_id": attr["entity_id"],
                        "attribute_type": attr["attribute_type"]
                    },
                    "time": attr["timestamp"].isoformat(),
                    "fields": {
                        "value": attr["value"]
                    }
                }

                # 如果有单位信息，添加到字段中
                if "unit" in attr:
                    point["fields"]["unit"] = attr["unit"]

                points.append(point)

            # 写入数据库
            result = self.influx_client.write_points(points)

            if result:
                self.logger.info(f"成功存储 {len(points)} 条天气属性数据")
                return True
            else:
                self.logger.error("存储天气属性数据失败")
                return False

        except Exception as e:
            self.logger.error(f"存储天气属性到InfluxDB时发生错误: {str(e)}")
            return False

    def get_latest_weather(self, entity_id: str, attribute_type: Optional[str] = None) -> List[Dict]:
        """
        获取指定实体的最新天气属性

        :param entity_id: 实体ID
        :param attribute_type: 可选，指定属性类型
        :return: 最新的天气属性数据
        """
        try:
            # 构建查询条件
            if attribute_type:
                query = f'''
                    SELECT * FROM weather_attributes 
                    WHERE entity_id = '{entity_id}' AND attribute_type = '{attribute_type}'
                    ORDER BY time DESC LIMIT 1
                '''
            else:
                query = f'''
                    SELECT * FROM weather_attributes 
                    WHERE entity_id = '{entity_id}'
                    GROUP BY attribute_type
                    ORDER BY time DESC LIMIT 1
                '''

            # 执行查询
            result = self.client.query(query)

            # 处理查询结果
            points = list(result.get_points())
            return points

        except Exception as e:
            self.logger.error(f"查询天气属性时发生错误: {str(e)}")
            return []
