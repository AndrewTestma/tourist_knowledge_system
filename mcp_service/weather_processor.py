import logging
from datetime import datetime
from typing import List, Dict, Optional
from dateutil import parser  # 需要安装python-dateutil库

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WeatherDataProcessor:
    """天气数据处理器，将API返回的原始数据转换为标准化格式"""

    @staticmethod
    def process_weather_data(entity_id: str, raw_data: Dict) -> Optional[List[Dict]]:
        """
        处理天气原始数据，转换为"实体-时间-属性值"三元组结构

        :param entity_id: 知识图谱实体ID
        :param raw_data: 从API获取的原始天气数据
        :return: 标准化的动态属性列表，失败时返回None
        """
        if not raw_data or "now" not in raw_data:
            logger.warning(f"无效的天气数据，实体ID: {entity_id}")
            return None

        try:
            # 提取观测时间并转换为datetime对象
            obs_time_str = raw_data["now"]["obsTime"]
            obs_time = parser.parse(obs_time_str)  # 处理带时区的时间格式

            # 提取更新时间（可选）
            update_time_str = raw_data.get("updateTime")
            update_time = parser.parse(update_time_str) if update_time_str else None

            # 从now字段中提取需要的天气属性
            weather_data = raw_data["now"]

            # 构建标准化的动态属性列表
            # 每个属性都是一个"实体-时间-属性值"三元组
            processed_attributes = [
                {
                    "entity_id": entity_id,
                    "attribute_type": "weather_condition",
                    "value": weather_data["text"],
                    "timestamp": obs_time,
                    "update_time": update_time
                },
                {
                    "entity_id": entity_id,
                    "attribute_type": "temperature",
                    "value": float(weather_data["temp"]),
                    "unit": "℃",
                    "timestamp": obs_time,
                    "update_time": update_time
                },
                {
                    "entity_id": entity_id,
                    "attribute_type": "feels_like",
                    "value": float(weather_data["feelsLike"]),
                    "unit": "℃",
                    "timestamp": obs_time,
                    "update_time": update_time
                },
                {
                    "entity_id": entity_id,
                    "attribute_type": "humidity",
                    "value": int(weather_data["humidity"]),
                    "unit": "%",
                    "timestamp": obs_time,
                    "update_time": update_time
                },
                {
                    "entity_id": entity_id,
                    "attribute_type": "wind_direction",
                    "value": weather_data["windDir"],
                    "timestamp": obs_time,
                    "update_time": update_time
                },
                {
                    "entity_id": entity_id,
                    "attribute_type": "wind_scale",
                    "value": int(weather_data["windScale"]),
                    "unit": "级",
                    "timestamp": obs_time,
                    "update_time": update_time
                },
                {
                    "entity_id": entity_id,
                    "attribute_type": "wind_speed",
                    "value": float(weather_data["windSpeed"]),
                    "unit": "km/h",
                    "timestamp": obs_time,
                    "update_time": update_time
                },
                {
                    "entity_id": entity_id,
                    "attribute_type": "precipitation",
                    "value": float(weather_data["precip"]),
                    "unit": "mm",
                    "timestamp": obs_time,
                    "update_time": update_time
                },
                {
                    "entity_id": entity_id,
                    "attribute_type": "pressure",
                    "value": int(weather_data["pressure"]),
                    "unit": "hPa",
                    "timestamp": obs_time,
                    "update_time": update_time
                },
                {
                    "entity_id": entity_id,
                    "attribute_type": "visibility",
                    "value": float(weather_data["vis"]),
                    "unit": "km",
                    "timestamp": obs_time,
                    "update_time": update_time
                }
            ]

            logger.info(f"已处理 {entity_id} 的天气数据，共 {len(processed_attributes)} 个属性")
            return processed_attributes

        except KeyError as e:
            logger.error(f"天气数据解析失败，缺少字段: {str(e)}, 实体ID: {entity_id}")
            return None
        except Exception as e:
            logger.error(f"处理天气数据时发生错误: {str(e)}, 实体ID: {entity_id}")
            return None
