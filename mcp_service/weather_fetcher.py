import requests
import logging
from typing import Optional, Dict
from common.config import load_config
import logging


class WeatherDataFetcher:
    """天气数据获取器，专门用于调用QWeather API获取天气信息"""

    def __init__(self):
        self.config = load_config()
        self.logger = logging.getLogger("weather_fetcher")
        self.logger.setLevel(logging.INFO)
        self.api_endpoint = self.config.weather.api_url
        self.api_key = self.config.weather.api_key
        # 构造请求头，完全匹配API要求
        self.headers = {
            "X-QW-Api-Key": self.api_key,
        }

    def fetch_weather(self, entity_id: str, longitude: float, latitude: float) -> Optional[Dict]:
        """
        调用天气API获取指定经纬度的天气数据

        :param entity_id: 知识图谱中的实体ID
        :param longitude: 经度
        :param latitude: 纬度
        :return: 天气数据字典，失败时返回None
        """
        try:
            # 构造请求参数，location格式为"经度,纬度"
            params = {
                "location": f"{longitude},{latitude}"
            }

            # 添加调试日志：记录请求端点和参数（关键排查信息）
            self.logger.info(f"准备调用天气API | 端点: {self.api_endpoint} | 参数: {params}")
            self.logger.info(f"天气API请求URL: {self.api_endpoint}")
            self.logger.info(f"请求参数: {params}")
            self.logger.info(f"请求头: {self.headers}")


            # 发送GET请求
            response = requests.get(
                url=self.api_endpoint,
                params=params,
                headers=self.headers,
                timeout=15
            )

            # 新增：打印响应内容用于调试
            self.logger.info(f"API响应状态码: {response.status_code}")
            self.logger.info(f"API响应内容: {response.text[:500]}")  # 只打印前500字符

            # 检查响应状态
            response.raise_for_status()

            # 解析JSON响应
            result = response.json()

            # 检查API返回的状态码
            if result.get("code") != "200":
                self.logger.error(f"天气API返回错误代码: {result.get('code')}, 实体ID: {entity_id}")
                return None

            self.logger.info(f"成功获取实体 {entity_id} 的天气数据")
            return result

        except requests.exceptions.RequestException as e:
            self.logger.error(f"天气API请求失败: {str(e)}, 实体ID: {entity_id}")
            return None
        except Exception as e:
            self.logger.error(f"处理天气数据时发生错误: {str(e)}, 实体ID: {entity_id}")
            return None
