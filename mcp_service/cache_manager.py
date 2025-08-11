from cachetools import TTLCache
from typing import Dict, Any, Optional
import logging
from common.config import load_config


class DynamicCacheManager:
    """动态属性缓存管理器，支持不同类型属性独立设置TTL"""
    # 支持的缓存类型常量
    SUPPORTED_TYPES = ["weather", "passenger_flow"]

    def __init__(self):
        self.config = load_config()
        self.logger = logging.getLogger("cache_manager")
        self.logger.setLevel(logging.INFO)

        # 严格检查配置
        if not hasattr(self.config, 'cache'):
            raise ValueError("配置中缺少cache配置节")

        # 初始化缓存
        self.caches = {}
        self._init_caches()


    def _init_caches(self):
        """初始化各类型缓存"""
        try:
            self.logger.info(f"缓存配置属性: {dir(self.config.cache)}")  # 新增日志
            self.logger.info(f"weather_max_size: {self.config.cache.weather_max_size}")  # 新增日志
            self.logger.info(f"weather_ttl: {self.config.cache.weather_ttl}")  # 新增日志

            # 天气缓存
            if hasattr(self.config.cache, 'weather_max_size') and hasattr(self.config.cache, 'weather_ttl'):
                self.caches["weather"] = TTLCache(
                    maxsize=self.config.cache.weather_max_size,
                    ttl=self.config.cache.weather_ttl
                )
                self.logger.info("天气缓存初始化成功")
            else:
                self.logger.error("天气缓存配置不完整")

        except Exception as e:
            self.logger.error(f"缓存初始化失败: {str(e)}")
            raise

    def get(self, cache_type: str, entity_id: str) -> Optional[Dict[str, Any]]:
        """
        获取缓存数据
        :param cache_type: 缓存类型 (weather/passenger_flow)
        :param entity_id: 实体ID
        :return: 缓存数据或None
        """
        self.logger.info(f"当前缓存类型列表: {list(self.caches.keys())}")
        cache = self.caches.get(cache_type)
        if not cache:
            self.logger.warning(f"不支持的缓存类型: {cache_type}")
            return None

        self.logger.info(f"缓存实例类型: {type(cache)}")  # 应输出 <class 'cachetools.ttl.TTLCache'>
        self.logger.info(f"缓存实例状态: {cache}")  # 应输出 TTLCache(maxsize=1000, ttl=3600, ...)

        return cache.get(entity_id)

    def set(self, cache_type: str, entity_id: str, data: Dict[str, Any]) -> None:
        """
        设置缓存数据
        :param cache_type: 缓存类型 (weather/passenger_flow)
        :param entity_id: 实体ID
        :param data: 要缓存的数据
        """
        cache = self.caches.get(cache_type)
        if not cache:
            self.logger.warning(f"不支持的缓存类型: {cache_type}")
            return

        cache[entity_id] = data
        self.logger.debug(f"已缓存 {cache_type} 数据: {entity_id}")

    def clear(self, cache_type: str = None) -> None:
        """
        清空指定类型缓存或所有缓存
        :param cache_type: 可选，指定要清空的缓存类型
        """
        if cache_type:
            if cache_type in self.caches:
                self.caches[cache_type].clear()
                self.logger.info(f"已清空 {cache_type} 缓存")
        else:
            for name, cache in self.caches.items():
                cache.clear()
            self.logger.info("已清空所有缓存")
