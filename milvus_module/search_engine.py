import requests  # 新增：添加requests依赖
from pymilvus import connections, Collection, utility
from common.config import MilvusConfig, AppConfig
from kg_module.entity_manager import EntityManager
import logging
from typing import List, Dict, Any


class SearchEngine:
    """Milvus向量检索引擎，提供景点相似度搜索功能"""

    def __init__(self, milvus_config: MilvusConfig, app_config: AppConfig):
        """初始化检索引擎"""
        self.logger = logging.getLogger("search_engine")
        self.milvus_config = milvus_config
        self.app_config = app_config

        # 新增：从配置中获取API认证信息和接口地址（与vector_manager一致）
        self.API_TOKEN = milvus_config.token
        self.API_URL = milvus_config.api_url

        # 初始化Milvus连接（与vector_manager保持一致的连接方式）
        connections.connect(
            host=milvus_config.host,
            port=milvus_config.port,
            user=milvus_config.user,
            password=milvus_config.password
        )

        # 初始化实体管理器，用于获取实体详情
        self.entity_manager = EntityManager()

        # 验证集合是否存在并获取集合对象
        self._ensure_collection_exists()
        self.collection = Collection(self.milvus_config.collection_name)

    def _ensure_collection_exists(self):
        """确保集合存在（参考vector_manager的验证逻辑）"""
        try:
            if not utility.has_collection(self.milvus_config.collection_name):
                self.logger.error(f"集合 {self.milvus_config.collection_name} 不存在")
                raise ValueError(f"集合 {self.milvus_config.collection_name} 不存在")
        except Exception as e:
            self.logger.error(f"验证集合存在失败: {str(e)}")
            raise

    def _text_to_vector(self, text: str) -> List[float]:
        """将文本转换为向量（通过API调用，与vector_manager的_get_embeddings逻辑一致）"""
        headers = {
            "Authorization": f"Bearer {self.API_TOKEN}",
            "Content-Type": "application/json"
        }
        data = {
            "model": "BAAI/bge-large-zh-v1.5",  # 与vector_manager使用相同模型
            "input": [text]  # 包装为列表（API要求批量输入）
        }
        try:
            response = requests.post(self.API_URL, headers=headers, json=data)
            response.raise_for_status()
            # 从响应中提取第一个文本的嵌入向量（因输入是单文本）
            embeddings = [embedding["embedding"] for embedding in response.json()["data"]]
            return embeddings[0]
        except requests.RequestException as e:
            self.logger.error(f"文本向量化失败: {str(e)}")
            raise

    def search_similar_attractions(self, query: str, top_k: int = 10, threshold: float = 0.8) -> List[Dict[str, Any]]:
        """搜索相似景点（逻辑与原方法一致，仅向量化方式修改）"""
        try:
            # 1. 将查询文本转换为向量（通过API调用）
            query_vector = self._text_to_vector(query)

            # 2. 执行向量相似度搜索（参考vector_manager的搜索参数配置）
            search_params = {
                "data": [query_vector],
                "anns_field": "description_vector",  # 使用描述向量进行匹配
                "param": {"metric_type": "L2", "params": {"nprobe": 20}},  # 与vector_manager保持一致的metric_type
                "limit": top_k,
                "output_fields": ["entity_id"]  # 返回实体ID
            }

            # 执行搜索（pymilvus的Collection.search接口）
            results = self.collection.search(**search_params)

            # 3. 对检索结果进行相关性排序与过滤
            structured_results = []
            for hits in results:
                for hit in hits:
                    # 过滤低于阈值的结果（注意：L2距离越小越相似，阈值需根据实际调整）
                    if hit.distance > threshold:
                        continue

                    # 获取实体基本信息
                    entity_info = self.entity_manager.get_entity_by_id(hit.entity.get("entity_id"))

                    # 构建结构化结果（与vector_manager返回格式保持一致）
                    structured_results.append({
                        "entity_id": hit.entity.get("entity_id"),
                        "similarity_score": 1 / (1 + hit.distance),  # 与vector_manager一致的相似度转换
                        "basic_info": entity_info
                    })

            # 按相似度分数降序排序
            structured_results.sort(key=lambda x: x["similarity_score"], reverse=True)

            # 4. 返回结构化的检索结果
            logging.info(f"structured_results: {structured_results}")
            return structured_results

        except Exception as e:
            self.logger.error(f"景点相似度搜索失败: {str(e)}")
            raise

    def close(self):
        """关闭Milvus连接（参考vector_manager的连接管理方式）"""
        try:
            connections.disconnect("default")
            self.logger.info("Milvus连接已关闭")
        except Exception as e:
            self.logger.error(f"关闭Milvus连接失败: {str(e)}")
