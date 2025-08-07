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
        """搜索相似景点（支持多向量字段联合搜索）"""
        try:
            # 1. 将查询文本转换为向量（通过API调用）
            query_vector = self._text_to_vector(query)

            # 2. 定义需要搜索的向量字段列表
            search_fields = ["name", "alias", "description_vector"]  # 新增多字段搜索
            all_results = []  # 存储所有字段的搜索结果

            # 3. 对每个向量字段执行搜索
            for field in search_fields:
                # 配置当前字段的搜索参数
                search_params = {
                    "data": [query_vector],
                    "anns_field": field,  # 动态指定搜索字段
                    "param": {"metric_type": "L2", "params": {"nprobe": 20}},
                    "limit": top_k,  # 每个字段先取top_k结果
                    "output_fields": ["entity_id"]
                }

                # 执行搜索并收集结果
                results = self.collection.search(**search_params)
                for hits in results:
                    for hit in hits:
                        # 过滤超过阈值的结果（L2距离越小越相似，超过阈值则跳过）
                        if hit.distance > threshold:
                            continue
                        # 记录来源字段（可选，用于调试）
                        all_results.append({
                            "entity_id": hit.entity.get("entity_id"),
                            "similarity_score": 1 / (1 + hit.distance),
                            "source_field": field
                        })

            # 4. 合并去重：保留同一实体的最高相似度记录
            merged = {}
            for item in all_results:
                entity_id = item["entity_id"]
                score = item["similarity_score"]
                if entity_id not in merged or score > merged[entity_id]["similarity_score"]:
                    merged[entity_id] = {
                        "entity_id": entity_id,
                        "similarity_score": score,
                        "basic_info": self.entity_manager.get_entity_by_id(entity_id)
                    }

            # 5. 按相似度降序排序，取前top_k
            final_results = sorted(merged.values(), key=lambda x: x["similarity_score"], reverse=True)[:top_k]

            logging.info(f"多字段搜索完成，共找到{len(final_results)}个有效结果")
            return final_results

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
