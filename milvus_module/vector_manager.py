from tkinter.font import names

from pymilvus import connections, Collection, FieldSchema, CollectionSchema, DataType, utility
import requests
from typing import List, Dict, Any
import logging
from common.config import load_config  # 导入 AppConfig 类

logger = logging.getLogger(__name__)

class VectorManager:
    def __init__(self):
        """初始化Milvus连接"""
        config = load_config()
        self.host = config.milvus.host
        self.port = config.milvus.port
        self.user = config.milvus.user
        self.password = config.milvus.password
        self.collection_name = config.milvus.collection_name
        self.API_TOKEN = config.milvus.token  # 从配置中获取 API_TOKEN
        self.API_URL = config.milvus.api_url  # 从配置中获取 API_URL
        # 连接Milvus
        connections.connect(host=self.host, port=self.port, user=self.user, password=self.password)

        # 确保集合存在
        self._create_collection_if_not_exists()

    def _create_collection_if_not_exists(self):
        """如果集合不存在则创建"""
        if not utility.has_collection(self.collection_name):
            # 由于无法提前知道维度，这里先假设一个默认值，实际使用时需要根据API返回结果调整
            default_dim = 1024

            # 定义集合结构
            fields = [
                FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
                FieldSchema(name="entity_id", dtype=DataType.VARCHAR, max_length=20),  # 知识图谱中的实体ID
                FieldSchema(name="name", dtype=DataType.FLOAT_VECTOR,
                            dim=default_dim),
                FieldSchema(name="alias", dtype=DataType.FLOAT_VECTOR,
                            dim=default_dim),
                FieldSchema(name="description_vector", dtype=DataType.FLOAT_VECTOR,
                            dim=default_dim),
                FieldSchema(name="tags_vector", dtype=DataType.FLOAT_VECTOR,
                            dim=default_dim)
            ]

            schema = CollectionSchema(fields, description="景点向量集合")
            self.collection = Collection(self.collection_name, schema)

            # 创建索引
            self._create_index()

            logger.info(f"创建Milvus集合: {self.collection_name}")
        else:
            # 若集合已存在，检查并补全缺失的索引
            self.collection = Collection(self.collection_name)
            # 获取当前索引字段
            current_index_fields = [index.field_name for index in self.collection.indexes]
            # 需要的向量字段
            required_fields = ["name", "alias", "description_vector", "tags_vector"]
            # 检查是否有缺失的索引
            missing_fields = [field for field in required_fields if field not in current_index_fields]
            if missing_fields:
                logger.warning(f"检测到缺失索引字段: {missing_fields}，正在补全...")
                self._create_index()  # 补全缺失的索引
            logger.info(f"集合 {self.collection_name} 已存在，索引检查完成")


    def _create_index(self):
        """创建集合索引"""
        index_params = {
            "index_type": "IVF_FLAT",
            "metric_type": "L2",
            "params": {"nlist": 1024}
        }
        vector_fields = ["name", "alias", "description_vector", "tags_vector"]

        for field in vector_fields:
            # 检查字段是否已有索引
            existing_indexes = [idx for idx in self.collection.indexes if idx.field_name == field]
            if existing_indexes:
                logger.info(f"字段 {field} 已存在索引: {existing_indexes[0].to_dict()}")
                continue
            # 无索引时创建
            self.collection.create_index(field_name=field, index_params=index_params)
            logger.info(f"为字段 {field} 创建索引成功")

        logger.info(f"集合 {self.collection_name} 索引检查/创建完成")

    def _get_embeddings(self, texts: List[str]) -> List[List[float]]:
        """调用 API 生成嵌入向量"""
        headers = {
            "Authorization": f"Bearer {self.API_TOKEN}",
            "Content-Type": "application/json"
        }
        data = {
            "model": "BAAI/bge-large-zh-v1.5",
            "input": texts
        }
        try:
            response = requests.post(self.API_URL, headers=headers, json=data)
            response.raise_for_status()
            return [embedding["embedding"] for embedding in response.json()["data"]]
        except requests.RequestException as e:
            logger.error(f"获取嵌入向量失败: {str(e)}")
            raise

    def generate_and_insert_vectors(self, attractions: List[Dict[str, Any]]) -> int:
        """生成并插入景点向量数据"""
        if not attractions:
            return 0

        # 准备数据
        entity_ids = []
        names = []
        alias = []
        descriptions = []
        tags_texts = []

        for attr in attractions:
            entity_ids.append(attr['id'])
            names.append(attr.get('name', ''))
            alias.append(attr.get('alias', ''))
            descriptions.append(attr.get('description', ''))
            tags_texts.append(' '.join(attr.get('tags', [])))

        # 批量获取嵌入向量
        names_vectors = self._get_embeddings(names)
        alias_vectors = self._get_embeddings(alias)
        description_vectors = self._get_embeddings(descriptions)
        tags_vectors = self._get_embeddings(tags_texts)

        # 批量插入
        data = [
            entity_ids,
            names_vectors,
            alias_vectors,
            description_vectors,
            tags_vectors
        ]

        insert_result = self.collection.insert(data)
        self.collection.flush()  # 确保数据写入磁盘

        logger.info(f"成功插入 {len(insert_result.primary_keys)} 条向量数据")
        return len(insert_result.primary_keys)

    # def search_similar_vectors(self, query_text: str, top_k: int = 3) -> List[Dict[str, Any]]:
    #     """根据查询文本搜索相似向量数据"""
    #     # 生成查询文本的向量
    #     query_vector = self._get_embeddings([query_text])
    #
    #     # 搜索参数配置
    #     search_params = {
    #         "data": query_vector,
    #         "anns_field": "description_vector",  # 使用描述向量进行匹配
    #         "param": {"metric_type": "L2", "params": {"nprobe": 20}},  # nprobe值越大精度越高但速度越慢
    #         "limit": top_k,
    #         "output_fields": ["entity_id"]  # 返回实体ID
    #     }
    #
    #     # 执行搜索
    #     results = self.collection.search(**search_params)
    #
    #     # 处理搜索结果
    #     matched_entities = []
    #     for hits in results:
    #         for hit in hits:
    #             matched_entities.append({
    #                 "entity_id": hit.entity.get("entity_id"),
    #                 "similarity_score": 1 / (1 + hit.distance)  # 将L2距离转换为相似度分数(0-1)
    #             })
    #
    #     return matched_entities
