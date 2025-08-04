from pydantic import BaseModel
from typing import List, Dict, Any

class SearchRequest(BaseModel):
    query: str  # 用户搜索文本
    top_k: int = 10  # 返回结果数量
    threshold: float = 0.5  # 相似度阈值

class SearchResult(BaseModel):
    entity_id: str
    similarity_score: float
    basic_info: Dict[str, Any]  # 实体基本信息（动态字段）

class SearchResponse(BaseModel):
    results: List[SearchResult]  # 搜索结果列表
