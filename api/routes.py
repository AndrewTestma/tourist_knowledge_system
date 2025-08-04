import logging
from flask import Blueprint, request, jsonify
from .schemas import SearchRequest, SearchResponse, SearchResult
from milvus_module.search_engine import SearchEngine
from common.config import load_config
from typing import cast

# 初始化日志器（使用当前模块名）
logger = logging.getLogger(__name__)

# 创建API蓝图
api_bp = Blueprint('api', __name__, url_prefix='/api')

# 全局搜索引擎实例（实际项目建议使用Flask扩展管理生命周期）
# search_engine = None


def init_search_engine():
    """初始化搜索引擎（在Flask应用启动时调用）"""
    logger.info("搜索引擎初始化")  # 初始化成功日志
    global search_engine
    try:
        config = load_config()
        search_engine = SearchEngine(
            milvus_config=config.milvus,
            app_config=config
        )
        logger.info("搜索引擎初始化成功")  # 初始化成功日志
    except Exception as e:
        logger.error(f"搜索引擎初始化失败: {str(e)}", exc_info=True)  # 记录初始化异常堆栈
        raise


@api_bp.route('/attractions/search', methods=['POST'])
def search_attractions():
    """景点搜索接口"""
    if not search_engine:
        error_msg = "搜索引擎未初始化"
        logger.warning(error_msg)  # 未初始化警告日志
        return jsonify({"error": error_msg}), 503

    try:
        # 记录接收的请求参数
        logger.info(
            f"收到搜索请求 | 参数: query={request.json.get('query')}, top_k={request.json.get('top_k')}, threshold={request.json.get('threshold')}")

        # 解析请求数据
        req_data = SearchRequest(**request.json)

        # 记录开始搜索
        logger.debug(f"开始执行搜索 | 查询文本: {req_data.query}, top_k: {req_data.top_k}, 阈值: {req_data.threshold}")

        # 执行搜索
        results = search_engine.search_similar_attractions(
            query=req_data.query,
            top_k=req_data.top_k,
            threshold=req_data.threshold
        )

        # 记录搜索结果数量
        logger.info(f"搜索完成 | 返回结果数: {len(results)}")

        # 转换为响应模型
        response = SearchResponse(results=[
            SearchResult(
                entity_id=item["entity_id"],
                similarity_score=item["similarity_score"],
                basic_info=item["basic_info"]
            ) for item in results
        ])

        return jsonify(response.dict())

    except Exception as e:
        # 记录详细异常信息（包含堆栈）
        logger.error(f"搜索处理失败: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500
