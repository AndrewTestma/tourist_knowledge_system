import logging
from flask import Blueprint, request, jsonify

from mcp_service.sync_service import WeatherSyncService
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

        # 新增：初始化天气同步服务
        weather_service = WeatherSyncService()

        # 遍历搜索结果，添加天气数据
        for item in results:
            # 从basic_info中获取POINT格式的经纬度（假设字段名为"location"）
            location_str = item["basic_info"].get("location")
            if not location_str:
                logger.warning(f"景点 {item['entity_id']} 缺少经纬度信息")
                continue

            # 解析经纬度
            latitude, longitude = parse_point_location(location_str)
            if not all([latitude, longitude]):
                logger.warning(f"景点 {item['entity_id']} 经纬度解析失败")
                continue

            # 检查并更新天气数据（默认1小时间隔）
            latest_weather = weather_service.check_and_update_weather(
                entity_id=item["entity_id"],
                longitude=longitude,  # 注意POINT格式是"纬度 经度"，此处取longitude为第二个值
                latitude=latitude  # latitude为第一个值
            )

            # 将天气数据添加到结果中（可根据需求调整字段名）
            item["basic_info"]["weather_data"] = latest_weather

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

def parse_point_location(point_str: str) -> tuple[float, float]:
    """
    解析知识图谱中POINT格式的经纬度（如"POINT(39.8823 116.4066)"）
    :param point_str: POINT格式字符串
    :return: (纬度, 经度) 元组，解析失败返回(None, None)
    """
    try:
        # 提取括号内的数字部分并分割
        coords = point_str.strip("POINT()").split()
        if len(coords) != 2:
            return None, None
        # 转换为浮点数（注意POINT格式是"纬度 经度"）
        latitude = float(coords[0])
        longitude = float(coords[1])
        return latitude, longitude
    except (ValueError, IndexError):
        logger.warning(f"无效的POINT格式: {point_str}")
        return None, None
