import logging
from flask import Flask
from api.routes import api_bp, init_search_engine
from flask_cors import CORS  # 可选，用于跨域支持


def configure_logging():
    """配置基础日志（输出到控制台，级别为INFO）"""
    logging.basicConfig(
        level=logging.INFO,  # 显示INFO及以上级别日志
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",  # 日志格式
        handlers=[logging.StreamHandler()]  # 输出到控制台
    )


def create_app():
    """工厂函数创建Flask应用"""
    # 初始化日志配置（关键！）
    configure_logging()

    app = Flask(__name__)

    # 初始化跨域支持（可选）
    CORS(app)

    # 注册API蓝图
    app.register_blueprint(api_bp)

    # 初始化搜索引擎
    init_search_engine()

    return app


if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=5000, debug=True)
