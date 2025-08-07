import unittest
from common.config import load_config
from mcp_service.sync_service import DataSyncService
from kg_module.connection import Neo4jConnection
from milvus_module.search_engine import SearchEngine
from pymilvus import connections


class TestDataSync(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # 加载配置
        cls.config = load_config()
        # 初始化同步服务
        cls.sync_service = DataSyncService()
        # 初始化 Neo4j 连接
        cls.neo4j_conn = Neo4jConnection()
        cls.neo4j_conn.connect()
        # 初始化 Milvus 连接
        connections.connect(
            host=cls.config.milvus.host,
            port=cls.config.milvus.port,
            user=cls.config.milvus.user,
            password=cls.config.milvus.password
        )
        cls.milvus_manager = SearchEngine(cls.config.milvus,cls.config)

    def test_sync_data(self):
        # 执行数据同步
        # self.sync_service.sync_data()

        # 验证 Neo4j 中是否有数据
        with self.neo4j_conn.get_session() as session:
            result = session.run("MATCH (n) RETURN count(n) as count")
            node_count = result.single()["count"]
            self.assertGreater(node_count, 0, "Neo4j 中没有同步到数据")

        # 验证 Milvus 中是否有数据
        self.milvus_manager.collection.load()
        # 确保数据已刷新到磁盘
        self.milvus_manager.collection.flush()
        # 新增：基于文本"故宫"的向量相似性搜索验证
        search_results = self.milvus_manager.search_similar_attractions("公园", top_k=100)
        # 输出匹配结果
        print("\n与'公园'匹配的实体:")
        for i, result in enumerate(search_results, 1):
            print(f"{i}. entity_id: {result['entity_id']}, 相似度: {result['similarity_score']:.4f}")

        # num_entities = self.milvus_manager.collection.num_entities
        # self.assertGreater(num_entities, 0, "Milvus 中没有同步到数据")
    @classmethod
    def tearDownClass(cls):
        # 关闭 Neo4j 连接
        cls.neo4j_conn.close()
        # 断开 Milvus 连接
        connections.disconnect(alias='default')


if __name__ == '__main__':
    unittest.main()

