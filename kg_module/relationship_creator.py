from .connection import Neo4jConnection
import logging


class RelationshipCreator:
    """关系创建工具类"""

    def __init__(self):
        self.connection = Neo4jConnection()
        self.connection.connect()
        self.logger = logging.getLogger("relationship_creator")

    def create_contains_relationship(self, attraction_id, sub_attraction_id):
        """创建景点包含子景点的关系"""
        with self.connection.get_session() as session:
            try:
                session.run(
                    """
                    MATCH (a:Attraction {id: $attraction_id})
                    MATCH (s:SubAttraction {id: $sub_attraction_id})
                    MERGE (a)-[r:CONTAINS]->(s)
                    RETURN count(r) AS count
                    """,
                    attraction_id=attraction_id,
                    sub_attraction_id=sub_attraction_id
                )
                self.logger.info(f"包含关系创建成功: {attraction_id} -> {sub_attraction_id}")
            except Exception as e:
                self.logger.error(f"包含关系创建失败: {str(e)}")
                raise

    def create_nearby_relationship(self, attraction_id1, attraction_id2, distance):
        """创建景点间的邻近关系"""
        with self.connection.get_session() as session:
            try:
                session.run(
                    """
                    MATCH (a1:Attraction {id: $id1})
                    MATCH (a2:Attraction {id: $id2})
                    MERGE (a1)-[r:NEARBY {distance: $distance}]->(a2)
                    MERGE (a2)-[r2:NEARBY {distance: $distance}]->(a1)  # 双向关系
                    RETURN count(r) AS count
                    """,
                    id1=attraction_id1,
                    id2=attraction_id2,
                    distance=distance
                )
                self.logger.info(f"邻近关系创建成功: {attraction_id1} -[{distance}km]-> {attraction_id2}")
            except Exception as e:
                self.logger.error(f"邻近关系创建失败: {str(e)}")
                raise

    def create_reachable_by_relationship(self, transport_id, attraction_id, duration, transport_type):
        """创建交通枢纽到景点的可达关系"""
        with self.connection.get_session() as session:
            try:
                session.run(
                    """
                    MATCH (t:TransportHub {id: $transport_id})
                    MATCH (a:Attraction {id: $attraction_id})
                    MERGE (t)-[r:REACHABLE_BY {duration: $duration, type: $type}]->(a)
                    RETURN count(r) AS count
                    """,
                    transport_id=transport_id,
                    attraction_id=attraction_id,
                    duration=duration,
                    type=transport_type
                )
                self.logger.info(f"可达关系创建成功: {transport_id} -> {attraction_id}")
            except Exception as e:
                self.logger.error(f"可达关系创建失败: {str(e)}")
                raise

    def create_has_facility_relationship(self, attraction_id, facility_id, facility_type):
        """创建景点与周边设施的关系"""
        with self.connection.get_session() as session:
            try:
                session.run(
                    """
                    MATCH (a:Attraction {id: $attraction_id})
                    MATCH (f:Facility {id: $facility_id})
                    MERGE (a)-[r:HAS_FACILITY {type: $type}]->(f)
                    RETURN count(r) AS count
                    """,
                    attraction_id=attraction_id,
                    facility_id=facility_id,
                    type=facility_type
                )
                self.logger.info(f"周边设施关系创建成功: {attraction_id} -> {facility_id}")
            except Exception as e:
                self.logger.error(f"周边设施关系创建失败: {str(e)}")
                raise

    def create_visit_order_relationship(self, attraction_id1, attraction_id2, priority):
        """创建游览顺序推荐关系"""
        with self.connection.get_session() as session:
            try:
                session.run(
                    """
                    MATCH (a1:Attraction {id: $id1})
                    MATCH (a2:Attraction {id: $id2})
                    MERGE (a1)-[r:VISIT_ORDER {priority: $priority}]->(a2)
                    RETURN count(r) AS count
                    """,
                    id1=attraction_id1,
                    id2=attraction_id2,
                    priority=priority
                )
                self.logger.info(f"游览顺序关系创建成功: {attraction_id1} -> {attraction_id2}")
            except Exception as e:
                self.logger.error(f"游览顺序关系创建失败: {str(e)}")
                raise

    def create_theme_related_relationship(self, attraction_id1, attraction_id2, similarity):
        """创建主题关联关系"""
        with self.connection.get_session() as session:
            try:
                session.run(
                    """
                    MATCH (a1:Attraction {id: $id1})
                    MATCH (a2:Attraction {id: $id2})
                    MERGE (a1)-[r:THEME_RELATED {similarity: $similarity}]->(a2)
                    MERGE (a2)-[r2:THEME_RELATED {similarity: $similarity}]->(a1)
                    RETURN count(r) AS count
                    """,
                    id1=attraction_id1,
                    id2=attraction_id2,
                    similarity=similarity
                )
                self.logger.info(f"主题关联关系创建成功: {attraction_id1} -> {attraction_id2}")
            except Exception as e:
                self.logger.error(f"主题关联关系创建失败: {str(e)}")
                raise

    def create_suitable_for_relationship(self, attraction_id, season_name, score):
        """创建季节适配关系"""
        with self.connection.get_session() as session:
            try:
                session.run(
                    """
                    MATCH (a:Attraction {id: $attraction_id})
                    MATCH (s:Season {name: $season_name})
                    MERGE (a)-[r:SUITABLE_FOR {score: $score}]->(s)
                    RETURN count(r) AS count
                    """,
                    attraction_id=attraction_id,
                    season_name=season_name,
                    score=score
                )
                self.logger.info(f"季节适配关系创建成功: {attraction_id} -> {season_name}")
            except Exception as e:
                self.logger.error(f"季节适配关系创建失败: {str(e)}")
                raise

    def create_requires_booking_relationship(self, attraction_id, advance_days):
        """创建需预约关系"""
        with self.connection.get_session() as session:
            try:
                session.run(
                    """
                    MATCH (a:Attraction {id: $attraction_id})
                    MERGE (a)-[r:REQUIRES_BOOKING {advance_days: $advance_days}]->()
                    RETURN count(r) AS count
                    """,
                    attraction_id=attraction_id,
                    advance_days=advance_days
                )
                self.logger.info(f"需预约关系创建成功: {attraction_id}")
            except Exception as e:
                self.logger.error(f"需预约关系创建失败: {str(e)}")
                raise
