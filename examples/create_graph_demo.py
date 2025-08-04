from kg_module.schema_initializer import SchemaInitializer
from kg_module.entity_creator import EntityCreator
from kg_module.relationship_creator import RelationshipCreator


def main():
    # 1. 初始化Schema
    initializer = SchemaInitializer()
    initializer.full_initialize()

    # 2. 创建实体
    entity_creator = EntityCreator()

    # 创建景点
    attraction = {
        "id": "AT0010023",
        "name": "故宫博物院",
        "alias": ["紫禁城", "故宫"],
        "category": ["人文景观", "世界遗产"],
        "location": (39.916527, 116.397128),  # (lat, lon)
        "address": "北京市东城区景山前街4号",
        "area": 720000.0,
        "opening_hours_regular": "08:30-17:00(周一闭馆)",
        "ticket_price_regular": "60元/人(旺季)",
        "description": "明清两代皇家宫殿...",
        "tags": ["历史", "拍照", "亲子"],
        "official_website": "https://www.dpm.org.cn",
        "contact": "010-65132255",
        "built_year": 1420,
        "grade": "AAAAA"
    }
    attraction_id = entity_creator.create_attraction(attraction)

    # 创建子景点
    sub_attraction = {
        "id": "SA001002301",
        "name": "太和殿",
        "parent_id": "AT0010023",
        "location_relative": "故宫中轴线南段",
        "visiting_duration": 20,
        "highlight": "金銮殿,皇权象征"
    }
    sub_id = entity_creator.create_sub_attraction(sub_attraction)

    # 创建交通枢纽
    transport_hub = {
        "id": "TH0010023",
        "name": "故宫博物院地铁站",
        "type": "subway",
        "location": (39.915527, 116.398128),
        "distance_to_attraction": 0.8,
        "accessible": True
    }
    transport_id = entity_creator.create_transport_hub(transport_hub)

    # 创建周边设施
    facility = {
        "id": "FA0010023",
        "name": "故宫角楼餐厅",
        "type": "restaurant",
        "location": (39.917527, 116.396128),
        "distance_to_attraction": 0.5,
        "price_level": 3,
        "business_hours": "09:00-21:00"
    }
    facility_id = entity_creator.create_facility(facility)

    # 3. 创建关系
    rel_creator = RelationshipCreator()

    # 包含关系
    rel_creator.create_contains_relationship(attraction_id, sub_id)

    # 可达关系
    rel_creator.create_reachable_by_relationship(transport_id, attraction_id, 10, "步行")

    # 周边设施关系
    rel_creator.create_has_facility_relationship(attraction_id, facility_id, "restaurant")

    # 季节适配关系
    rel_creator.create_suitable_for_relationship(attraction_id, "秋季", 4.5)

    # 需预约关系
    rel_creator.create_requires_booking_relationship(attraction_id, 3)

    print("知识图谱创建完成")


if __name__ == "__main__":
    main()
