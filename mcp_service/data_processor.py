from typing import List, Dict, Any
import re


class DataProcessor:
    """数据处理与标准化工具"""

    def standardize_attraction_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """标准化景点数据"""
        standardized = []

        for item in raw_data:
            # 标准化开放时间格式
            if 'opening_hours' in item:
                item['opening_hours_regular'] = self._standardize_time_format(item['opening_hours'])

            # 标准化价格格式
            if 'ticket_price' in item:
                item['ticket_price_regular'] = self._standardize_price_format(item['ticket_price'])

            # 确保必要字段存在
            required_fields = ['id', 'name', 'location']
            for field in required_fields:
                if field not in item:
                    raise ValueError(f"景点数据缺少必要字段: {field}, 数据: {item}")

            standardized.append(item)

        return standardized

    def _standardize_time_format(self, time_str: str) -> str:
        """标准化时间格式为 HH:mm-HH:mm"""
        if not time_str:
            return ""

        # 简单的时间格式转换示例，可根据实际情况扩展
        time_str = time_str.replace('：', ':').replace('－', '-').replace(' ', '')

        # 匹配 "8:30-17:00" 格式
        if re.match(r'^\d+:\d+-\d+:\d+$', time_str):
            # 补全前导零
            parts = time_str.split('-')
            return '-'.join([self._pad_time(part) for part in parts])

        return time_str  # 无法标准化的格式保持原样

    def _pad_time(self, time_str: str) -> str:
        """补全时间格式的前导零，如 "8:30" -> "08:30" """
        hours, minutes = time_str.split(':')
        return f"{hours.zfill(2)}:{minutes.zfill(2)}"

    def _standardize_price_format(self, price_str: str) -> str:
        """标准化价格格式"""
        if not price_str:
            return ""

        # 提取价格数字并格式化
        price_str = price_str.replace(' ', '')
        return price_str

    def create_entity_id_mapping(self, entities: List[Dict[str, Any]], entity_type: str) -> Dict[str, str]:
        """创建原始ID到系统标准ID的映射"""
        # 系统标准ID格式: 前缀+原始ID，确保跨系统唯一性
        prefix_map = {
            'attraction': 'AT',
            'sub_attraction': 'SA',
            'transport_hub': 'TH',
            'facility': 'FA'
        }

        if entity_type not in prefix_map:
            raise ValueError(f"不支持的实体类型: {entity_type}")

        prefix = prefix_map[entity_type]
        mapping = {}

        for entity in entities:
            original_id = str(entity['id'])
            system_id = f"{prefix}{original_id.zfill(8)}"  # 统一8位数字格式
            mapping[original_id] = system_id
            entity['system_id'] = system_id  # 在实体数据中添加系统ID

        return mapping

    def process_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行完整的数据处理流程，包含数据标准化和ID映射。
        这里假设 raw_data 是从某个数据源获取的原始数据，实际使用时需要替换获取方式。
        """
        processed_data = {}

        # 处理景点数据
        if "attractions" in raw_data:
            processed_attractions = self.standardize_attraction_data(raw_data["attractions"])
            id_mapping = self.create_entity_id_mapping(processed_attractions, "attraction")
            processed_data["attractions"] = processed_attractions

        # 处理子景点数据
        if "sub_attractions" in raw_data:
            id_mapping = self.create_entity_id_mapping(raw_data["sub_attractions"], "sub_attraction")
            processed_data["sub_attractions"] = raw_data["sub_attractions"]

        # 处理交通枢纽数据
        if "transport_hubs" in raw_data:
            id_mapping = self.create_entity_id_mapping(raw_data["transport_hubs"], "transport_hub")
            processed_data["transport_hubs"] = raw_data["transport_hubs"]

        # 处理周边设施数据
        if "facilities" in raw_data:
            id_mapping = self.create_entity_id_mapping(raw_data["facilities"], "facility")
            processed_data["facilities"] = raw_data["facilities"]

        # 保留向量数据
        if "vectors" in raw_data:
            processed_data["vectors"] = raw_data["vectors"]

        return processed_data
